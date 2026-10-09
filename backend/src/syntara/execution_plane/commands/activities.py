"""Atomic command intent and journal-owned encrypted Temporal correlation."""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, UUID, uuid5

from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.protocol import CommandScope, ExecutionPolicy, MessageEnvelope

from syntara.core.config.base import get_settings
from syntara.execution_plane.activity_context import (
    TemporalActivityContext,
    activity_delivered,
    activity_effect_entries,
    workflow_correlation_key,
)
from syntara.execution_plane.runtime import get_ao_runtime

if TYPE_CHECKING:
    from collections.abc import Callable

    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.service import MediatorService
    from sqlmodel.ext.asyncio.session import AsyncSession

    from syntara.execution_plane.activity_context import TemporalIdentity


def stable_command_id(identity: TemporalIdentity) -> UUID:
    """Allocate one work identity per logical activity generation, never per retry."""
    return uuid5(NAMESPACE_URL, "ao-work-v1:" + identity.model_dump_json())


async def enqueue_script(
    session: AsyncSession,
    *,
    command_id: UUID,
    project_id: UUID,
    temporal_identity: TemporalIdentity,
    task_token: bytes,
    activity_attempt: int,
    payload: dict[str, Any] | Callable[[], dict[str, Any]],
    runtime: MediatorService | None = None,
) -> MessageEnvelope:
    """Freeze work and encrypted Temporal context in the caller's transaction."""
    runtime = runtime or get_ao_runtime()
    if command_id != stable_command_id(temporal_identity):
        msg = "Command identity does not match the logical Temporal activity"
        raise ValueError(msg)
    journal = runtime.journal
    scope = CommandScope(client_id=get_settings().ep_participant_id, project_id=project_id)
    data = TemporalActivityContext(
        identity=temporal_identity, attempt=activity_attempt, task_token=base64.b64encode(task_token).decode()
    )

    async def create_command() -> JournalEntry:
        frozen_payload = payload() if callable(payload) else payload
        work = WorkExecute.model_validate({**frozen_payload, "work_correlation_id": str(uuid5(command_id, "work"))})
        return await runtime.commands.work_execute(
            session,
            command_id=command_id,
            scope=scope,
            payload=work,
            execution_policy=ExecutionPolicy(timeout_seconds=work.invocation.timeout_seconds),
        )

    prepared = await journal.ensure_outbound(
        session,
        scope,
        command_id,
        command_factory=create_command,
        context_factory=lambda: data.model_dump(mode="json"),
        correlation_key=workflow_correlation_key(temporal_identity.workflow_id),
    )
    local = prepared.local_context
    if not prepared.created:
        if local is None:
            msg = "Activity command has no durable Temporal context"
            raise RuntimeError(msg)
        previous = TemporalActivityContext.model_validate(local.data)
        if previous.identity != temporal_identity:
            msg = "Command identity is already associated with a different activity"
            raise ValueError(msg)
        if activity_attempt > previous.attempt:
            await journal.set_local_context(
                session,
                scope,
                command_id,
                data=data.model_dump(mode="json"),
                correlation_key=local.correlation_key,
                expected_generation=local.generation,
            )
            # The journal owns responsibility. Refresh only the latest received
            # completion (or local rejection), fencing every older prepared RPC.
            entries = await activity_effect_entries(journal, session, scope, command_id)
            if entries:
                latest = entries[-1]
                await journal.rearm_handling(session, scope, latest.command_id, latest.direction)
    return prepared.message


async def enqueue_workflow_cancellations(
    session: AsyncSession, temporal_workflow_id: str, *, runtime: MediatorService | None = None
) -> int:
    """Append cancellation with caller-owned workflow state, without committing or HTTP."""
    runtime = runtime or get_ao_runtime()
    journal = runtime.journal
    count = 0
    after = None
    while True:
        originals = await journal.list_local_context_entries(
            session,
            client_id=get_settings().ep_participant_id,
            correlation_key=workflow_correlation_key(temporal_workflow_id),
            command_type="work.execute",
            after=after,
            limit=100,
        )
        if not originals:
            return count
        for original in originals:
            scope = original.scope
            await journal.lock_command_family(session, scope, original.command_id)
            local = await journal.get_local_context(session, scope, original.command_id, for_update=True)
            if local is None:
                continue
            entries = await activity_effect_entries(journal, session, scope, original.command_id)
            if await activity_delivered(journal, session, entries, local.generation):
                continue
            command_id = uuid5(original.command_id, "cancel")

            async def create_cancel_command(
                original_entry_id: UUID = original.entry_id, cancellation_id: UUID = command_id
            ) -> JournalEntry:
                target = await journal.load_message(session, original_entry_id)
                return await runtime.commands.cancel_command(
                    session,
                    command_id=cancellation_id,
                    target=target,
                )

            prepared = await journal.ensure_outbound(
                session,
                scope,
                command_id,
                command_factory=create_cancel_command,
            )
            if prepared.created:
                count += 1
        after = originals[-1].entry_id
