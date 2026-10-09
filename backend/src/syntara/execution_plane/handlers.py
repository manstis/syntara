"""AO domain adapters for durable completions and fenced Temporal RPC effects."""

from __future__ import annotations

import base64
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands import ResourceRevision
from execution_plane_client.commands.control import Completion
from execution_plane_client.errors import HandlingDeferred, ProtocolConflict
from execution_plane_client.handlers import EffectResult, HandlerContext, HandlingResult, PreparedEffect
from execution_plane_client.protocol import CommandStatus
from temporalio.api.enums.v1 import PendingActivityState
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError

from syntara.execution_plane.activity_context import (
    TemporalActivityContext,
    activity_delivered,
    activity_effect_entries,
)

if TYPE_CHECKING:
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import MessageEnvelope
    from sqlalchemy.ext.asyncio import AsyncSession
    from temporalio.api.history.v1 import HistoryEvent
    from temporalio.client import Client

_RPC_TIMEOUT = timedelta(seconds=10)
_HISTORY_EVENT_LIMIT = 10_000


class TemporalCompletionHandler:
    """Persist attempt responsibility before RPC and observe ambiguous outcomes."""

    def __init__(self, journal: ExchangeJournalStore, temporal_client: Client | None) -> None:
        """Inject the participant journal and the worker's Temporal client."""
        self.journal = journal
        self.temporal = temporal_client

    async def prepare(self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession) -> PreparedEffect:
        """Prepare a journal effect using the current private activity destination."""
        if self.temporal is None:
            raise HandlingDeferred
        target = message.command_id if context.delivery_rejected else message.command.parent_command_id
        if target is None:
            raise ProtocolConflict
        local = await self.journal.get_local_context(session, message.scope, target, for_update=True)
        if local is None:
            raise ProtocolConflict
        activity = TemporalActivityContext.model_validate(local.data)
        entry = await self.journal.get(
            session, message.scope, message.command_id, "outbound" if context.delivery_rejected else "inbound"
        )
        if entry is None:
            raise ProtocolConflict
        completion = (
            Completion(status="failed", error_code=context.error_code)
            if context.delivery_rejected
            else Completion.model_validate(message.command.payload)
        )
        entries = await activity_effect_entries(self.journal, session, message.scope, target)
        delivered = await activity_delivered(self.journal, session, entries, local.generation)
        ignored = delivered or bool(entries and (entry.parent_sequence or 0) < (entries[-1].parent_sequence or 0))
        if not ignored:
            await self._validate_predecessors(session, entries, entry, local.generation, completion)
        return PreparedEffect(
            entry.entry_id,
            message.command_id,
            local.generation,
            {
                "original_command_id": str(target),
                "namespace": activity.identity.namespace,
                "workflow_id": activity.identity.workflow_id,
                "run_id": activity.identity.run_id,
                "activity_id": activity.identity.activity_id,
                "attempt": activity.attempt,
                "task_token": activity.task_token,
                "completion": completion.model_dump(mode="json"),
                "rejected": context.delivery_rejected,
                "already_delivered": ignored,
            },
        )

    async def _validate_predecessors(
        self,
        session: AsyncSession,
        entries: list[JournalEntry],
        entry: JournalEntry,
        generation: int,
        completion: Completion,
    ) -> None:
        """Preserve unresolved effects and immutable work identity across outcomes."""
        for previous in entries:
            if previous.entry_id == entry.entry_id:
                continue
            effect = await self.journal.load_prepared_effect(session, previous.entry_id)
            if (
                effect is not None
                and effect.domain_generation == generation
                and not effect.context.get("already_delivered")
                and previous.handling_state in {"prepared", "reconciling"}
            ):
                # An uncertain older effect must be observed before a new
                # completion can use the same Temporal task token.
                raise HandlingDeferred
            if completion.resource and completion.resource.kind == "work_item" and previous.direction == "inbound":
                frozen = await self.journal.load_message(session, previous.entry_id)
                resource = Completion.model_validate(frozen.command.payload).resource
                if (
                    resource
                    and resource.kind == "work_item"
                    and resource.resource_id != completion.resource.resource_id
                ):
                    raise ProtocolConflict

    def _failure(self, effect: PreparedEffect, completion: Completion) -> ApplicationError:
        if effect.context.get("rejected"):
            return ApplicationError(
                "Execution Plane rejected the command", type="ExecutionPlaneRejected", non_retryable=True
            )
        if completion.error_code == "completion_result_too_large":
            return ApplicationError(
                "Execution Plane result exceeds the completion size limit",
                type="ExecutionPlaneResultTooLarge",
                non_retryable=True,
            )
        errors = {
            "cancelled": ("Execution Plane work was cancelled", "ExecutionPlaneWorkCancelled"),
            "outcome_unknown": ("Execution Plane outcome needs reconciliation", "ExecutionPlaneOutcomeUnknown"),
            "failed": ("Script execution failed", "ScriptExecutionError"),
            "superseded": ("Execution Plane command was superseded", "ExecutionPlaneSuperseded"),
        }
        message, kind = errors.get(completion.status, errors["failed"])
        return ApplicationError(message, completion.result.get("result", {}), type=kind, non_retryable=True)

    def _client(self) -> Client:
        if self.temporal is None:
            raise HandlingDeferred
        return self.temporal

    async def _ready(self, effect: PreparedEffect) -> bool:
        """Read Temporal's exact started attempt; a wall-clock delay proves nothing."""
        temporal = self._client()
        if temporal.namespace != effect.context["namespace"]:
            raise ProtocolConflict
        handle = temporal.get_workflow_handle(str(effect.context["workflow_id"]), run_id=str(effect.context["run_id"]))
        description = await handle.describe(rpc_timeout=_RPC_TIMEOUT)
        return any(
            pending.activity_id == effect.context["activity_id"]
            and pending.attempt == effect.context["attempt"]
            and pending.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
            for pending in description.raw_description.pending_activities
        )

    async def execute(self, effect: PreparedEffect) -> EffectResult:
        """Complete/fail the observed current activity only after preparation commits."""
        if effect.context.get("already_delivered"):
            return EffectResult("succeeded")
        temporal = self._client()
        try:
            ready = await self._ready(effect)
        except RPCError:
            # Only a read was attempted: retrying preparation has no external effect.
            raise HandlingDeferred from None
        if not ready:
            observed = await self._observe_history(effect)
            if observed is not None:
                return observed
            raise HandlingDeferred
        handle = temporal.get_async_activity_handle(task_token=base64.b64decode(str(effect.context["task_token"])))
        completion = Completion.model_validate(effect.context["completion"])
        try:
            if completion.status == "succeeded":
                await handle.complete(completion.result.get("result", {}), rpc_timeout=_RPC_TIMEOUT)
            else:
                await handle.fail(self._failure(effect, completion), rpc_timeout=_RPC_TIMEOUT)
        except RPCError:
            # NOT_FOUND may mean accepted-before-response-loss, timed out, or stale
            # token. It never proves success or that resending the effect is safe.
            observed = await self._observe_history(effect)
            return observed or EffectResult("unknown", error_code="temporal_rpc_uncertain")
        return EffectResult("succeeded")

    async def finish(
        self, effect: PreparedEffect, result: EffectResult, context: HandlerContext, session: AsyncSession
    ) -> HandlingResult:
        """Fence stale destinations; the worker records outcome on the journal effect."""
        local = await self.journal.get_local_context(
            session, context.scope, UUID(str(effect.context["original_command_id"])), for_update=True
        )
        if local is None:
            raise ProtocolConflict
        if local.generation != effect.domain_generation:
            raise HandlingDeferred
        return HandlingResult(
            status=CommandStatus.SUCCEEDED if result.outcome == "succeeded" else CommandStatus.OUTCOME_UNKNOWN,
            error_code=None if result.outcome == "succeeded" else result.error_code or "temporal_rpc_uncertain",
        )

    async def reconcile(self, effect: PreparedEffect) -> EffectResult:
        """Observe the journal's durable prepared effect after a crash."""
        observed = await self._observe_history(effect)
        return observed or EffectResult("unknown", error_code="temporal_rpc_uncertain")

    async def _observe_history(self, effect: PreparedEffect) -> EffectResult | None:
        """Observe a matching terminal history event; never reissue an uncertain RPC."""
        if effect.context.get("already_delivered"):
            return EffectResult("succeeded")
        temporal = self._client()
        handle = temporal.get_workflow_handle(str(effect.context["workflow_id"]), run_id=str(effect.context["run_id"]))
        scheduled_id = started_id = None
        completion = Completion.model_validate(effect.context["completion"])
        count = 0
        async for event in handle.fetch_history_events(page_size=100, rpc_timeout=_RPC_TIMEOUT):
            count += 1
            if count > _HISTORY_EVENT_LIMIT:
                return None
            if event.HasField("activity_task_scheduled_event_attributes"):
                if event.activity_task_scheduled_event_attributes.activity_id == effect.context["activity_id"]:
                    scheduled_id = event.event_id
            elif event.HasField("activity_task_started_event_attributes"):
                attrs = event.activity_task_started_event_attributes
                if attrs.scheduled_event_id == scheduled_id and attrs.attempt == effect.context["attempt"]:
                    started_id = event.event_id
            elif started_id is not None and await self._matches_terminal(event, started_id, completion, effect):
                return EffectResult("succeeded")
        return None

    async def _matches_terminal(
        self, event: HistoryEvent, started_id: int, completion: Completion, effect: PreparedEffect
    ) -> bool:
        temporal = self._client()
        if event.HasField("activity_task_completed_event_attributes") and completion.status == "succeeded":
            done = event.activity_task_completed_event_attributes
            if done.started_event_id == started_id:
                decoded = await temporal.data_converter.decode(done.result.payloads)
                return decoded == [completion.result.get("result", {})]
        if event.HasField("activity_task_failed_event_attributes") and completion.status != "succeeded":
            failed = event.activity_task_failed_event_attributes
            if failed.started_event_id == started_id and failed.failure.HasField("application_failure_info"):
                info = failed.failure.application_failure_info
                expected = self._failure(effect, completion)
                decoded = await temporal.data_converter.decode(info.details.payloads)
                return info.type == expected.type and info.non_retryable and decoded == list(expected.details)
        return False


class AODispatchRejectionHandler(TemporalCompletionHandler):
    """Local rejection uses the same durable preparation and token-generation fence."""


class CancellationCompletionHandler:
    """Cancellation observation is separate from the eventual work completion."""

    async def handle(
        self, _message: MessageEnvelope, _context: HandlerContext, _session: AsyncSession
    ) -> HandlingResult:
        """Keep the result queryable in the journal without finishing a script early."""
        return HandlingResult()


def register_handlers(registry: CommandRegistry, journal: ExchangeJournalStore, temporal_client: Client | None) -> None:
    """Install ingress capabilities in API processes and external execution in workers."""
    catalog = CommandCatalog()
    registry.register(
        catalog.definition("work.execute.completed", 1), TemporalCompletionHandler(journal, temporal_client)
    )
    registry.register_rejection("work.execute", 1, AODispatchRejectionHandler(journal, temporal_client))
    registry.register(catalog.definition("command.cancel.completed", 1), CancellationCompletionHandler())
    integration = IntegrationCompletionHandler(journal)
    for action in ("cluster.provision", "cluster.delete"):
        registry.register(catalog.definition(f"{action}.completed", 1), integration)
        registry.register_rejection(action, 1, integration)


class IntegrationCompletionHandler:
    """Project only the current immutable integration revision into AO UI state."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Inject the shared journal containing the frozen source revision."""
        self.journal = journal

    async def handle(self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Ignore deleted or superseded source state without recreating resources."""
        from syntara.integrations.models.integration import ExecutionPlaneSyncStatus, Integration  # noqa: PLC0415

        parent_id = message.command_id if context.delivery_rejected else message.command.parent_command_id
        if parent_id is None:
            raise ProtocolConflict
        entry = await self.journal.get(session, message.scope, parent_id, "outbound")
        if entry is None:
            raise ProtocolConflict
        original = await self.journal.load_message(session, entry.entry_id)
        desired = ResourceRevision.model_validate(
            {
                "resource_key": original.command.payload["resource_key"],
                "desired_revision": original.command.payload["desired_revision"],
            }
        )
        resource_id, revision = desired.resource_key, desired.desired_revision
        integration = await session.get(Integration, resource_id, with_for_update=True, populate_existing=True)
        if integration is None or integration.execution_plane_revision != revision:
            return HandlingResult()
        completion = (
            Completion(status="failed", error_code=context.error_code)
            if context.delivery_rejected
            else Completion.model_validate(message.command.payload)
        )
        reference = completion.resource
        if reference is not None and (reference.resource_key != resource_id or reference.revision != revision):
            raise ProtocolConflict
        draining = original.command.command_type == "cluster.delete" or original.command.payload.get("enabled") is False
        if completion.status == "succeeded":
            integration.execution_plane_status = (
                ExecutionPlaneSyncStatus.DELETING if draining else ExecutionPlaneSyncStatus.READY
            )
            integration.execution_plane_error = None
        else:
            integration.execution_plane_status = ExecutionPlaneSyncStatus.ERROR
            integration.execution_plane_error = completion.error_code or f"command_{completion.status}"
        return HandlingResult()
