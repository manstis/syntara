"""Temporal correlation stored privately on the original journal command."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    from uuid import UUID

    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import CommandScope
    from sqlalchemy.ext.asyncio import AsyncSession


class TemporalIdentity(SQLModel):
    """Stable logical activity identity independent of delivery attempts."""

    namespace: str
    workflow_id: str
    run_id: str
    activity_id: str


class TemporalActivityContext(SQLModel):
    """Private destination facts; journal effects own completion and recovery state."""

    identity: TemporalIdentity
    attempt: int
    task_token: str = Field(repr=False)


def workflow_correlation_key(workflow_id: str) -> str:
    """Group workflow commands without exposing workflow names in journal indexes."""
    return hashlib.sha256(b"ao-workflow-v1:" + workflow_id.encode()).hexdigest()


async def activity_effect_entries(
    journal: ExchangeJournalStore, session: AsyncSession, scope: CommandScope, command_id: UUID
) -> list[JournalEntry]:
    """Read journal-owned rejection and completion obligations in sequence order."""
    entries = []
    original = await journal.get(session, scope, command_id, "outbound")
    if original is not None and original.delivery_state == "rejected":
        entries.append(original)
    after = 0
    while True:
        page = await journal.list_children(session, scope, command_id, after=after, limit=100)
        for child in page.items:
            if child.command_type == "work.execute.completed":
                entry = await journal.get(session, scope, child.command_id, "inbound")
                if entry is not None:
                    entries.append(entry)
        if page.next_after is None:
            return entries
        after = page.next_after


async def activity_delivered(
    journal: ExchangeJournalStore, session: AsyncSession, entries: list[JournalEntry], generation: int
) -> bool:
    """Recognize an actual successful effect for this token generation."""
    for entry in entries:
        if entry.handling_state != "handled":
            continue
        effect = await journal.load_prepared_effect(session, entry.entry_id)
        if (
            effect is not None
            and effect.domain_generation == generation
            and not effect.context.get("already_delivered")
        ):
            return True
    return False
