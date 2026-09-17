"""WorkStore — the only path for mutating WorkItem state.

Nothing outside this module should construct, modify, or commit WorkItem
objects. The public methods express the intended lifecycle transitions;
any state not reachable through them is not a valid transition.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlmodel import col

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from execution_plane.models.work_item import WorkItem, WorkItemStatus

_NOTIFY_CHANNEL = "execution_plane_work_items"


class WorkStore:
    """Persist work item lifecycle transitions and callback delivery state."""

    def __init__(self, session: AsyncSession) -> None:
        """Use the caller-owned async database session."""
        self._session = session

    async def dispatch(
        self,
        activity_handle: str,
        work_correlation_id: uuid.UUID,
        payload: dict,
    ) -> WorkItem:
        """Insert a new PENDING WorkItem and wake the EP worker via pg_notify."""
        item = WorkItem(
            id=uuid.uuid4(),
            work_correlation_id=work_correlation_id,
            activity_handle=activity_handle,
            status=WorkItemStatus.PENDING,
            payload=payload,
            created_at=datetime.now(UTC),
        )
        self._session.add(item)
        # pg_notify is transactional — delivered only after this commit.
        await self._session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
        await self._session.commit()
        return item

    async def claim_one(self) -> WorkItem | None:
        """Claim the oldest PENDING item for this worker, or return None."""
        result = await self._session.execute(
            select(WorkItem)
            .where(col(WorkItem.status) == WorkItemStatus.PENDING)
            .order_by(col(WorkItem.created_at))
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        item = result.scalars().first()
        if item is None:
            return None
        item.status = WorkItemStatus.CLAIMED
        item.claimed_at = datetime.now(UTC)
        await self._session.commit()
        return item

    async def set_result(
        self,
        item: WorkItem,
        result: dict,
        status: WorkItemStatus,
    ) -> None:
        """Persist the terminal result before signalling Temporal.

        Committing here means the startup recovery pass can retry the signal
        if the process crashes between this commit and mark_signal_delivered.
        """
        item.result = result
        item.status = status
        item.completed_at = datetime.now(UTC)
        await self._session.commit()

    async def mark_signal_delivered(self, item: WorkItem) -> None:
        """Record that the Temporal async-completion callback was confirmed sent."""
        item.signaled_at = datetime.now(UTC)
        await self._session.commit()

    async def find_undelivered(self) -> list[WorkItem]:
        """Return terminal items whose Temporal signal was never confirmed."""
        result = await self._session.execute(
            select(WorkItem)
            .where(col(WorkItem.status).in_([WorkItemStatus.COMPLETED, WorkItemStatus.FAILED]))
            .where(col(WorkItem.signaled_at).is_(None))
        )
        return list(result.scalars().all())
