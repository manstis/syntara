"""WorkStore — the only path for mutating WorkItem state.

Nothing outside this module should construct, modify, or commit WorkItem
objects. The public methods express the intended lifecycle transitions;
any state not reachable through them is not a valid transition.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.store_errors import StoreConfigurationError, StoreSessionError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlalchemy.pool import Pool

_NOTIFY_CHANNEL = "execution_plane_work_items"


class WorkItemNotFoundError(LookupError):
    """Raised when a lifecycle transition targets an unknown work item."""

    def __init__(self, item_id: uuid.UUID) -> None:
        """Identify the missing work item."""
        super().__init__(f"Work item {item_id} does not exist")


class WorkStore:
    """Persist work item lifecycle transitions and own database resources."""

    def __init__(
        self,
        database_url: str | None = None,
        poolclass: type[Pool] | None = None,
        *,
        engine: AsyncEngine | None = None,
        session: AsyncSession | None = None,
    ) -> None:
        """Create a store backed by a borrowed session, URL, or engine."""
        self._session = session
        if session is not None:
            self._engine = None
            self._session_factory = None
            self._owns_engine = False
        elif engine is not None:
            self._engine = engine
            self._owns_engine = False
        else:
            if database_url is None:
                raise StoreConfigurationError
            if poolclass is None:
                self._engine = create_async_engine(database_url)
            else:
                self._engine = create_async_engine(database_url, poolclass=poolclass)
            self._owns_engine = True
        self._session_factory = async_sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)

    @classmethod
    def from_session(cls, session: AsyncSession) -> Self:
        """Create a store borrowing a request-scoped session."""
        return cls(session=session)

    @classmethod
    def from_engine(cls, engine: AsyncEngine) -> Self:
        """Create a store borrowing an application-owned engine."""
        return cls(engine=engine)

    @classmethod
    def from_database_url(cls, database_url: str, poolclass: type[Pool] | None = None) -> Self:
        """Create a store that owns an engine created from a database URL."""
        return cls(database_url, poolclass=poolclass)

    async def __aenter__(self) -> Self:
        """Return this store for use as an async context manager."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Dispose the store's engine."""
        await self.close()

    async def close(self) -> None:
        """Dispose all pooled database connections owned by this store."""
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()

    @asynccontextmanager
    async def _session_context(self) -> AsyncGenerator[AsyncSession, None]:
        """Yield a borrowed request session or an owned short-lived session."""
        if self._session is not None:
            yield self._session
            return
        if self._session_factory is None:
            raise StoreSessionError
        async with self._session_factory() as session:
            yield session

    async def dispatch(
        self,
        activity_handle: str,
        work_correlation_id: uuid.UUID,
        payload: dict[str, Any],
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
        async with self._session_context() as session:
            try:
                session.add(item)
                # pg_notify is transactional — delivered only after this commit.
                await session.execute(text(f"SELECT pg_notify('{_NOTIFY_CHANNEL}', '')"))
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return item

    async def claim_one(self) -> WorkItem | None:
        """Claim the oldest PENDING item for this worker, or return None."""
        async with self._session_context() as session:
            try:
                result = await session.execute(
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
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def set_result(
        self,
        item_id: uuid.UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
    ) -> WorkItem:
        """Persist the terminal result before signalling Temporal.

        Committing here means the startup recovery pass can retry the signal
        if the process crashes between this commit and mark_signal_delivered.
        """
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                item.result = result
                item.status = status
                item.completed_at = datetime.now(UTC)
                await session.commit()
                return item
            except Exception:
                await session.rollback()
                raise

    async def mark_signal_delivered(self, item_id: uuid.UUID) -> None:
        """Record that the Temporal async-completion callback was confirmed sent."""
        async with self._session_context() as session:
            try:
                item = await session.get(WorkItem, item_id)
                if item is None:
                    raise WorkItemNotFoundError(item_id)  # noqa: TRY301
                item.signaled_at = datetime.now(UTC)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def find_undelivered(self) -> list[WorkItem]:
        """Return terminal items whose Temporal signal was never confirmed."""
        async with self._session_context() as session:
            result = await session.execute(
                select(WorkItem)
                .where(col(WorkItem.status).in_([WorkItemStatus.COMPLETED, WorkItemStatus.FAILED]))
                .where(col(WorkItem.signaled_at).is_(None))
            )
            return list(result.scalars().all())

    async def is_target_drained(self, target_id: uuid.UUID) -> bool:
        """Return whether no claimed or dispatched work remains on a target."""
        async with self._session_context() as session:
            result = await session.execute(
                select(col(WorkItem.id))
                .where(col(WorkItem.execution_target_id) == target_id)
                .where(col(WorkItem.status).in_([WorkItemStatus.CLAIMED, WorkItemStatus.DISPATCHED]))
                .limit(1)
            )
            return result.scalar_one_or_none() is None
