"""Persistence operations for Cluster lifecycle transitions."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane.store_errors import StoreConfigurationError, StoreSessionError

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncGenerator
    from typing import Any

    from sqlalchemy.pool import Pool


class ClusterNotFoundError(LookupError):
    """Raised when a lifecycle operation targets an unknown Cluster."""

    def __init__(self, cluster_id: uuid.UUID) -> None:
        """Identify the missing Cluster."""
        super().__init__(f"Cluster {cluster_id} does not exist")


class ClusterHasTargetsError(ValueError):
    """Raised when a Cluster is finalized before its targets are removed."""


class ClusterNotDrainedError(ValueError):
    """Raised when a Cluster is finalized before entering DRAINING."""


class ClusterStore:
    """Persist Cluster state and own the database resources it uses."""

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
            self._engine = (
                create_async_engine(database_url)
                if poolclass is None
                else create_async_engine(database_url, poolclass=poolclass)
            )
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
        """Dispose the store's database engine."""
        await self.close()

    async def close(self) -> None:
        """Dispose all pooled database connections owned by the store."""
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

    async def create(
        self,
        name: str,
        endpoint: str,
        api_key: str,
        created_by: uuid.UUID,
        labels: dict[str, Any] | None = None,
    ) -> Cluster:
        """Persist a new Cluster in REGISTERING state."""
        now = datetime.now(UTC)
        cluster = Cluster(
            name=name,
            endpoint=endpoint,
            api_key=api_key,
            labels=labels or {},
            status=ClusterStatus.REGISTERING,
            created_by=created_by,
            created_at=now,
            updated_by=created_by,
            updated_at=now,
        )
        async with self._session_context() as session:
            try:
                session.add(cluster)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def get(self, cluster_id: uuid.UUID) -> Cluster | None:
        """Return a Cluster without its API credential."""
        async with self._session_context() as session:
            cluster = await session.get(Cluster, cluster_id)
            return None if cluster is None else self._without_secret(cluster)

    async def list(self, *, status: ClusterStatus | None = None, enabled: bool | None = None) -> list[Cluster]:
        """List Clusters for administrative or recovery workflows."""
        statement = select(Cluster)
        if status is not None:
            statement = statement.where(col(Cluster.status) == status)
        if enabled is not None:
            statement = statement.where(col(Cluster.enabled).is_(enabled))
        async with self._session_context() as session:
            result = await session.execute(statement)
            return [self._without_secret(cluster) for cluster in result.scalars().all()]

    async def record_discovery_state(
        self,
        cluster_id: uuid.UUID,
        status: ClusterStatus,
        status_message: str | None,
        updated_by: uuid.UUID,
    ) -> Cluster:
        """Persist registration/discovery state on the existing Cluster."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                cluster.status = status
                cluster.enabled = status is not ClusterStatus.ERROR
                cluster.status_message = status_message
                cluster.updated_by = updated_by
                cluster.updated_at = datetime.now(UTC)
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def mark_active(
        self, cluster_id: uuid.UUID, updated_by: uuid.UUID, status_message: str | None = None
    ) -> Cluster:
        """Mark a successfully registered Cluster active."""
        return await self.record_discovery_state(cluster_id, ClusterStatus.ACTIVE, status_message, updated_by)

    async def request_delete(self, cluster_id: uuid.UUID, updated_by: uuid.UUID) -> Cluster:
        """Disable a Cluster and all targets before asynchronous draining."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                now = datetime.now(UTC)
                cluster.enabled = False
                cluster.status = ClusterStatus.DRAINING
                cluster.updated_by = updated_by
                cluster.updated_at = now
                result = await session.execute(
                    select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster_id)
                )
                for target in result.scalars().all():
                    target.enabled = False
                    target.status = TargetStatus.DRAINING
                    target.updated_by = updated_by
                    target.updated_at = now
                await session.commit()
                return self._without_secret(cluster)
            except Exception:
                await session.rollback()
                raise

    async def finalize_delete(self, cluster_id: uuid.UUID) -> None:
        """Delete a Cluster only after all associated targets are gone."""
        async with self._session_context() as session:
            try:
                cluster = await session.get(Cluster, cluster_id)
                if cluster is None:
                    raise ClusterNotFoundError(cluster_id)  # noqa: TRY301
                if cluster.enabled or cluster.status is not ClusterStatus.DRAINING:
                    raise ClusterNotDrainedError(cluster_id)  # noqa: TRY301
                result = await session.execute(
                    select(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster_id)
                )
                if result.scalars().all():
                    raise ClusterHasTargetsError(cluster_id)  # noqa: TRY301
                await session.delete(cluster)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @staticmethod
    def _without_secret(cluster: Cluster) -> Cluster:
        return cluster.model_copy(update={"api_key": ""})
