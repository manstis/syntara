"""Tests for process-owned drain monitoring."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from execution_plane.drain_monitor import DrainMonitor
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus


def _target(*, cluster_id: uuid.UUID | None = None, is_default: bool = False) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster_id or uuid.uuid4(),
        name="target-a",
        backend_type="vanilla_k8s",
        endpoint="https://target.example",
        api_key="secret",
        status=TargetStatus.DRAINING,
        enabled=False,
        is_default=is_default,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        api_key="secret",
        status=ClusterStatus.DRAINING,
        enabled=False,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _TargetStore:
    def __init__(self, targets: list[ExecutionTarget]) -> None:
        self.targets = targets
        self.finalized: list[uuid.UUID] = []
        self.failed: list[uuid.UUID] = []

    async def list(self, **_: object) -> list[ExecutionTarget]:
        return self.targets

    async def get(self, target_id: uuid.UUID) -> ExecutionTarget | None:
        return next((target for target in self.targets if target.id == target_id), None)

    async def finalize_delete(self, target_id: uuid.UUID) -> None:
        self.finalized.append(target_id)
        self.targets[:] = [target for target in self.targets if target.id != target_id]

    async def finalize_cluster_delete(self, target_id: uuid.UUID) -> None:
        self.finalized.append(target_id)
        self.targets[:] = [target for target in self.targets if target.id != target_id]

    async def mark_failed(self, target_id: uuid.UUID, _updated_by: uuid.UUID) -> None:
        self.failed.append(target_id)


class _ClusterStore:
    def __init__(self, clusters: list[Cluster]) -> None:
        self.clusters = clusters
        self.finalized: list[uuid.UUID] = []
        self.failed: list[uuid.UUID] = []

    async def list(self, **_: object) -> list[Cluster]:
        return self.clusters

    async def finalize_delete(self, cluster_id: uuid.UUID) -> None:
        self.finalized.append(cluster_id)

    async def record_discovery_state(
        self, cluster_id: uuid.UUID, _status: ClusterStatus, _message: str, _updated_by: uuid.UUID
    ) -> Cluster:
        self.failed.append(cluster_id)
        return self.clusters[0]


class _WorkStore:
    def __init__(self, *, drained: bool) -> None:
        self.drained = drained

    async def is_target_drained(self, _target_id: uuid.UUID) -> bool:
        return self.drained


@pytest.mark.asyncio
async def test_monitor_recovers_and_finalizes_persisted_target_and_cluster_drains() -> None:
    cluster = _cluster()
    targets = [_target(cluster_id=cluster.id, is_default=True)]
    target_id = targets[0].id
    target_store = _TargetStore(targets)
    cluster_store = _ClusterStore([cluster])

    monitor = DrainMonitor(target_store, cluster_store, _WorkStore(drained=True), poll_interval=0)  # type: ignore[arg-type]

    await monitor.start()
    await monitor.wait_for_idle()

    assert target_store.finalized == [target_id]
    assert cluster_store.finalized == [cluster.id]
    await monitor.stop()


@pytest.mark.asyncio
async def test_monitor_stop_cancels_a_waiting_drain_task() -> None:
    target = _target()
    target_store = _TargetStore([target])
    cluster_store = _ClusterStore([])
    gate = asyncio.Event()

    async def is_drained(_target_id: uuid.UUID) -> bool:
        await gate.wait()
        return True

    class WaitingWorkStore:
        async def is_target_drained(self, target_id: uuid.UUID) -> bool:
            return await is_drained(target_id)

    monitor = DrainMonitor(
        target_store,  # type: ignore[arg-type]
        cluster_store,  # type: ignore[arg-type]
        WaitingWorkStore(),  # type: ignore[arg-type]
        poll_interval=0,
        monitor_interval=60,
    )
    await monitor.start()
    await monitor.stop()

    assert target_store.finalized == []


@pytest.mark.asyncio
async def test_monitor_polls_for_newly_persisted_target_drains() -> None:
    target = _target()
    target_store = _TargetStore([])
    cluster_store = _ClusterStore([])
    monitor = DrainMonitor(
        target_store,  # type: ignore[arg-type]
        cluster_store,  # type: ignore[arg-type]
        _WorkStore(drained=True),  # type: ignore[arg-type]
        poll_interval=0,
        monitor_interval=0,
    )

    await monitor.start()
    target_store.targets.append(target)
    for _ in range(100):
        if target.id in target_store.finalized:
            break
        await asyncio.sleep(0)
    await monitor.stop()

    assert target_store.finalized == [target.id]
