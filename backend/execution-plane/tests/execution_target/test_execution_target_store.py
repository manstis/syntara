"""Additional unit tests for ExecutionTargetStore persistence branches."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Self

import pytest
from execution_plane.execution_target.execution_target_store import (
    DefaultExecutionTargetError,
    ExecutionTargetNotFoundError,
    ExecutionTargetStore,
)
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus

_DATABASE_UNAVAILABLE = "database unavailable"


def _target(*, is_default: bool = False, status: TargetStatus = TargetStatus.ACTIVE) -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=uuid.uuid4(),
        name="target-a",
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://target.example",
        api_key="secret",
        is_default=is_default,
        status=status,
        enabled=status is TargetStatus.ACTIVE,
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


class _Result:
    def __init__(self, target: ExecutionTarget | None = None, targets: list[ExecutionTarget] | None = None) -> None:
        self.target = target
        self.targets = targets or ([] if target is None else [target])

    def scalar_one_or_none(self) -> ExecutionTarget | None:
        return self.target

    def scalars(self) -> Self:
        return self

    def all(self) -> list[ExecutionTarget]:
        return self.targets


class _Session:
    def __init__(self, *, result: _Result | None = None, fail_commit: bool = False) -> None:
        self.result = result or _Result()
        self.fail_commit = fail_commit
        self.deleted: ExecutionTarget | None = None
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get(self, _model: object, _target_id: uuid.UUID) -> ExecutionTarget | None:
        return self.result.target

    async def execute(self, _statement: object) -> _Result:
        return self.result

    async def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError(_DATABASE_UNAVAILABLE)

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def delete(self, target: ExecutionTarget) -> None:
        self.deleted = target


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


def _store(session: _Session) -> ExecutionTargetStore:
    store = ExecutionTargetStore("postgresql+asyncpg://localhost/syntara")
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]
    return store


@pytest.mark.asyncio
async def test_get_redacts_credentials_or_returns_secret_when_requested() -> None:
    target = _target()
    store = _store(_Session(result=_Result(target)))

    redacted = await store.get(target.id)
    included = await store.get(target.id, include_secret=True)

    assert redacted is not None
    assert redacted.api_key == ""
    assert included is target
    await store.close()


@pytest.mark.asyncio
async def test_get_returns_none_for_an_unknown_target() -> None:
    store = _store(_Session())

    assert await store.get(uuid.uuid4()) is None
    await store.close()


@pytest.mark.asyncio
async def test_list_applies_cluster_status_eligibility_and_limit_filters() -> None:
    target = _target()
    store = _store(_Session(result=_Result(target, [target])))

    result = await store.list(
        cluster_id=target.cluster_id,
        eligible_only=True,
        status=TargetStatus.ACTIVE,
        limit=1,
    )

    assert len(result) == 1
    assert result[0].api_key == ""
    await store.close()


@pytest.mark.asyncio
async def test_request_delete_rejects_missing_and_default_targets() -> None:
    missing_store = _store(_Session())
    with pytest.raises(ExecutionTargetNotFoundError):
        await missing_store.request_delete(uuid.uuid4(), uuid.uuid4())
    await missing_store.close()

    default = _target(is_default=True)
    default_store = _store(_Session(result=_Result(default)))
    with pytest.raises(DefaultExecutionTargetError):
        await default_store.request_delete(default.id, uuid.uuid4())
    await default_store.close()


@pytest.mark.asyncio
async def test_activate_updates_target_state_and_rolls_back_when_target_is_missing() -> None:
    target = _target(status=TargetStatus.REGISTERING)
    store = _store(_Session(result=_Result(target)))

    result = await store.activate(target.id, uuid.uuid4())
    assert result.status is TargetStatus.ACTIVE
    assert result.enabled is True

    missing_session = _Session()
    missing_store = _store(missing_session)
    with pytest.raises(ExecutionTargetNotFoundError):
        await missing_store.activate(uuid.uuid4(), uuid.uuid4())
    assert missing_session.rollbacks == 1
    await store.close()
    await missing_store.close()


@pytest.mark.asyncio
async def test_update_can_replace_api_key_and_rejects_missing_target() -> None:
    target = _target()
    store = _store(_Session(result=_Result(target)))

    result = await store.update(target.id, updated_by=uuid.uuid4(), api_key="new-secret")

    assert result.api_key == ""
    assert target.api_key == "new-secret"

    missing_session = _Session()
    missing_store = _store(missing_session)
    with pytest.raises(ExecutionTargetNotFoundError):
        await missing_store.update(uuid.uuid4(), updated_by=uuid.uuid4())
    assert missing_session.rollbacks == 1
    await store.close()
    await missing_store.close()


@pytest.mark.asyncio
async def test_mark_failed_records_failure_and_rejects_missing_target() -> None:
    target = _target(status=TargetStatus.DRAINING)
    store = _store(_Session(result=_Result(target)))

    result = await store.mark_failed(target.id, uuid.uuid4())

    assert result.status is TargetStatus.FAILED
    assert result.enabled is False
    assert result.status_message == "target drain failed"

    missing_session = _Session()
    missing_store = _store(missing_session)
    with pytest.raises(ExecutionTargetNotFoundError):
        await missing_store.mark_failed(uuid.uuid4(), uuid.uuid4())
    assert missing_session.rollbacks == 1
    await store.close()
    await missing_store.close()


@pytest.mark.asyncio
async def test_finalize_cluster_delete_allows_a_drained_default_target() -> None:
    target = _target(is_default=True, status=TargetStatus.DRAINING)
    target.enabled = False
    session = _Session(result=_Result(target))
    store = _store(session)

    await store.finalize_cluster_delete(target.id)

    assert session.deleted is target
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_rejects_missing_target() -> None:
    session = _Session()
    store = _store(session)

    with pytest.raises(ExecutionTargetNotFoundError):
        await store.finalize_delete(uuid.uuid4())

    assert session.rollbacks == 1
    await store.close()


@pytest.mark.asyncio
async def test_finalize_delete_rolls_back_when_deletion_commit_fails() -> None:
    target = _target(status=TargetStatus.DRAINING)
    target.enabled = False
    session = _Session(result=_Result(target), fail_commit=True)
    store = _store(session)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await store.finalize_delete(target.id)

    assert session.rollbacks == 1
    await store.close()
