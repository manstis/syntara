"""AO command routes keep explicit machine authentication and user query scopes."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from syntara.api.execution_plane_facade import _list_in_authorized_projects
from syntara.authz.engine import AllowedProjectsResult
from syntara.execution_plane.router import create_command_router


def test_peer_router_mounts_shared_paths_and_explicit_permission_marker():
    router = create_command_router(Mock())
    assert {route.path for route in router.routes} == {
        "/api/execution_plane/v1/commands",
        "/api/execution_plane/v1/commands/{command_id}",
        "/api/execution_plane/v1/commands/{command_id}/children",
        "/api/execution_plane/v1/capabilities",
    }
    assert all(route.dependant.dependencies for route in router.routes)
    assert "/events" not in {route.path for route in router.routes}


@pytest.mark.asyncio
async def test_facade_preserves_each_authorized_project_scope():
    client = Mock(list_work_items=AsyncMock(return_value=[]))
    projects = [uuid4(), uuid4()]
    await _list_in_authorized_projects(
        client,
        operation="work_items",
        allowed_projects=AllowedProjectsResult(all_projects=False, project_ids=projects),
        limit=20,
    )
    assert {call.args[0].project_id for call in client.list_work_items.await_args_list} == set(projects)
    assert all(not call.args[0].all_projects for call in client.list_work_items.await_args_list)


@pytest.mark.asyncio
async def test_facade_denies_empty_scope_before_remote_read():
    client = Mock(list_work_items=AsyncMock())
    with pytest.raises(HTTPException) as caught:
        await _list_in_authorized_projects(
            client,
            operation="work_items",
            allowed_projects=AllowedProjectsResult(all_projects=False, project_ids=[]),
            limit=20,
        )
    assert caught.value.status_code == 403
    client.list_work_items.assert_not_called()


@pytest.mark.asyncio
async def test_mediator_closes_after_worker_cancellation(monkeypatch):
    import asyncio

    from syntara.execution_plane import runtime

    entered = asyncio.Event()

    async def run() -> None:
        entered.set()
        await asyncio.Event().wait()

    service = Mock(run=run, close=AsyncMock())
    monkeypatch.setattr(runtime, "build_ao_runtime", lambda *_args, **_kwargs: service)
    task = asyncio.create_task(runtime.run_ao_mediator(Mock(), Mock()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    service.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_mediator_failure_stops_temporal_worker():
    import asyncio

    from syntara.workflows.workflow_engine.services.temporal_worker import TemporalWorkerService

    entered, stopped = asyncio.Event(), asyncio.Event()

    async def work() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def fail() -> None:
        await entered.wait()
        msg = "exchange failed"
        raise RuntimeError(msg)

    service = TemporalWorkerService("test", "default", "test")
    service.worker = Mock(run=work)
    runtime = Mock(run=fail, close=AsyncMock())
    with pytest.raises(ExceptionGroup):
        await service._run_workers(runtime)
    assert stopped.is_set()
    runtime.close.assert_awaited_once()
