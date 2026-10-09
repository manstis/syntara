"""Script activities freeze durable work, then hand completion to the exchange."""

from types import SimpleNamespace
from typing import NoReturn
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID

import pytest
from temporalio.exceptions import ApplicationError

from syntara.workflows.workflow_engine.activities.ep import ep_dispatch_activity as activity_module

PROJECT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def dispatch(monkeypatch):
    events = []
    info = SimpleNamespace(
        attempt=1, workflow_id="workflow", workflow_run_id="run", activity_id="script", task_token=b"token"
    )
    monkeypatch.setattr(activity_module.activity, "info", lambda: info)
    monkeypatch.setattr(activity_module.activity, "heartbeat", Mock())
    settings = SimpleNamespace(
        script_nodes_enabled=True,
        temporal_namespace="default",
        node_container_images={"script": "original-image"},
        workflow_http_request_allowed_hosts=[],
        aap_poll_interval_seconds=1,
    )
    monkeypatch.setattr(activity_module, "get_settings", lambda: settings)

    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(side_effect=lambda: events.append("begin"))
    transaction.__aexit__ = AsyncMock(side_effect=lambda *_args: events.append("commit"))
    session = Mock(begin=Mock(return_value=transaction))
    manager = MagicMock()
    manager.__aenter__ = AsyncMock(return_value=session)
    manager.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(activity_module, "AsyncSessionLocal", lambda: manager)
    enqueue = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("enqueue"))
    monkeypatch.setattr(activity_module, "enqueue_script", enqueue)

    def handoff() -> NoReturn:
        events.append("async")
        msg = "complete asynchronously"
        raise RuntimeError(msg)

    monkeypatch.setattr(activity_module.activity, "raise_complete_async", handoff)
    return SimpleNamespace(events=events, info=info, settings=settings, enqueue=enqueue)


@pytest.mark.asyncio
async def test_commit_precedes_async_handoff_without_http(dispatch):
    with pytest.raises(RuntimeError, match="complete asynchronously"):
        await activity_module.execute_script_activity({"language": "bash", "code": "echo hello"}, None, PROJECT_ID)
    assert dispatch.events == ["begin", "enqueue", "commit", "async"]
    payload = dispatch.enqueue.await_args.kwargs["payload"]()
    assert payload["image"] == "original-image"
    assert payload["invocation"]["inputs"]["code"] == "echo hello"
    assert isinstance(dispatch.enqueue.await_args.kwargs["command_id"], UUID)


@pytest.mark.asyncio
@pytest.mark.parametrize("config", [{}, {"language": "bash", "code": ""}, {"language": "ruby", "code": "puts 'hi'"}])
async def test_invalid_input_fails_before_enqueue(dispatch, config):
    with pytest.raises(ApplicationError) as caught:
        await activity_module.execute_script_activity(config, None, PROJECT_ID)
    assert caught.value.non_retryable
    assert caught.value.type == "ConfigError"
    dispatch.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_script_feature_gate_precedes_enqueue(dispatch):
    dispatch.settings.script_nodes_enabled = False
    with pytest.raises(ApplicationError) as caught:
        await activity_module.execute_script_activity({"language": "bash", "code": "echo hello"}, None, PROJECT_ID)
    assert caught.value.type == "ScriptNodeDisabled"
    dispatch.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_activity_retry_uses_identical_command_identity(dispatch):
    for attempt in [1, 2]:
        dispatch.info.attempt = attempt
        with pytest.raises(RuntimeError, match="complete asynchronously"):
            await activity_module.execute_script_activity({"language": "bash", "code": "echo hello"}, None, PROJECT_ID)
    first, second = dispatch.enqueue.await_args_list
    assert first.kwargs["command_id"] == second.kwargs["command_id"]
    assert second.kwargs["activity_attempt"] == 2
