"""Already frozen activity retries do not depend on mutable image configuration."""

from types import SimpleNamespace

from syntara.execution_plane.commands import activities
from syntara.workflows.workflow_engine.activities.ep import ep_dispatch_activity as activity_module


async def test_retry_reuses_frozen_work_when_image_setting_is_removed(temporal_case, monkeypatch):
    original = await temporal_case.submit()
    monkeypatch.setattr(
        activity_module, "get_settings", lambda: SimpleNamespace(temporal_namespace="default", node_container_images={})
    )
    monkeypatch.setattr(
        activity_module.activity,
        "info",
        lambda: SimpleNamespace(
            workflow_id="workflow", workflow_run_id="run", activity_id="script", task_token=b"token-2", attempt=2
        ),
    )
    monkeypatch.setattr(activity_module, "AsyncSessionLocal", temporal_case.sessions)
    monkeypatch.setattr(activities, "get_ao_runtime", lambda: temporal_case.runtime)
    await activity_module._dispatch_to_ep({"code": "changed", "language": "bash"}, None, temporal_case.project_id)
    activity = await temporal_case.activity_context()
    assert activity.attempt == 2
    async with temporal_case.sessions() as session:
        entry = await temporal_case.journal.get(session, temporal_case.scope, temporal_case.command_id, "outbound")
        assert await temporal_case.journal.load_message(session, entry.entry_id) == original
