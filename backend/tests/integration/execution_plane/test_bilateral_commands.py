"""Crash boundaries across AO and an independently installed EP process."""

import pytest


@pytest.mark.parametrize(
    "failure_point",
    [
        "before_sender_commit",
        "after_receiver_commit",
        "after_acceptance_commit",
        "after_handler_claim",
        "after_result_commit",
        "after_completion_receipt",
        "after_temporal_rpc_before_finish",
    ],
)
async def test_restarts_preserve_responsibility(bilateral_case, failure_point):
    await bilateral_case.execute_with_failure(failure_point)
    await bilateral_case.restart_and_reconcile()
    await bilateral_case.assert_durable_or_explicitly_unknown()
    assert bilateral_case.provider_execute_count <= 1


async def test_provision_execute_cancel_query_and_drain_delete(bilateral_case):
    from datetime import UTC, datetime
    from uuid import uuid4

    from execution_plane_client.commands import CommandIdentity, ResourceRevision
    from execution_plane_client.commands.control import CancelCommand
    from execution_plane_client.protocol import CommandScope

    await bilateral_case.execute_with_failure("after_completion_receipt")
    await bilateral_case.restart_and_reconcile()
    await bilateral_case.assert_durable_or_explicitly_unknown()
    case = bilateral_case.case
    result = await bilateral_case.query("execution-plane", case.scope, case.command_id)
    assert result.status == "succeeded"
    page = await bilateral_case.children("execution-plane", case.scope, case.command_id)
    assert len(page.items) == 1
    assert (await bilateral_case.children("execution-plane", case.scope, case.command_id, after=1)).items == []
    cancel_id = uuid4()
    async with case.sessions() as session, session.begin():
        await case.runtime.client.cancel_command(
            session,
            identity=CommandIdentity(command_id=cancel_id, message_id=uuid4(), occurred_at=datetime.now(UTC)),
            route=case.runtime.client.route(case.scope, peer_id="execution-plane"),
            payload=CancelCommand(target_command_id=case.command_id),
        )
    await case.runtime.delivery.run_once()
    await bilateral_case.rpc("handle")
    await bilateral_case.rpc("deliver")
    await case.runtime.handling.run_once()
    assert (await bilateral_case.query("execution-plane", case.scope, cancel_id)).status == "succeeded"
    scope = CommandScope(client_id="syntara-orchestration", project_id=None, all_projects=True)
    deletion = uuid4()
    async with case.sessions() as session, session.begin():
        await case.runtime.client.cluster_delete(
            session,
            identity=CommandIdentity(command_id=deletion, message_id=uuid4(), occurred_at=datetime.now(UTC)),
            route=case.runtime.client.route(scope, peer_id="execution-plane"),
            payload=ResourceRevision(resource_key=bilateral_case.cluster_key, desired_revision=2),
        )
    await case.runtime.delivery.run_once()
    await bilateral_case.rpc("handle")
    assert (await bilateral_case.query("execution-plane", scope, deletion)).status == "running"
    state = await bilateral_case.rpc("drain")
    assert state["clusters"] == state["targets"] == 0
    assert (await bilateral_case.query("execution-plane", scope, deletion)).status == "succeeded"
    await bilateral_case.rpc("deliver")
    await case.runtime.handling.run_once()
    await case.runtime.delivery.run_once()


async def test_completion_before_lost_acceptance_and_project_query_isolation(bilateral_case):
    from uuid import uuid4

    from execution_plane_client.protocol import CommandScope

    await bilateral_case.start()
    await bilateral_case.provision()
    case = bilateral_case.case
    await case.submit()
    await case.runtime.delivery.run_once()
    await bilateral_case.rpc("drop_acceptance", enabled=True)
    await bilateral_case.rpc("handle")
    await bilateral_case.rpc("execute")
    await bilateral_case.rpc("deliver")
    await case.runtime.handling.run_once()
    await case.runtime.delivery.run_once()
    # Completion handling is independent of the still-missing explicit ACK.
    assert (await case.effect_entry()).handling_state == "handled"
    async with case.sessions() as session:
        entry = await case.journal.get(session, case.scope, case.command_id, "outbound")
        assert entry.delivery_state == "sent"
    await bilateral_case.rpc("drop_acceptance", enabled=False)
    await bilateral_case.rpc("expire")
    await bilateral_case.rpc("deliver")
    await bilateral_case.assert_durable_or_explicitly_unknown()
    path = f"/v1/commands/{case.command_id}"
    for scope, expected in [(case.scope, 200), (CommandScope(client_id=case.scope.client_id, project_id=uuid4()), 404)]:
        result = await bilateral_case.rpc(
            "read_http", path=path, assertion=bilateral_case.signer.sign_query("execution-plane", scope, path)
        )
        assert result["status"] == expected
        assert "echo hello" not in str(result)
        assert "token-1" not in str(result)
