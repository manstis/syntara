"""Contract tests for AO's shared-command HTTP transport."""

import json
from uuid import uuid4

import httpx
import pytest
from execution_plane_client.commands.control import CancelCommand

from syntara.execution_plane import client as client_module
from syntara.execution_plane.client import ExecutionPlaneHttpClient, ExecutionPlaneRejectedError


def cancellation():
    return CancelCommand(target_command_id=uuid4()).to_message(
        source_id="syntara-orchestration", destination_id="execution-plane", client_id="syntara-orchestration"
    )


@pytest.mark.asyncio
async def test_command_transport_sends_shared_envelope_and_checks_receipt(monkeypatch):
    message = cancellation()
    requests = []

    def handle(request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, json={"received": True, "message_id": str(message.message_id)})

    monkeypatch.setattr(client_module, "create_ep_service_token", lambda: "test-token")
    client = object.__new__(ExecutionPlaneHttpClient)
    client._client = httpx.AsyncClient(base_url="https://ep.example", transport=httpx.MockTransport(handle))
    async with client:
        receipt = await client.submit_command(message)
    assert receipt.message_id == message.message_id
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/v1/commands"
    assert requests[0].headers["Authorization"] == "Bearer test-token"
    assert json.loads(requests[0].content) == message.model_dump(mode="json")
    assert "command.cancel" in client_module.EP_SCOPES.split()
    assert "work.execute" in client_module.EP_SCOPES.split()
    assert "cluster.register" in client_module.EP_SCOPES.split()
    assert "clusters:read" in client_module.EP_SCOPES.split()


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [{"received": True, "message_id": str(uuid4())}, {"status": "accepted"}])
async def test_command_transport_rejects_invalid_or_unrelated_receipt(monkeypatch, body):
    monkeypatch.setattr(client_module, "create_ep_service_token", lambda: "test-token")
    client = object.__new__(ExecutionPlaneHttpClient)
    client._client = httpx.AsyncClient(
        base_url="https://ep.example", transport=httpx.MockTransport(lambda _: httpx.Response(202, json=body))
    )
    async with client:
        with pytest.raises(ExecutionPlaneRejectedError):
            await client.submit_command(cancellation())


@pytest.mark.asyncio
async def test_conflict_preserves_status_for_work_recovery(monkeypatch):
    monkeypatch.setattr(client_module, "create_ep_service_token", lambda: "test-token")
    client = object.__new__(ExecutionPlaneHttpClient)
    client._client = httpx.AsyncClient(
        base_url="https://ep.example",
        transport=httpx.MockTransport(lambda _: httpx.Response(409, json={"detail": "Resource conflict"})),
    )
    async with client:
        with pytest.raises(ExecutionPlaneRejectedError) as error:
            await client.submit_command(cancellation())
    assert error.value.status_code == 409


@pytest.mark.asyncio
async def test_cluster_observation_uses_resource_query(monkeypatch):
    resource_id = uuid4()
    paths = []

    def handle(request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"observed_revision": 3, "status": "ready"})

    monkeypatch.setattr(client_module, "create_ep_service_token", lambda: "test-token")
    client = object.__new__(ExecutionPlaneHttpClient)
    client._client = httpx.AsyncClient(base_url="https://ep.example", transport=httpx.MockTransport(handle))
    async with client:
        result = await client.get_cluster_binding(source_integration_id=resource_id)
    assert paths == [f"/v1/clusters/{resource_id}"]
    assert result["observed_revision"] == 3
