"""Contract tests for AO requests sent to the independent Execution Plane."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from execution_plane_client.commands.clusters import ClusterRegister

from syntara.execution_plane.client import ExecutionPlaneHttpClient


@pytest.mark.asyncio
async def test_cluster_binding_request_keeps_namespace_as_kubernetes_integration_config() -> None:
    client = object.__new__(ExecutionPlaneHttpClient)
    message_id = uuid4()
    request = AsyncMock(return_value={"received": True, "message_id": str(message_id)})
    object.__setattr__(client, "_request", request)

    command = ClusterRegister(
        resource_key=uuid4(),
        desired_revision=3,
        name="workers",
        endpoint="https://cluster.example",
        namespace="ep-workers",
        credential_ref=uuid4(),
        project_ids=None,
        labels={"environment": "test"},
        enabled=True,
    )

    await client.submit_command(
        command.to_message(
            source_id="syntara-orchestration",
            destination_id="execution-plane",
            client_id="syntara-orchestration",
            command_id=message_id,
        )
    )

    await_args = request.await_args
    assert await_args is not None
    body = await_args.kwargs["json_body"]["command"]["payload"]
    assert body["namespace"] == "ep-workers"
    assert "placement" not in body
