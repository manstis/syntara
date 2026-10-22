"""Integration sync retains its durable outbox while sending typed desired state."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from execution_plane_client.catalog import CommandCatalog

from syntara.execution_plane import integration_sync
from syntara.execution_plane.integration_sync_model import ExecutionPlaneIntegrationSync


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_outbox_delivers_stable_command_and_retains_disabled_tombstone(monkeypatch, enabled):
    now = datetime.now(UTC)
    row = ExecutionPlaneIntegrationSync(
        integration_id=uuid4(),
        source_revision=3,
        operation="upsert" if enabled else "delete",
        name="workers",
        endpoint="https://cluster.example",
        namespace="ep-workers",
        created_at=now,
        next_attempt_at=now,
    )
    payload = {
        "name": row.name,
        "endpoint": row.endpoint,
        "namespace": row.namespace,
        "credential_ref": str(uuid4()) if enabled else "00000000-0000-0000-0000-000000000000",
        "project_ids": None if enabled else [],
        "labels": {},
        "enabled": enabled,
    }
    monkeypatch.setattr(integration_sync, "_desired_payload", AsyncMock(return_value=payload))
    mark = AsyncMock()
    monkeypatch.setattr(integration_sync, "_mark_outbox", mark)
    transport = AsyncMock()
    transport.__aenter__.return_value = transport
    monkeypatch.setattr(integration_sync, "ExecutionPlaneHttpClient", lambda: transport)

    await integration_sync._deliver(row)
    await integration_sync._deliver(row)

    messages = [call.args[0] for call in transport.submit_command.await_args_list]
    assert len(messages) == 2
    assert messages[0] == messages[1]
    message = messages[0]
    CommandCatalog().validate(message)
    assert message.command_id == row.id
    assert message.command.occurred_at == now
    assert message.command.command_type == "cluster.register"
    assert message.command.payload["resource_key"] == str(row.integration_id)
    assert message.command.payload["desired_revision"] == 3
    assert message.command.payload["enabled"] is enabled
    assert message.command.payload["namespace"] == "ep-workers"
    assert "insecure_skip_tls_verify" not in message.command.payload
    mark.assert_awaited_with(row.id, delivered=True)


@pytest.mark.asyncio
async def test_failed_command_delivery_retains_outbox_without_credential_details(monkeypatch):
    now = datetime.now(UTC)
    row = ExecutionPlaneIntegrationSync(
        integration_id=uuid4(),
        source_revision=1,
        operation="upsert",
        name="workers",
        endpoint="https://cluster.example",
        namespace="ep-workers",
        created_at=now,
        next_attempt_at=now,
    )
    monkeypatch.setattr(integration_sync, "_desired_payload", AsyncMock(side_effect=ValueError("secret-token")))
    mark = AsyncMock()
    monkeypatch.setattr(integration_sync, "_mark_outbox", mark)
    await integration_sync._deliver(row)
    assert mark.await_args.kwargs["delivered"] is False
    assert "secret-token" not in mark.await_args.kwargs["error"]
