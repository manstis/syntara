"""AO integration-sync outbox tests for OpenShift resources."""

from uuid import UUID

import pytest
from execution_plane_client.protocol import MessageEnvelope
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from syntara.core.models import User
from syntara.core.services.secret_service import SecretService, create_secret_service
from syntara.execution_plane import runtime as runtime_module
from syntara.integrations.exceptions import IntegrationCredentialRequiredError
from syntara.integrations.models.integration import IntegrationCreate, IntegrationType, IntegrationUpdate
from syntara.integrations.services.integration_service import IntegrationService
from tests.integration.helpers.credential import CredentialFactory


def _openshift_create(name: str = "Test OpenShift", credential_id: UUID | None = None) -> IntegrationCreate:
    return IntegrationCreate(
        name=name,
        integration_type=IntegrationType.OPENSHIFT,
        configuration={
            "integration_type": "openshift",
            "base_url": "https://openshift.example.com:6443",
            "namespace": "syntara-workers",
        },
        management_credential_id=credential_id,
    )


async def _make_bearer_credential(
    credential_factory: CredentialFactory,
) -> tuple[UUID, SecretService]:
    credential_type = await credential_factory.create_type("HTTP Bearer Token")
    credential_type.injectors = {"extra_vars": {"bearer_token": "{{token}}"}, "env": {}, "file": {}}
    project = await credential_factory.create_project()
    credential = await credential_factory.create(credential_type, project)
    secret_service = create_secret_service(credential_factory.session)
    credential.secret_id = await secret_service.create_secret({"token": "test-api-key"})
    await credential_factory.session.flush()
    return UUID(str(credential.id)), secret_service


async def _sync_rows(session: AsyncSession, integration_id: UUID) -> list[MessageEnvelope]:
    runtime = runtime_module.get_ao_runtime()
    table = runtime.journal.schema.table
    ids = (
        (
            await session.execute(
                select(table.c.entry_id)
                .where(
                    table.c.direction == "outbound", table.c.command_type.in_(["cluster.provision", "cluster.delete"])
                )
                .order_by(table.c.created_at)
            )
        )
        .scalars()
        .all()
    )
    messages = [await runtime.journal.load_message(session, entry_id) for entry_id in ids]
    return [message for message in messages if message.command.payload["resource_key"] == str(integration_id)]


class TestIntegrationSyncOutbox:
    """AO CRUD commits EP desired-state intents without calling EP inline."""

    @pytest.mark.asyncio
    async def test_openshift_create_records_versioned_upsert(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        credential_id, _ = await _make_bearer_credential(credential_factory)
        service = IntegrationService(test_db_session, test_user)

        integration = await service.create_integration(_openshift_create(credential_id=credential_id))

        rows = await _sync_rows(test_db_session, integration.id)
        assert len(rows) == 1
        assert rows[0].command.command_type == "cluster.provision"
        assert rows[0].command.payload["desired_revision"] == 1
        assert rows[0].command.payload["name"] == "Test OpenShift"
        assert rows[0].command.payload["endpoint"] == "https://openshift.example.com:6443"
        assert integration.execution_plane_status == "pending"

    @pytest.mark.asyncio
    async def test_openshift_create_requires_a_management_credential(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        service = IntegrationService(test_db_session, test_user)
        with pytest.raises(IntegrationCredentialRequiredError):
            await service.create_integration(_openshift_create())

    @pytest.mark.asyncio
    async def test_non_openshift_create_does_not_queue_ep_state(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        service = IntegrationService(test_db_session, test_user)
        integration = await service.create_integration(
            IntegrationCreate(
                name="Test MCP",
                integration_type=IntegrationType.MCP_SERVER,
                configuration={"integration_type": "mcp_server", "base_url": "http://localhost:8080"},
            )
        )
        assert await _sync_rows(test_db_session, integration.id) == []

    @pytest.mark.asyncio
    async def test_delete_records_a_disable_tombstone(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        credential_id, _ = await _make_bearer_credential(credential_factory)
        service = IntegrationService(test_db_session, test_user)
        integration = await service.create_integration(_openshift_create(credential_id=credential_id))

        await service.delete_integration(integration.id)

        rows = await _sync_rows(test_db_session, integration.id)
        assert [row.command.command_type for row in rows] == ["cluster.provision", "cluster.delete"]
        assert rows[-1].command.payload["desired_revision"] == 2

    @pytest.mark.asyncio
    async def test_relevant_update_increments_the_outbox_revision(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        credential_id, _ = await _make_bearer_credential(credential_factory)
        service = IntegrationService(test_db_session, test_user)
        integration = await service.create_integration(_openshift_create(credential_id=credential_id))

        await service.update_integration(integration.id, IntegrationUpdate(name="Renamed OpenShift"))

        rows = await _sync_rows(test_db_session, integration.id)
        assert [row.command.payload["desired_revision"] for row in rows] == [1, 2]
        assert rows[-1].command.payload["name"] == "Renamed OpenShift"

    @pytest.mark.asyncio
    async def test_create_returns_while_ep_sync_is_pending(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        credential_id, _ = await _make_bearer_credential(credential_factory)
        service = IntegrationService(test_db_session, test_user)

        integration = await service.create_integration(_openshift_create(credential_id=credential_id))

        assert integration.execution_plane_status == "pending"
        assert (await _sync_rows(test_db_session, integration.id))[0].command.command_type == "cluster.provision"
