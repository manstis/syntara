"""Tests for cluster synchronization in IntegrationService.

Tests that OpenShift integration CRUD syncs cluster records via ClusterRegistry.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from execution_plane.models.execution_target_placement import KubernetesPlacement
from sqlmodel.ext.asyncio.session import AsyncSession

from syntara.core.models import User
from syntara.core.services.secret_service import SecretService, create_secret_service
from syntara.integrations.models.integration import (
    IntegrationCreate,
    IntegrationType,
    IntegrationUpdate,
)
from syntara.integrations.services.integration_service import IntegrationService
from tests.integration.helpers.credential import CredentialFactory


def _openshift_create(
    name: str = "Test OpenShift",
    credential_id: UUID | None = None,
) -> IntegrationCreate:
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
    token: str = "test-api-key",  # noqa: S107
) -> tuple[UUID, SecretService]:
    ct = await credential_factory.create_type("HTTP Bearer Token")
    # HTTP Bearer Token maps the 'token' input to 'bearer_token' in extra_vars
    ct.injectors = {"extra_vars": {"bearer_token": "{{token}}"}, "env": {}, "file": {}}
    project = await credential_factory.create_project()
    cred = await credential_factory.create(ct, project)
    secret_service = create_secret_service(credential_factory.session)
    cred.secret_id = await secret_service.create_secret({"token": token})
    await credential_factory.session.flush()
    return UUID(str(cred.id)), secret_service


def _mock_registry() -> MagicMock:
    registry = MagicMock()
    registry.provision = AsyncMock(return_value=MagicMock())
    registry.get_by_name = AsyncMock(return_value=None)
    registry.request_delete = AsyncMock(return_value=MagicMock())
    registry.sync_update = AsyncMock(return_value=None)
    return registry


class TestClusterSyncOnCreate:
    """Tests for cluster sync during integration creation."""

    @pytest.mark.asyncio
    async def test_openshift_create_without_cluster_registry(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Cluster sync is skipped when no registry is configured; integration is still created."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        service = IntegrationService(test_db_session, test_user, secret_service=secret_service, cluster_registry=None)
        result = await service.create_integration(_openshift_create(credential_id=cred_id))
        assert result.name == "Test OpenShift"
        assert result.integration_type == IntegrationType.OPENSHIFT

    @pytest.mark.asyncio
    async def test_openshift_create_syncs_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Creating an OpenShift integration calls registry.register with the right args."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        registry = _mock_registry()

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        assert result.name == "Test OpenShift"
        registry.provision.assert_called_once()
        kw = registry.provision.call_args[1]
        assert kw["name"] == "Test OpenShift"
        assert kw["endpoint"] == "https://openshift.example.com:6443"
        assert kw["api_key"] == "test-api-key"
        assert kw["placement"] == KubernetesPlacement(namespace="syntara-workers")
        assert kw["created_by"] == test_user.id
        assert kw["labels"]["integration_name"] == "Test OpenShift"

    @pytest.mark.asyncio
    async def test_openshift_create_without_credential_raises(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        """Creating an OpenShift integration without a credential raises before cluster sync."""
        from syntara.integrations.exceptions import IntegrationCredentialRequiredError

        service = IntegrationService(test_db_session, test_user, cluster_registry=_mock_registry())
        with pytest.raises(IntegrationCredentialRequiredError):
            await service.create_integration(_openshift_create(credential_id=None))

    @pytest.mark.asyncio
    async def test_mcp_create_does_not_sync_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        """Creating a non-OpenShift integration does not touch the cluster registry."""
        registry = _mock_registry()
        service = IntegrationService(test_db_session, test_user, cluster_registry=registry)

        data = IntegrationCreate(
            name="Test MCP",
            integration_type=IntegrationType.MCP_SERVER,
            configuration={"integration_type": "mcp_server", "base_url": "http://localhost:8080"},
        )
        result = await service.create_integration(data)

        assert result.integration_type == IntegrationType.MCP_SERVER
        registry.register.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_sync_failure_fails_integration_create(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """If registry.provision raises, integration creation must also fail (hard dependency)."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        registry = _mock_registry()
        registry.provision = AsyncMock(side_effect=RuntimeError("Cluster sync failed"))

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        with pytest.raises(RuntimeError, match="Cluster sync failed"):
            await service.create_integration(_openshift_create(credential_id=cred_id))

    @pytest.mark.asyncio
    async def test_missing_api_key_fails_integration_create(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """If the credential has no bearer_token/token/api_key, integration creation fails."""
        ct = await credential_factory.create_type("HTTP Bearer Token")
        project = await credential_factory.create_project()
        cred = await credential_factory.create(ct, project)
        secret_service = create_secret_service(credential_factory.session)
        cred.secret_id = await secret_service.create_secret({"some_other_field": "value"})
        await credential_factory.session.flush()

        service = IntegrationService(
            test_db_session,
            test_user,
            secret_service=secret_service,
            cluster_registry=_mock_registry(),
        )
        with pytest.raises(ValueError, match="bearer_token"):
            await service.create_integration(_openshift_create(credential_id=UUID(str(cred.id))))


class TestClusterSyncOnDelete:
    """Tests for cluster sync during integration deletion."""

    @pytest.mark.asyncio
    async def test_openshift_delete_syncs_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Deleting an OpenShift integration calls get_by_name then request_delete."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        mock_cluster = MagicMock()
        mock_cluster.id = uuid4()
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=mock_cluster)

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        await service.delete_integration(result.id)

        registry.get_by_name.assert_called_once_with("Test OpenShift")
        registry.request_delete.assert_called_once_with(mock_cluster.id, test_user.id)

    @pytest.mark.asyncio
    async def test_mcp_delete_does_not_sync_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        """Deleting a non-OpenShift integration does not touch the cluster registry."""
        registry = _mock_registry()
        service = IntegrationService(test_db_session, test_user, cluster_registry=registry)

        data = IntegrationCreate(
            name="Test MCP",
            integration_type=IntegrationType.MCP_SERVER,
            configuration={"integration_type": "mcp_server", "base_url": "http://localhost:8080"},
        )
        result = await service.create_integration(data)
        await service.delete_integration(result.id)

        registry.get_by_name.assert_not_called()
        registry.request_delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_sync_delete_failure_fails_integration_delete(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """If request_delete raises, integration deletion must also fail (hard dependency)."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        mock_cluster = MagicMock()
        mock_cluster.id = uuid4()
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=mock_cluster)
        registry.request_delete = AsyncMock(side_effect=RuntimeError("Delete failed"))

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        with pytest.raises(RuntimeError, match="Delete failed"):
            await service.delete_integration(result.id)

    @pytest.mark.asyncio
    async def test_cluster_not_found_fails_integration_delete(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """If get_by_name returns None, integration deletion fails (no orphaned integrations)."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=None)

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        with pytest.raises(ValueError, match="not found"):
            await service.delete_integration(result.id)


class TestClusterSyncOnUpdate:
    """Tests for cluster sync during integration updates."""

    @pytest.mark.asyncio
    async def test_openshift_update_namespace_syncs_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Updating configuration (namespace) calls sync_update with the new namespace."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        mock_cluster = MagicMock()
        mock_cluster.id = uuid4()
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=mock_cluster)

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        patch = IntegrationUpdate(
            configuration={
                "integration_type": "openshift",
                "base_url": "https://openshift.example.com:6443",
                "namespace": "new-namespace",
            }
        )
        await service.update_integration(result.id, patch)

        registry.sync_update.assert_called_once()
        kw = registry.sync_update.call_args[1]
        assert kw["placement"] == KubernetesPlacement(namespace="new-namespace")
        assert kw["endpoint"] == "https://openshift.example.com:6443"
        assert kw["updated_by"] == test_user.id

    @pytest.mark.asyncio
    async def test_openshift_update_name_syncs_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Updating name calls sync_update with the new name."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        mock_cluster = MagicMock()
        mock_cluster.id = uuid4()
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=mock_cluster)

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        patch = IntegrationUpdate(name="Renamed OpenShift")
        await service.update_integration(result.id, patch)

        registry.sync_update.assert_called_once()
        kw = registry.sync_update.call_args[1]
        assert kw["name"] == "Renamed OpenShift"

    @pytest.mark.asyncio
    async def test_openshift_update_irrelevant_field_does_not_sync(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """Updating description (not cluster-relevant) does not call sync_update."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        registry = _mock_registry()

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        patch = IntegrationUpdate(description="Updated description")
        await service.update_integration(result.id, patch)

        registry.sync_update.assert_not_called()

    @pytest.mark.asyncio
    async def test_mcp_update_does_not_sync_cluster(
        self,
        test_db_session: AsyncSession,
        test_user: User,
    ) -> None:
        """Updating a non-OpenShift integration never touches sync_update."""
        registry = _mock_registry()
        service = IntegrationService(test_db_session, test_user, cluster_registry=registry)

        data = IntegrationCreate(
            name="Test MCP",
            integration_type=IntegrationType.MCP_SERVER,
            configuration={"integration_type": "mcp_server", "base_url": "http://localhost:8080"},
        )
        result = await service.create_integration(data)

        patch = IntegrationUpdate(name="Renamed MCP")
        await service.update_integration(result.id, patch)

        registry.sync_update.assert_not_called()

    @pytest.mark.asyncio
    async def test_cluster_not_found_fails_integration_update(
        self,
        test_db_session: AsyncSession,
        test_user: User,
        credential_factory: CredentialFactory,
    ) -> None:
        """If cluster is not found during update, the update fails."""
        cred_id, secret_service = await _make_bearer_credential(credential_factory)
        registry = _mock_registry()
        registry.get_by_name = AsyncMock(return_value=None)

        service = IntegrationService(
            test_db_session, test_user, secret_service=secret_service, cluster_registry=registry
        )
        result = await service.create_integration(_openshift_create(credential_id=cred_id))

        patch = IntegrationUpdate(name="New Name")
        with pytest.raises(ValueError, match="not found"):
            await service.update_integration(result.id, patch)
