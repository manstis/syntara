"""Freeze integration revisions as commands in the caller's domain transaction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal
from uuid import UUID, uuid5

from execution_plane_client.commands import ResourceRevision
from execution_plane_client.commands.clusters import ClusterProvision
from execution_plane_client.commands.targets import KubernetesPlacement, TargetDefinition
from execution_plane_client.protocol import CommandScope
from sqlmodel import col, select

from syntara.core.config.base import get_settings
from syntara.core.exceptions import SafeValueError
from syntara.core.models.secret import EncryptedSecret
from syntara.core.services.secret_service import create_secret_service
from syntara.credentials.exceptions import CredentialDisabledError
from syntara.credentials.lib.injector_resolver import InjectorResolver
from syntara.credentials.models.credential import Credential
from syntara.credentials.models.credential_type import CredentialType
from syntara.execution_plane.runtime import get_ao_runtime
from syntara.integrations.exceptions import IntegrationCredentialRequiredError
from syntara.integrations.lib.credential_resolver import fetch_credential_with_type
from syntara.integrations.models.integration import Integration, IntegrationProjectAssignment, IntegrationScope
from syntara.integrations.models.integration_configuration import OpenShiftConfiguration

if TYPE_CHECKING:
    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.service import MediatorService
    from sqlmodel.ext.asyncio.session import AsyncSession


def integration_command_id(integration_id: UUID, revision: int) -> UUID:
    """Bind each desired revision to exactly one command identity, including deletes."""
    return uuid5(integration_id, f"ep-integration-revision:{revision}")


async def _credential_snapshot(session: AsyncSession, integration: Integration) -> str:
    """Resolve the installed database backend using the same transaction and locks."""
    if integration.management_credential_id is None:
        raise IntegrationCredentialRequiredError(integration.integration_type.value)
    # Lock before resolving to prevent token/type changes midway through a snapshot.
    await session.get(Credential, integration.management_credential_id, with_for_update=True, populate_existing=True)
    credential, credential_type = await fetch_credential_with_type(session, integration.management_credential_id)
    await session.get(CredentialType, credential_type.id, with_for_update=True, populate_existing=True)
    # Draining uses the stored credential for cleanup while disabling new work.
    if integration.enabled and not credential.enabled:
        raise CredentialDisabledError(credential.name)
    if credential.secret_id is None:
        raise IntegrationCredentialRequiredError(integration.integration_type.value)
    await session.exec(
        select(EncryptedSecret).where(EncryptedSecret.secret_id == credential.secret_id).with_for_update()
    )
    secret = await create_secret_service(session).retrieve_secret(credential.secret_id)
    resolved = InjectorResolver.resolve(credential_type.injectors or {}, secret).extra_vars
    token = str(resolved.get("bearer_token") or resolved.get("token") or resolved.get("api_key") or "")
    if not token:
        raise IntegrationCredentialRequiredError(integration.integration_type.value)
    return token


async def enqueue_integration_revision(
    session: AsyncSession,
    integration: Integration,
    operation: Literal["upsert", "delete"],
    *,
    runtime: MediatorService | None = None,
) -> MessageEnvelope:
    """Serialize one fixed desired revision without committing or contacting EP."""
    runtime = runtime or get_ao_runtime()
    scope = CommandScope(client_id=get_settings().ep_participant_id, project_id=None, all_projects=True)
    command_id = integration_command_id(integration.id, integration.execution_plane_revision)

    async def create_command() -> JournalEntry:
        resource = ResourceRevision(resource_key=integration.id, desired_revision=integration.execution_plane_revision)
        if operation == "delete":
            return await runtime.commands.cluster_delete(
                session,
                command_id=command_id,
                scope=scope,
                payload=resource,
            )
        configuration = integration.configuration
        if not isinstance(configuration, OpenShiftConfiguration) or configuration.insecure_skip_tls_verify:
            msg = "Execution Plane integrations require OpenShift configuration and verified TLS"
            raise SafeValueError(msg)
        token = await _credential_snapshot(session, integration)
        projects = None
        if integration.scope == IntegrationScope.PROJECT:
            projects = list(
                (
                    await session.exec(
                        select(IntegrationProjectAssignment.project_id)
                        .where(IntegrationProjectAssignment.integration_id == integration.id)
                        .order_by(col(IntegrationProjectAssignment.project_id))
                    )
                ).all()
            )
        labels = {
            **(integration.labels or {}),
            "integration_id": str(integration.id),
            "integration_name": integration.name,
        }
        payload = ClusterProvision(
            **resource.model_dump(),
            name=integration.name,
            endpoint=configuration.base_url,
            credential=token,
            ca_certificate=configuration.ca_certificate,
            enabled=integration.enabled,
            project_ids=projects,
            labels=labels,
            default_target=TargetDefinition(
                name=f"{integration.name[:247]}-default",
                backend_type="vanilla_k8s",
                endpoint=configuration.base_url,
                credential=token,
                labels=labels,
                placement=KubernetesPlacement(namespace=configuration.namespace),
            ),
        )
        return await runtime.commands.cluster_provision(
            session,
            command_id=command_id,
            scope=scope,
            payload=payload,
        )

    prepared = await runtime.journal.ensure_outbound(
        session,
        scope,
        command_id,
        command_factory=create_command,
    )
    expected = "cluster.delete" if operation == "delete" else "cluster.provision"
    if prepared.entry.command_type != expected:
        msg = "Integration revision is already bound to a different operation"
        raise SafeValueError(msg)
    return prepared.message
