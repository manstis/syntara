"""Actual AO journal workers with independently controlled Temporal RPC outcomes."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4, uuid5

import pytest_asyncio
from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.errors import HandlingDeferred, PermanentRejection
from execution_plane_client.handlers import CommandRegistry, EffectResult, HandlerContext
from execution_plane_client.journal.schema import build_journal_schema
from execution_plane_client.journal.store import ExchangeJournalStore
from execution_plane_client.protocol import Command, CommandScope, MessageEnvelope, ResourceReference
from execution_plane_client.security import AssertionVerifier, AuthenticatedIdentity, PeerConfig, PeerGrant
from execution_plane_client.service import build_service
from sqlalchemy import select, update
from temporalio.api.enums.v1 import PendingActivityState
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

from syntara.execution_plane.activity_context import TemporalActivityContext, TemporalIdentity, activity_effect_entries
from syntara.execution_plane.commands.activities import enqueue_script, stable_command_id
from syntara.execution_plane.handlers import TemporalCompletionHandler, register_handlers

if TYPE_CHECKING:
    from temporalio.client import Client


class FakeTemporal:
    """External boundary with controlled completion responses and history."""

    namespace = "default"

    def __init__(self) -> None:
        """Initialize controlled external state."""
        self.calls: list[Any] = []
        self.ready = True
        self.attempt = 1
        self.ambiguous = False
        self.history: list[Any] = []
        self.data_converter = DataConverter.default

    def get_workflow_handle(self, *_args: object, **_kwargs: object):
        return self

    async def describe(self, **_kwargs: object):
        state = (
            PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
            if self.ready
            else PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED
        )
        pending = SimpleNamespace(activity_id="script", attempt=self.attempt, state=state)
        return SimpleNamespace(raw_description=SimpleNamespace(pending_activities=[pending]))

    async def fetch_history_events(self, **_kwargs: object):
        for event in self.history:
            yield event

    def get_async_activity_handle(self, *, task_token):
        temporal = self

        class Handle:
            async def complete(self, result, **_kwargs: object) -> None:
                temporal.calls.append((task_token, "complete", result))
                if temporal.ambiguous:
                    msg = "test response lost"
                    raise RPCError(msg, RPCStatusCode.DEADLINE_EXCEEDED, b"")

            async def fail(self, error, **_kwargs: object) -> None:
                temporal.calls.append((task_token, "fail", error))
                if temporal.ambiguous:
                    msg = "test response lost"
                    raise RPCError(msg, RPCStatusCode.DEADLINE_EXCEEDED, b"")

        return Handle()


class TemporalCase:
    """Exercise production adapters through fresh committed sessions."""

    def __init__(self, sessions, temporal=None) -> None:
        """Compose actual AO adapters and shared exchange workers."""
        self.sessions = sessions
        self.temporal = temporal or FakeTemporal()
        self.identity = TemporalIdentity(
            namespace="default", workflow_id="workflow", run_id="run", activity_id="script"
        )
        self.command_id = stable_command_id(self.identity)
        self.work_item_id = uuid4()
        self.project_id = uuid4()
        self.scope = CommandScope(client_id="syntara-orchestration", project_id=self.project_id)
        self.payload = {
            "image": "test-node:original",
            "invocation": {
                "version": 1,
                "operation": "execute",
                "inputs": {"code": "echo hello", "language": "bash"},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": 60,
                "max_output_bytes": 1024,
            },
        }
        self.journal = ExchangeJournalStore(
            build_journal_schema(), AESGCMCipher(active_key_id="test", keys={"test": b"a" * 32})
        )
        registry = CommandRegistry()
        register_handlers(registry, self.journal, cast("Client", self.temporal))
        permissions = frozenset(
            {
                "work.execute",
                "work.execute.completed",
                "command.cancel",
                "command.cancel.completed",
                "command.accepted",
                "commands.read",
                "commands:read",
                "capabilities:read",
                "cluster.provision",
                "cluster.provision.completed",
                "cluster.delete",
                "cluster.delete.completed",
            }
        )
        self.peers = {
            name: PeerConfig(
                peer_id=name,
                issuer=name,
                audience=name,
                role=role,
                command_url=f"https://{host}{'/api/execution_plane' if role == 'controller' else ''}/v1/commands",
                query_base_url=f"https://{host}{'/api/execution_plane' if role == 'controller' else ''}/v1",
                grants={"syntara-orchestration": PeerGrant(project_ids=None, permissions=permissions)},
                verification_keys={"test": "test-verifier-not-used"},
                permitted_networks=["127.0.0.0/8"],
            )
            for name, role, host in [
                ("syntara-orchestration", "controller", "ao.test"),
                ("execution-plane", "executor", "ep.test"),
            ]
        }
        self.transport = Mock(
            send=AsyncMock(),
            aclose=AsyncMock(),
            get_capabilities=AsyncMock(return_value=SimpleNamespace(commands={"work.execute": [1]})),
        )
        self.runtime = build_service(
            store=self.journal,
            sessions=sessions,
            local_peer=self.peers["syntara-orchestration"],
            peers=self.peers,
            registry=registry,
            transport=self.transport,
            verifier=AssertionVerifier(
                local_id="syntara-orchestration",
                audience="syntara-orchestration",
                command_url="https://ao.test/api/execution_plane/v1/commands",
                peers=self.peers,
            ),
            certificate_auth=AsyncMock(return_value="execution-plane"),
        )
        self.handler = TemporalCompletionHandler(self.journal, cast("Client", self.temporal))
        self.context = HandlerContext(
            self.scope, "syntara-orchestration", "execution-plane", self.journal, lambda: datetime.now(UTC)
        )
        self.completion = None

    async def enqueue(self, session, *, attempt=1):
        self.temporal.attempt = attempt
        return await enqueue_script(
            session,
            command_id=self.command_id,
            project_id=self.project_id,
            temporal_identity=self.identity,
            task_token=f"token-{attempt}".encode(),
            activity_attempt=attempt,
            payload=self.payload,
            runtime=self.runtime,
        )

    async def submit(self, *, attempt=1):
        async with self.sessions() as session, session.begin():
            return await self.enqueue(session, attempt=attempt)

    async def local_context(self):
        async with self.sessions() as session:
            return await self.journal.get_local_context(session, self.scope, self.command_id)

    async def activity_context(self):
        return TemporalActivityContext.model_validate((await self.local_context()).data)

    async def effect_entry(self):
        async with self.sessions() as session:
            entries = await activity_effect_entries(self.journal, session, self.scope, self.command_id)
            return entries[-1]

    async def receive_completion(self, *, status="succeeded", attempt=1, sequence=1):
        original = await self.submit(attempt=attempt)
        command_id = uuid5(original.command_id, f"completion:{sequence}")
        self.completion = MessageEnvelope(
            message_id=uuid5(command_id, "submit"),
            command_id=command_id,
            source_id="execution-plane",
            destination_id="syntara-orchestration",
            scope=self.scope,
            command=Command(
                command_id=command_id,
                command_type="work.execute.completed",
                command_revision=1,
                parent_command_id=original.command_id,
                parent_sequence=sequence,
                target_url=original.command.callback_url,
                callback_url=original.command.target_url,
                occurred_at=datetime.now(UTC),
                payload={
                    "status": status,
                    "result": {"result": {"value": 42}},
                    "resource": ResourceReference(kind="work_item", resource_id=self.work_item_id).model_dump(
                        mode="json"
                    ),
                },
            ),
        )
        identity = AuthenticatedIdentity(
            "execution-plane",
            self.scope,
            self.peers["execution-plane"].grants[self.scope.client_id].permissions,
            "executor",
        )
        async with self.sessions() as session, session.begin():
            await self.runtime.ingress.receive(self.completion, identity, session)
        return self.completion

    async def prepare_completion(self, *, attempt=1):
        await self.receive_completion(attempt=attempt)
        async with self.sessions() as session, session.begin():
            return await self.handler.prepare(self.completion, self.context, session)

    async def retry_activity(self, *, attempt):
        return await self.submit(attempt=attempt)

    async def finish_success(self, effect):
        async with self.sessions() as session:
            try:
                await self.handler.finish(effect, EffectResult("succeeded"), self.context, session)
                await session.commit()
            except HandlingDeferred:
                await session.rollback()

    async def pump(self):
        return await self.runtime.handling.run_once()

    async def make_due(self):
        table = self.journal.schema.table
        async with self.sessions() as session, session.begin():
            await session.execute(
                update(table).values(
                    handling_due_at=datetime.now(UTC) - timedelta(seconds=1),
                    reconciliation_due_at=datetime.now(UTC) - timedelta(seconds=1),
                )
            )

    async def reject_submission(self):
        self.transport.send.side_effect = PermanentRejection()
        await self.runtime.delivery.run_once()


@pytest_asyncio.fixture
async def temporal_case(test_db_session_factory, test_db_session):
    case = TemporalCase(test_db_session_factory)
    yield case
    await case.runtime.close()


class IntegrationCase:
    """Real IntegrationService mutations with database-backed credential snapshots."""

    def __init__(self, temporal_case, user, credential_factory) -> None:
        """Share only participant composition with the activity test driver."""
        self.runtime = temporal_case.runtime
        self.journal = temporal_case.journal
        self.sessions = temporal_case.sessions
        self.user = user
        self.factory = credential_factory
        self.integration_id = None
        self.credential_id = None
        self.secret_id = None
        self.project_id = None
        self.project_scoped = False

    async def credentials(self, token):
        from syntara.core.services.secret_service import create_secret_service

        kind = await self.factory.create_type("HTTP Bearer Token")
        kind.injectors = {"extra_vars": {"bearer_token": "{{token}}"}, "env": {}, "file": {}}
        project = await self.factory.create_project()
        credential = await self.factory.create(kind, project)
        self.project_id = project.id
        self.secret_id = await create_secret_service(self.factory.session).create_secret({"token": token})
        credential.secret_id = self.secret_id
        self.credential_id = credential.id
        await self.factory.session.commit()

    async def create(self, session):
        from syntara.integrations.models.integration import IntegrationCreate
        from syntara.integrations.services.integration_service import IntegrationService

        created = await IntegrationService(session, self.user).create_integration(
            IntegrationCreate(
                name="Test cluster",
                integration_type="openshift",
                management_credential_id=self.credential_id,
                scope="project" if self.project_scoped else "global",
                configuration={
                    "integration_type": "openshift",
                    "base_url": "https://cluster.example.com:6443",
                    "namespace": "test-workers",
                },
            )
        )
        self.integration_id = created.id
        return created

    async def enqueue_with_token(self, token, *, project_scoped=False):
        self.project_scoped = project_scoped
        await self.credentials(token)
        async with self.sessions() as session:
            await self.create(session)
        return await self.latest_entry()

    async def latest_entry(self):
        from syntara.execution_plane.commands.integrations import integration_command_id

        integration = await self.integration()
        if integration:
            self.revision = integration.execution_plane_revision
        scope = CommandScope(client_id="syntara-orchestration", project_id=None, all_projects=True)
        async with self.sessions() as session:
            entry = await self.journal.get(
                session, scope, integration_command_id(self.integration_id, self.revision), "outbound"
            )
            return entry.entry_id

    async def rotate_token(self, token):
        from syntara.core.services.secret_service import create_secret_service

        async with self.sessions() as session, session.begin():
            await create_secret_service(session).update_secret(self.secret_id, {"token": token})

    async def load_journal_message(self, entry_id):
        async with self.sessions() as session:
            return await self.journal.load_message(session, entry_id)

    async def raw_journal_text(self):
        async with self.sessions() as session:
            rows = (await session.execute(select(self.journal.schema.table))).mappings().all()
            return str(rows)

    async def integration(self):
        from syntara.integrations.models.integration import Integration

        async with self.sessions() as session:
            return await session.get(Integration, self.integration_id)

    async def update(self, changes):
        from syntara.integrations.services.integration_service import IntegrationService

        async with self.sessions() as session:
            await IntegrationService(session, self.user).update_integration(self.integration_id, changes)
        return await self.latest_entry()

    async def assign_project(self):
        from syntara.integrations.services.integration_service import IntegrationService

        async with self.sessions() as session:
            await IntegrationService(session, self.user).assign_project(self.integration_id, self.project_id)
        return await self.latest_entry()

    async def delete(self):
        from syntara.integrations.services.integration_service import IntegrationService

        self.revision = (await self.integration()).execution_plane_revision + 1
        async with self.sessions() as session:
            await IntegrationService(session, self.user).delete_integration(self.integration_id)
        return await self.latest_entry()

    async def complete(self, entry_id, *, status="succeeded"):
        original = await self.load_journal_message(entry_id)
        command_id = uuid5(original.command_id, "completion:1")
        message = MessageEnvelope(
            message_id=uuid5(command_id, "submit"),
            command_id=command_id,
            source_id=original.destination_id,
            destination_id=original.source_id,
            scope=original.scope,
            command=Command(
                command_id=command_id,
                command_type=f"{original.command.command_type}.completed",
                command_revision=1,
                parent_command_id=original.command_id,
                parent_sequence=1,
                target_url=original.command.callback_url,
                callback_url=original.command.target_url,
                occurred_at=datetime.now(UTC),
                payload={"status": status},
            ),
        )
        identity = AuthenticatedIdentity(
            "execution-plane", original.scope, frozenset({message.command.command_type}), "executor"
        )
        async with self.sessions() as session, session.begin():
            await self.runtime.ingress.receive(message, identity, session)
        await self.runtime.handling.run_once()


@pytest_asyncio.fixture
async def integration_case(temporal_case, test_user, credential_factory, monkeypatch):
    from syntara.execution_plane.commands import integrations
    from syntara.integrations.services.integration_service import IntegrationService

    monkeypatch.setattr(integrations, "get_ao_runtime", lambda: temporal_case.runtime)
    # This suite exercises durable domain transactions; DNS egress policy has its
    # own route/service tests and no network is needed to enqueue these commands.
    monkeypatch.setattr(IntegrationService, "_validate_configuration_ssrf", lambda *_args: None)
    return IntegrationCase(temporal_case, test_user, credential_factory)


class AOHTTPCase:
    """Run actual AO middleware with transport-injected verified TLS metadata."""

    def __init__(self, case, app, signer, message) -> None:
        """Retain only test-specific transport controls."""
        self.case, self.app, self.signer, self.message = case, app, signer, message
        self.certificate_name = "execution-plane.ao.svc"
        self.commit_failure = False

    async def post_valid_message(
        self, *, cert_authenticated=True, signed=True, certificate_name=None, wrong_scope=False, commit_failure=False
    ):
        import httpx

        self.certificate_name = (certificate_name or "execution-plane.ao.svc") if cert_authenticated else None
        self.commit_failure = commit_failure

        async def tls(scope, receive, send) -> None:
            if self.certificate_name:
                scope.setdefault("extensions", {})["tls"] = {
                    "peercert": {"subject": ((("commonName", self.certificate_name),),)}
                }
            await self.app(scope, receive, send)

        signed_message = self.message
        if wrong_scope:
            signed_message = self.message.model_copy(
                update={"scope": CommandScope(client_id="syntara-orchestration", project_id=uuid4())}
            )
        headers = {"Authorization": f"Bearer {self.signer.sign(signed_message)}"} if signed else {}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=tls, raise_app_exceptions=False), base_url="https://ao.test"
        ) as client:
            return await client.post(
                "/api/execution_plane/v1/commands", json=self.message.model_dump(mode="json"), headers=headers
            )

    async def journal_count(self):
        from sqlalchemy import func

        table = self.case.journal.schema.table
        async with self.case.sessions() as session:
            return await session.scalar(select(func.count()).select_from(table).where(table.c.direction == "inbound"))


@pytest_asyncio.fixture
async def ao_http_case(temporal_case, session_app, monkeypatch):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from execution_plane_client.security import AssertionSigner
    from sqlalchemy import event

    from syntara.core.config.base import get_settings
    from syntara.core.database.session import get_db
    from syntara.execution_plane import router
    from syntara.execution_plane.runtime import build_certificate_auth

    private = ec.generate_private_key(ec.SECP256R1())
    public = (
        private.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    peers = {
        name: peer.model_copy(update={"verification_keys": {"test": public}})
        for name, peer in temporal_case.peers.items()
    }
    temporal_case.runtime.verifier = AssertionVerifier(
        local_id="syntara-orchestration",
        audience="syntara-orchestration",
        command_url="https://ao.test/api/execution_plane/v1/commands",
        peers=peers,
    )
    temporal_case.runtime.certificate_auth = build_certificate_auth({"execution-plane.ao.svc": "execution-plane"})
    signer = AssertionSigner(
        local_id="execution-plane",
        issuer="execution-plane",
        key_id="test",
        private_key=private.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ).decode(),
        peers=peers,
    )
    original = await temporal_case.submit()
    command_id = uuid5(original.command_id, "http-completion")
    message = MessageEnvelope(
        message_id=uuid5(command_id, "submit"),
        command_id=command_id,
        source_id=original.destination_id,
        destination_id=original.source_id,
        scope=original.scope,
        command=Command(
            command_id=command_id,
            command_type="work.execute.completed",
            command_revision=1,
            parent_command_id=original.command_id,
            parent_sequence=1,
            target_url=original.command.callback_url,
            callback_url=original.command.target_url,
            occurred_at=datetime.now(UTC),
            payload={"status": "succeeded", "result": {"result": {"value": 42}}},
        ),
    )
    case = AOHTTPCase(temporal_case, session_app, signer, message)
    monkeypatch.setattr(router, "get_ao_runtime", lambda: temporal_case.runtime)
    settings = get_settings()
    monkeypatch.setattr(settings, "s2s_tls_enabled", True)
    monkeypatch.setattr(settings, "s2s_tls_cn_allowlist", ["execution-plane.ao.svc"])
    previous_stack = session_app.middleware_stack
    session_app.middleware_stack = None
    previous_overrides = dict(session_app.dependency_overrides)

    async def session_override() -> AsyncIterator[Any]:
        async with temporal_case.sessions() as session:
            if case.commit_failure:

                def fail_commit(_session) -> None:
                    msg = "test commit failure"
                    raise RuntimeError(msg)

                event.listen(session.sync_session, "before_commit", fail_commit)
            yield session

    session_app.dependency_overrides[get_db] = session_override
    try:
        yield case
    finally:
        session_app.dependency_overrides.clear()
        session_app.dependency_overrides.update(previous_overrides)
        session_app.middleware_stack = previous_stack


@pytest_asyncio.fixture
async def bilateral_case(test_db_session_factory, test_db_session, test_db_admin_url, test_db_engine, monkeypatch):
    from .bilateral import BilateralCase

    case = BilateralCase(
        lambda temporal=None: TemporalCase(test_db_session_factory, temporal),
        test_db_admin_url.set(database=test_db_engine.url.database).render_as_string(hide_password=False),
        monkeypatch,
    )
    try:
        yield case
    finally:
        await case.close()
