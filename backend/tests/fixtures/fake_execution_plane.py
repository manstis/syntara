"""Test-only EP participant: real journals and workers, local script execution."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
from typing import TYPE_CHECKING, Any, Never, Self, cast
from uuid import uuid4, uuid5

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.configuration import ExchangeOptions
from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.handlers import CommandRegistry, EffectResult, HandlingResult, PreparedEffect
from execution_plane_client.journal.schema import build_journal_schema
from execution_plane_client.journal.store import ExchangeJournalStore
from execution_plane_client.protocol import CommandStatus, MessageEnvelope, ResourceReference
from execution_plane_client.security import AssertionVerifier, AuthenticatedIdentity, PeerConfig, PeerGrant
from execution_plane_client.service import MediatorService, build_service
from execution_plane_client.transport import TransportReceipt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from syntara.execution_plane.handlers import register_handlers

if TYPE_CHECKING:
    from execution_plane_client.protocol import CapabilitiesRead, ChildPage, CommandRead
    from execution_plane_client.transport import HttpCommandTransport


class _PeerTransport:
    """Inject only the network boundary; receipts still commit real journal rows."""

    def __init__(self, services: dict[str, MediatorService]) -> None:
        self.services = services

    async def send(self, message) -> TransportReceipt:
        remote = self.services[message.destination_id]
        identity = AuthenticatedIdentity(
            message.source_id,
            message.scope,
            frozenset({message.command.command_type}),
            "controller" if message.source_id == "syntara-orchestration" else "executor",
        )
        async with remote.sessions() as session, session.begin():
            await remote.ingress.receive(message, identity, session)
        return TransportReceipt(received=True, message_id=message.message_id)

    async def get_capabilities(self, peer_id, scope=None) -> CapabilitiesRead:
        return self.services[peer_id].capabilities.advertise()

    async def get_command(self, peer_id, scope, command_id) -> CommandRead:
        remote = self.services[peer_id]
        async with remote.sessions() as session:
            return await remote.queries.get(session, scope, command_id)

    async def get_children(self, peer_id, scope, command_id, after=0) -> ChildPage:
        remote = self.services[peer_id]
        async with remote.sessions() as session:
            return await remote.queries.children(
                session,
                scope,
                command_id,
                after,
                1,
                peer_id="syntara-orchestration" if peer_id == "execution-plane" else "execution-plane",
                include_recovery=True,
            )

    async def aclose(self) -> None:
        pass


class _ScriptHandler:
    """Keep subprocess I/O outside the transaction, using the real effect boundary."""

    async def prepare(self, message, context, session) -> PreparedEffect:
        entry = await context.journal.get(session, message.scope, message.command_id, "inbound")
        return PreparedEffect(entry.entry_id, message.command_id, 1, {"message": message.model_dump(mode="json")})

    async def execute(self, effect) -> EffectResult:
        message = MessageEnvelope.model_validate(effect.context["message"])
        result, status = await execute_fixture_script(message.command.payload)
        return EffectResult("succeeded", {"status": status, "result": result})

    async def finish(self, effect, result, context, session) -> HandlingResult:
        return HandlingResult(
            status=CommandStatus.SUCCEEDED if result.result["status"] == "completed" else CommandStatus.FAILED,
            resource=ResourceReference(kind="work_item", resource_id=uuid5(effect.command_id, "work")),
            result={"result": result.result["result"]},
            completion_required=True,
        )

    async def reconcile(self, effect) -> EffectResult:
        return EffectResult("unknown", error_code="test_peer_restarted")


class _CancelHandler:
    async def handle(self, message, context, session) -> HandlingResult:
        return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)


class FakeExecutionPlanePeer:
    """Pair AO's actual handlers with an independent durable executor journal."""

    schema: str
    engine: AsyncEngine
    ao: MediatorService
    ep: MediatorService

    @classmethod
    async def create(cls, ao_sessions, temporal_client, database_url) -> Self:
        self = cls()
        self.schema = "test_ep_" + uuid4().hex
        self.engine = create_async_engine(database_url, poolclass=NullPool)
        ep_schema = build_journal_schema(self.schema)
        async with self.engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{self.schema}"'))
            await connection.run_sync(ep_schema.metadata.create_all)
        ep_sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        catalog = CommandCatalog()
        ep_registry, ao_registry = CommandRegistry(), CommandRegistry()
        ep_registry.register(catalog.definition("work.execute", 1), _ScriptHandler())
        ep_registry.register(catalog.definition("command.cancel", 1), _CancelHandler())
        cipher = AESGCMCipher(active_key_id="fixture", keys={"fixture": b"f" * 32})
        ao_journal = ExchangeJournalStore(build_journal_schema(), cipher)
        ep_journal = ExchangeJournalStore(ep_schema, cipher)
        register_handlers(ao_registry, ao_journal, temporal_client)
        permissions = frozenset(
            {
                "work.execute",
                "work.execute.completed",
                "command.cancel",
                "command.cancel.completed",
                "command.accepted",
                "commands.read",
            }
        )
        peers = {
            name: PeerConfig(
                peer_id=name,
                issuer=name,
                audience=name,
                role=role,
                command_url=f"https://{host}/v1/commands",
                query_base_url=f"https://{host}/v1",
                grants={"syntara-orchestration": PeerGrant(project_ids=None, permissions=permissions)},
                verification_keys={"fixture": "injected-transport"},
                permitted_networks=["127.0.0.0/8"],
            )
            for name, role, host in [
                ("syntara-orchestration", "controller", "ao.test"),
                ("execution-plane", "executor", "ep.test"),
            ]
        }
        services: dict[str, MediatorService] = {}
        transport = _PeerTransport(services)
        for name, sessions, journal, registry in [
            ("syntara-orchestration", ao_sessions, ao_journal, ao_registry),
            ("execution-plane", ep_sessions, ep_journal, ep_registry),
        ]:
            peer = peers[name]
            services[name] = build_service(
                store=journal,
                sessions=sessions,
                local_peer=peer,
                peers=peers,
                registry=registry,
                transport=cast("HttpCommandTransport", transport),
                verifier=AssertionVerifier(local_id=name, audience=name, command_url=peer.command_url, peers=peers),
                certificate_auth=self._no_http,
                options=ExchangeOptions(poll_seconds=0.02, reconciliation_seconds=0.1),
            )
        self.ao, self.ep = services["syntara-orchestration"], services["execution-plane"]
        return self

    @staticmethod
    async def _no_http(_request) -> Never:
        msg = "HTTP must use the independently tested authenticated ingress"
        raise AssertionError(msg)

    async def pump(self) -> None:
        """Move each durable boundary forward once without synchronous submit shortcuts."""
        await self.ao.delivery.run_once()
        await self.ep.delivery.run_once()
        await self.ep.handling.run_once()
        await self.ep.delivery.run_once()
        await self.ao.handling.run_once()
        await self.ao.delivery.run_once()

    async def run(self) -> None:
        """Drive both services for real Temporal workflow fixtures."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self.ao.run())
            group.create_task(self.ep.run())

    async def close(self) -> None:
        """Close workers and remove only this fixture's independently owned schema."""
        await self.ao.close()
        await self.ep.close()
        async with self.engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        await self.engine.dispose()


async def execute_fixture_script(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Run a test script as a local stand-in for a separately deployed EP service."""
    if "invocation" in payload:
        invocation = payload["invocation"]
        input_config = invocation["inputs"]
    else:
        input_config = payload["input_config"]
    language = input_config.get("language", "python")
    code = input_config["code"]
    command = ["bash", "-c", code] if language == "bash" else [sys.executable, "-c", code]
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in {"HOME", "LANG", "LC_ALL", "LC_CTYPE", "PATH", "SHELL", "TEMP", "TERM", "TMPDIR", "TZ", "USER"}
    }
    environment.update({key: str(value) for key, value in (input_config.get("environment") or {}).items()})
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )
    timeout = max(1, int(input_config.get("_engine_timeout_seconds", 300)))
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        process.kill()
        await process.wait()
        return {"error": "Script execution timed out", "error_type": "TimeoutError"}, "failed"
    stdout = stdout_bytes[: 1024 * 1024].decode("utf-8", errors="replace")
    stderr = stderr_bytes[: 1024 * 1024].decode("utf-8", errors="replace")
    if process.returncode:
        return {
            "error": f"Script failed with exit code {process.returncode}",
            "error_type": "ScriptExecutionError",
            "exit_code": process.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }, "failed"

    stdout_json: Any = None
    if language == "python" and stdout.strip():
        try:
            stdout_json = json.loads(stdout)
        except json.JSONDecodeError:
            with contextlib.suppress(json.JSONDecodeError):
                stdout_json = json.loads([line for line in stdout.splitlines() if line.strip()][-1])
    output = {
        "return_code": process.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_json": stdout_json,
    }
    output_config = payload.get("output_config")
    if output_config is not None:
        output = {key: output[key] for key in output_config if key in output}
    return {"output": output}, "completed"
