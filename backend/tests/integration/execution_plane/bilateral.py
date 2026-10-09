"""Control an installed EP image without importing its runtime into AO."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Never
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from execution_plane_client.commands import CommandIdentity
from execution_plane_client.commands.clusters import ClusterProvision
from execution_plane_client.errors import RetryableDelivery
from execution_plane_client.protocol import CapabilitiesRead, ChildPage, CommandRead, CommandScope, MessageEnvelope
from execution_plane_client.security import AssertionSigner, AuthenticatedIdentity
from sqlalchemy import select, update

from syntara.execution_plane.handlers import TemporalCompletionHandler


class BilateralCase:
    """Restart real participants while retaining their committed responsibility."""

    def __init__(self, factory, database_url, monkeypatch) -> None:
        """Compose an AO test runtime and a separately installed EP process."""
        self.factory, self.database_url, self.monkeypatch = factory, database_url, monkeypatch
        self.case = factory()
        self.schema = "bilateral_" + uuid4().hex
        self.process: asyncio.subprocess.Process | None = None
        self.sent = 0
        self.provider_execute_count = 0
        self.rpc_lock = asyncio.Lock()
        self.failure = None
        self.project_id = self.case.project_id
        self.cluster_key = uuid4()
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.public = (
            self.key.public_key()
            .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
            .decode()
        )
        self.private = self.key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ).decode()
        self.finish = TemporalCompletionHandler.finish
        self.configure()

    def configure(self) -> None:
        self.case.project_id = self.project_id
        self.case.scope = CommandScope(client_id="syntara-orchestration", project_id=self.project_id)
        self.peers = {
            name: peer.model_copy(update={"verification_keys": {"test": self.public}})
            for name, peer in self.case.peers.items()
        }
        self.signer = AssertionSigner(
            local_id="syntara-orchestration",
            issuer="syntara-orchestration",
            key_id="test",
            private_key=self.private,
            peers=self.peers,
        )
        self.case.transport.send.side_effect = self.send
        self.case.transport.get_capabilities.side_effect = self.capabilities
        self.case.transport.get_command = self.query
        self.case.transport.get_children = self.children

    async def start(self):
        image = os.environ.get("EP_BILATERAL_IMAGE")
        if not image:
            pytest.skip("EP_BILATERAL_IMAGE must name the separately built EP image")
        driver = Path(
            os.environ.get("EP_BILATERAL_DRIVER", "../../syntara-execution-plane/tests/fixtures/bilateral_peer.py")
        ).resolve()
        assert driver.is_file(), driver
        executable = shutil.which("podman")
        assert executable, "podman is required for the installed EP peer"
        self.container_name = "codex-ep-bilateral-" + uuid4().hex
        self.process = await asyncio.create_subprocess_exec(
            executable,
            "run",
            "--rm",
            "--network=host",
            "-i",
            "--log-driver=none",
            "--name",
            self.container_name,
            "--volume",
            f"{driver}:/test-peer.py:ro,z",
            "--entrypoint",
            "/opt/app-root/src/.venv/bin/python",
            image,
            "/test-peer.py",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await self.rpc(
            "initialize",
            schema=self.schema,
            database_url=self.database_url,
            peers={name: peer.model_dump(mode="json") for name, peer in self.peers.items()},
        )

    async def rpc(self, operation, **values: object):
        async with self.rpc_lock:
            return await self.request(operation, **values)

    async def request(self, operation, **values: object):
        assert self.process
        assert self.process.stdin
        assert self.process.stdout
        assert self.process.stderr
        self.process.stdin.write((json.dumps({"operation": operation, **values}) + "\n").encode())
        await self.process.stdin.drain()
        while True:
            line = await asyncio.wait_for(self.process.stdout.readline(), 30)
            if not line:
                error = await self.process.stderr.read()
                pytest.fail(f"EP test process ended: {error.decode()[-4000:]}")
            if not line.startswith(b"@@bilateral@@"):
                continue
            response = json.loads(line.removeprefix(b"@@bilateral@@"))
            if "result" in response:
                return response["result"]
            if "delivery" in response:
                message = MessageEnvelope.model_validate(response["delivery"])
                identity = AuthenticatedIdentity(
                    "execution-plane",
                    message.scope,
                    self.peers["execution-plane"].grants[message.scope.client_id].permissions,
                    "executor",
                )
                async with self.case.sessions() as session, session.begin():
                    await self.case.runtime.ingress.receive(message, identity, session)
                reply = {"received": True}
            else:
                assert "capabilities" in response
                reply = self.case.runtime.capabilities.advertise().model_dump(mode="json")
            self.process.stdin.write((json.dumps(reply) + "\n").encode())
            await self.process.stdin.drain()

    async def send(self, message):
        self.sent += 1
        response = await self.rpc(
            "receive", message=message.model_dump(mode="json"), assertion=self.signer.sign(message)
        )
        assert response["status"] == 202, response
        if self.failure == "after_receiver_commit":
            raise RetryableDelivery

    async def capabilities(self, *_args: object):
        return CapabilitiesRead.model_validate(await self.rpc("capabilities"))

    async def query(self, peer_id, scope, command_id):
        return CommandRead.model_validate(
            await self.rpc("query", scope=scope.model_dump(mode="json"), id=str(command_id))
        )

    async def children(self, peer_id, scope, command_id, after=0):
        return ChildPage.model_validate(
            await self.rpc("query", scope=scope.model_dump(mode="json"), id=str(command_id), children=True, after=after)
        )

    async def provision(self):
        scope = CommandScope(client_id="syntara-orchestration", project_id=None, all_projects=True)
        command_id = uuid4()
        payload = ClusterProvision(
            resource_key=self.cluster_key,
            desired_revision=1,
            name="bilateral-cluster",
            endpoint="https://cluster.test",
            credential="private-test-token",
            default_target={
                "name": "default",
                "backend_type": "vanilla_k8s",
                "endpoint": "https://cluster.test",
                "placement": {"type": "kubernetes", "namespace": "nodes"},
            },
        )
        async with self.case.sessions() as session, session.begin():
            await self.case.runtime.client.cluster_provision(
                session,
                identity=CommandIdentity(command_id=command_id, message_id=uuid4(), occurred_at=datetime.now(UTC)),
                route=self.case.runtime.client.route(scope, peer_id="execution-plane"),
                payload=payload,
            )
        await self.case.runtime.delivery.run_once()
        await self.rpc("handle")
        await self.rpc("deliver")
        await self.case.runtime.handling.run_once()
        await self.case.runtime.delivery.run_once()
        state = await self.rpc("snapshot")
        assert (state["clusters"], state["targets"]) == (1, 1)
        self.sent = 0

    async def execute_with_failure(self, failure_point):
        await self.start()
        await self.provision()
        self.failure = failure_point
        if failure_point == "before_sender_commit":
            async with self.case.sessions() as session:
                await self.case.enqueue(session)
                await session.rollback()
            await self.case.runtime.delivery.run_once()
            assert self.sent == 0
            return
        await self.case.submit()
        await self.case.runtime.delivery.run_once()
        if failure_point == "after_receiver_commit":
            return
        await self.rpc("deliver")
        if failure_point == "after_acceptance_commit":
            return
        if failure_point == "after_handler_claim":
            assert (await self.rpc("claim"))["claims"] == 1
            return
        await self.rpc("handle")
        assert (await self.rpc("execute"))["provider_calls"] == 1
        if failure_point == "after_result_commit":
            return
        await self.rpc("deliver")
        if failure_point == "after_completion_receipt":
            return
        assert failure_point == "after_temporal_rpc_before_finish"

        async def crash(*_args: object, **_kwargs: object) -> Never:
            raise asyncio.CancelledError

        self.monkeypatch.setattr(TemporalCompletionHandler, "finish", crash)
        with pytest.raises(asyncio.CancelledError):
            await self.case.runtime.handling.run_once()
        assert len(self.case.temporal.calls) == 1

    async def stop(self):
        if self.process and self.process.returncode is None:
            killer = await asyncio.create_subprocess_exec(
                shutil.which("podman"),
                "kill",
                self.container_name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await killer.wait()
            await asyncio.wait_for(self.process.communicate(), 20)
        await self.case.runtime.close()

    async def restart_and_reconcile(self):
        await self.stop()
        temporal = self.case.temporal
        self.case = self.factory(temporal)
        self.configure()
        self.monkeypatch.setattr(TemporalCompletionHandler, "finish", self.finish)
        await self.start()
        await self.rpc("expire")
        past = datetime.now(UTC) - timedelta(seconds=1)
        async with self.case.sessions() as session, session.begin():
            await session.execute(
                update(self.case.journal.schema.table).values(
                    delivery_due_at=past,
                    acceptance_due_at=past,
                    handling_due_at=past,
                    delivery_lease_until=past,
                    handling_lease_until=past,
                    reconciliation_due_at=past,
                )
            )
        failure, self.failure = self.failure, None
        for _ in range(3):
            await self.case.runtime.delivery.run_once()
            await self.rpc("handle")
            await self.rpc("execute")
            await self.rpc("deliver")
            await self.case.runtime.handling.run_once()
            await self.case.runtime.delivery.run_once()
        self.failure = failure

    async def assert_durable_or_explicitly_unknown(self):
        snapshot = await self.rpc("snapshot")
        self.provider_execute_count = snapshot["provider_calls"]
        async with self.case.sessions() as session:
            entry = await self.case.journal.get(session, self.case.scope, self.case.command_id, "outbound")
            rows = (await session.execute(select(self.case.journal.schema.table))).mappings().all()
            assert "echo hello" not in str(rows)
            assert "token-1" not in str(rows)
        if self.failure == "before_sender_commit":
            assert entry is None
            assert snapshot["work"] == self.provider_execute_count == 0
            assert not self.case.temporal.calls
            return
        assert entry.delivery_state == "acknowledged"
        assert self.provider_execute_count == snapshot["work"] == 1
        completions = [row for row in snapshot["journal"] if row["command_type"] == "work.execute.completed"]
        assert len(completions) == 1
        assert completions[0]["delivery_state"] == "acknowledged"
        entry = await self.case.effect_entry()
        if self.failure == "after_temporal_rpc_before_finish":
            assert entry.handling_state == "reconciling"
            inbound = [row for row in rows if row["command_type"] == "work.execute.completed"]
            assert len(inbound) == 1
            assert inbound[0]["command_status"] == "outcome_unknown"
        else:
            assert entry.handling_state == "handled"
        assert len(self.case.temporal.calls) == 1

    async def close(self):
        if self.process and self.process.returncode is None:
            with contextlib.suppress(Exception):
                await self.rpc("cleanup")
        await self.stop()
