"""AO composes the exchange without importing the execution service."""

import importlib
import sys
from unittest.mock import Mock

import pytest
from fastapi import HTTPException, Request
from sqlmodel import SQLModel


def test_journal_metadata_is_private_and_in_default_schema():
    before = set(SQLModel.metadata.tables)
    from syntara.execution_plane.runtime import journal_schema

    schema = journal_schema()
    assert schema.table.schema is None
    assert set(SQLModel.metadata.tables) == before
    assert "exchange_journal" not in SQLModel.metadata.tables
    assert not any(name == "execution_plane" or name.startswith("execution_plane.") for name in sys.modules)


def test_factory_uses_supplied_sessions_and_dependency(monkeypatch):
    runtime = importlib.import_module("syntara.execution_plane.runtime")
    settings = Mock(ep_command_peers_path=None)
    monkeypatch.setattr(runtime, "get_settings", lambda: settings)
    with pytest.raises(ValueError, match="APP_EP_COMMAND_PEERS_PATH"):
        runtime.build_ao_runtime(Mock())


@pytest.mark.asyncio
async def test_certificate_auth_requires_verified_middleware_identity():
    from syntara.execution_plane.runtime import build_certificate_auth

    auth = build_certificate_auth({"ep.service": "ep"})
    request = Request({"type": "http", "headers": [(b"x-client-cn", b"ep.service")]})
    with pytest.raises(HTTPException) as caught:
        await auth(request)
    assert caught.value.status_code == 403
    request.state.is_cert_authenticated = True
    request.state.cert_cn = "unknown.service"
    with pytest.raises(HTTPException):
        await auth(request)
    request.state.cert_cn = "ep.service"
    assert await auth(request) == "ep"


def test_runtime_keeps_injected_sessions_and_fastapi_dependency(monkeypatch):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from execution_plane_client.configuration import PeerTrust
    from execution_plane_client.security import PeerConfig, PeerGrant
    from pydantic import SecretStr

    from syntara.execution_plane import runtime

    private = ec.generate_private_key(ec.SECP256R1())
    public = (
        private.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    local = PeerConfig(
        peer_id="ao",
        issuer="ao",
        audience="ao",
        role="controller",
        command_url="https://ao.test/v1/commands",
        query_base_url="https://ao.test/v1",
        grants={"ao": PeerGrant(project_ids=None, permissions=frozenset({"commands.read"}))},
        verification_keys={"ao-key": public},
        permitted_networks=["127.0.0.0/8"],
    )
    settings = Mock(
        ep_command_peers_path="injected.json",
        ep_participant_id="ao",
        s2s_tls_enabled=True,
        s2s_tls_ca_cert_path="ca.pem",
        s2s_tls_cert_path="cert.pem",
        s2s_tls_key_path="key.pem",
        ep_journal_decryption_keys={},
        ep_journal_key_id="test",
        ep_request_timeout_seconds=10,
    )
    key_manager = Mock(key_id="ao-key")
    key_manager.get_private_key.return_value = private
    monkeypatch.setattr(runtime, "get_settings", lambda: settings)
    monkeypatch.setattr(runtime, "get_encryption_key", lambda: SecretStr("ab" * 32))
    monkeypatch.setattr(runtime, "get_key_manager", lambda: key_manager)
    monkeypatch.setattr(runtime, "load_peer_trust", lambda _: PeerTrust({"ao": local}, {"ao.service": "ao"}))
    transport_factory = Mock()
    monkeypatch.setattr(runtime, "HttpCommandTransport", transport_factory)
    sessions, dependency = Mock(), Mock()
    service = runtime.build_ao_runtime(sessions, session_dependency=dependency)
    assert service.sessions is sessions
    assert service.session_dependency is dependency
    sessions.assert_not_called()
    transport_factory.return_value.send.assert_not_called()
