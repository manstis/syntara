"""AO-owned dependencies for the shared durable command exchange."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from cryptography.hazmat.primitives import serialization
from execution_plane_client.configuration import ExchangeOptions, load_peer_trust
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.journal.schema import JournalSchema, build_journal_schema
from execution_plane_client.service import JournalServiceBuilder, MediatorService
from execution_plane_client.transport import TLSConfig
from fastapi import HTTPException, Request

from syntara.auth.services.token_service import get_key_manager
from syntara.core.config.base import get_encryption_key, get_settings
from syntara.core.lib.encryption import key_from_string

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from execution_plane_client.api import CertificateAuthenticator, SessionDependency
    from execution_plane_client.observers import MetricValue
    from sqlalchemy.ext.asyncio import AsyncSession
    from temporalio.client import Client


def journal_schema() -> JournalSchema:
    """Return private journal metadata in AO's default database schema."""
    return build_journal_schema()


def build_certificate_auth(common_names: Mapping[str, str]) -> CertificateAuthenticator:
    """Trust only identities verified by AO's existing TLS middleware."""

    async def authenticate(request: Request) -> str:
        name = getattr(request.state, "cert_cn", None)
        if (
            not getattr(request.state, "is_cert_authenticated", False)
            or not isinstance(name, str)
            or name not in common_names
        ):
            raise HTTPException(status_code=403, detail="Registered client certificate required")
        return common_names[name]

    return authenticate


class AOExchangeObserver:
    """Publish shared safe correlation, latency and backlog fields."""

    def record(self, event: str, fields: Mapping[str, MetricValue]) -> None:
        """Record protocol metadata without encrypted payloads or tokens."""
        structlog.get_logger("syntara.execution_plane.exchange").info(event, **dict(fields))


def build_ao_runtime(
    session_factory: Callable[[], AsyncSession],
    *,
    temporal_client: Client | None = None,
    session_dependency: SessionDependency | None = None,
) -> MediatorService:
    """Compose API ingress or worker handling with caller-owned database sessions."""
    # Read AO's peer identity, verification keys and exchange settings from application config.
    settings = get_settings()
    if not settings.ep_command_peers_path:
        msg = "APP_EP_COMMAND_PEERS_PATH is required for the command exchange"
        raise ValueError(msg)
    trust = load_peer_trust(settings.ep_command_peers_path)

    # AO uses its configured service-to-service certificates for mutually authenticated HTTP.
    ca, cert, key = settings.s2s_tls_ca_cert_path, settings.s2s_tls_cert_path, settings.s2s_tls_key_path
    if not settings.s2s_tls_enabled or not ca or not cert or not key:
        msg = "AO command transport requires verified S2S TLS"
        raise ValueError(msg)

    # Retain old journal keys for decryption and add the active key for new journal entries.
    keys = {kid: key_from_string(value) for kid, value in settings.ep_journal_decryption_keys.items()}
    keys[settings.ep_journal_key_id] = key_from_string(get_encryption_key().get_secret_value())

    # Serialize AO's current private signing key in the format expected by the shared transport.
    key_manager = get_key_manager()
    signing_private_key = (
        key_manager.get_private_key()
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    observer = AOExchangeObserver()

    # The shared builder validates AO's controller identity and composes its durable exchange service.
    builder = JournalServiceBuilder(
        sessions=session_factory,
        trust=trust,
        local_peer_id=settings.ep_participant_id,
        expected_role="controller",
        schema=journal_schema(),
        journal_key_id=settings.ep_journal_key_id,
        journal_keys=keys,
        signing_key_id=key_manager.key_id,
        signing_private_key=signing_private_key,
        tls=TLSConfig(ca, cert, key),
        certificate_auth=build_certificate_auth(trust.certificate_names),
        options=ExchangeOptions(request_timeout_seconds=settings.ep_request_timeout_seconds),
        observer=observer,
        default_peer_id=settings.ep_peer_id,
    )
    journal = builder.journal

    # Register AO callback handlers against this journal; they update Temporal and integration state.
    from syntara.execution_plane.handlers import register_handlers  # noqa: PLC0415 - avoid factory/adapter import cycle

    registry = CommandRegistry()
    register_handlers(registry, journal, temporal_client)

    # Expose the service with AO's database-session dependency for its HTTP query routes.
    return builder.build(registry, session_dependency=session_dependency)


_process_runtime: MediatorService | None = None


def get_ao_runtime() -> MediatorService:
    """Return this process's lazy exchange using the installed lifecycle owner."""
    global _process_runtime  # noqa: PLW0603 - one process-owned transport lifecycle
    from syntara.core.database.session import AsyncSessionLocal  # noqa: PLC0415 - lazy database wiring

    if _process_runtime is None:
        _process_runtime = build_ao_runtime(AsyncSessionLocal)
    return _process_runtime


async def close_ao_runtime() -> None:
    """Close the lazy participant transport, including after failed startup."""
    global _process_runtime  # noqa: PLW0603 - clear closed process-owned resources
    if _process_runtime is not None:
        await _process_runtime.close()
        _process_runtime = None


async def run_ao_mediator(
    temporal_client: Client | None,
    session_factory: Callable[[], AsyncSession],
    *,
    service: MediatorService | None = None,
) -> None:
    """Supervise one shared worker group with injected Temporal and database clients."""
    global _process_runtime  # noqa: PLW0603 - activities and workers share this process runtime
    service = service or build_ao_runtime(session_factory, temporal_client=temporal_client)
    await close_ao_runtime()
    _process_runtime = service
    try:
        await service.run()
    finally:
        await service.close()
        if _process_runtime is service:
            _process_runtime = None
