"""Versioned HTTP client for the standalone Execution Plane service."""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Self, cast
from uuid import UUID, uuid4

import httpx
import jwt
from execution_plane_client.commands.control import CancelCommand
from execution_plane_client.protocol import MessageEnvelope, TransportReceipt

if TYPE_CHECKING:
    from collections.abc import Mapping

from syntara.auth.services.token_service import get_key_manager
from syntara.core.config.base import get_settings
from syntara.core.tls.http_client import build_internal_http_client

EP_CLIENT_ID = "syntara-orchestration"
EP_AUDIENCE = "execution-plane"
EP_TOKEN_LIFETIME_SECONDS = 300
EP_SCOPES = "work.execute command.cancel cluster.register work-items:read execution-targets:read clusters:read"


class ExecutionPlaneError(RuntimeError):
    """Base exception for failed Execution Plane requests."""


class ExecutionPlaneUnavailableError(ExecutionPlaneError):
    """Raised when EP cannot accept or answer a request temporarily."""

    def __init__(self, message: str = "Execution Plane is unavailable") -> None:
        """Describe a retryable communication failure."""
        super().__init__(message)


class ExecutionPlaneRejectedError(ExecutionPlaneError):
    """Raised when EP rejects an invalid or unauthorized request."""

    def __init__(
        self, message: str = "Execution Plane rejected the request", *, status_code: int | None = None
    ) -> None:
        """Retain HTTP status so callers can recover existing domain resources."""
        super().__init__(message)
        self.status_code = status_code


def create_ep_service_token() -> str:
    """Create a short-lived, audience-bound AO service assertion."""
    settings = get_settings()
    key_manager = get_key_manager()
    now = datetime.now(UTC)
    claims: dict[str, Any] = {
        "sub": EP_CLIENT_ID,
        "client_id": EP_CLIENT_ID,
        "scope": EP_SCOPES,
        "iss": settings.jwt_issuer,
        "aud": EP_AUDIENCE,
        "iat": now,
        "exp": now + timedelta(seconds=EP_TOKEN_LIFETIME_SECONDS),
        "jti": str(uuid4()),
    }
    return jwt.encode(
        claims,
        key_manager.get_private_key(),
        algorithm="ES256",
        headers={"kid": key_manager.key_id},
    )


class ExecutionPlaneHttpClient:
    """HTTP transport for the Execution Plane service."""

    def __init__(self, *, timeout: float | None = None) -> None:
        """Initialize the client from AO's EP URL and mTLS settings."""
        settings = get_settings()
        if not settings.ep_api_url:
            msg = "Execution Plane is enabled for this activity but APP_EP_API_URL is not configured"
            raise ExecutionPlaneUnavailableError(msg)
        self._settings = settings
        self._client = build_internal_http_client(
            base_url=settings.ep_api_url.rstrip("/"),
            timeout=timeout or settings.ep_request_timeout_seconds,
        )

    async def __aenter__(self) -> Self:
        """Return this reusable HTTP client."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the HTTP connection pool."""
        await self._client.aclose()

    async def submit_command(self, message: MessageEnvelope) -> TransportReceipt:
        """Send a shared command and require its matching transport receipt."""
        response = await self._request("POST", "/v1/commands", json_body=message.model_dump(mode="json"))
        try:
            receipt = TransportReceipt.model_validate(response)
        except ValueError:
            msg = "Execution Plane returned an invalid command receipt"
            raise ExecutionPlaneRejectedError(msg) from None
        if receipt.message_id != message.message_id:
            msg = "Execution Plane returned a receipt for a different message"
            raise ExecutionPlaneRejectedError(msg)
        return receipt

    async def get_work_item(self, *, work_item_id: UUID) -> dict[str, Any]:
        """Read accepted work by its UUID."""
        response = await self._request("GET", f"/v1/work-items/{work_item_id}")
        return self._as_object(response)

    async def cancel_work_item(self, *, work_item_id: UUID) -> TransportReceipt:
        """Request cancellation through the shared command contract."""
        message = CancelCommand(target_command_id=work_item_id).to_message(
            source_id=EP_CLIENT_ID,
            destination_id=EP_AUDIENCE,
            client_id=EP_CLIENT_ID,
        )
        return await self.submit_command(message)

    async def list_work_items(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """List a bounded page of work items for this AO client."""
        response = await self._request("GET", "/v1/work-items", params={"limit": limit})
        return self._as_list(response)

    async def list_execution_targets(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """List a bounded page of safe target metadata."""
        response = await self._request("GET", "/v1/execution-targets", params={"limit": limit})
        return self._as_list(response)

    async def get_cluster_binding(self, *, source_integration_id: UUID) -> dict[str, Any]:
        """Read the safe observed state of an integration binding from EP."""
        response = await self._request("GET", f"/v1/clusters/{source_integration_id}")
        if response is None or isinstance(response, list):
            msg = "Execution Plane returned an invalid cluster-binding response"
            raise ExecutionPlaneRejectedError(msg)
        return response

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
        params: Mapping[str, str | int] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        token = create_ep_service_token()
        try:
            response = await self._client.request(
                method,
                path,
                headers={"Authorization": f"Bearer {token}"},
                json=json_body,
                params=params,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if (
                exc.response.status_code == HTTPStatus.TOO_MANY_REQUESTS
                or exc.response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
            ):
                raise ExecutionPlaneUnavailableError from exc
            detail = "Execution Plane rejected the request"
            with suppress(ValueError, AttributeError):
                error_body = exc.response.json()
                if isinstance(error_body, dict):
                    detail = str(error_body.get("detail", detail))
            raise ExecutionPlaneRejectedError(detail, status_code=exc.response.status_code) from exc
        except httpx.RequestError as exc:
            raise ExecutionPlaneUnavailableError from exc
        if response.status_code == HTTPStatus.NO_CONTENT:
            return None
        body = response.json()
        if isinstance(body, list):
            return cast("list[dict[str, Any]]", body)
        return cast("dict[str, Any]", body)

    @staticmethod
    def _as_object(response: dict[str, Any] | list[dict[str, Any]] | None) -> dict[str, Any]:
        """Require an object response for single-resource endpoints."""
        if response is None or isinstance(response, list):
            msg = "Execution Plane returned an invalid object response"
            raise ExecutionPlaneRejectedError(msg)
        return response

    @staticmethod
    def _as_list(response: dict[str, Any] | list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        """Require an array response for list endpoints."""
        if not isinstance(response, list):
            msg = "Execution Plane returned an invalid list response"
            raise ExecutionPlaneRejectedError(msg)
        return response
