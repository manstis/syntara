"""Atomically enqueue script work and hand completion to AO journal workers."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import structlog
from temporalio import activity
from temporalio.exceptions import ApplicationError

from syntara.core.config.base import get_settings
from syntara.core.database.session import AsyncSessionLocal
from syntara.execution_plane.activity_context import TemporalIdentity
from syntara.execution_plane.commands.activities import enqueue_script, stable_command_id
from syntara.workflows.workflow_engine.activities.common import HEARTBEAT_STOP_MONITOR
from syntara.workflows.workflow_engine.constants import (
    DEFAULT_MAX_OUTPUT_BYTES,
    ENGINE_MAX_OUTPUT_BYTES_KEY,
    ENGINE_TIMEOUT_SECONDS_KEY,
)
from syntara.workflows.workflow_engine.models.workflow_definition import (
    ActivityName,
    ScriptExecutorParameters,
)

logger = structlog.stdlib.get_logger(__name__)

__all__ = ["activity", "asyncio", "execute_script_activity"]
_NODE_TYPE = "script"
_DEFAULT_TIMEOUT_SECONDS = 300


def _build_invocation(input_config: dict[str, Any], settings: Any, info: Any) -> dict[str, Any]:  # noqa: ANN401
    """Build the versioned SDK request before AO freezes it for activity retries."""
    return {
        "version": 1,
        "operation": "execute",
        "inputs": {key: value for key, value in input_config.items() if not key.startswith("_")},
        "credentials": {"resolved": {}},
        "workflow_context": {
            "workflow_id": info.workflow_id,
            "run_id": info.workflow_run_id,
            "activity_id": info.activity_id,
        },
        "settings": {
            "workflow_http_request_allowed_hosts": settings.workflow_http_request_allowed_hosts,
            "aap_poll_interval_seconds": settings.aap_poll_interval_seconds,
        },
        "timeout_seconds": int(
            input_config.get(
                ENGINE_TIMEOUT_SECONDS_KEY,
                input_config.get("timeout") or _DEFAULT_TIMEOUT_SECONDS,
            )
        ),
        "max_output_bytes": int(input_config.get(ENGINE_MAX_OUTPUT_BYTES_KEY, DEFAULT_MAX_OUTPUT_BYTES)),
    }


async def _dispatch_to_ep(
    input_config: dict[str, Any], output_config: dict[str, str] | None, project_id: uuid.UUID
) -> None:
    """Commit one frozen work command and private token context without network delivery."""
    settings, info = get_settings(), activity.info()
    if info.workflow_id is None or info.workflow_run_id is None:
        msg = "Temporal activity context is missing workflow or run identity"
        raise ApplicationError(msg, type="ContextError", non_retryable=True)

    def new_payload() -> dict[str, Any]:
        # Evaluate mutable settings only when no frozen command exists.
        image = settings.node_container_images.get(_NODE_TYPE)
        if not image:
            msg = "No Execution Plane image configured for script nodes"
            raise ApplicationError(msg, type="ConfigError", non_retryable=True)
        return {
            "invocation": _build_invocation(input_config, settings, info),
            "image": image,
            "output_config": output_config,
        }

    identity = TemporalIdentity(
        namespace=settings.temporal_namespace,
        workflow_id=info.workflow_id,
        run_id=info.workflow_run_id,
        activity_id=info.activity_id,
    )
    async with AsyncSessionLocal() as session, session.begin():
        await enqueue_script(
            session,
            command_id=stable_command_id(identity),
            project_id=project_id,
            temporal_identity=identity,
            task_token=info.task_token,
            activity_attempt=info.attempt,
            payload=new_payload,
        )


@activity.defn(name=ActivityName.SCRIPT)
async def execute_script_activity(
    input_config: dict[str, Any],
    output_config: dict[str, str] | None,
    project_id: str,
) -> dict[str, Any]:
    """Validate and submit a script, then await AO-owned callback completion."""
    activity.heartbeat({HEARTBEAT_STOP_MONITOR: True})

    if not get_settings().script_nodes_enabled:
        msg = "Script node execution is not enabled."
        raise ApplicationError(msg, type="ScriptNodeDisabled", non_retryable=True)

    try:
        ScriptExecutorParameters.model_validate(input_config)
        project_uuid = uuid.UUID(project_id)
    except Exception:  # noqa: BLE001
        msg = "Script activity configuration or project scope is invalid"
        raise ApplicationError(msg, type="ConfigError", non_retryable=True) from None

    await _dispatch_to_ep(input_config, output_config, project_uuid)
    activity.raise_complete_async()
