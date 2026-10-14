"""The EP completion callback: route, view, and the logic that completes the activity.

EP POSTs a work.execute.completed command here when a work item finishes, and retries delivery
until AO returns 2xx. The view completes the activity addressed by the work item
UUID (= ``ActivityExecution.id``): it resolves that activity's
``(temporal_workflow_id, temporal_activity_id)`` and completes or fails it via the
same async-activity mechanism used for approval and agentic callbacks — no task
token or AO-side dispatch state. Re-delivery is idempotent: an already-resolved
activity is a Temporal no-op.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID  # noqa: TC003 — Pydantic resolves model annotations at runtime

import structlog
from execution_plane_client import CommandCatalog, CommandRegistry
from execution_plane_client.commands.control import Completion
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference, TransportReceipt
from fastapi import Depends, HTTPException, Request, status
from pydantic import ValidationError
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession  # noqa: TC002 — FastAPI resolves dependency annotations
from temporalio.exceptions import ApplicationError

from syntara.core.config.base import get_settings
from syntara.core.database.session import get_db
from syntara.core.syntara_router import NO_PERMISSION, SyntaraRouter
from syntara.execution_plane.client import EP_AUDIENCE, EP_CLIENT_ID
from syntara.workflows.executions_router import get_temporal_execution_service
from syntara.workflows.models.activity_execution import ActivityExecution
from syntara.workflows.models.execution import Execution
from syntara.workflows.workflow_engine.services.temporal_execution_service import (
    TemporalExecutionService,  # noqa: TC001 — FastAPI resolves dependency annotations
)

if TYPE_CHECKING:
    from execution_plane_client.commands.base import CommandPayload

logger = structlog.stdlib.get_logger(__name__)

router = SyntaraRouter(prefix="/api/execution_plane/v1", tags=["Execution Plane"])


def _failure_error(status: str, result: dict[str, Any]) -> ApplicationError:
    """Translate a non-completed EP status into a non-retryable activity failure."""
    if status == "cancelled":
        return ApplicationError(
            "Execution Plane work was cancelled before execution",
            result,
            type="ExecutionPlaneWorkCancelled",
            non_retryable=True,
        )
    if status == "reconciliation_required":
        return ApplicationError(
            "Execution Plane could not determine the work outcome",
            result,
            type="ExecutionPlaneReconciliationRequired",
            non_retryable=True,
        )
    error_message = str(result.get("error", "Execution Plane work failed"))
    error_type = str(result.get("error_type", "ScriptExecutionError"))
    return ApplicationError(error_message, result, type=error_type, non_retryable=True)


async def _complete_activity(
    *,
    session: AsyncSession,
    temporal_service: TemporalExecutionService,
    work_id: UUID,
    status: str,
    result: dict[str, Any],
) -> bool:
    """Complete or fail the async activity named by a work item UUID.

    Returns ``False`` if no activity matches ``work_id`` (the view maps that to a
    404 so EP retries); ``True`` once the completion has been delivered.
    """
    row = (
        await session.exec(
            select(Execution.temporal_workflow_id, ActivityExecution.temporal_activity_id)
            .join(Execution, col(ActivityExecution.execution_id) == col(Execution.id))
            .where(col(ActivityExecution.id) == work_id)
        )
    ).first()
    if row is None:
        return False
    temporal_workflow_id, temporal_activity_id = row

    if status == "completed":
        await temporal_service.complete_async_activity(
            temporal_workflow_id=temporal_workflow_id,
            activity_id=temporal_activity_id,
            result=result,
        )
    else:
        await temporal_service.fail_async_activity(
            temporal_workflow_id=temporal_workflow_id,
            activity_id=temporal_activity_id,
            error=_failure_error(status, result),
        )
    return True


@dataclass(frozen=True)
class CompletionContext:
    """AO services needed to deliver a completion to Temporal."""

    session: AsyncSession
    temporal_service: TemporalExecutionService


class CompletionCommandHandler:
    """Resolve an AO activity using the completed work command's UUID."""

    async def handle(
        self, message: MessageEnvelope, payload: CommandPayload, context: CompletionContext
    ) -> HandlingResult:
        """Apply the validated completion without retaining command history."""
        if not isinstance(payload, Completion):
            raise UnsupportedCommand
        work_id = message.command.parent_command_id
        if work_id is None or (
            payload.resource is not None
            and (payload.resource.kind != "work_item" or payload.resource.resource_id != work_id)
        ):
            raise ProtocolConflict
        statuses = {
            "succeeded": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "outcome_unknown": "reconciliation_required",
        }
        delivered = await _complete_activity(
            session=context.session,
            temporal_service=context.temporal_service,
            work_id=work_id,
            status=statuses[payload.status],
            result=payload.result,
        )
        if not delivered:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No matching AO activity for this work item"
            )
        logger.info(
            "Delivered Execution Plane completion command", command_id=str(message.command_id), status=payload.status
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=work_id))


_catalog = CommandCatalog()
_registry = CommandRegistry[CompletionContext]()
_registry.register(_catalog.definition("work.execute.completed", 1), CompletionCommandHandler())


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[NO_PERMISSION],
    operation_id="accept_execution_plane_event",
    summary="Accept an Execution Plane completion command",
    response_model=TransportReceipt,
)
async def accept_execution_plane_event(
    request: Request,
    message: MessageEnvelope,
    session: Annotated[AsyncSession, Depends(get_db)],
    temporal_service: Annotated[TemporalExecutionService | None, Depends(get_temporal_execution_service)],
) -> TransportReceipt:
    """Authenticate a completion command and acknowledge it after Temporal delivery."""
    settings = get_settings()
    if (
        not getattr(request.state, "is_cert_authenticated", False)
        or getattr(request.state, "cert_cn", None) != settings.ep_callback_service_cn
        or message.source_id != EP_AUDIENCE
        or message.destination_id != EP_CLIENT_ID
        or message.scope.client_id != EP_CLIENT_ID
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Execution Plane service identity required")
    if temporal_service is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Temporal is unavailable")
    try:
        payload = _catalog.validate(message)
        handler = _registry.get(message.command.command_type, message.command.command_revision)
        result = await handler.handle(message, payload, CompletionContext(session, temporal_service))
    except (ValidationError, UnsupportedCommand, ProtocolConflict):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Invalid completion command"
        ) from None
    return TransportReceipt(message_id=message.message_id, resource=result.resource)
