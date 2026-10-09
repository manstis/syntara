"""Execution Plane API endpoints."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Annotated, Literal
from uuid import UUID  # noqa: TC003 — Pydantic resolves model annotations at runtime

import structlog
from execution_plane_client.errors import PermanentRejection, RetryableDelivery, UnauthorizedPeer
from execution_plane_client.protocol import CommandScope
from execution_plane_client.resources import ExecutionTargetRead, WorkItemRead
from fastapi import Depends, HTTPException, Query, status
from sqlmodel import SQLModel

from syntara.authz.dependencies import ProjectScopeFilter
from syntara.authz.engine import AllowedProjectsResult  # noqa: TC001 — FastAPI resolves dependency annotations
from syntara.core.config.base import get_settings
from syntara.core.syntara_router import SyntaraRouter
from syntara.execution_plane.runtime import get_ao_runtime

if TYPE_CHECKING:
    from execution_plane_client import MediatorClient

logger = structlog.stdlib.get_logger(__name__)

router = SyntaraRouter(prefix="/api/execution_plane/v1", tags=["Execution Plane"])

_target_scope = ProjectScopeFilter("execution_target", "read")
_work_item_scope = ProjectScopeFilter("work_item", "read")


class ExecutionTargetListResponse(SQLModel):
    """Paginated list response for ExecutionTarget."""

    resources: list[ExecutionTargetFacadeRead]
    next: str | None = None
    prev: str | None = None
    total: int | None = None


class WorkItemListResponse(SQLModel):
    """Paginated list response for WorkItem."""

    resources: list[WorkItemFacadeRead]
    next: str | None = None
    prev: str | None = None
    total: int | None = None


class ExecutionTargetFacadeRead(ExecutionTargetRead):
    """Safe shared target fields after AO project authorization."""


class WorkItemFacadeRead(WorkItemRead):
    """Safe shared work state after AO project authorization."""


@router.get(
    "/execution_targets",
    operation_id="list_execution_targets",
    summary="List execution targets",
    description="Retrieve registered execution targets.",
)
async def list_execution_targets(
    allowed_projects: Annotated[AllowedProjectsResult, Depends(_target_scope)],
    limit: int = Query(default=20, ge=1, le=100),
) -> ExecutionTargetListResponse:
    """Read registered execution targets from EP after AO permission checks."""
    try:
        items = await _list_in_authorized_projects(
            get_ao_runtime().client,
            operation="execution_targets",
            allowed_projects=allowed_projects,
            limit=limit,
        )
    except (RetryableDelivery, PermanentRejection, UnauthorizedPeer, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Execution Plane unavailable",
        ) from exc
    logger.info("Listed execution targets", count=len(items))
    return ExecutionTargetListResponse(resources=[ExecutionTargetFacadeRead.model_validate(item) for item in items])


@router.get(
    "/work_items",
    operation_id="list_work_items",
    summary="List work items",
    description="Retrieve work items.",
)
async def list_work_items(
    allowed_projects: Annotated[AllowedProjectsResult, Depends(_work_item_scope)],
    limit: int = Query(default=20, ge=1, le=100),
) -> WorkItemListResponse:
    """Read work-item state from EP after AO permission checks."""
    try:
        items = await _list_in_authorized_projects(
            get_ao_runtime().client,
            operation="work_items",
            allowed_projects=allowed_projects,
            limit=limit,
        )
    except (RetryableDelivery, PermanentRejection, UnauthorizedPeer, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Execution Plane unavailable",
        ) from exc
    logger.info("Listed work items", count=len(items))
    return WorkItemListResponse(resources=[WorkItemFacadeRead.model_validate(item) for item in items])


async def _list_in_authorized_projects(
    client: MediatorClient,
    *,
    operation: Literal["execution_targets", "work_items"],
    allowed_projects: AllowedProjectsResult,
    limit: int,
) -> list[dict[str, object]]:
    """List EP resources only within AO's resolved project access scope."""
    if allowed_projects.all_projects:
        return await _run_list_operation(client, operation, all_projects=True, limit=limit)
    if not allowed_projects.project_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No project-scoped access to EP resources")
    pages = await asyncio.gather(
        *(
            _run_list_operation(client, operation, project_id=project_id, limit=limit)
            for project_id in allowed_projects.project_ids
        )
    )
    resources = [item for page in pages for item in page]
    resources.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
    return resources[:limit]


async def _run_list_operation(
    client: MediatorClient,
    operation: Literal["execution_targets", "work_items"],
    *,
    limit: int,
    project_id: UUID | None = None,
    all_projects: bool = False,
) -> list[dict[str, object]]:
    """Invoke one supported project-scoped list method on the shared transport."""
    scope = CommandScope(client_id=get_settings().ep_participant_id, project_id=project_id, all_projects=all_projects)
    if operation == "work_items":
        items = await client.list_work_items(scope, limit=limit, peer_id=get_settings().ep_peer_id)
    else:
        targets = await client.list_execution_targets(scope, limit=limit, peer_id=get_settings().ep_peer_id)
        return [item.model_dump(mode="json") for item in targets]
    return [item.model_dump(mode="json") for item in items]
