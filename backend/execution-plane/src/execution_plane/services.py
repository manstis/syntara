"""Execution Plane service layer — read-only DB access behind the router."""

from __future__ import annotations

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem


class ExecutionTargetRegistry:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list(self, limit: int) -> list[ExecutionTarget]:
        result = await self.db.exec(select(ExecutionTarget).limit(limit))
        return list(result.all())


class WorkItemRegistry:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def list(self, limit: int) -> list[WorkItem]:
        result = await self.db.exec(select(WorkItem).limit(limit))
        return list(result.all())
