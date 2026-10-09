"""Mount shared machine command ingress behind AO's normal request middleware."""

from execution_plane_client.api import create_router
from execution_plane_client.service import MediatorService
from fastapi import HTTPException

from syntara.core.database.session import get_db
from syntara.core.syntara_router import NO_PERMISSION, SyntaraRouter
from syntara.execution_plane.runtime import get_ao_runtime


def _current_runtime() -> MediatorService:
    try:
        return get_ao_runtime()
    except ValueError:
        raise HTTPException(status_code=503, detail="Command exchange is not configured") from None


def create_command_router(runtime: MediatorService | None = None) -> SyntaraRouter:
    """Require certificate plus signed assertions while preserving get_db overrides."""
    router = SyntaraRouter(
        prefix="/api/execution_plane/v1", tags=["Execution Plane Commands"], dependencies=[NO_PERMISSION]
    )
    create_router(runtime or _current_runtime, router=router, session_dependency=get_db)
    return router
