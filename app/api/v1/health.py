"""Health and Readiness Probes Router"""

from fastapi import APIRouter, status, Response
from sqlalchemy import text
from app.core.database import async_session_factory

router = APIRouter(tags=["Health"])


@router.get("/healthz", status_code=status.HTTP_200_OK)
async def liveness_probe():
    """Liveness probe returning HTTP 200 if server process is running."""
    return {"status": "healthy", "service": "moveinsync-hr-roster-sync"}


@router.get("/readyz", status_code=status.HTTP_200_OK)
async def readiness_probe(response: Response):
    """Readiness probe validating database connectivity and pool health."""
    try:
        async with async_session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ready", "database": "connected"}
    except Exception as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unready", "database": f"error: {str(exc)}"}
