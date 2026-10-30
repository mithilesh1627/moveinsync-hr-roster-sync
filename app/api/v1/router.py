"""API v1 Router Aggregator"""

from fastapi import APIRouter
from app.core.config import settings
from app.api.v1.auth import router as auth_router
from app.api.v1.roster import router as roster_router
from app.api.v1.transport import router as transport_router
from app.api.v1.employees import router as employees_router
from app.api.v1.reconciliation import router as recon_router
from app.api.v1.outbox import router as outbox_router
from app.api.v1.health import router as health_router
from app.api.v1.metrics import router as metrics_router
from app.api.v1.mock_downstream import router as mock_downstream_router

api_v1_router = APIRouter(prefix="/api/v1")

api_v1_router.include_router(auth_router)
api_v1_router.include_router(roster_router)
api_v1_router.include_router(transport_router)
api_v1_router.include_router(employees_router)
api_v1_router.include_router(recon_router)
api_v1_router.include_router(outbox_router)

# Only expose mock downstream simulator outside production
if settings.app_env != "production":
    api_v1_router.include_router(mock_downstream_router)
