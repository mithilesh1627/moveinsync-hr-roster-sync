"""Prometheus Metrics Endpoint Router"""

from fastapi import APIRouter, Response
from app.core.metrics import get_prometheus_metrics, CONTENT_TYPE_LATEST

router = APIRouter(tags=["Monitoring"])


@router.get("/metrics")
async def prometheus_metrics():
    """Expose application metrics in standard Prometheus exposition format."""
    data = get_prometheus_metrics()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
