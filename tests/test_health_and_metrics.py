"""Tests for Observability, Health Checks & Prometheus Metrics (REQ-G2, REQ-G3)"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_liveness_probe_returns_200(client: AsyncClient):
    """Test /healthz endpoint."""
    res = await client.get("/healthz")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_readiness_probe_returns_200_when_database_is_up(client: AsyncClient):
    """Test /readyz endpoint."""
    res = await client.get("/readyz")
    assert res.status_code == 200
    assert res.json()["status"] == "ready"
    assert res.json()["database"] == "connected"


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_3am_operational_counters(
    client: AsyncClient, hr_headers: dict
):
    """Test /metrics endpoint renders Prometheus metrics with required operational counters."""
    # Generate some traffic first
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": "EMP-METRIC-01",
                    "source_version": 1,
                    "first_name": "Metric",
                    "last_name": "Test",
                    "email": "metric.test@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    res = await client.get("/metrics")
    assert res.status_code == 200
    text_content = res.text

    # Verify presence of essential 3 AM operational counters
    assert "roster_events_total" in text_content
    assert "roster_reconciliation_duration_seconds" in text_content
    assert "roster_safety_brake_triggers_total" in text_content
    assert "roster_outbox_queue_depth" in text_content
    assert "roster_outbox_publish_retries_total" in text_content
    assert "roster_outbox_dlq_total" in text_content
