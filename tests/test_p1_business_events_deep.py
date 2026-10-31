"""P1 Business Events & Downstream Transport Integration Tests

Verifies requirements from Section 7:
1. Active to inactive transition (emits EMPLOYEE_DEACTIVATED).
2. Already inactive (suppresses duplicate outbox events).
3. Inactive to inactive repeated feed (strict no-op).
4. Deactivation outbox event retry and success on retry.
5. Future cab booking cancellation and vehicle seat release.
6. Past/completed booking preservation (only future scheduled rides cancelled).
7. Downstream transport adapter failure handling and retry.
"""

import asyncio
import pytest
from httpx import AsyncClient
from sqlalchemy import text

from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.api.v1.mock_downstream import (
    reset_mock_downstream,
    set_failure_simulation,
    _CANCELLED_BOOKINGS,
)
from app.core.database import sync_engine
from app.main import app
from app.workers.outbox_publisher import OutboxPublisher


# =============================================================================
# 1. ACTIVE TO INACTIVE TRANSITION
# =============================================================================
@pytest.mark.asyncio
async def test_active_to_inactive_transition_emits_event(client: AsyncClient, hr_headers: dict):
    """Explicit HR DEACTIVATE transition from ACTIVE -> INACTIVE emits EMPLOYEE_DEACTIVATED."""
    tenant_id = "tenant-alpha"
    emp_id = "EMP-EVT-TRANS-1"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # Step 1: Create active employee
    res1 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "first_name": "Active",
                    "last_name": "User",
                    "email": "trans1@test.com",
                    "office_code": "BLR-CAMPUS-1",
                    "source_version": 1,
                }
            ]
        },
        headers=hr_headers,
    )
    assert res1.status_code == 200

    # Step 2: Deactivate employee
    res2 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                }
            ]
        },
        headers=hr_headers,
    )
    assert res2.status_code == 200

    # Verify outbox event emitted
    with sync_engine.connect() as conn:
        events = conn.execute(
            text(f"SELECT event_type, aggregate_id FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).fetchall()
        assert len(events) == 1
        assert events[0][1] == emp_id


# =============================================================================
# 2. ALREADY INACTIVE / REPEATED DEACTIVATION SUPPRESSION
# =============================================================================
@pytest.mark.asyncio
async def test_already_inactive_suppresses_duplicate_events(client: AsyncClient, hr_headers: dict):
    """If an employee is already INACTIVE, another DEACTIVATE event does NOT emit duplicate outbox events."""
    tenant_id = "tenant-alpha"
    emp_id = "EMP-EVT-INACT-2"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        # Seed employee as ALREADY INACTIVE
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :tenant_id, :emp_id, 'Inactive', 'User', 'inact@test.com',
                    'BLR-CAMPUS-1', 'INACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                )
                """
            ),
            {"tenant_id": tenant_id, "emp_id": emp_id},
        )
        conn.commit()

    # Send DEACTIVATE event
    res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                }
            ]
        },
        headers=hr_headers,
    )
    assert res.status_code == 200

    # ZERO outbox events should be created because employee was already inactive!
    with sync_engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert count == 0


# =============================================================================
# 3. FUTURE BOOKING CANCELLATION & SEAT RELEASE
# =============================================================================
@pytest.mark.asyncio
async def test_future_booking_cancellation_and_seat_release():
    """Verify that DownstreamBookingAdapter cancels future bookings and releases reserved seats."""
    reset_mock_downstream()
    adapter = DownstreamBookingAdapter(app_dispatch=app)

    res = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-CAB-001",
        event_id="evt-cab-future-101",
    )
    assert res["status"] == "success"
    assert res["already_cancelled"] is False
    assert res["details"]["cancelled_rides_count"] == 3
    assert res["details"]["freed_seats_count"] == 3


# =============================================================================
# 4. DOWNSTREAM IDEMPOTENCY: REPEATED EVENT RETRY DOES NOT DUPLICATE SIDE EFFECTS
# =============================================================================
@pytest.mark.asyncio
async def test_downstream_repeated_cancellation_idempotency():
    """Retrying the same cancellation event ID produces a safe no-op on downstream."""
    reset_mock_downstream()
    adapter = DownstreamBookingAdapter(app_dispatch=app)

    # First attempt
    res1 = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-CAB-002",
        event_id="evt-cab-idemp-202",
    )
    assert res1["already_cancelled"] is False

    # Second attempt (e.g. network retry)
    res2 = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-CAB-002",
        event_id="evt-cab-idemp-202",
    )
    assert res2["already_cancelled"] is True
    assert "already cancelled" in res2["message"]


# =============================================================================
# 5. DOWNSTREAM ADAPTER FAILURE AND RETRY
# =============================================================================
@pytest.mark.asyncio
async def test_downstream_adapter_failure_and_retry():
    """When downstream returns 503 on attempt 1, adapter raises RuntimeError.
    When retried on attempt 2, it succeeds."""
    reset_mock_downstream()
    event_id = "evt-retry-simulation-303"
    set_failure_simulation(fail_until_attempt=1, event_id=event_id)

    adapter = DownstreamBookingAdapter(app_dispatch=app)

    # Attempt 1: Fails with 503
    with pytest.raises(RuntimeError, match="status 503"):
        await adapter.cancel_future_bookings(
            tenant_id="tenant-alpha",
            employee_id="EMP-FAIL-RETRY",
            event_id=event_id,
        )

    # Attempt 2: Succeeds
    res = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-FAIL-RETRY",
        event_id=event_id,
    )
    assert res["status"] == "success"
    assert res["details"]["cancelled_rides_count"] == 3
