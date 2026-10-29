"""Tests for Downstream Deactivation Side-Effects & Downstream Idempotency (REQ-E5)"""

import pytest
from httpx import AsyncClient
from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.workers.outbox_publisher import OutboxPublisher
from app.main import app
from app.api.v1.mock_downstream import reset_mock_downstream, _CANCELLED_BOOKINGS


@pytest.mark.asyncio
async def test_employee_deactivation_cancels_cab_bookings_and_frees_seats(
    client: AsyncClient, hr_headers: dict
):
    """OFFICIAL CASE STUDY REQUIREMENT:
    'Deactivation Side Effects — an employee who leaves has their future cab bookings
    cancelled and their seat freed.'
    """
    reset_mock_downstream()
    emp_id = "EMP-DOWN-001"

    # 1. Create employee
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Karan",
                    "last_name": "Johar",
                    "email": "karan.j@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # 2. Deactivate employee
    deact_res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                    "first_name": "Karan",
                    "last_name": "Johar",
                    "email": "karan.j@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )
    assert deact_res.status_code == 200

    # 3. Publish outbox events
    publisher = OutboxPublisher(batch_size=10, booking_adapter=DownstreamBookingAdapter(app_dispatch=app))
    published = await publisher.poll_and_publish_batch()
    assert published >= 1

    # 4. Verify downstream cab service received cancellation
    matching = [v for v in _CANCELLED_BOOKINGS.values() if v["employee_id"] == emp_id]
    assert len(matching) == 1
    assert matching[0]["cancelled_rides_count"] == 3
    assert matching[0]["freed_seats_count"] == 3


@pytest.mark.asyncio
async def test_downstream_booking_cancellation_is_strictly_idempotent():
    """Test that calling downstream cancellation multiple times with the same event ID is a safe no-op."""
    adapter = DownstreamBookingAdapter(app_dispatch=app)

    # First call
    res1 = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-IDEMP-DOWN-01",
        event_id="evt-unique-cancellation-999",
    )
    assert res1["status"] == "success"
    assert res1["already_cancelled"] is False

    # Second call with same event_id
    res2 = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id="EMP-IDEMP-DOWN-01",
        event_id="evt-unique-cancellation-999",
    )
    assert res2["status"] == "success"
    assert res2["already_cancelled"] is True
    assert "already cancelled" in res2["message"]
