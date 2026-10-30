"""Outbox Crash Recovery & Repeated Delivery Tests (Check 2)

Simulates:
1. Downstream call succeeds (cab booking cancelled).
2. Worker crashes before marking the outbox event as PUBLISHED in PostgreSQL.
3. Restarted worker retries the still-PENDING outbox event.
4. Downstream service receives repeated call and handles it idempotently without
   duplicate cancellations or corrupted seat allocations.
5. Outbox event is marked PUBLISHED in database.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine, async_session_factory
from app.workers.outbox_publisher import OutboxPublisher
from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.api.v1.mock_downstream import (
    reset_mock_downstream,
    _CANCELLED_BOOKINGS,
    _ATTEMPT_COUNTERS,
)
from app.repositories.outbox_repo import OutboxRepository
from app.main import app


@pytest.mark.asyncio
async def test_downstream_success_followed_by_worker_crash_and_restart(
    client: AsyncClient, hr_headers: dict
):
    """Simulate downstream success followed by worker crash before marking PUBLISHED."""
    reset_mock_downstream()
    emp_id = "EMP-CRASH-POST-DOWNSTREAM-001"

    # 1. Create employee
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "PostDownstream",
                    "last_name": "Crash",
                    "email": "crash.downstream@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # 2. Deactivate employee -> Outbox event committed as PENDING
    deact_res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                    "first_name": "PostDownstream",
                    "last_name": "Crash",
                    "email": "crash.downstream@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )
    assert deact_res.status_code == 200

    with sync_engine.connect() as conn:
        outbox_row = conn.execute(
            text(
                f"SELECT id, status FROM outbox_events WHERE aggregate_id = '{emp_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'"
            )
        ).fetchone()
        assert outbox_row is not None
        assert outbox_row[1] == "PENDING"
        event_id = str(outbox_row[0])

    # 3. Simulate first worker: Calls downstream, downstream succeeds, but worker crashes before marking PUBLISHED
    adapter = DownstreamBookingAdapter(app_dispatch=app)
    downstream_result = await adapter.cancel_future_bookings(
        tenant_id="tenant-alpha",
        employee_id=emp_id,
        event_id=event_id,
    )
    assert downstream_result.get("status") == "success"
    assert event_id in _CANCELLED_BOOKINGS
    assert _ATTEMPT_COUNTERS[event_id] == 1

    # In database, the outbox event is STILL PENDING (simulating crash before mark_published commit)
    with sync_engine.connect() as conn:
        current_status = conn.execute(
            text(f"SELECT status FROM outbox_events WHERE id = '{event_id}'")
        ).scalar()
        assert current_status == "PENDING"

    # 4. RESTART: New worker instance starts up and polls pending outbox events
    restarted_publisher = OutboxPublisher(batch_size=10, booking_adapter=adapter)
    published_count = await restarted_publisher.poll_and_publish_batch()
    assert published_count >= 1

    # 5. VERIFY IDEMPOTENT COMPLETION:
    # Downstream service received the repeated call (attempt count = 2)
    assert _ATTEMPT_COUNTERS[event_id] == 2
    # But only exactly ONE cancellation entry exists in the downstream system
    assert len([k for k in _CANCELLED_BOOKINGS if k == event_id]) == 1

    # In database, outbox event is now marked PUBLISHED
    with sync_engine.connect() as conn:
        final_status = conn.execute(
            text(f"SELECT status, published_at FROM outbox_events WHERE id = '{event_id}'")
        ).fetchone()
        assert final_status[0] == "PUBLISHED"
        assert final_status[1] is not None
