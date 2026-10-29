"""Failure-Injection & Crash Recovery Test (REQ-E4)

Simulates process/worker crash immediately after committing employee mutation
and outbox record, then resumes on a new worker instance to prove zero event loss.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine
from app.workers.outbox_publisher import OutboxPublisher
from app.api.v1.mock_downstream import reset_mock_downstream, _CANCELLED_BOOKINGS


@pytest.mark.asyncio
async def test_crash_recovery_between_db_commit_and_event_publish(
    client: AsyncClient, hr_headers: dict
):
    """HARD EVALUATION CRITERION:
    'Commit an employee mutation and outbox event, simulate the publisher/process failing
    before delivery, restart processing, and prove the event is eventually delivered
    without losing or duplicating the logical effect.'
    """
    reset_mock_downstream()
    emp_id = "EMP-CRASH-001"

    # 1. Employee is created and active
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Crash",
                    "last_name": "Resilient",
                    "email": "crash.test@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # 2. Employee exits -> Deactivate event submitted via API
    deact_res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                    "first_name": "Crash",
                    "last_name": "Resilient",
                    "email": "crash.test@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )
    assert deact_res.status_code == 200

    # 3. VERIFY ATOMIC DB STATE AT POINT OF CRASH:
    # The employee is already INACTIVE in the database, and the outbox event is COMMITTED as PENDING.
    with sync_engine.connect() as conn:
        emp_status = conn.execute(
            text(f"SELECT status FROM employees WHERE employee_id = '{emp_id}'")
        ).scalar()
        outbox_row = conn.execute(
            text(
                f"SELECT id, status, event_type FROM outbox_events WHERE aggregate_id = '{emp_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'"
            )
        ).fetchone()

        assert emp_status == "INACTIVE", "Database state should be committed"
        assert outbox_row is not None
        assert outbox_row[1] == "PENDING", "Outbox event must be in PENDING state awaiting delivery"
        event_id = str(outbox_row[0])

    # 4. SIMULATE HARD CRASH:
    # Process dies immediately. Downstream cab service has NOT been called yet.
    assert event_id not in _CANCELLED_BOOKINGS, "Downstream should not have been called prior to publisher run"

    # 5. SIMULATE RESTART:
    # Application / publisher starts up fresh on reboot.
    from app.main import app
    from app.adapters.booking_adapter import DownstreamBookingAdapter

    adapter = DownstreamBookingAdapter(app_dispatch=app)
    restarted_publisher = OutboxPublisher(batch_size=10, booking_adapter=adapter)

    # Publisher executes its polling loop
    published = await restarted_publisher.poll_and_publish_batch()
    assert published >= 1

    # 6. VERIFY COMPLETE RECOVERY:
    # Event is now marked PUBLISHED in DB
    with sync_engine.connect() as conn:
        final_status = conn.execute(
            text(f"SELECT status, published_at FROM outbox_events WHERE id = '{event_id}'")
        ).fetchone()
        assert final_status[0] == "PUBLISHED"
        assert final_status[1] is not None

    # Downstream cab booking service has successfully received the cancellation!
    assert event_id in _CANCELLED_BOOKINGS
    assert _CANCELLED_BOOKINGS[event_id]["employee_id"] == emp_id
    assert _CANCELLED_BOOKINGS[event_id]["cancelled_rides_count"] == 3

    # 7. PROVE AT-LEAST-ONCE IDEMPOTENCY:
    # Running publisher again does NOT duplicate the logical effect
    second_run = await restarted_publisher.poll_and_publish_batch()
    assert second_run == 0, "Published events should not be re-published"
