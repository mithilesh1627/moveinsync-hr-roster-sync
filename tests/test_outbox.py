"""Tests for Transactional Outbox Pattern, Retries & Dead-Letter Queue (REQ-E1 to REQ-E3)"""

import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine, async_session_factory
from app.repositories.outbox_repo import OutboxRepository
from app.workers.outbox_publisher import OutboxPublisher
from app.adapters.booking_adapter import DownstreamBookingAdapter
from app.main import app
from app.api.v1.mock_downstream import set_failure_simulation


@pytest.mark.asyncio
async def test_atomic_outbox_commit_with_employee_mutation(client: AsyncClient, hr_headers: dict):
    """KEY REQUIREMENT: Commit an employee mutation and its outbox event in the same PostgreSQL transaction."""
    emp_id = "EMP-OUTBOX-001"
    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Outbox",
                "last_name": "Tester",
                "email": "outbox.tester@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    response = await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)
    assert response.status_code == 200

    # Verify both employee and outbox event exist in PostgreSQL
    with sync_engine.connect() as conn:
        emp_exists = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE employee_id = '{emp_id}'")
        ).scalar()
        outbox_row = conn.execute(
            text(f"SELECT id, status, event_type FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).fetchone()

        assert emp_exists == 1
        assert outbox_row is not None
        assert outbox_row[1] == "PENDING"
        assert outbox_row[2] == "EMPLOYEE_CREATED"


@pytest.mark.asyncio
async def test_outbox_publisher_successfully_publishes_event(client: AsyncClient, hr_headers: dict):
    """Test that the Outbox Publisher polls pending events with SKIP LOCKED and marks them PUBLISHED."""
    emp_id = "EMP-OUTBOX-PUB-01"
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Pub",
                    "last_name": "Test",
                    "email": "pub.test@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    publisher = OutboxPublisher(batch_size=10, booking_adapter=DownstreamBookingAdapter(app_dispatch=app))
    published_count = await publisher.poll_and_publish_batch()
    assert published_count >= 1

    # Verify status changed to PUBLISHED
    with sync_engine.connect() as conn:
        status = conn.execute(
            text(f"SELECT status, published_at FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).fetchone()
        assert status[0] == "PUBLISHED"
        assert status[1] is not None


@pytest.mark.asyncio
async def test_outbox_retries_and_dlq_transition_on_downstream_failure(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Test exponential backoff retries and DLQ escalation when downstream fails repeatedly."""
    emp_id = "EMP-OUTBOX-DLQ-01"

    # Create employee and deactivate to trigger downstream cab booking cancellation
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "DLQ",
                    "last_name": "User",
                    "email": "dlq.user@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # Deactivate employee -> triggers EMPLOYEE_DEACTIVATED outbox event
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "DEACTIVATE",
                    "employee_id": emp_id,
                    "source_version": 2,
                    "first_name": "DLQ",
                    "last_name": "User",
                    "email": "dlq.user@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # Find the DEACTIVATED outbox event
    with sync_engine.connect() as conn:
        event_row = conn.execute(
            text(f"SELECT id FROM outbox_events WHERE aggregate_id = '{emp_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).fetchone()
        event_id = str(event_row[0])

    # Configure mock downstream to fail repeatedly (max_retries = 3 for this test)
    set_failure_simulation(fail_until_attempt=10, event_id=event_id)

    publisher = OutboxPublisher(
        batch_size=10,
        max_retries=3,
        base_backoff_seconds=0.01,
        booking_adapter=DownstreamBookingAdapter(app_dispatch=app),
    )

    # Attempt 1: Fails -> status FAILED, retry_count=1
    await publisher.poll_and_publish_batch()
    with sync_engine.connect() as conn:
        row = conn.execute(text(f"SELECT status, retry_count FROM outbox_events WHERE id = '{event_id}'")).fetchone()
        assert row[0] == "FAILED"
        assert row[1] == 1

    # Force next_retry_at to NOW so it can be immediately retried in test
    with sync_engine.connect() as conn:
        conn.execute(text(f"UPDATE outbox_events SET next_retry_at = NOW() - interval '1 second' WHERE id = '{event_id}'"))
        conn.commit()

    # Attempt 2: Fails -> retry_count=2
    await publisher.poll_and_publish_batch()

    with sync_engine.connect() as conn:
        conn.execute(text(f"UPDATE outbox_events SET next_retry_at = NOW() - interval '1 second' WHERE id = '{event_id}'"))
        conn.commit()

    # Attempt 3: Fails -> retry_count reaches max_retries (3) -> Moves to DEAD_LETTER!
    await publisher.poll_and_publish_batch()
    with sync_engine.connect() as conn:
        row = conn.execute(text(f"SELECT status, retry_count, last_error FROM outbox_events WHERE id = '{event_id}'")).fetchone()
        assert row[0] == "DEAD_LETTER"
        assert row[1] == 2  # new_retries was 3, exceeded max
        assert row[2] is not None

    # Verify Operator can inspect the DLQ
    dlq_res = await client.get("/api/v1/outbox/dlq", headers=operator_headers)
    assert dlq_res.status_code == 200
    dlq_list = dlq_res.json()
    assert any(item["aggregate_id"] == emp_id for item in dlq_list)

    # Operator retries the event
    retry_res = await client.post(f"/api/v1/outbox/dlq/{event_id}/retry", headers=operator_headers)
    assert retry_res.status_code == 200
    assert retry_res.json()["status"] == "PENDING"
