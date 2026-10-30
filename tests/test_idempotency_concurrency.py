"""Idempotency Concurrency Verification Tests (Check 1)

Verifies:
1. Simultaneous identical requests using the same idempotency key result in exactly
   one logical application, with all callers receiving HTTP 200 (cached responses).
2. Simultaneous conflicting requests with the same key but different payloads result
   in exactly one winner (HTTP 200) and the conflicting caller receiving HTTP 409 Conflict.
3. Zero duplicate side effects in the database or outbox table.
"""

import asyncio
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from app.core.database import sync_engine
from app.main import app


@pytest.mark.asyncio
async def test_simultaneous_identical_requests_single_execution(hr_headers: dict):
    """Send 5 simultaneous identical requests using the exact same idempotency key."""
    emp_id = "EMP-IDEMP-CONCUR-001"
    key = "IDEMP-KEY-SIMULTANEOUS-IDENTICAL"

    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Concurrent",
                "last_name": "Idempotent",
                "email": "concurrent.idemp@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    headers = dict(hr_headers)
    headers["X-Idempotency-Key"] = key

    # Launch 5 concurrent requests using separate AsyncClients
    async def send_req():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post("/api/v1/roster/events", json=payload, headers=headers)

    responses = await asyncio.gather(*[send_req() for _ in range(5)])

    # All 5 callers must receive HTTP 200 OK
    for res in responses:
        assert res.status_code == 200, f"Expected 200, got {res.status_code}: {res.text}"
        data = res.json()
        assert data["accepted_count"] == 1 or data["duplicate_count"] == 0

    # Verify PostgreSQL state: Exactly ONE employee record and ONE outbox event
    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE employee_id = '{emp_id}'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).scalar()
        idemp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = '{key}'")
        ).scalar()

        assert emp_count == 1, f"Expected exactly 1 employee, found {emp_count}"
        assert outbox_count == 1, f"Expected exactly 1 outbox event, found {outbox_count}"
        assert idemp_count == 1, f"Expected exactly 1 idempotency record, found {idemp_count}"


@pytest.mark.asyncio
async def test_simultaneous_conflicting_payloads_same_key(hr_headers: dict):
    """Send 2 simultaneous requests with the SAME idempotency key but DIFFERENT payloads.
    
    Exactly one must succeed (200), and the other must be rejected with 409 Conflict.
    """
    emp_id = "EMP-IDEMP-CONFLICT-001"
    key = "IDEMP-KEY-SIMULTANEOUS-CONFLICT"

    payload_alice = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Alice",
                "last_name": "Conflict",
                "email": "alice.conflict@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    payload_bob = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Bob",
                "last_name": "Conflict",
                "email": "bob.conflict@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }

    headers = dict(hr_headers)
    headers["X-Idempotency-Key"] = key

    async def send_alice():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post("/api/v1/roster/events", json=payload_alice, headers=headers)

    async def send_bob():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post("/api/v1/roster/events", json=payload_bob, headers=headers)

    res_alice, res_bob = await asyncio.gather(send_alice(), send_bob())

    statuses = [res_alice.status_code, res_bob.status_code]
    assert 200 in statuses, f"Expected at least one 200: {statuses}"
    assert 409 in statuses, f"Expected at least one 409 Conflict: {statuses}"

    # Verify that only ONE employee was committed and outbox has only ONE event
    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE employee_id = '{emp_id}'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).scalar()

        assert emp_count == 1
        assert outbox_count == 1
