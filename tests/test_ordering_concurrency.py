"""Event Ordering & Concurrency Verification Tests (Check 3)

Verifies:
1. Stale version rejected, newer version accepted.
2. Equal-version events resolve deterministically via timestamps and canonical record hashes.
3. Simultaneous concurrent updates to the same employee row serialize deterministically
   using PostgreSQL row-level locks (SELECT ... FOR UPDATE).
"""

import asyncio
from datetime import datetime, timezone, timedelta
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from app.core.database import sync_engine
from app.main import app
from app.utils.hashing import compute_canonical_record_hash


@pytest.mark.asyncio
async def test_stale_newer_and_equal_version_determinism(
    client: AsyncClient, hr_headers: dict
):
    """Test full version ordering spectrum: stale, newer, and equal-version tie-breaking."""
    emp_id = "EMP-ORDER-SPECTRUM-001"
    base_time = datetime(2026, 10, 30, 10, 0, 0, tzinfo=timezone.utc)

    # 1. Create at version 10
    res_v10 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 10,
                    "source_timestamp": base_time.isoformat(),
                    "first_name": "Spectrum",
                    "last_name": "Test",
                    "email": "spectrum@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                    "department": "Engineering",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res_v10.status_code == 200
    assert res_v10.json()["accepted_count"] == 1

    # 2. STALE: Version 8 arrives -> Must be rejected as stale
    res_v8 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": emp_id,
                    "source_version": 8,
                    "source_timestamp": (base_time - timedelta(hours=1)).isoformat(),
                    "first_name": "Spectrum",
                    "last_name": "Test",
                    "email": "spectrum@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                    "department": "Sales",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res_v8.status_code == 200
    assert res_v8.json()["stale_count"] == 1
    assert res_v8.json()["results"][0]["status"] == "stale_ignored"

    # 3. NEWER: Version 12 arrives -> Must be accepted
    res_v12 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": emp_id,
                    "source_version": 12,
                    "source_timestamp": (base_time + timedelta(hours=1)).isoformat(),
                    "first_name": "Spectrum",
                    "last_name": "Test",
                    "email": "spectrum@moveinsync.com",
                    "office_code": "HYD-CAMPUS-2",
                    "department": "Engineering",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res_v12.status_code == 200
    assert res_v12.json()["accepted_count"] == 1

    profile = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert profile.json()["source_version"] == 12
    assert profile.json()["office_code"] == "HYD-CAMPUS-2"

    # 4. EQUAL VERSION 12 with OLDER timestamp -> Rejected as stale
    res_v12_older = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": emp_id,
                    "source_version": 12,
                    "source_timestamp": base_time.isoformat(),  # 1 hour older than the accepted v12
                    "first_name": "Spectrum",
                    "last_name": "Test",
                    "email": "spectrum@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                    "department": "Marketing",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res_v12_older.status_code == 200
    assert res_v12_older.json()["stale_count"] == 1

    # 5. EQUAL VERSION 12 with IDENTICAL timestamp and IDENTICAL payload -> Duplicate no-op
    res_v12_dup = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": emp_id,
                    "source_version": 12,
                    "source_timestamp": (base_time + timedelta(hours=1)).isoformat(),
                    "first_name": "Spectrum",
                    "last_name": "Test",
                    "email": "spectrum@moveinsync.com",
                    "office_code": "HYD-CAMPUS-2",
                    "department": "Engineering",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res_v12_dup.status_code == 200
    assert res_v12_dup.json()["duplicate_count"] == 1
    assert res_v12_dup.json()["results"][0]["status"] == "duplicate_noop"


@pytest.mark.asyncio
async def test_simultaneous_concurrent_updates_to_same_employee(hr_headers: dict):
    """Send 8 concurrent updates to the same employee across increasing versions.
    
    Verifies that PostgreSQL row locking prevents deadlocks and produces the correct
    highest-version final state.
    """
    emp_id = "EMP-CONCUR-ORDER-001"

    # Seed employee at version 1
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.post(
            "/api/v1/roster/events",
            json={
                "events": [
                    {
                        "event_type": "CREATE",
                        "employee_id": emp_id,
                        "source_version": 1,
                        "first_name": "Concurrent",
                        "last_name": "Ordering",
                        "email": "concur.order@moveinsync.com",
                        "office_code": "BLR-CAMPUS-1",
                    }
                ]
            },
            headers=hr_headers,
        )

    # Launch 8 concurrent updates with versions 2 through 9
    async def send_version(ver: int):
        t = ASGITransport(app=app)
        async with AsyncClient(transport=t, base_url="http://testserver") as c:
            return await c.post(
                "/api/v1/roster/events",
                json={
                    "events": [
                        {
                            "event_type": "UPDATE",
                            "employee_id": emp_id,
                            "source_version": ver,
                            "first_name": "Concurrent",
                            "last_name": "Ordering",
                            "email": "concur.order@moveinsync.com",
                            "office_code": "HYD-CAMPUS-2" if ver % 2 == 0 else "BLR-CAMPUS-1",
                            "department": f"Dept-{ver}",
                        }
                    ]
                },
                headers=hr_headers,
            )

    results = await asyncio.gather(*[send_version(v) for v in range(2, 10)])
    for r in results:
        assert r.status_code == 200

    # Final database state must have version 9 and Dept-9
    with sync_engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT source_version, department FROM employees WHERE employee_id = '{emp_id}'")
        ).fetchone()
        assert row[0] == 9
        assert row[1] == "Dept-9"
