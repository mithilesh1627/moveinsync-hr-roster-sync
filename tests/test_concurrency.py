"""Concurrency Safety Test: Simultaneous Updates & Row-Level Locking (REQ-B5)"""

import asyncio
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_concurrent_updates_to_same_employee_preserves_version_integrity(
    client: AsyncClient, hr_headers: dict
):
    """ADVANCED TRACK CONCURRENCY REQUIREMENT:
    'Test concurrent updates on the same employee, proving PostgreSQL row-level
    locking guarantees monotonic version ordering and prevents lost updates.'
    """
    emp_id = "EMP-CONCUR-001"

    # 1. Create base employee at version 1
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Concurrent",
                    "last_name": "User",
                    "email": "concur.user@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # 2. Fire 8 concurrent update requests with varying versions (v2 to v9)
    async def send_update(version: int, dept: str):
        payload = {
            "events": [
                {
                    "event_type": "UPDATE",
                    "employee_id": emp_id,
                    "source_version": version,
                    "first_name": "Concurrent",
                    "last_name": "User",
                    "email": "concur.user@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                    "department": dept,
                }
            ]
        }
        return await client.post("/api/v1/roster/events", json=payload, headers=hr_headers)

    tasks = [
        send_update(v, f"Dept-{v}")
        for v in [3, 7, 2, 9, 5, 8, 4, 6]
    ]

    responses = await asyncio.gather(*tasks)
    for r in responses:
        assert r.status_code == 200

    # 3. Final Verification:
    # Row-level locking guarantees that version 9 (the highest version) MUST be the final state!
    profile_res = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert profile_res.status_code == 200
    profile = profile_res.json()

    assert profile["source_version"] == 9
    assert profile["department"] == "Dept-9"
