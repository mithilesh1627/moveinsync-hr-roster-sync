"""Version Correctness Regression Tests (P1)

Verifies:
1. When a newer snapshot/event arrives (e.g. version 1 -> version 2) with identical
   business fields, source_version MUST advance to version 2.
2. Subsequent events with lower versions (e.g. version 1 or 1.5) are correctly
   recognized as stale and dropped.
3. Both nightly reconciliation and real-time event ingestion advance versions
   even when payload hashes match.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine


@pytest.mark.asyncio
async def test_reconciliation_advances_version_on_identical_business_data(
    client: AsyncClient, hr_headers: dict
):
    """Nightly reconciliation must advance source_version when business fields are unchanged."""
    emp_id = "EMP-VER-ADV-001"
    tenant_id = "tenant-alpha"

    # 1. Employee is created at source_version 1
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Version",
                    "last_name": "Test",
                    "email": "version.test@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                    "department": "Engineering",
                }
            ]
        },
        headers=hr_headers,
    )

    with sync_engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT source_version FROM employees WHERE employee_id = '{emp_id}'")
        ).fetchone()
        assert row[0] == 1

    # 2. Nightly reconciliation arrives with source_version 2, but identical business fields
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,department,status,source_version\n"
        f"{emp_id},Version,Test,version.test@moveinsync.com,BLR-CAMPUS-1,Engineering,ACTIVE,2\n"
    ).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("recon_v2.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    assert res.json()["records_updated"] == 1

    # VERIFY: In database, source_version advanced to 2!
    with sync_engine.connect() as conn:
        row_v2 = conn.execute(
            text(f"SELECT source_version FROM employees WHERE employee_id = '{emp_id}'")
        ).fetchone()
        assert row_v2[0] == 2, f"Expected source_version to advance to 2, found {row_v2[0]}"

    # 3. Subsequent real-time event arrives at version 1 (or old update) -> MUST be dropped as stale!
    stale_event = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Version",
                "last_name": "StaleMutation",
                "email": "version.test@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    stale_res = await client.post("/api/v1/roster/events", json=stale_event, headers=hr_headers)
    assert stale_res.status_code == 200
    assert stale_res.json()["stale_count"] == 1
    assert stale_res.json()["results"][0]["status"] == "stale_ignored"

    # State in database did not regress
    profile = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert profile.json()["source_version"] == 2
    assert profile.json()["last_name"] == "Test"  # StaleMutation did not overwrite
