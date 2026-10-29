"""Tests for Per-Employee Ordering, Stale Drops & Tie-Breaking (REQ-B3, REQ-B4)"""

import pytest
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_out_of_order_office_transfer_v7_before_shift_change_v6(
    client: AsyncClient, hr_headers: dict
):
    """OFFICIAL CASE STUDY EXAMPLE:
    'An office transfer (version 7) arrives before the shift change (version 6).
    The shift change is ignored as stale and the employee ends up in the new office with the version 7 shift.'
    """
    emp_id = "EMP-ORDER-001"

    # Initial employee creation at version 5
    initial_event = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 5,
                "first_name": "Raj",
                "last_name": "Kumar",
                "email": "raj.kumar@moveinsync.com",
                "phone": "+919876540001",
                "office_code": "BLR-CAMPUS-1",
                "department": "Engineering",
            }
        ]
    }
    res_init = await client.post("/api/v1/roster/events", json=initial_event, headers=hr_headers)
    assert res_init.status_code == 200

    # 1. OUT-OF-ORDER: Version 7 (Office transfer to HYD-CAMPUS-2) arrives FIRST
    v7_transfer = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 7,
                "first_name": "Raj",
                "last_name": "Kumar",
                "email": "raj.kumar@moveinsync.com",
                "office_code": "HYD-CAMPUS-2",  # New office
                "department": "Engineering",
            }
        ]
    }
    res_v7 = await client.post("/api/v1/roster/events", json=v7_transfer, headers=hr_headers)
    assert res_v7.status_code == 200
    assert res_v7.json()["accepted_count"] == 1

    # Verify employee is in HYD-CAMPUS-2 with version 7
    profile_v7 = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert profile_v7.json()["office_code"] == "HYD-CAMPUS-2"
    assert profile_v7.json()["source_version"] == 7

    # 2. Delayed Version 6 (Old shift change / update in BLR-CAMPUS-1) arrives LATER
    v6_stale = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 6,
                "first_name": "Raj",
                "last_name": "Kumar",
                "email": "raj.kumar@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",  # Old office
                "department": "Engineering",
            }
        ]
    }
    res_v6 = await client.post("/api/v1/roster/events", json=v6_stale, headers=hr_headers)
    assert res_v6.status_code == 200
    data_v6 = res_v6.json()
    assert data_v6["accepted_count"] == 0
    assert data_v6["stale_count"] == 1
    assert data_v6["results"][0]["status"] == "stale_ignored"

    # Verify state did NOT regress: employee still ends up in HYD-CAMPUS-2 with version 7!
    final_profile = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert final_profile.json()["office_code"] == "HYD-CAMPUS-2"
    assert final_profile.json()["source_version"] == 7


@pytest.mark.asyncio
async def test_deterministic_tie_breaking_for_equal_versions(
    client: AsyncClient, hr_headers: dict
):
    """Test deterministic tie-breaking when two events arrive with identical source_version."""
    emp_id = "EMP-TIE-001"
    base_time = datetime.now(timezone.utc)

    # Version 10 at T0
    event_10_t0 = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 10,
                "source_timestamp": base_time.isoformat(),
                "first_name": "Amit",
                "last_name": "Sharma",
                "email": "amit.sharma@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    res1 = await client.post("/api/v1/roster/events", json=event_10_t0, headers=hr_headers)
    assert res1.status_code == 200
    assert res1.json()["accepted_count"] == 1

    # Equal version 10 with OLDER timestamp (T - 5s): Must be rejected as stale
    event_10_older = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 10,
                "source_timestamp": (base_time - timedelta(seconds=5)).isoformat(),
                "first_name": "Amit",
                "last_name": "Sharma",
                "email": "amit.sharma@moveinsync.com",
                "office_code": "HYD-CAMPUS-2",
            }
        ]
    }
    res2 = await client.post("/api/v1/roster/events", json=event_10_older, headers=hr_headers)
    assert res2.status_code == 200
    assert res2.json()["stale_count"] == 1

    # Equal version 10 with identical timestamp and identical payload: Duplicate no-op
    res3 = await client.post("/api/v1/roster/events", json=event_10_t0, headers=hr_headers)
    assert res3.status_code == 200
    assert res3.json()["duplicate_count"] == 1
    assert res3.json()["results"][0]["status"] == "duplicate_noop"
