"""Tests for Field Ownership Invariants & Office Transfer Protocol (REQ-C1, REQ-C2, REQ-C3)"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_hr_update_never_overwrites_transport_admin_owned_fields(
    client: AsyncClient, hr_headers: dict, transport_admin_headers: dict
):
    """KEY REQUIREMENT: HR owns name, email, office. Transport admin owns pickup point, assistance.
    An HR update must NEVER overwrite transport-owned fields!
    """
    emp_id = "EMP-OWN-001"

    # 1. HR creates employee
    create_event = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "source_version": 1,
                "first_name": "Rohan",
                "last_name": "Verma",
                "email": "rohan.verma@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    res_hr1 = await client.post("/api/v1/roster/events", json=create_event, headers=hr_headers)
    assert res_hr1.status_code == 200

    # 2. Transport Administrator assigns preferred pickup point and special assistance
    transport_update = {
        "preferred_pickup_point_id": "STOP-BLR-WHITEFIELD-09",
        "special_assistance_required": True,
        "special_assistance_notes": "Wheelchair ramp required at pickup",
        "shift_timing": "08:30-17:30",
    }
    res_trans = await client.put(
        f"/api/v1/transport/employees/{emp_id}",
        json=transport_update,
        headers=transport_admin_headers,
    )
    assert res_trans.status_code == 200
    trans_data = res_trans.json()
    assert trans_data["preferred_pickup_point_id"] == "STOP-BLR-WHITEFIELD-09"
    assert trans_data["special_assistance_required"] is True

    # 3. HR updates employee (Version 2) with new name, new email, and new manager
    hr_v2_update = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 2,
                "first_name": "Rohan",
                "last_name": "Verma-Senior",
                "email": "rohan.senior@moveinsync.com",
                "manager_id": "MGR-999",
                "office_code": "BLR-CAMPUS-1",
            }
        ]
    }
    res_hr2 = await client.post("/api/v1/roster/events", json=hr_v2_update, headers=hr_headers)
    assert res_hr2.status_code == 200
    assert res_hr2.json()["accepted_count"] == 1

    # 4. CRITICAL VERIFICATION: Inspect final profile
    final_res = await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)
    assert final_res.status_code == 200
    profile = final_res.json()

    # HR fields updated
    assert profile["last_name"] == "Verma-Senior"
    assert profile["email"] == "rohan.senior@moveinsync.com"
    assert profile["manager_id"] == "MGR-999"

    # Transport-owned fields STRICTLY PRESERVED!
    assert profile["preferred_pickup_point_id"] == "STOP-BLR-WHITEFIELD-09"
    assert profile["special_assistance_required"] is True
    assert profile["special_assistance_notes"] == "Wheelchair ramp required at pickup"
    assert profile["shift_timing"] == "08:30-17:30"


@pytest.mark.asyncio
async def test_office_transfer_flags_transport_review_required(
    client: AsyncClient, hr_headers: dict, transport_admin_headers: dict
):
    """Test Office Relocation Conflict Protocol:
    When an employee with an existing pickup point is transferred to a new office,
    transport_review_required is set to TRUE.
    """
    emp_id = "EMP-OWN-002"

    # 1. Create employee in BLR-CAMPUS-1
    res1 = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Pooja",
                    "last_name": "Hegde",
                    "email": "pooja.h@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res1.status_code == 200

    # 2. Transport admin sets Bangalore pickup point
    await client.put(
        f"/api/v1/transport/employees/{emp_id}",
        json={"preferred_pickup_point_id": "STOP-BLR-KORAMANGALA-04"},
        headers=transport_admin_headers,
    )

    # 3. HR transfers employee to HYD-CAMPUS-2 (Hyderabad)
    transfer_event = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_id,
                "source_version": 2,
                "first_name": "Pooja",
                "last_name": "Hegde",
                "email": "pooja.h@moveinsync.com",
                "office_code": "HYD-CAMPUS-2",  # Different office!
            }
        ]
    }
    res_transfer = await client.post("/api/v1/roster/events", json=transfer_event, headers=hr_headers)
    assert res_transfer.status_code == 200

    # 4. Verify transport_review_required is True
    profile = (await client.get(f"/api/v1/employees/{emp_id}", headers=hr_headers)).json()
    assert profile["office_code"] == "HYD-CAMPUS-2"
    assert profile["transport_review_required"] is True
    # Pickup point is preserved
    assert profile["preferred_pickup_point_id"] == "STOP-BLR-KORAMANGALA-04"

    # 5. Transport Admin assigns new Hyderabad pickup point and resolves review
    res_resolve = await client.put(
        f"/api/v1/transport/employees/{emp_id}",
        json={
            "preferred_pickup_point_id": "STOP-HYD-HITEC-CITY-01",
            "resolve_transport_review": True,
        },
        headers=transport_admin_headers,
    )
    assert res_resolve.status_code == 200
    resolved_profile = res_resolve.json()
    assert resolved_profile["preferred_pickup_point_id"] == "STOP-HYD-HITEC-CITY-01"
    assert resolved_profile["transport_review_required"] is False
