"""Tests for Multi-Tenant Data Isolation (REQ-F1)"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_tenant_isolation_employee_access(
    client: AsyncClient, hr_headers: dict, tenant_beta_hr_headers: dict
):
    """KEY REQUIREMENT: A caller from tenant A must not access tenant B's records."""
    emp_alpha = "EMP-ALPHA-SECRET-01"
    emp_beta = "EMP-BETA-SECRET-01"

    # Tenant Alpha creates an employee
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_alpha,
                    "source_version": 1,
                    "first_name": "Alpha",
                    "last_name": "Employee",
                    "email": "alpha.emp@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )

    # Tenant Beta creates an employee
    await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_beta,
                    "source_version": 1,
                    "first_name": "Beta",
                    "last_name": "Employee",
                    "email": "beta.emp@moveinsync.com",
                    "office_code": "MUM-CAMPUS-1",
                }
            ]
        },
        headers=tenant_beta_hr_headers,
    )

    # 1. Tenant Alpha reads own employee -> 200 OK
    res_alpha_own = await client.get(f"/api/v1/employees/{emp_alpha}", headers=hr_headers)
    assert res_alpha_own.status_code == 200
    assert res_alpha_own.json()["employee_id"] == emp_alpha

    # 2. CROSS-TENANT READ ATTEMPT:
    # Tenant Beta tries to read Tenant Alpha's employee -> Must return 404 Not Found!
    res_cross_read = await client.get(f"/api/v1/employees/{emp_alpha}", headers=tenant_beta_hr_headers)
    assert res_cross_read.status_code == 404

    # 3. CROSS-TENANT WRITE ATTEMPT:
    # Tenant Beta attempts to update Tenant Alpha's employee -> Must not modify Tenant Alpha's employee!
    cross_write = {
        "events": [
            {
                "event_type": "UPDATE",
                "employee_id": emp_alpha,  # Target Tenant Alpha's employee ID
                "source_version": 10,
                "first_name": "Hacked",
                "last_name": "Name",
                "email": "hacked@moveinsync.com",
                "office_code": "MUM-CAMPUS-1",
            }
        ]
    }
    await client.post("/api/v1/roster/events", json=cross_write, headers=tenant_beta_hr_headers)

    # Verify Tenant Alpha's employee was NOT modified
    verify_res = await client.get(f"/api/v1/employees/{emp_alpha}", headers=hr_headers)
    assert verify_res.status_code == 200
    assert verify_res.json()["first_name"] == "Alpha"
    assert verify_res.json()["email"] == "alpha.emp@moveinsync.com"
