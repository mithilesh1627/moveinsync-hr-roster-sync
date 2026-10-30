"""Comprehensive Cross-Tenant Isolation Tests (Check 5)

Verifies:
1. Tenant B cannot read Tenant A's employees (GET /api/v1/employees/{id} -> 404).
2. Tenant B cannot mutate Tenant A's transport fields (PUT /api/v1/transport/employees/{id} -> 404).
3. Tenant B cannot view Tenant A's reconciliation run reports (GET /api/v1/reconciliation/{run_id}/report -> 404).
4. Tenant B cannot approve Tenant A's halted reconciliation runs (POST /api/v1/reconciliation/{run_id}/approve -> 404).
5. Tenant B cannot abort Tenant A's halted reconciliation runs (POST /api/v1/reconciliation/{run_id}/abort -> 404).
"""

import pytest
from httpx import AsyncClient
from app.core.auth import create_access_token


@pytest.fixture
def tenant_b_headers():
    token = create_access_token(
        user_id="hr-integration-user-beta",
        tenant_id="tenant-beta",
        roles=["hr_integration", "transport_admin", "operator"],
    )
    return {
        "Authorization": f"Bearer {token}",
        "X-Tenant-ID": "tenant-beta",
    }


@pytest.mark.asyncio
async def test_cross_tenant_isolation_on_all_operations(
    client: AsyncClient, hr_headers: dict, operator_headers: dict, tenant_b_headers: dict
):
    emp_id = "EMP-ISO-CROSS-001"

    # 1. Tenant Alpha creates an employee
    create_res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "AlphaEmp",
                    "last_name": "Confidential",
                    "email": "alpha.confidential@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,  # tenant-alpha
    )
    assert create_res.status_code == 200

    # 2. READ ISOLATION: Tenant Beta tries to read Tenant Alpha's employee
    read_res = await client.get(
        f"/api/v1/employees/{emp_id}",
        headers=tenant_b_headers,  # tenant-beta
    )
    assert read_res.status_code == 404, f"Tenant Beta read Tenant Alpha's employee: {read_res.status_code}"

    # 3. WRITE ISOLATION: Tenant Beta tries to update Tenant Alpha's transport fields
    write_res = await client.put(
        f"/api/v1/transport/employees/{emp_id}",
        json={
            "preferred_pickup_point_id": "STOP-MALICIOUS-01",
            "special_assistance_required": True,
        },
        headers=tenant_b_headers,  # tenant-beta
    )
    assert write_res.status_code == 404, f"Tenant Beta mutated Tenant Alpha's employee: {write_res.status_code}"

    # 4. RECONCILIATION RUN ISOLATION: Tenant Alpha uploads a run that triggers safety brake
    # Upload empty file to trigger safety brake for tenant-alpha
    csv_bytes = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    recon_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("alpha_run.csv", csv_bytes, "text/csv")},
        headers=hr_headers,  # tenant-alpha
    )
    assert recon_res.status_code == 200
    alpha_run_id = recon_res.json()["run_id"]

    # 5. REPORT ISOLATION: Tenant Beta tries to inspect Tenant Alpha's run report
    report_res = await client.get(
        f"/api/v1/reconciliation/{alpha_run_id}/report",
        headers=tenant_b_headers,  # tenant-beta
    )
    assert report_res.status_code == 404, f"Tenant Beta accessed Tenant Alpha's run report: {report_res.status_code}"

    # 6. APPROVAL ISOLATION: Tenant Beta tries to approve Tenant Alpha's halted run
    approve_res = await client.post(
        f"/api/v1/reconciliation/{alpha_run_id}/approve",
        json={"operator_id": "malicious-op-beta", "reason": "Unauthorized cross-tenant approval"},
        headers=tenant_b_headers,  # tenant-beta
    )
    assert approve_res.status_code == 404, f"Tenant Beta approved Tenant Alpha's run: {approve_res.status_code}"

    # 7. ABORT ISOLATION: Tenant Beta tries to abort Tenant Alpha's halted run
    abort_res = await client.post(
        f"/api/v1/reconciliation/{alpha_run_id}/abort",
        json={"operator_id": "malicious-op-beta", "reason": "Unauthorized cross-tenant abort"},
        headers=tenant_b_headers,  # tenant-beta
    )
    assert abort_res.status_code == 404, f"Tenant Beta aborted Tenant Alpha's run: {abort_res.status_code}"
