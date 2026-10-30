"""Safety Brake Boundary & Anomaly Detection Tests (Check 4)

Verifies:
1. Exact boundary behavior at 29%, 30%, and 31% missing active employees.
2. Empty, truncated, duplicate-ID, and malformed export handling.
3. Verification that ZERO destructive actions occur before operator approval
   when the safety brake triggers.
"""

import io
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine


def seed_active_employees(tenant_id: str, count: int):
    """Synchronously seed N active employees."""
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        params = [
            {
                "id": str(uuid.uuid4()),
                "tenant_id": tenant_id,
                "employee_id": f"EMP-BRAKE-{i:03d}",
                "first_name": f"User{i}",
                "last_name": "Test",
                "email": f"user{i}@brake.test",
                "phone": "+919876543210",
                "office_code": "BLR-CAMPUS-1",
                "department": "Engineering",
                "manager_id": "MGR-001",
                "status": "ACTIVE",
                "source_version": 1,
                "record_hash": f"hash_{i}",
            }
            for i in range(1, count + 1)
        ]
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email, phone,
                    office_code, department, manager_id, status, source_version, source_timestamp,
                    record_hash, special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES (
                    CAST(:id AS uuid), :tenant_id, :employee_id, :first_name, :last_name, :email, :phone,
                    :office_code, :department, :manager_id, :status, :source_version, NOW(),
                    :record_hash, false, false, NOW(), NOW()
                )
                """
            ),
            params,
        )
        conn.commit()


@pytest.mark.asyncio
async def test_safety_brake_boundary_29_percent(client: AsyncClient, hr_headers: dict):
    """29% missing active employees (< 30% threshold) -> Deactivations applied automatically."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 100)

    # 71 employees present in export -> 29 missing (29.0%)
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 72):
        csv_lines.append(f"EMP-BRAKE-{i:03d},User{i},Test,user{i}@brake.test,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("export_29.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "COMPLETED"
    assert data["missing_percentage"] == 29.0
    assert data["records_deactivated"] == 29

    # Verify database: 71 remain ACTIVE, 29 are INACTIVE
    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        inactive = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'INACTIVE'")
        ).scalar()
        assert active == 71
        assert inactive == 29


@pytest.mark.asyncio
async def test_safety_brake_boundary_30_percent(client: AsyncClient, hr_headers: dict):
    """30% missing active employees (>= 30% threshold) -> Safety brake TRIGGERED, ZERO deactivations."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 100)

    # 70 employees present in export -> 30 missing (30.0%)
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 71):
        csv_lines.append(f"EMP-BRAKE-{i:03d},User{i},Test,user{i}@brake.test,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("export_30.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["missing_percentage"] == 30.0
    assert data["records_deactivated"] == 0
    assert data["operator_review_required"] is True

    # Critical Guarantee: ZERO destructive actions in PostgreSQL
    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert active == 100, "All 100 employees must remain ACTIVE when safety brake triggers"


@pytest.mark.asyncio
async def test_safety_brake_boundary_31_percent(client: AsyncClient, hr_headers: dict):
    """31% missing active employees (> 30% threshold) -> Safety brake TRIGGERED, ZERO deactivations."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 100)

    # 69 employees present in export -> 31 missing (31.0%)
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 70):
        csv_lines.append(f"EMP-BRAKE-{i:03d},User{i},Test,user{i}@brake.test,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("export_31.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["missing_percentage"] == 31.0
    assert data["records_deactivated"] == 0

    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert active == 100


@pytest.mark.asyncio
async def test_empty_export_triggers_safety_brake(client: AsyncClient, hr_headers: dict):
    """Empty export file missing 100% of employees -> Triggers brake, 0 deactivations."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 50)

    # Empty CSV with header only
    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["records_deactivated"] == 0

    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert active == 50


@pytest.mark.asyncio
async def test_truncated_export_triggers_safety_brake(client: AsyncClient, hr_headers: dict):
    """Truncated export file (only 5 out of 100 records present) -> Triggers brake."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 100)

    # 5 out of 100 -> 95% missing
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 6):
        csv_lines.append(f"EMP-BRAKE-{i:03d},User{i},Test,user{i}@brake.test,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("truncated.csv", content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()

    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["records_deactivated"] == 0

    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert active == 100


@pytest.mark.asyncio
async def test_duplicate_id_and_malformed_export_detected(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Export containing duplicate IDs and malformed lines is audited in anomaly report."""
    tenant_id = "tenant-alpha"
    seed_active_employees(tenant_id, 10)

    # Export with 10 records, but contains duplicate IDs and malformed rows without employee_id
    csv_lines = [
        "employee_id,first_name,last_name,email,office_code,status,source_version\n",
        "EMP-BRAKE-001,User1,Test,user1@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-001,User1,Test,user1@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",  # Duplicate ID
        ",Malformed,NoId,noid@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",           # Missing employee_id
        "EMP-BRAKE-002,User2,Test,user2@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-003,User3,Test,user3@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-004,User4,Test,user4@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-005,User5,Test,user5@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-006,User6,Test,user6@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-007,User7,Test,user7@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-008,User8,Test,user8@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-009,User9,Test,user9@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
        "EMP-BRAKE-010,User10,Test,user10@brake.test,BLR-CAMPUS-1,ACTIVE,1\n",
    ]
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("anomalies.csv", content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    run_id = res.json()["run_id"]

    # Fetch operator anomaly report
    rep_res = await client.get(
        f"/api/v1/reconciliation/{run_id}/report",
        headers=operator_headers,
    )
    assert rep_res.status_code == 200
    report = rep_res.json()

    summary = report["anomaly_summary"]
    assert summary["duplicate_ids_in_file_count"] >= 1
    assert "EMP-BRAKE-001" in summary["duplicate_sample_ids"]
    assert summary["malformed_rows_count"] >= 1
