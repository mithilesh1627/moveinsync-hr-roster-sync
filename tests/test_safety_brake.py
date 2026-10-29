"""Tests for Configurable Safety Brake, Truncated Exports & Operator Approvals (REQ-D4, REQ-D5)"""

import io
import csv
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine


def generate_roster_csv(employee_list: list[dict]) -> io.BytesIO:
    """Generate in-memory CSV file buffer from employee dictionary list."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "employee_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "office_code",
            "department",
            "manager_id",
            "status",
            "source_version",
        ],
    )
    writer.writeheader()
    for row in employee_list:
        writer.writerow(row)
    return io.BytesIO(output.getvalue().encode("utf-8"))


@pytest.mark.asyncio
async def test_safety_brake_halts_deactivations_when_40_percent_missing(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """OFFICIAL CASE STUDY REQUIREMENT:
    'The nightly file is cut off at 120,000 rows. The safety brake stops the run,
    nobody is deactivated, and the operator sees how many records the file appears to be missing.'
    """
    tenant_id = "tenant-alpha"

    # 1. Seed 100 active employees in the database for tenant-alpha
    seed_events = [
        {
            "event_type": "CREATE",
            "employee_id": f"EMP-BRAKE-{i:03d}",
            "source_version": 1,
            "first_name": f"User{i}",
            "last_name": "Test",
            "email": f"user{i}@moveinsync.com",
            "phone": f"+919876540{i:03d}",
            "office_code": "BLR-CAMPUS-1",
        }
        for i in range(1, 101)
    ]
    # Chunk into 50s for ingest
    await client.post("/api/v1/roster/events", json={"events": seed_events[:50]}, headers=hr_headers)
    await client.post("/api/v1/roster/events", json={"events": seed_events[50:]}, headers=hr_headers)

    # Verify 100 active employees in DB
    with sync_engine.connect() as conn:
        active_before = conn.execute(
            text("SELECT COUNT(*) FROM employees WHERE tenant_id = 'tenant-alpha' AND status = 'ACTIVE'")
        ).scalar()
        assert active_before == 100

    # 2. Simulate nightly file truncated or missing 40% (only 60 of 100 present)
    export_rows = [
        {
            "employee_id": f"EMP-BRAKE-{i:03d}",
            "first_name": f"User{i}",
            "last_name": "Test",
            "email": f"user{i}@moveinsync.com",
            "phone": f"+919876540{i:03d}",
            "office_code": "BLR-CAMPUS-1",
            "status": "ACTIVE",
            "source_version": 2,
        }
        for i in range(1, 61)  # Only 1 to 60 present! 40 missing
    ]
    csv_bytes = generate_roster_csv(export_rows)

    # 3. Upload export file
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("nightly_export_truncated.csv", csv_bytes, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_data = upload_res.json()

    # Safety brake MUST trigger!
    assert run_data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert run_data["operator_review_required"] is True
    assert run_data["missing_active_records"] == 40
    assert run_data["missing_percentage"] == 40.0
    assert run_data["records_deactivated"] == 0, "Deactivations occurred despite safety brake!"

    run_id = run_data["run_id"]

    # 4. Verify that NOBODY was deactivated in PostgreSQL: all 100 remain ACTIVE!
    with sync_engine.connect() as conn:
        active_after = conn.execute(
            text("SELECT COUNT(*) FROM employees WHERE tenant_id = 'tenant-alpha' AND status = 'ACTIVE'")
        ).scalar()
        assert active_after == 100, "Employees were wrongfully deactivated when safety brake triggered!"

    # 5. Operator inspects report
    report_res = await client.get(f"/api/v1/reconciliation/{run_id}/report", headers=operator_headers)
    assert report_res.status_code == 200
    report = report_res.json()
    assert report["safety_brake_triggered"] is True
    assert report["missing_active_count"] == 40
    assert report["missing_percentage"] == 40.0
    assert "REJECT / ABORT" in report["recommendation"]
    assert len(report["sample_missing_employee_ids"]) > 0

    # 6. Operator Override / Approval flow:
    # Operator confirms it was a legitimate corporate restructuring and approves the deactivations
    approval_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "operator-sec-99", "reason": "Authorized mass redundancy export verified with HR VP"},
        headers=operator_headers,
    )
    assert approval_res.status_code == 200
    approved_data = approval_res.json()
    assert approved_data["status"] == "COMPLETED"
    assert approved_data["approved_by_operator"] is True
    assert approved_data["records_deactivated"] == 40

    # 7. Verify deactivations applied after human approval
    with sync_engine.connect() as conn:
        active_final = conn.execute(
            text("SELECT COUNT(*) FROM employees WHERE tenant_id = 'tenant-alpha' AND status = 'ACTIVE'")
        ).scalar()
        assert active_final == 60


@pytest.mark.asyncio
async def test_operator_can_abort_halted_reconciliation_run(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Test that an operator can abort a halted reconciliation run, safely discarding it."""
    # Seed 10 employees
    seed_events = [
        {
            "event_type": "CREATE",
            "employee_id": f"EMP-ABORT-{i:02d}",
            "source_version": 1,
            "first_name": f"User{i}",
            "last_name": "AbortTest",
            "email": f"abort{i}@moveinsync.com",
            "office_code": "BLR-CAMPUS-1",
        }
        for i in range(1, 11)
    ]
    await client.post("/api/v1/roster/events", json={"events": seed_events}, headers=hr_headers)

    # Upload file with 5 employees (50% missing -> safety brake triggers)
    export_rows = [
        {
            "employee_id": f"EMP-ABORT-{i:02d}",
            "first_name": f"User{i}",
            "last_name": "AbortTest",
            "email": f"abort{i}@moveinsync.com",
            "office_code": "BLR-CAMPUS-1",
            "status": "ACTIVE",
            "source_version": 2,
        }
        for i in range(1, 6)
    ]
    csv_bytes = generate_roster_csv(export_rows)

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("export_bad.csv", csv_bytes, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Operator aborts
    abort_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/abort",
        json={"operator_id": "operator-01", "reason": "Corrupted FTP export confirmed by HR ops"},
        headers=operator_headers,
    )
    assert abort_res.status_code == 200
    assert abort_res.json()["status"] == "ABORTED"

    # Verify nobody was deactivated: all 10 remain active
    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text("SELECT COUNT(*) FROM employees WHERE tenant_id = 'tenant-alpha' AND status = 'ACTIVE'")
        ).scalar()
        assert active_count == 10


@pytest.mark.asyncio
async def test_normal_reconciliation_under_threshold_completes_automatically(
    client: AsyncClient, hr_headers: dict
):
    """Test that an export with < 30% missing records completes automatically without operator halt."""
    # Seed 20 employees
    seed_events = [
        {
            "event_type": "CREATE",
            "employee_id": f"EMP-NORM-{i:02d}",
            "source_version": 1,
            "first_name": f"User{i}",
            "last_name": "Norm",
            "email": f"norm{i}@moveinsync.com",
            "office_code": "BLR-CAMPUS-1",
        }
        for i in range(1, 21)
    ]
    await client.post("/api/v1/roster/events", json={"events": seed_events}, headers=hr_headers)

    # Export has 18 employees (2 missing = 10% missing < 30% threshold)
    export_rows = [
        {
            "employee_id": f"EMP-NORM-{i:02d}",
            "first_name": f"User{i}",
            "last_name": "Norm",
            "email": f"norm{i}@moveinsync.com",
            "office_code": "BLR-CAMPUS-1",
            "status": "ACTIVE",
            "source_version": 2,
        }
        for i in range(1, 19)
    ]
    csv_bytes = generate_roster_csv(export_rows)

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("export_normal.csv", csv_bytes, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    data = upload_res.json()

    assert data["status"] == "COMPLETED"
    assert data["records_deactivated"] == 2
    assert data["missing_percentage"] == 10.0
    assert data["operator_review_required"] is False
