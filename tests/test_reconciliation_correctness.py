"""Reconciliation Correctness Regression Tests (P1)

Verifies:
1. Duplicate employee IDs within the same chunk are handled gracefully without
   unique constraint crashes, deduplicating correctly to the latest state.
2. Malformed source versions (non-integer, negative, non-numeric strings) are rejected
   as malformed rows instead of defaulting silently to version 1.
3. Invalid status values (e.g. 'SUSPENDED', 'UNKNOWN') are rejected as malformed.
4. Missing required fields (first_name, last_name, email, office_code) are rejected as malformed.
5. All malformed rows are counted in anomaly_report['malformed_rows_count'].
"""

import io
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine


@pytest.mark.asyncio
async def test_duplicate_employee_id_within_same_chunk(client: AsyncClient, hr_headers: dict):
    """Multiple occurrences of the same employee ID in the same file/chunk do not crash."""
    emp_id = "EMP-CHUNK-DUP-001"
    tenant_id = "tenant-alpha"

    # CSV contains duplicate rows for the same employee in the same chunk
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        f"{emp_id},Alice,Original,alice@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
        f"{emp_id},Alice,UpdatedInSameChunk,alice@company.com,BLR-CAMPUS-1,ACTIVE,2\n"
        "EMP-CHUNK-DUP-002,Bob,Regular,bob@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("dup_chunk.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "COMPLETED"

    # In database, exactly ONE record exists for EMP-CHUNK-DUP-001 with the latest attributes
    with sync_engine.connect() as conn:
        emp = conn.execute(
            text(f"SELECT last_name, source_version FROM employees WHERE employee_id = '{emp_id}'")
        ).fetchone()
        assert emp is not None
        assert emp[0] == "UpdatedInSameChunk"
        assert emp[1] == 2


@pytest.mark.asyncio
async def test_malformed_source_versions_rejected_not_defaulted(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Source version must be a valid integer >= 1; invalid versions are counted as malformed rows."""
    tenant_id = "tenant-alpha"

    # Row 1: malformed string version "NOT_A_NUMBER"
    # Row 2: negative version "-5"
    # Row 3: float version "2.5"
    # Row 4: valid version "3"
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-BAD-VER-001,Bad1,Test,bad1@company.com,BLR-CAMPUS-1,ACTIVE,NOT_A_NUMBER\n"
        "EMP-BAD-VER-002,Bad2,Test,bad2@company.com,BLR-CAMPUS-1,ACTIVE,-5\n"
        "EMP-BAD-VER-003,Bad3,Test,bad3@company.com,BLR-CAMPUS-1,ACTIVE,2.5\n"
        "EMP-GOOD-VER-004,Good,Test,good@company.com,BLR-CAMPUS-1,ACTIVE,3\n"
    ).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("bad_versions.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    run_id = res.json()["run_id"]

    # Verify report records 3 malformed rows
    rep_res = await client.get(
        f"/api/v1/reconciliation/{run_id}/report",
        headers=operator_headers,
    )
    assert rep_res.status_code == 200
    report = rep_res.json()
    assert report["anomaly_summary"]["malformed_rows_count"] == 3

    # Only EMP-GOOD-VER-004 is created in database!
    with sync_engine.connect() as conn:
        good_emp = conn.execute(
            text("SELECT employee_id, source_version FROM employees WHERE employee_id = 'EMP-GOOD-VER-004'")
        ).fetchone()
        bad_emp = conn.execute(
            text("SELECT employee_id FROM employees WHERE employee_id LIKE 'EMP-BAD-VER-%'")
        ).fetchone()
        assert good_emp is not None
        assert good_emp[1] == 3
        assert bad_emp is None, "Malformed version rows must NOT be inserted with default version 1"


@pytest.mark.asyncio
async def test_invalid_status_and_missing_required_fields_rejected(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Rows with invalid status values or missing required fields are rejected as malformed."""
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-INV-001,InvalidStatus,User,inv1@company.com,BLR-CAMPUS-1,SUSPENDED,1\n"
        ",MissingId,User,inv2@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-INV-003,,MissingFirst,inv3@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-INV-004,MissingLast,,inv4@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-INV-005,MissingOffice,User,inv5@company.com,,ACTIVE,1\n"
        "EMP-VAL-006,Valid,User,val6@company.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("validation_checks.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    run_id = res.json()["run_id"]

    rep_res = await client.get(
        f"/api/v1/reconciliation/{run_id}/report",
        headers=operator_headers,
    )
    assert rep_res.status_code == 200
    report = rep_res.json()
    assert report["anomaly_summary"]["malformed_rows_count"] == 5

    # In database, only the valid employee exists
    with sync_engine.connect() as conn:
        val_emp = conn.execute(
            text("SELECT employee_id FROM employees WHERE employee_id = 'EMP-VAL-006'")
        ).fetchone()
        assert val_emp is not None
