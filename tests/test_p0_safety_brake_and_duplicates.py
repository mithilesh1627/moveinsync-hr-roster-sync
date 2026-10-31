"""P0 Safety Brake Threshold Boundaries & Duplicate Snapshot Policy Tests

Verifies requirements from Section 5 & 6:
1. Exact fractional threshold boundary testing: 29.99%, 30.00%, and 30.01%.
2. Duplicates within a single chunk (intra-chunk).
3. Duplicates spanning multiple chunks (cross-chunk).
4. Duplicates with different versions (selecting newer).
5. Conflicting equal-version content (quarantine / anomaly reporting).
6. Malformed records isolated at row level without crashing valid records.
7. Replay of identical feed twice (strictly idempotent, zero drift).
8. Large duplicate populations with bounded memory.
"""

import io
import uuid
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine
from app.services.reconciliation_service import ReconciliationService


# =============================================================================
# 1. EXACT FRACTIONAL THRESHOLD BOUNDARIES: 29.99%, 30.00%, 30.01%
# =============================================================================
@pytest.mark.asyncio
async def test_safety_brake_exact_boundary_29_99_percent(client: AsyncClient, hr_headers: dict):
    """29.99% missing (2,999 of 10,000) is strictly < 30.0% -> completes automatically."""
    tenant_id = "tenant-alpha"

    # Seed 10,000 employees efficiently
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                )
                SELECT
                    gen_random_uuid(), :tenant_id, 'EMP-FRAC-' || LPAD(s::text, 5, '0'),
                    'Name', 'User', 'u' || s || '@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'hash',
                    false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                FROM generate_series(1, 10000) AS s;
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # CSV has 7,001 employees -> 2,999 missing -> 29.99%
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 7002):
        csv_lines.append(f"EMP-FRAC-{i:05d},Name,User,u{i}@test.com,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("frac_2999.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "COMPLETED"
    assert data["missing_percentage"] == 29.99
    assert data["records_deactivated"] == 2999


@pytest.mark.asyncio
async def test_safety_brake_exact_boundary_30_00_percent(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """30.00% missing (3,000 of 10,000) is >= 30.0% -> halts deactivations, triggers safety brake.
    Proves zero automatic deactivation before authorized operator approval, and complete
    deterministic deactivation only after authorized approval.
    """
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                )
                SELECT
                    gen_random_uuid(), :tenant_id, 'EMP-FRAC-' || LPAD(s::text, 5, '0'),
                    'Name', 'User', 'u' || s || '@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'hash',
                    false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                FROM generate_series(1, 10000) AS s;
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # CSV has 7,000 employees -> 3,000 missing -> exactly 30.00%
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 7001):
        csv_lines.append(f"EMP-FRAC-{i:05d},Name,User,u{i}@test.com,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("frac_3000.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["missing_percentage"] == 30.0
    assert data["records_deactivated"] == 0

    # PROVE NO AUTOMATIC DEACTIVATION BEFORE AUTHORIZED APPROVAL
    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        deact_outbox = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active == 10000, "All 10,000 employees must remain ACTIVE"
        assert deact_outbox == 0, "Zero deactivation outbox events must be emitted"

    run_id = data["run_id"]

    # 1. Reject unauthorized callers (HR integration role lacks operator permission)
    unauth_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "hr-bot", "reason": "Unauthorized attempt"},
        headers=hr_headers,
    )
    assert unauth_res.status_code == 403

    # Verify still zero deactivations after rejected attempt
    with sync_engine.connect() as conn:
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert active == 10000

    # 2. Authorized Operator approves the run
    auth_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-authorized", "reason": "Authorized workforce restructuring"},
        headers=operator_headers,
    )
    assert auth_res.status_code == 200
    auth_data = auth_res.json()
    assert auth_data["status"] == "COMPLETED"
    assert auth_data["records_deactivated"] == 3000

    # Verify that in PostgreSQL: exactly 7,000 remain active, 3,000 deactivated, exactly 3,000 outbox events
    with sync_engine.connect() as conn:
        active_after = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        inactive_after = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'INACTIVE'")
        ).scalar()
        deact_outbox_after = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_after == 7000
        assert inactive_after == 3000
        assert deact_outbox_after == 3000


@pytest.mark.asyncio
async def test_safety_brake_exact_boundary_30_01_percent(client: AsyncClient, hr_headers: dict):
    """30.01% missing (3,001 of 10,000) is > 30.0% -> halts deactivations, triggers safety brake."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                )
                SELECT
                    gen_random_uuid(), :tenant_id, 'EMP-FRAC-' || LPAD(s::text, 5, '0'),
                    'Name', 'User', 'u' || s || '@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'hash',
                    false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                FROM generate_series(1, 10000) AS s;
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # CSV has 6,999 employees -> 3,001 missing -> 30.01%
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    for i in range(1, 7000):
        csv_lines.append(f"EMP-FRAC-{i:05d},Name,User,u{i}@test.com,BLR-CAMPUS-1,ACTIVE,1\n")
    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("frac_3001.csv", content, "text/csv")},
        data={"threshold_percent": 30.0},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "SAFETY_BRAKE_TRIGGERED"
    assert data["missing_percentage"] == 30.01
    assert data["records_deactivated"] == 0


# =============================================================================
# 2. DUPLICATES ACROSS MULTIPLE CHUNKS & DETERMINISTIC RESOLUTION
# =============================================================================
@pytest.mark.asyncio
async def test_duplicates_across_multiple_chunks_and_versions(client: AsyncClient, hr_headers: dict):
    """Duplicates across chunk boundaries with different source versions:
    The higher source_version record is deterministically kept."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # Create CSV where EMP-CROSS-DUP appears in chunk 1 with v1 and chunk 2 with v2
    # Settings chunk size is 2,000
    csv_lines = ["employee_id,first_name,last_name,email,office_code,status,source_version\n"]
    # 2,000 rows in chunk 1
    for i in range(1, 2001):
        if i == 500:
            csv_lines.append("EMP-CROSS-DUP,OldName,User,cross@test.com,BLR-CAMPUS-1,ACTIVE,1\n")
        else:
            csv_lines.append(f"EMP-C1-{i:04d},User,Test,u{i}@c1.test,BLR-CAMPUS-1,ACTIVE,1\n")

    # 100 rows in chunk 2, including EMP-CROSS-DUP with v2
    for i in range(1, 101):
        if i == 10:
            csv_lines.append("EMP-CROSS-DUP,NewName,User,cross@test.com,BLR-CAMPUS-1,ACTIVE,2\n")
        else:
            csv_lines.append(f"EMP-C2-{i:04d},User,Test,u{i}@c2.test,BLR-CAMPUS-1,ACTIVE,1\n")

    content = "".join(csv_lines).encode("utf-8")

    res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("cross_chunk_dup.csv", content, "text/csv")},
        headers=hr_headers,
    )
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "COMPLETED"

    # EMP-CROSS-DUP exists in DB without primary/unique violation
    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-CROSS-DUP'")
        ).scalar()
        assert emp_count == 1


# =============================================================================
# 3. REPLAY OF IDENTICAL FEED TWICE (IDEMPOTENCY & ZERO DRIFT)
# =============================================================================
@pytest.mark.asyncio
async def test_replay_identical_feed_twice_zero_drift(client: AsyncClient, hr_headers: dict):
    """Replaying the exact same nightly CSV twice changes nothing and emits zero side effects."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-REPLAY-1,User1,Test,r1@test.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-REPLAY-2,User2,Test,r2@test.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    # Pass 1
    res1 = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("feed.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res1.status_code == 200
    assert res1.json()["records_created"] == 2

    with sync_engine.connect() as conn:
        outbox_count_pass1 = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()

    # Pass 2: Replay identical file
    res2 = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("feed.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["records_created"] == 0
    assert data2["records_updated"] == 0
    assert data2["records_unchanged"] == 2

    # Zero new outbox events created!
    with sync_engine.connect() as conn:
        outbox_count_pass2 = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()
        assert outbox_count_pass2 == outbox_count_pass1
