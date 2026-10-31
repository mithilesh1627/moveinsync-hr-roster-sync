"""P0 Deep Concurrency Tests: Snapshot & Live Event Interleaving

Verifies the 10 concurrency scenarios required by Section 4:
1. Snapshot v8 versus live update v10.
2. Snapshot v10 versus live update v8.
3. Concurrent create.
4. Concurrent deactivation.
5. Equal version, identical content.
6. Equal version, conflicting content.
7. Live update during approval.
8. Live update during approval recovery.
9. Two reconciliation runs for the same tenant.
10. Different tenants processing simultaneously.
"""

import asyncio
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from unittest.mock import patch

from app.core.database import sync_engine, async_engine
from app.main import app
from app.services.reconciliation_service import ReconciliationService


# =============================================================================
# 1. SNAPSHOT V8 VS LIVE UPDATE V10
# =============================================================================
@pytest.mark.asyncio
async def test_snapshot_v8_vs_live_update_v10(client: AsyncClient, hr_headers: dict):
    """Scenario 1: Live event updated employee to v10. Older snapshot v8 arrives.
    Snapshot v8 must be dropped as stale under row lock. DB record preserved."""
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
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-CONC-1', 'LiveName', 'User', 'live@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 10, NOW(), 'hash-live-10', false, false, NOW(), NOW()
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Snapshot with older v8
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-CONC-1,StaleName,User,live@test.com,BLR-CAMPUS-1,ACTIVE,8\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("v8.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    data = upload_res.json()
    assert data["status"] == "COMPLETED"
    assert data["records_stale"] == 1
    assert data["records_updated"] == 0

    with sync_engine.connect() as conn:
        emp = conn.execute(
            text(f"SELECT first_name, source_version FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-CONC-1'")
        ).fetchone()
        assert emp[0] == "LiveName"
        assert emp[1] == 10


# =============================================================================
# 2. SNAPSHOT V10 VS LIVE UPDATE V8
# =============================================================================
@pytest.mark.asyncio
async def test_snapshot_v10_vs_live_update_v8(client: AsyncClient, hr_headers: dict):
    """Scenario 2: Live employee is at v8. Snapshot with newer v10 arrives.
    Snapshot v10 updates the employee to v10."""
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
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-CONC-2', 'OldLiveName', 'User', 'live2@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 8, NOW(), 'hash-live-8', false, false, NOW(), NOW()
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-CONC-2,NewerSnapshotName,User,live2@test.com,BLR-CAMPUS-1,ACTIVE,10\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("v10.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    data = upload_res.json()
    assert data["status"] == "COMPLETED"
    assert data["records_updated"] == 1

    with sync_engine.connect() as conn:
        emp = conn.execute(
            text(f"SELECT first_name, source_version FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-CONC-2'")
        ).fetchone()
        assert emp[0] == "NewerSnapshotName"
        assert emp[1] == 10


# =============================================================================
# 3. CONCURRENT CREATE: LIVE INGESTION & SNAPSHOT
# =============================================================================
@pytest.mark.asyncio
async def test_concurrent_create_live_and_snapshot(client: AsyncClient, hr_headers: dict):
    """Scenario 3: Live event creates employee right before snapshot staging delta executes.
    on_conflict_do_nothing absorbs duplicate safely."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-CONCUR-CREATE,SnapshotCreate,User,cc@test.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    original_flush = ReconciliationService._flush_staging_chunk
    injected = False

    async def race_flush(self, staging_rows, duplicate_sample_ids):
        res = await original_flush(self, staging_rows, duplicate_sample_ids)
        nonlocal injected
        if not injected:
            injected = True
            with sync_engine.connect() as c:
                c.execute(
                    text(
                        """
                        INSERT INTO employees (
                            id, tenant_id, employee_id, first_name, last_name, email,
                            office_code, status, source_version, source_timestamp, record_hash,
                            special_assistance_required, transport_review_required, created_at, updated_at
                        ) VALUES (
                            gen_random_uuid(), :tenant_id, 'EMP-CONCUR-CREATE', 'LiveCreate', 'User', 'cc@test.com',
                            'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash-live-cc', false, false, NOW(), NOW()
                        )
                        """
                    ),
                    {"tenant_id": tenant_id},
                )
                c.commit()
        return res

    with patch.object(ReconciliationService, "_flush_staging_chunk", new=race_flush):
        upload_res = await client.post(
            "/api/v1/reconciliation/upload",
            files={"file": ("create.csv", csv_content, "text/csv")},
            headers=hr_headers,
        )

    assert upload_res.status_code == 200
    assert upload_res.json()["status"] == "COMPLETED"

    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-CONCUR-CREATE'")
        ).scalar()
        assert emp_count == 1


# =============================================================================
# 4. CONCURRENT DEACTIVATION: LIVE INGESTION & SNAPSHOT
# =============================================================================
@pytest.mark.asyncio
async def test_concurrent_deactivation_live_and_snapshot(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 4: Live event deactivates employee to INACTIVE before approval deactivates.
    Already inactive employee does NOT produce a duplicate EMPLOYEE_DEACTIVATED outbox event."""
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
                ) VALUES 
                (gen_random_uuid(), :tenant_id, 'EMP-CONC-DEACT-1', 'Active1', 'User', 'a1@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h1', false, false, NOW(), NOW()),
                (gen_random_uuid(), :tenant_id, 'EMP-CONC-DEACT-2', 'Active2', 'User', 'a2@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h2', false, false, NOW(), NOW())
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Before approval executes: Live HR event deactivates EMP-CONC-DEACT-1
    with sync_engine.connect() as conn:
        conn.execute(
            text(f"UPDATE employees SET status = 'INACTIVE' WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-CONC-DEACT-1'")
        )
        # Clear outbox so we can test approval emission
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # Operator approves run
    approve_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-deact", "reason": "Approve deactivation"},
        headers=operator_headers,
    )
    assert approve_res.status_code == 200

    # ONLY EMP-CONC-DEACT-2 emits an outbox event! EMP-CONC-DEACT-1 was already inactive!
    with sync_engine.connect() as conn:
        events = conn.execute(
            text(f"SELECT aggregate_id FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalars().all()
        assert events == ["EMP-CONC-DEACT-2"]


# =============================================================================
# 5. EQUAL VERSION, IDENTICAL CONTENT (IDEMPOTENT NO-OP)
# =============================================================================
@pytest.mark.asyncio
async def test_equal_version_identical_content(client: AsyncClient, hr_headers: dict):
    """Scenario 5: Snapshot row has same source_version and identical content.
    Classified as records_unchanged. Zero mutations, zero events."""
    tenant_id = "tenant-alpha"

    from app.utils.hashing import compute_canonical_record_hash
    chash = compute_canonical_record_hash(
        employee_id="EMP-EQ-SAME",
        first_name="SameName",
        last_name="SameLast",
        email="same@test.com",
        phone=None,
        office_code="BLR-CAMPUS-1",
        department=None,
        manager_id=None,
        status="ACTIVE",
    )

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
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-EQ-SAME', 'SameName', 'SameLast', 'same@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 5, NOW(), :hash, false, false, NOW(), NOW()
                )
                """
            ),
            {"tenant_id": tenant_id, "hash": chash},
        )
        conn.commit()

    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-EQ-SAME,SameName,SameLast,same@test.com,BLR-CAMPUS-1,ACTIVE,5\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("same.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    data = upload_res.json()
    assert data["status"] == "COMPLETED"
    assert data["records_unchanged"] == 1
    assert data["records_updated"] == 0

    with sync_engine.connect() as conn:
        outbox_count = conn.execute(text(f"SELECT count(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")).scalar()
        assert outbox_count == 0


# =============================================================================
# 6. EQUAL VERSION, CONFLICTING CONTENT (DETERMINISTIC HASH TIE-BREAKING)
# =============================================================================
@pytest.mark.asyncio
async def test_equal_version_conflicting_content(client: AsyncClient, hr_headers: dict):
    """Scenario 6: Snapshot row has equal version but conflicting content.
    Deterministic hash tie-break: if snapshot hash is lower, dropped as stale."""
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
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-EQ-DIFF', 'HighHashName', 'User', 'diff@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 5, NOW(), 'zzzzzzzzzzzzzzzz', false, false, NOW(), NOW()
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Snapshot with lower hash
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-EQ-DIFF,LowHashName,User,diff@test.com,BLR-CAMPUS-1,ACTIVE,5\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("diff.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    data = upload_res.json()
    assert data["status"] == "COMPLETED"
    assert data["records_stale"] == 1

    with sync_engine.connect() as conn:
        emp = conn.execute(
            text(f"SELECT first_name FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-EQ-DIFF'")
        ).fetchone()
        assert emp[0] == "HighHashName"


# =============================================================================
# 7. LIVE UPDATE DURING APPROVAL (WATERMARK CUTOFF SAFETY)
# =============================================================================
@pytest.mark.asyncio
async def test_live_update_during_approval(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 7: Employee is created/updated AFTER snapshot started.
    Approval must NOT deactivate this newly created/updated employee."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        # Seed 1 old employee
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-OLD-001', 'Old', 'User', 'old@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'h1', false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Empty export missing EMP-OLD-001
    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Now, while halted awaiting approval: A new employee EMP-NEW-002 is created via live HR event
    with sync_engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-NEW-002', 'New', 'User', 'new@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() + INTERVAL '1 second', 'h2', false, false, NOW() + INTERVAL '1 second', NOW() + INTERVAL '1 second'
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Operator approves run
    approve_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-watermark", "reason": "Approve old deactivations"},
        headers=operator_headers,
    )
    assert approve_res.status_code == 200

    # EMP-OLD-001 MUST be INACTIVE. EMP-NEW-002 MUST REMAIN ACTIVE!
    with sync_engine.connect() as conn:
        old_st = conn.execute(
            text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-OLD-001'")
        ).scalar()
        new_st = conn.execute(
            text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-NEW-002'")
        ).scalar()
        assert old_st == "INACTIVE"
        assert new_st == "ACTIVE"


# =============================================================================
# 8. LIVE UPDATE DURING APPROVAL RECOVERY
# =============================================================================
@pytest.mark.asyncio
async def test_live_update_during_approval_recovery(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 8: During resumed approval, employee updated after snapshot began
    remains protected from wrongful deactivation."""
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
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-REC-OLD', 'Old', 'User', 'ro@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'h1', false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Mark APPROVING
    with sync_engine.connect() as conn:
        conn.execute(
            text(f"UPDATE reconciliation_runs SET status = 'APPROVING' WHERE run_id = '{run_id}'")
        )
        # Concurrent live update with updated_at in future
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), :tenant_id, 'EMP-REC-LATE', 'Late', 'User', 'rl@test.com',
                    'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() + INTERVAL '2 seconds', 'h2', false, false, NOW() + INTERVAL '2 seconds', NOW() + INTERVAL '2 seconds'
                )
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Resume approval
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-rec-late", "reason": "Resume approval late update"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200

    with sync_engine.connect() as conn:
        old_st = conn.execute(
            text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-REC-OLD'")
        ).scalar()
        late_st = conn.execute(
            text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-REC-LATE'")
        ).scalar()
        assert old_st == "INACTIVE"
        assert late_st == "ACTIVE"


# =============================================================================
# 9. TWO RECONCILIATION RUNS FOR THE SAME TENANT
# =============================================================================
@pytest.mark.asyncio
async def test_two_reconciliation_runs_for_same_tenant(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 9: Two reconciliation runs for the same tenant are partitioned by run_id
    and advisory locked per run_id without interfering with each other."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 5):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Name', 'Test', 'email@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW() - INTERVAL '1 hour', 'hash', false, false, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-TWO-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")

    res1 = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty1.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    res2 = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty2.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id1 = res1.json()["run_id"]
    run_id2 = res2.json()["run_id"]
    assert run_id1 != run_id2

    # Operator approves run 1
    appr1 = await client.post(
        f"/api/v1/reconciliation/{run_id1}/approve",
        json={"operator_id": "op-run1", "reason": "Approve run 1"},
        headers=operator_headers,
    )
    assert appr1.status_code == 200

    # Operator aborts run 2
    abort2 = await client.post(
        f"/api/v1/reconciliation/{run_id2}/abort",
        json={"operator_id": "op-run2", "reason": "Abort superseded run 2"},
        headers=operator_headers,
    )
    assert abort2.status_code == 200
    assert abort2.json()["status"] == "ABORTED"


# =============================================================================
# 10. DIFFERENT TENANTS PROCESSING SIMULTANEOUSLY
# =============================================================================
@pytest.mark.asyncio
async def test_different_tenants_processing_simultaneously(client: AsyncClient):
    """Scenario 10: Tenant Alpha and Tenant Beta upload reconciliation snapshots simultaneously.
    Full concurrent execution with zero cross-tenant contamination."""
    tenant_a = "tenant-alpha"
    tenant_b = "tenant-beta"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id IN ('{tenant_a}', '{tenant_b}')"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id IN ('{tenant_a}', '{tenant_b}')"))
        conn.commit()

    # Get JWT tokens for both tenants
    async def get_token(t_id: str, c_id: str, sec: str):
        res = await client.post(
            "/api/v1/auth/token",
            json={"tenant_id": t_id, "client_id": c_id, "client_secret": sec},
        )
        return {"Authorization": f"Bearer {res.json()['access_token']}"}

    headers_a = await get_token(tenant_a, "hr-service", "hr-client-secret-alpha-2026")
    headers_b = await get_token(tenant_b, "hr-service", "hr-client-secret-beta-2026")

    csv_a = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-ALPHA-1,AlphaUser,One,a1@alpha.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-ALPHA-2,AlphaUser,Two,a2@alpha.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    csv_b = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-BETA-1,BetaUser,One,b1@beta.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-BETA-2,BetaUser,Two,b2@beta.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    async def upload_for(headers, csv_data, fname):
        return await client.post(
            "/api/v1/reconciliation/upload",
            files={"file": (fname, csv_data, "text/csv")},
            headers=headers,
        )

    res_a, res_b = await asyncio.gather(
        upload_for(headers_a, csv_a, "alpha.csv"),
        upload_for(headers_b, csv_b, "beta.csv"),
    )

    assert res_a.status_code == 200
    assert res_b.status_code == 200
    assert res_a.json()["records_created"] == 2
    assert res_b.json()["records_created"] == 2

    with sync_engine.connect() as conn:
        count_a = conn.execute(text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_a}'")).scalar()
        count_b = conn.execute(text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_b}'")).scalar()
        assert count_a == 2
        assert count_b == 2
