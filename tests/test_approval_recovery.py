"""Operator Approval Concurrency & Crash Recovery Tests (P1)

Verifies:
1. Operator approval is concurrency-safe: simultaneous /approve requests on the same
   halted run result in exactly one winner (200 OK) and the other receiving 409 Conflict.
2. The APPROVING state is atomically acquired to prevent race conditions.
3. Already-deactivated employees do not generate duplicate cancellation outbox events.
4. If approval process crashes and restarts, re-running approval deactivates remaining
   employees idempotently without duplicating outbox events.
"""

import asyncio
import io
import pytest
from unittest.mock import patch
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from app.core.database import async_engine, sync_engine
from app.main import app
from app.services.reconciliation_service import ReconciliationService


@pytest.mark.asyncio
async def test_simultaneous_operator_approvals_concurrency_safety(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Simultaneous /approve requests on the same run: exactly one succeeds, other gets 409."""
    tenant_id = "tenant-alpha"

    # Seed 10 active employees
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 11):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Name', 'Test', 'email@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-CONCUR-APPROVE-{i:03d}"},
            )
        conn.commit()

    # Trigger safety brake (empty export, 100% missing)
    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Send 2 simultaneous /approve requests
    payload = {"operator_id": "operator-concur", "reason": "Authorized bulk deactivation"}

    async def call_approve(op_id: str):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": op_id, "reason": "Simultaneous approval test"},
                headers=operator_headers,
            )

    res1, res2 = await asyncio.gather(call_approve("op-1"), call_approve("op-2"))

    statuses = [res1.status_code, res2.status_code]
    assert 200 in statuses, f"Expected one 200 OK: {statuses}"
    assert 409 in statuses, f"Expected one 409 Conflict: {statuses}"

    # Verify that all 10 employees were deactivated, and exactly 10 outbox events exist
    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        deact_outbox_count = conn.execute(
            text(
                f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'"
            )
        ).scalar()
        assert active_count == 0, f"Expected 0 active employees, found {active_count}"
        assert deact_outbox_count == 10, f"Expected exactly 10 outbox events, found {deact_outbox_count}"


@pytest.mark.asyncio
async def test_approval_prevents_duplicate_events_for_already_inactive_employees(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """If an employee is already INACTIVE prior to approval, NO duplicate deactivation event is emitted."""
    tenant_id = "tenant-alpha"

    # Seed 2 employees: 1 ACTIVE, 1 ALREADY INACTIVE
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES 
                (gen_random_uuid(), :tenant_id, 'EMP-ACT-001', 'Active', 'User', 'act@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h1', false, false, NOW(), NOW()),
                (gen_random_uuid(), :tenant_id, 'EMP-INACT-002', 'Inactive', 'User', 'inact@test.com', 'BLR-CAMPUS-1', 'INACTIVE', 1, NOW(), 'h2', false, false, NOW(), NOW())
                """
            ),
            {"tenant_id": tenant_id},
        )
        # Clear outbox
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # Empty export missing EMP-ACT-001 (100% missing of active workforce)
    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Operator approves run
    approve_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-single", "reason": "Approve deactivation"},
        headers=operator_headers,
    )
    assert approve_res.status_code == 200
    assert approve_res.json()["records_deactivated"] == 1

    # Verify that ONLY EMP-ACT-001 received an outbox deactivation event! EMP-INACT-002 did not!
    with sync_engine.connect() as conn:
        outbox_emps = conn.execute(
            text(
                f"SELECT aggregate_id FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'"
            )
        ).scalars().all()
        assert outbox_emps == ["EMP-ACT-001"], f"Unexpected outbox events: {outbox_emps}"


# =============================================================================
# 3. ACTUAL OVERLAPPING CONCURRENT APPROVALS
# =============================================================================
@pytest.mark.asyncio
async def test_actual_overlapping_simultaneous_approvals(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Verify that when an approval request is in flight, an overlapping concurrent request
    is rejected with 409 Conflict via PostgreSQL advisory locking."""
    tenant_id = "tenant-alpha"

    # Seed 5 active employees
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 6):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Name', 'Test', 'email@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-OVERLAP-{i:03d}"},
            )
        conn.commit()

    # Trigger safety brake
    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Hook into deactivations to introduce an artificial 0.15s delay to ensure genuine overlap
    original_apply = ReconciliationService._apply_deactivations_in_bounded_batches

    async def slow_apply(self, *args, **kwargs):
        await asyncio.sleep(0.15)
        return await original_apply(self, *args, **kwargs)

    async def first_request():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": "op-primary", "reason": "Primary approval with work"},
                headers=operator_headers,
            )

    async def racing_request():
        await asyncio.sleep(0.04)  # Ensure first request has acquired the advisory lock
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": "op-racing", "reason": "Racing approval during active lock"},
                headers=operator_headers,
            )

    with patch.object(ReconciliationService, "_apply_deactivations_in_bounded_batches", new=slow_apply):
        res_primary, res_racing = await asyncio.gather(first_request(), racing_request())

    assert res_primary.status_code == 200, f"Primary request failed: {res_primary.text}"
    assert res_racing.status_code == 409, f"Racing request did not get 409: {res_racing.text}"
    error_msg = res_racing.json().get("detail") or res_racing.json().get("error", {}).get("message", "")
    assert "already" in error_msg.lower()

    # Verify final database state: all 5 deactivated and 5 outbox events
    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_count == 0
        assert outbox_count == 5


# =============================================================================
# 4. ADVISORY LOCK RELEASED ON UNHANDLED EXCEPTION
# =============================================================================
@pytest.mark.asyncio
async def test_advisory_lock_released_on_unhandled_exception(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """When an unhandled exception crashes deactivation inside approve_run,
    the advisory lock must be released in the finally block so subsequent requests are not locked out."""
    tenant_id = "tenant-alpha"

    # Seed 2 employees
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 3):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Name', 'Test', 'email@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-EXC-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]

    # Patch _apply_deactivations_in_bounded_batches to raise unexpected error
    async def crash_apply(self, *args, **kwargs):
        raise ZeroDivisionError("Simulated catastrophic crash inside deactivation loop!")

    with patch.object(ReconciliationService, "_apply_deactivations_in_bounded_batches", new=crash_apply):
        try:
            await client.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": "op-crash", "reason": "Crash test"},
                headers=operator_headers,
            )
        except Exception:
            pass  # Exception may bubble up in test client

    # Crucial check: verify that the advisory lock was released and is NOT held in PostgreSQL!
    async with async_engine.connect() as conn:
        lock_res = await conn.execute(
            text("SELECT pg_try_advisory_lock(hashtext('recon_approve'), hashtext(:run_id))"),
            {"run_id": run_id},
        )
        acquired = lock_res.scalar()
        assert acquired is True, f"Advisory lock for {run_id} remained held after crash!"
        # Clean up test lock
        await conn.execute(
            text("SELECT pg_advisory_unlock(hashtext('recon_approve'), hashtext(:run_id))"),
            {"run_id": run_id},
        )


# =============================================================================
# 5. CONNECTION POOL REUSE AFTER APPROVAL
# =============================================================================
@pytest.mark.asyncio
async def test_connection_pool_reuse_after_approval(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Verify that multiple connections borrowed from the pool after approval
    have no residual advisory locks or poisoned transaction state."""
    tenant_id = "tenant-alpha"

    # Seed 2 employees
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 3):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Name', 'Test', 'email@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-POOL-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    approve_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-pool", "reason": "Pool test"},
        headers=operator_headers,
    )
    assert approve_res.status_code == 200

    # Borrow 5 connections sequentially and verify all are clean
    for c_idx in range(5):
        async with async_engine.connect() as conn:
            lock_res = await conn.execute(
                text("SELECT pg_try_advisory_lock(hashtext('recon_approve'), hashtext(:run_id))"),
                {"run_id": run_id},
            )
            assert lock_res.scalar() is True, f"Connection {c_idx} was poisoned with residual lock!"
            await conn.execute(
                text("SELECT pg_advisory_unlock(hashtext('recon_approve'), hashtext(:run_id))"),
                {"run_id": run_id},
            )


# =============================================================================
# 6. LIVE UPDATE RACING WITH DELTA SYNC (VERSION REVALIDATION UNDER LOCK)
# =============================================================================
@pytest.mark.asyncio
async def test_live_update_racing_with_delta_sync(
    client: AsyncClient, hr_headers: dict
):
    """Verify that delta application acquires row locks and revalidates source versions:
    an older snapshot record cannot overwrite a newer live-updated employee."""
    tenant_id = "tenant-alpha"

    # Seed 2 employees:
    # EMP-LIVE-001 has already been updated to version 10 by a live HR event
    # EMP-LIVE-002 is at version 2
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO employees (
                    id, tenant_id, employee_id, first_name, last_name, email,
                    office_code, status, source_version, source_timestamp, record_hash,
                    special_assistance_required, transport_review_required, created_at, updated_at
                ) VALUES
                (gen_random_uuid(), :tenant_id, 'EMP-LIVE-001', 'LiveUpdatedName', 'User', 'live@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 10, NOW(), 'hash-live-10', false, false, NOW(), NOW()),
                (gen_random_uuid(), :tenant_id, 'EMP-LIVE-002', 'OldName', 'User', 'old@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 2, NOW(), 'hash-old-2', false, false, NOW(), NOW())
                """
            ),
            {"tenant_id": tenant_id},
        )
        conn.commit()

    # Nightly CSV snapshot contains:
    # EMP-LIVE-001 with source_version = 8 (STALE! Export generated before live event version 10)
    # EMP-LIVE-002 with source_version = 3 (FRESH! Newer than version 2)
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-LIVE-001,StaleSnapshotName,User,live@test.com,BLR-CAMPUS-1,ACTIVE,8\n"
        "EMP-LIVE-002,FreshSnapshotName,User,old@test.com,BLR-CAMPUS-1,ACTIVE,3\n"
    ).encode("utf-8")

    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("nightly.csv", csv_content, "text/csv")},
        headers=hr_headers,
    )
    assert upload_res.status_code == 200
    run_data = upload_res.json()
    assert run_data["status"] == "COMPLETED"
    assert run_data["records_stale"] == 1
    assert run_data["records_updated"] == 1

    # Verify in DB:
    # EMP-LIVE-001 MUST NOT have been overwritten: first_name stays 'LiveUpdatedName', version stays 10
    # EMP-LIVE-002 MUST have been updated: first_name is 'FreshSnapshotName', version is 3
    with sync_engine.connect() as conn:
        emp1 = conn.execute(
            text(f"SELECT first_name, source_version FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-LIVE-001'")
        ).fetchone()
        assert emp1[0] == "LiveUpdatedName"
        assert emp1[1] == 10

        emp2 = conn.execute(
            text(f"SELECT first_name, source_version FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-LIVE-002'")
        ).fetchone()
        assert emp2[0] == "FreshSnapshotName"
        assert emp2[1] == 3


# =============================================================================
# 7. EMPLOYEE TEMPORARILY LOCKED PREVENTS PREMATURE COMPLETION
# =============================================================================
@pytest.mark.asyncio
async def test_employee_temporarily_locked_prevents_premature_completion(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """When an eligible active employee is temporarily row-locked by a concurrent transaction,
    SKIP LOCKED skips that row. Reconciliation MUST NOT mark the run COMPLETED while eligible
    rows remain. It must raise/return 409 and stay in APPROVING state until resumed."""
    tenant_id = "tenant-alpha"

    # Seed 2 active employees
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
                (gen_random_uuid(), :tenant_id, 'EMP-LOCKED-001', 'Locked', 'User', 'locked@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h1', false, false, NOW(), NOW()),
                (gen_random_uuid(), :tenant_id, 'EMP-FREE-002', 'Free', 'User', 'free@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h2', false, false, NOW(), NOW())
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
    assert upload_res.status_code == 200
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Open a separate connection, start a transaction, and hold a row lock on EMP-LOCKED-001
    async with async_engine.connect() as lock_conn:
        lock_tx = await lock_conn.begin()
        await lock_conn.execute(
            text(f"SELECT id FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-LOCKED-001' FOR UPDATE")
        )

        # While EMP-LOCKED-001 is held with FOR UPDATE, operator calls /approve
        # _apply_deactivations_in_bounded_batches will deactivate EMP-FREE-002, skip EMP-LOCKED-001,
        # fail the final check, and raise RuntimeError which returns 409 Conflict.
        approve_res = await client.post(
            f"/api/v1/reconciliation/{run_id}/approve",
            json={"operator_id": "op-lock", "reason": "Test locked completion semantics"},
            headers=operator_headers,
        )
        assert approve_res.status_code == 409
        err_detail = approve_res.json().get("detail") or approve_res.json().get("error", {}).get("message", "")
        assert "temporarily locked" in err_detail.lower()

        # Verify run in DB is APPROVING (NOT COMPLETED!)
        with sync_engine.connect() as conn:
            run_status = conn.execute(
                text(f"SELECT status FROM reconciliation_runs WHERE run_id = '{run_id}'")
            ).scalar()
            assert run_status == "APPROVING", f"Expected APPROVING, got {run_status}"

            # Verify EMP-FREE-002 was deactivated and EMP-LOCKED-001 is still ACTIVE
            emp_locked_status = conn.execute(
                text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-LOCKED-001'")
            ).scalar()
            emp_free_status = conn.execute(
                text(f"SELECT status FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-FREE-002'")
            ).scalar()
            assert emp_locked_status == "ACTIVE"
            assert emp_free_status == "INACTIVE"

        # Release the lock by rolling back the locking transaction
        await lock_tx.rollback()

    # Now that the lock is released, operator resumes approval
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-lock", "reason": "Resume after lock release"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"

    # Both are now INACTIVE, and exactly 2 outbox events exist
    with sync_engine.connect() as conn:
        active_remaining = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_remaining == 0
        assert outbox_count == 2


# =============================================================================
# 8. CONCURRENT EMPLOYEE CREATION (RECONCILIATION & LIVE INGESTION)
# =============================================================================
@pytest.mark.asyncio
async def test_concurrent_employee_creation_reconciliation_and_live(
    client: AsyncClient, hr_headers: dict
):
    """When a new employee is concurrently created via live ingestion during reconciliation
    staging delta application, on_conflict_do_nothing prevents duplicate key violations."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # CSV snapshot contains 2 new employees
    csv_content = (
        "employee_id,first_name,last_name,email,office_code,status,source_version\n"
        "EMP-CONCUR-001,Concur1,Test,c1@test.com,BLR-CAMPUS-1,ACTIVE,1\n"
        "EMP-CONCUR-002,Concur2,Test,c2@test.com,BLR-CAMPUS-1,ACTIVE,1\n"
    ).encode("utf-8")

    # Hook _flush_staging_chunk to inject live employee creation right before delta processing
    original_flush = ReconciliationService._flush_staging_chunk
    injected = False

    async def race_flush_hook(self, staging_rows, duplicate_sample_ids):
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
                            gen_random_uuid(), :tenant_id, 'EMP-CONCUR-001', 'LiveConcur1', 'Test', 'c1@test.com',
                            'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash-live', false, false, NOW(), NOW()
                        )
                        """
                    ),
                    {"tenant_id": tenant_id},
                )
                c.commit()
        return res

    with patch.object(ReconciliationService, "_flush_staging_chunk", new=race_flush_hook):
        upload_res = await client.post(
            "/api/v1/reconciliation/upload",
            files={"file": ("concur_create.csv", csv_content, "text/csv")},
            headers=hr_headers,
        )

    # Reconciliation must complete without unique constraint violation crash
    assert upload_res.status_code == 200
    assert upload_res.json()["status"] == "COMPLETED"

    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}'")
        ).scalar()
        assert emp_count == 2
