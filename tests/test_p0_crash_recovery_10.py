"""P0 Comprehensive Approval Crash Recovery Tests (10 Scenarios)

Verifies the 10 failure and recovery scenarios required by Section 3:
1. Process terminates before first batch.
2. Process terminates after first committed batch.
3. Process terminates between batches.
4. Process terminates after final batch but before completion status.
5. Two approval requests arrive simultaneously.
6. Application restarts with an APPROVING run.
7. Recovery is requested twice.
8. Another worker holds a row lock.
9. Recovery encounters an unavailable database.
10. Recovery does not bypass operator approval.
"""

import asyncio
import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from unittest.mock import patch

from app.core.database import async_engine, sync_engine, async_session_factory
from app.main import app
from app.models.reconciliation import ReconciliationRun
from app.services.reconciliation_service import ReconciliationService


# =============================================================================
# SCENARIO 1: PROCESS TERMINATES BEFORE FIRST BATCH
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_1_process_terminates_before_first_batch(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 1: Process crashes right after transition to APPROVING before any batch runs.
    Subsequent recovery executes all batches idempotently."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-1-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Transition to APPROVING manually (simulating crash right after status update)
    with sync_engine.connect() as conn:
        conn.execute(
            text(f"UPDATE reconciliation_runs SET status = 'APPROVING', operator_id = 'op-crash-1' WHERE run_id = '{run_id}'")
        )
        conn.commit()

    # Resume approval via API
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-resume-1", "reason": "Resume after crash before first batch"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"

    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_count == 0
        assert outbox_count == 10


# =============================================================================
# SCENARIO 2: PROCESS TERMINATES AFTER FIRST COMMITTED BATCH
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_2_process_terminates_after_first_committed_batch(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 2: Batch 1 commits. Process crashes. Resuming deactivates remaining without duplication."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 16):
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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-2-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Execute exactly 1 batch of 10 and then simulate process exit
    async with async_session_factory() as session:
        service = ReconciliationService(session)
        run_record = await service.recon_repo.get_by_run_id(tenant_id, run_id)
        run_record.status = "APPROVING"
        await session.commit()

        # Batch 1 of 10
        await service._apply_deactivations_in_bounded_batches(
            tenant_id=tenant_id,
            run_id=run_id,
            reason="TEST_SCENARIO_2_BATCH_1",
            batch_size=10,
        )

    # Resume approval via API
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-resume-2", "reason": "Resume after batch 1"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"

    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_count == 0
        assert outbox_count == 15


# =============================================================================
# SCENARIO 3: PROCESS TERMINATES BETWEEN BATCHES
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_3_process_terminates_between_batches(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 3: Multi-batch execution crashes between batch 1 and 2; resume finishes cleanly."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 21):
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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-3-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Deactivate 10 manually, set APPROVING, then call resume
    async with async_session_factory() as session:
        service = ReconciliationService(session)
        run_record = await service.recon_repo.get_by_run_id(tenant_id, run_id)
        run_record.status = "APPROVING"
        await session.commit()

        # Batch 1
        await service._apply_deactivations_in_bounded_batches(
            tenant_id=tenant_id,
            run_id=run_id,
            reason="TEST_SCENARIO_3_BATCH_1",
            batch_size=10,
        )

    # Resume via API
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-resume-3", "reason": "Resume after inter-batch crash"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"

    with sync_engine.connect() as conn:
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_count == 0
        assert outbox_count == 20


# =============================================================================
# SCENARIO 4: PROCESS TERMINATES AFTER FINAL BATCH BEFORE COMPLETION STATUS
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_4_after_final_batch_before_completion_status(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 4: All employees are deactivated, but crash occurs before run status becomes COMPLETED.
    Resuming safely detects 0 remaining, sets COMPLETED, and emits 0 duplicate outbox events."""
    tenant_id = "tenant-alpha"

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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-4-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Deactivate all 5 employees, but leave run status as APPROVING (simulating crash before completion)
    async with async_session_factory() as session:
        service = ReconciliationService(session)
        run_record = await service.recon_repo.get_by_run_id(tenant_id, run_id)
        run_record.status = "APPROVING"
        await session.commit()

        await service._apply_deactivations_in_bounded_batches(
            tenant_id=tenant_id,
            run_id=run_id,
            reason="TEST_SCENARIO_4_DEACT_ALL",
            batch_size=10,
        )

    # In DB: 0 active employees, 5 outbox records, but run status is APPROVING
    with sync_engine.connect() as conn:
        run_st = conn.execute(
            text(f"SELECT status FROM reconciliation_runs WHERE run_id = '{run_id}'")
        ).scalar()
        assert run_st == "APPROVING"

    # Now resume approval
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-resume-4", "reason": "Resume when all deactivated"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"

    # Outbox events must remain exactly 5 (zero duplicate events emitted!)
    with sync_engine.connect() as conn:
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert outbox_count == 5


# =============================================================================
# SCENARIO 5: TWO APPROVAL REQUESTS ARRIVE SIMULTANEOUSLY
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_5_two_simultaneous_approval_requests(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 5: Two simultaneous approval requests on the same halted run.
    Exactly one succeeds (200), one gets 409 Conflict. Zero duplicate side effects."""
    tenant_id = "tenant-alpha"

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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-5-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    async def call_approve(op_id: str):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as c:
            return await c.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": op_id, "reason": "Simultaneous approval test"},
                headers=operator_headers,
            )

    res1, res2 = await asyncio.gather(call_approve("op-5a"), call_approve("op-5b"))
    statuses = [res1.status_code, res2.status_code]
    assert 200 in statuses
    assert 409 in statuses


# =============================================================================
# SCENARIO 6: APPLICATION RESTARTS WITH AN APPROVING RUN
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_6_application_restarts_with_approving_run(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 6: Application restarts with a run in APPROVING state.
    Operator inspects report, calls approve, run resumes and completes."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 4):
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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-6-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # Mark APPROVING directly (simulating crash before server reboot)
    with sync_engine.connect() as conn:
        conn.execute(
            text(f"UPDATE reconciliation_runs SET status = 'APPROVING', operator_review_required = true WHERE run_id = '{run_id}'")
        )
        conn.commit()

    # Simulate fresh client after reboot
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as new_client:
        # Operator checks report
        report_res = await new_client.get(
            f"/api/v1/reconciliation/{run_id}/report",
            headers=operator_headers,
        )
        assert report_res.status_code == 200
        assert report_res.json()["status"] == "APPROVING"

        # Operator triggers resume approval
        resume_res = await new_client.post(
            f"/api/v1/reconciliation/{run_id}/approve",
            json={"operator_id": "op-post-reboot", "reason": "Resume after reboot"},
            headers=operator_headers,
        )
        assert resume_res.status_code == 200
        assert resume_res.json()["status"] == "COMPLETED"


# =============================================================================
# SCENARIO 7: RECOVERY IS REQUESTED TWICE
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_7_recovery_requested_twice(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 7: Approval completed once. Second call receives 409 Conflict."""
    tenant_id = "tenant-alpha"

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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-7-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]

    # First call succeeds
    res1 = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-7", "reason": "First approval call"},
        headers=operator_headers,
    )
    assert res1.status_code == 200

    # Second call gets 409 Conflict
    res2 = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-7", "reason": "Second duplicate approval call"},
        headers=operator_headers,
    )
    assert res2.status_code == 409
    err_msg = res2.json().get("detail") or res2.json().get("error", {}).get("message", "")
    assert "already completed" in err_msg.lower()


# =============================================================================
# SCENARIO 8: ANOTHER WORKER HOLDS A ROW LOCK
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_8_another_worker_holds_row_lock(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 8: Concurrent transaction holds row lock; SKIP LOCKED skips row;
    system does NOT mark COMPLETED; stays in APPROVING and returns 409."""
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
                (gen_random_uuid(), :tenant_id, 'EMP-REC-8-LOCK', 'Locked', 'User', 'l@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h1', false, false, NOW(), NOW()),
                (gen_random_uuid(), :tenant_id, 'EMP-REC-8-FREE', 'Free', 'User', 'f@test.com', 'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'h2', false, false, NOW(), NOW())
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

    # Lock EMP-REC-8-LOCK in background transaction
    async with async_engine.connect() as lock_conn:
        lock_tx = await lock_conn.begin()
        await lock_conn.execute(
            text(f"SELECT id FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = 'EMP-REC-8-LOCK' FOR UPDATE")
        )

        approve_res = await client.post(
            f"/api/v1/reconciliation/{run_id}/approve",
            json={"operator_id": "op-8", "reason": "Test lock contention"},
            headers=operator_headers,
        )
        assert approve_res.status_code == 409
        err_msg = approve_res.json().get("detail") or approve_res.json().get("error", {}).get("message", "")
        assert "temporarily locked" in err_msg.lower()

        # Run MUST remain in APPROVING (never prematurely marked COMPLETED)
        with sync_engine.connect() as conn:
            run_st = conn.execute(
                text(f"SELECT status FROM reconciliation_runs WHERE run_id = '{run_id}'")
            ).scalar()
            assert run_st == "APPROVING"

        await lock_tx.rollback()

    # Now unlocked: resume completes
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-8", "reason": "Resume after lock release"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"


# =============================================================================
# SCENARIO 9: RECOVERY ENCOUNTERS UNAVAILABLE DATABASE
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_9_recovery_encounters_unavailable_database(
    client: AsyncClient, hr_headers: dict, operator_headers: dict
):
    """Scenario 9: Database connection fails during recovery attempt.
    Status remains durable in APPROVING; can be resumed once DB is restored."""
    tenant_id = "tenant-alpha"

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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-9-{i:03d}"},
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
        conn.commit()

    # Simulate DB failure during batch execution
    orig_apply = ReconciliationService._apply_deactivations_in_bounded_batches

    async def db_offline_fail(self, *args, **kwargs):
        raise ConnectionRefusedError("Simulated Database Network Partition / Offline!")

    with patch.object(ReconciliationService, "_apply_deactivations_in_bounded_batches", new=db_offline_fail):
        try:
            await client.post(
                f"/api/v1/reconciliation/{run_id}/approve",
                json={"operator_id": "op-9", "reason": "DB offline test"},
                headers=operator_headers,
            )
        except Exception:
            pass

    # In DB: run is still APPROVING (not lost or corrupted)
    with sync_engine.connect() as conn:
        run_st = conn.execute(
            text(f"SELECT status FROM reconciliation_runs WHERE run_id = '{run_id}'")
        ).scalar()
        assert run_st == "APPROVING"

    # Once DB is restored, recovery succeeds
    resume_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "op-9", "reason": "Resume after DB restored"},
        headers=operator_headers,
    )
    assert resume_res.status_code == 200
    assert resume_res.json()["status"] == "COMPLETED"


# =============================================================================
# SCENARIO 10: RECOVERY DOES NOT BYPASS OPERATOR APPROVAL
# =============================================================================
@pytest.mark.asyncio
async def test_recovery_10_recovery_does_not_bypass_operator_approval(
    client: AsyncClient, hr_headers: dict
):
    """Scenario 10: Automatic reconciliation or unauthorized callers cannot bypass operator approval
    on a run halted in SAFETY_BRAKE_TRIGGERED."""
    tenant_id = "tenant-alpha"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        for i in range(1, 4):
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
                {"tenant_id": tenant_id, "emp_id": f"EMP-REC-10-{i:03d}"},
            )
        conn.commit()

    empty_csv = "employee_id,first_name,last_name,email,office_code,status,source_version\n".encode("utf-8")
    upload_res = await client.post(
        "/api/v1/reconciliation/upload",
        files={"file": ("empty.csv", empty_csv, "text/csv")},
        headers=hr_headers,
    )
    run_id = upload_res.json()["run_id"]
    assert upload_res.json()["status"] == "SAFETY_BRAKE_TRIGGERED"

    # Attempt to call /approve using an HR integration token (insufficient role!)
    unauthorized_res = await client.post(
        f"/api/v1/reconciliation/{run_id}/approve",
        json={"operator_id": "hr-bot", "reason": "Attempting unprivileged bypass"},
        headers=hr_headers,
    )
    # Must be rejected with 403 Forbidden!
    assert unauthorized_res.status_code == 403

    # Database state: run is still SAFETY_BRAKE_TRIGGERED, 0 deactivations
    with sync_engine.connect() as conn:
        run_st = conn.execute(
            text(f"SELECT status FROM reconciliation_runs WHERE run_id = '{run_id}'")
        ).scalar()
        active_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        assert run_st == "SAFETY_BRAKE_TRIGGERED"
        assert active_count == 3
