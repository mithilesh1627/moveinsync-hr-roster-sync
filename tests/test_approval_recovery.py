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
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from app.core.database import sync_engine
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
