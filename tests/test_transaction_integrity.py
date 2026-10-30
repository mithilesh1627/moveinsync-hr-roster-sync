"""Transaction Boundary & Atomicity Verification Tests (Check 6)

Verifies:
1. Employee state mutations and transactional outbox events commit atomically.
2. If an error or crash occurs before the database commit finishes, all partial
   changes (employee records, outbox events, idempotency keys) roll back completely.
3. Zero partial or orphan records ever persist in PostgreSQL.
"""

from unittest.mock import patch
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine
from app.services.roster_service import RosterService


@pytest.mark.asyncio
async def test_atomic_employee_and_outbox_commit(client: AsyncClient, hr_headers: dict):
    """Happy path atomicity: Employee and outbox record commit together."""
    emp_id = "EMP-TX-ATOMIC-001"

    res = await client.post(
        "/api/v1/roster/events",
        json={
            "events": [
                {
                    "event_type": "CREATE",
                    "employee_id": emp_id,
                    "source_version": 1,
                    "first_name": "Atomic",
                    "last_name": "Success",
                    "email": "atomic.success@moveinsync.com",
                    "office_code": "BLR-CAMPUS-1",
                }
            ]
        },
        headers=hr_headers,
    )
    assert res.status_code == 200

    with sync_engine.connect() as conn:
        emp = conn.execute(
            text(f"SELECT employee_id FROM employees WHERE employee_id = '{emp_id}'")
        ).fetchone()
        outbox = conn.execute(
            text(f"SELECT id, aggregate_id FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).fetchone()

        assert emp is not None, "Employee record must exist"
        assert outbox is not None, "Outbox event must exist"
        assert outbox[1] == emp_id


@pytest.mark.asyncio
async def test_failure_at_transaction_boundary_rolls_back_everything(
    client: AsyncClient, hr_headers: dict
):
    """Simulate a failure at transaction boundary (e.g. database error during session.commit).
    
    Verifies that NEITHER employee nor outbox event nor idempotency record is persisted.
    """
    emp_id = "EMP-TX-ROLLBACK-001"
    key = "IDEMP-KEY-TX-ROLLBACK"

    headers = dict(hr_headers)
    headers["X-Idempotency-Key"] = key

    # Patch session.commit in app.api.v1.roster to simulate failure at the commit boundary
    with patch("sqlalchemy.ext.asyncio.AsyncSession.commit", side_effect=RuntimeError("Simulated DB commit crash")):
        try:
            await client.post(
                "/api/v1/roster/events",
                json={
                    "events": [
                        {
                            "event_type": "CREATE",
                            "employee_id": emp_id,
                            "source_version": 1,
                            "first_name": "Rollback",
                            "last_name": "Test",
                            "email": "rollback.test@moveinsync.com",
                            "office_code": "BLR-CAMPUS-1",
                        }
                    ]
                },
                headers=headers,
            )
        except RuntimeError:
            pass  # Expected simulated crash

    # CRITICAL INVARIANT: Verify that PostgreSQL contains ZERO trace of this transaction
    with sync_engine.connect() as conn:
        emp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE employee_id = '{emp_id}'")
        ).scalar()
        outbox_count = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '{emp_id}'")
        ).scalar()
        idemp_count = conn.execute(
            text(f"SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = '{key}'")
        ).scalar()

        assert emp_count == 0, "Employee must NOT be committed on transaction rollback"
        assert outbox_count == 0, "Outbox event must NOT be committed on transaction rollback"
        assert idemp_count == 0, "Idempotency key must NOT be committed on transaction rollback"
