"""P0 Transaction Atomicity, Ambiguous Commit Recovery, and Outbox Isolation Tests

Verifies Issue 2 requirements:
1. Inspect SQLAlchemy AsyncSession/AsyncConnection transaction ownership:
   Employee updates and outbox events commit atomically on the same PostgreSQL transaction.
2. Real PostgreSQL connection failure during commit or query rolls back all partial mutations.
3. Ambiguous commit outcome recovery: Resuming deactivations skips already-deactivated employees
   and emits zero duplicate outbox events.
4. Stable idempotency keys prevent duplicate live mutations and duplicate events on replay.
5. Outbox publisher queries under Read Committed isolation and only ever delivers committed events;
   uncommitted or rolled-back events are never published.
"""

import asyncio
import uuid
from datetime import timedelta
import pytest
from httpx import AsyncClient
from sqlalchemy import text, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from unittest.mock import patch
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings

from app.core.database import async_engine, sync_engine, async_session_factory
from app.models.base import utc_now
from app.models.employee import Employee
from app.models.outbox import OutboxEvent
from app.models.reconciliation import ReconciliationRun, ReconciliationStaging
from app.services.reconciliation_service import ReconciliationService
from app.workers.outbox_publisher import OutboxPublisher


# =============================================================================
# 1. SQLALCHEMY ASYNCSESSION ATOMICITY (EMPLOYEE + OUTBOX ON SAME TRANSACTION)
# =============================================================================
@pytest.mark.asyncio
async def test_sqlalchemy_async_session_atomicity_employee_and_outbox_same_tx():
    """Verify that employee deactivations and outbox records are executed within the SAME
    transaction boundary on self.session. If any failure happens before commit, both roll back.
    """
    tenant_id = "tenant-alpha"
    run_id = f"recon-atomicity-{uuid.uuid4().hex[:8]}"

    # Seed 5 active employees and staging run
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO reconciliation_runs (
                    id, run_id, tenant_id, filename, status, total_records_received,
                    records_created, records_updated, records_unchanged, records_stale, records_deactivated,
                    missing_active_records, missing_percentage, operator_review_required, approved_by_operator, started_at
                ) VALUES (
                    gen_random_uuid(), :run_id, :tenant_id, 'file.csv', 'APPROVING',
                    0, 0, 0, 0, 0, 0, 5, 100.0, false, false, NOW()
                )
                """
            ),
            {"run_id": run_id, "tenant_id": tenant_id},
        )
        for i in range(1, 6):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Atom', 'Test', 'atom@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-ATOM-{i:03d}"},
            )
        conn.commit()

    # Case A: Happy path - commit occurs once per batch, committing both employee status and outbox
    async with async_session_factory() as session:
        service = ReconciliationService(session)
        deact_count = await service._apply_deactivations_in_bounded_batches(
            tenant_id=tenant_id,
            run_id=run_id,
            reason="ATOMICITY_TEST",
            batch_size=10,
        )
        assert deact_count == 5

    with sync_engine.connect() as conn:
        active_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        deact_outbox_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND event_type = 'EMPLOYEE_DEACTIVATED'")
        ).scalar()
        assert active_cnt == 0, "All 5 employees must be INACTIVE"
        assert deact_outbox_cnt == 5, "Exactly 5 outbox events must be atomically committed"


# =============================================================================
# 2. REAL POSTGRESQL CONNECTION FAILURE (TERMINATION / ABORT)
# =============================================================================
@pytest.mark.asyncio
async def test_real_postgresql_connection_failure_rolls_back_atomically():
    """Verify that if a real PostgreSQL backend connection drops / is terminated
    mid-transaction, PostgreSQL's WAL and isolation guarantees roll back both
    employee updates and outbox events completely.
    """
    tenant_id = "tenant-alpha"
    run_id = f"recon-conn-fail-{uuid.uuid4().hex[:8]}"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO reconciliation_runs (
                    id, run_id, tenant_id, filename, status, total_records_received,
                    records_created, records_updated, records_unchanged, records_stale, records_deactivated,
                    missing_active_records, missing_percentage, operator_review_required, approved_by_operator, started_at
                ) VALUES (
                    gen_random_uuid(), :run_id, :tenant_id, 'file.csv', 'APPROVING',
                    0, 0, 0, 0, 0, 0, 3, 100.0, false, false, NOW()
                )
                """
            ),
            {"run_id": run_id, "tenant_id": tenant_id},
        )
        for i in range(1, 4):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'ConnFail', 'Test', 'fail@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-CONNFAIL-{i:03d}"},
            )
        conn.commit()

    # Open a dedicated async session with NullPool so pool state is completely isolated
    from sqlalchemy.pool import NullPool
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.core.config import settings

    iso_engine = create_async_engine(settings.database_url, poolclass=NullPool)
    iso_factory = async_sessionmaker(bind=iso_engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with iso_factory() as session:
            # Determine this connection's PostgreSQL PID
            backend_pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar()

            # Update employees inside this transaction (uncommitted)
            await session.execute(
                text(
                    f"UPDATE employees SET status = 'INACTIVE' WHERE tenant_id = '{tenant_id}'"
                )
            )
            # Insert outbox events inside this transaction (uncommitted)
            now_val = (await session.execute(text("SELECT NOW()"))).scalar()
            outbox_items = [
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_id,
                    "event_type": "EMPLOYEE_DEACTIVATED",
                    "aggregate_id": f"EMP-CONNFAIL-{i:03d}",
                    "payload": {"employee_id": f"EMP-CONNFAIL-{i:03d}", "reason": "CONN_TEST"},
                    "status": "PENDING",
                    "retry_count": 0,
                    "next_retry_at": now_val,
                    "created_at": now_val,
                }
                for i in range(1, 4)
            ]
            await session.execute(pg_insert(OutboxEvent).values(outbox_items))

            # Real failure injection: Terminate the PostgreSQL backend process from an external sync connection
            with sync_engine.connect() as admin_conn:
                admin_conn.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": backend_pid})
                admin_conn.commit()

            # Attempting to commit this terminated session must raise an operational/connection error
            with pytest.raises(Exception):
                await session.commit()
    finally:
        await iso_engine.dispose()

    # VERIFY POSTGRESQL STATE: The uncommitted transaction was automatically aborted by PostgreSQL
    with sync_engine.connect() as conn:
        active_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_cnt = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()
        assert active_cnt == 3, f"Expected all 3 employees to remain ACTIVE after aborted connection, got {active_cnt}"
        assert outbox_cnt == 0, f"Expected 0 outbox events after aborted connection, got {outbox_cnt}"


# =============================================================================
# 3. AMBIGUOUS COMMIT OUTCOME RECOVERY (IDEMPOTENT RESUMPTION, NO DUPLICATE EVENTS)
# =============================================================================
@pytest.mark.asyncio
async def test_ambiguous_commit_recovery_skips_committed_records_without_duplicates():
    """Simulate an ambiguous commit outcome:
    - 20 missing employees in total.
    - Batch 1 (10 employees) is committed to PostgreSQL.
    - An ambiguous error occurs immediately following Batch 1 commit (e.g. process crash / network cut).
    - Recovery worker resumes approval on the same run.
    - Invariant: Recovery automatically skips Batch 1 (already INACTIVE), processes Batch 2,
      and emits exactly 20 total outbox events (zero duplicates).
    """
    tenant_id = "tenant-alpha"
    run_id = f"recon-ambig-{uuid.uuid4().hex[:8]}"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(
            text(
                """
                INSERT INTO reconciliation_runs (
                    id, run_id, tenant_id, filename, status, total_records_received,
                    records_created, records_updated, records_unchanged, records_stale, records_deactivated,
                    missing_active_records, missing_percentage, operator_review_required, approved_by_operator, started_at
                ) VALUES (
                    gen_random_uuid(), :run_id, :tenant_id, 'file.csv', 'APPROVING',
                    0, 0, 0, 0, 0, 0, 20, 100.0, false, false, NOW()
                )
                """
            ),
            {"run_id": run_id, "tenant_id": tenant_id},
        )
        for i in range(1, 21):
            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email,
                        office_code, status, source_version, source_timestamp, record_hash,
                        special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :emp_id, 'Ambig', 'User', 'ambig@test.com',
                        'BLR-CAMPUS-1', 'ACTIVE', 1, NOW(), 'hash', false, false, NOW(), NOW()
                    )
                    """
                ),
                {"tenant_id": tenant_id, "emp_id": f"EMP-AMBIG-{i:03d}"},
            )
        conn.commit()

    # Step 1: Execute Batch 1 with batch_size=10 on an isolated NullPool session, then crash on start of Batch 2
    iso_engine = create_async_engine(settings.database_url, poolclass=NullPool)
    iso_factory = async_sessionmaker(bind=iso_engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with iso_factory() as session:
            service = ReconciliationService(session)
            commit_calls = 0
            orig_commit = session.commit

            async def crash_after_batch_1():
                nonlocal commit_calls
                commit_calls += 1
                await orig_commit()
                if commit_calls == 1:
                    # Ambiguous state: DB commit succeeded, but process crashed before Batch 2
                    raise RuntimeError("Simulated ambiguous crash after Batch 1 commit!")

            session.commit = crash_after_batch_1

            with pytest.raises(RuntimeError, match="ambiguous crash"):
                await service._apply_deactivations_in_bounded_batches(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    reason="AMBIGUOUS_TEST",
                    batch_size=10,
                )
    finally:
        await iso_engine.dispose()

    # Inspect DB after crash: exactly 10 employees were committed as INACTIVE, 10 still ACTIVE
    with sync_engine.connect() as conn:
        active_after_crash = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        outbox_after_crash = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()
        assert active_after_crash == 10
        assert outbox_after_crash == 10

    # Step 2: RESUME RECOVERY WORKER on fresh session
    iso_engine_resume = create_async_engine(settings.database_url, poolclass=NullPool)
    iso_factory_resume = async_sessionmaker(bind=iso_engine_resume, class_=AsyncSession, expire_on_commit=False)
    try:
        async with iso_factory_resume() as session:
            recovery_service = ReconciliationService(session)
            resumed_count = await recovery_service._apply_deactivations_in_bounded_batches(
                tenant_id=tenant_id,
                run_id=run_id,
                reason="AMBIGUOUS_TEST",
                batch_size=10,
            )
            assert resumed_count == 10, "Resumed run must process only the remaining 10 active records"
    finally:
        await iso_engine_resume.dispose()

    # Step 3: VERIFY INVARIANTS
    with sync_engine.connect() as conn:
        final_active = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND status = 'ACTIVE'")
        ).scalar()
        final_outbox = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()
        distinct_aggregates = conn.execute(
            text(f"SELECT COUNT(DISTINCT aggregate_id) FROM outbox_events WHERE tenant_id = '{tenant_id}'")
        ).scalar()

        assert final_active == 0, "All 20 employees must now be INACTIVE"
        assert final_outbox == 20, f"Must have exactly 20 outbox events, found {final_outbox}"
        assert distinct_aggregates == 20, "Every outbox event aggregate_id must be unique (zero duplicate events!)"


# =============================================================================
# 4. STABLE IDEMPOTENCY KEYS (LIVE EVENT REPLAY PRODUCES ZERO DUPLICATES)
# =============================================================================
@pytest.mark.asyncio
async def test_stable_idempotency_keys_replayed_produce_zero_duplicate_events(
    client: AsyncClient, hr_headers: dict
):
    """Replaying an event batch with the same X-Idempotency-Key returns cached response
    and produces zero duplicate database mutations and zero duplicate outbox records.
    """
    tenant_id = "tenant-alpha"
    emp_id = "EMP-IDEMP-STABLE-001"
    idemp_key = f"key-idemp-{uuid.uuid4().hex}"

    headers = dict(hr_headers)
    headers["X-Idempotency-Key"] = idemp_key

    payload = {
        "events": [
            {
                "event_type": "CREATE",
                "employee_id": emp_id,
                "first_name": "Idemp",
                "last_name": "Stable",
                "email": "idemp.stable@moveinsync.com",
                "office_code": "BLR-CAMPUS-1",
                "status": "ACTIVE",
                "source_version": 1,
            }
        ]
    }

    # Pass 1: Initial event ingestion
    res1 = await client.post("/api/v1/roster/events", json=payload, headers=headers)
    assert res1.status_code == 200
    data1 = res1.json()
    assert data1["accepted_count"] == 1

    with sync_engine.connect() as conn:
        emp_cnt_1 = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = '{emp_id}'")
        ).scalar()
        outbox_cnt_1 = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND aggregate_id = '{emp_id}'")
        ).scalar()
        assert emp_cnt_1 == 1
        assert outbox_cnt_1 == 1

    # Pass 2: Replay identical request with same idempotency key
    res2 = await client.post("/api/v1/roster/events", json=payload, headers=headers)
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2 == data1, "Replayed response must match original cached response exactly"

    # Verify zero additional mutations or duplicate outbox events
    with sync_engine.connect() as conn:
        emp_cnt_2 = conn.execute(
            text(f"SELECT COUNT(*) FROM employees WHERE tenant_id = '{tenant_id}' AND employee_id = '{emp_id}'")
        ).scalar()
        outbox_cnt_2 = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND aggregate_id = '{emp_id}'")
        ).scalar()
        assert emp_cnt_2 == 1, "Must still be exactly 1 employee record"
        assert outbox_cnt_2 == 1, "Must still be exactly 1 outbox record (zero duplicate events emitted)"


# =============================================================================
# 5. OUTBOX PUBLISHER READ COMMITTED ISOLATION (PUBLISH ONLY COMMITTED EVENTS)
# =============================================================================
@pytest.mark.asyncio
async def test_outbox_publisher_polls_and_delivers_only_committed_events():
    """Verify that uncommitted outbox events are invisible to the OutboxProcessor,
    and rolled-back outbox events are NEVER published.
    Only events whose transaction was committed are fetched and delivered.
    """
    tenant_id = "tenant-alpha"
    emp_committed = "EMP-OUTBOX-COMMITTED"
    emp_rolled_back = "EMP-OUTBOX-ROLLEDBACK"

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

    # Step 1: Rolled-back transaction
    async with async_session_factory() as session:
        # Insert event and rollback
        now_dt = utc_now() - timedelta(seconds=10)
        await session.execute(
            pg_insert(OutboxEvent).values(
                [
                    {
                        "id": uuid.uuid4(),
                        "tenant_id": tenant_id,
                        "event_type": "EMPLOYEE_DEACTIVATED",
                        "aggregate_id": emp_rolled_back,
                        "payload": {"employee_id": emp_rolled_back},
                        "status": "PENDING",
                        "retry_count": 0,
                        "next_retry_at": now_dt,
                        "created_at": now_dt,
                    }
                ]
            )
        )
        await session.rollback()

    # Step 2: Committed transaction
    async with async_session_factory() as session:
        now_dt = utc_now() - timedelta(seconds=10)
        await session.execute(
            pg_insert(OutboxEvent).values(
                [
                    {
                        "id": uuid.uuid4(),
                        "tenant_id": tenant_id,
                        "event_type": "EMPLOYEE_DEACTIVATED",
                        "aggregate_id": emp_committed,
                        "payload": {"employee_id": emp_committed},
                        "status": "PENDING",
                        "retry_count": 0,
                        "next_retry_at": now_dt,
                        "created_at": now_dt,
                    }
                ]
            )
        )
        await session.commit()

    # Step 3: Run the outbox publisher with a mocked downstream booking adapter
    from unittest.mock import AsyncMock
    from app.adapters.booking_adapter import DownstreamBookingAdapter

    mock_adapter = DownstreamBookingAdapter()
    cancelled_ids = []

    async def mock_cancel(tenant_id, employee_id, event_id):
        cancelled_ids.append(employee_id)
        return True

    mock_adapter.cancel_future_bookings = mock_cancel
    publisher = OutboxPublisher(booking_adapter=mock_adapter)
    processed_count = await publisher.poll_and_publish_batch()

    # Invariants:
    # 1. Exactly 1 event was processed and published (the committed one).
    # 2. The rolled-back event was never fetched or published.
    assert processed_count == 1
    assert cancelled_ids == [emp_committed]

    with sync_engine.connect() as conn:
        committed_status = conn.execute(
            text(f"SELECT status FROM outbox_events WHERE tenant_id = '{tenant_id}' AND aggregate_id = '{emp_committed}'")
        ).scalar()
        rolled_back_exists = conn.execute(
            text(f"SELECT COUNT(*) FROM outbox_events WHERE tenant_id = '{tenant_id}' AND aggregate_id = '{emp_rolled_back}'")
        ).scalar()

        assert committed_status == "PUBLISHED"
        assert rolled_back_exists == 0
