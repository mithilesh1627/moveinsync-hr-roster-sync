"""Benchmark Verification Test (REQ-D6)

Validates streaming reconciliation diffing with bounded memory and throughput.
"""

import io
import time
import pytest
import psutil
import os
from httpx import AsyncClient
from sqlalchemy import text
from app.core.database import sync_engine, async_session_factory, async_engine
from app.services.reconciliation_service import ReconciliationService
from app.utils.hashing import compute_canonical_record_hash
from tests.test_safety_brake import generate_roster_csv


@pytest.mark.asyncio
async def test_streaming_reconciliation_bounded_memory_and_high_throughput():
    """Verify that streaming reconciliation diffs thousands of rows with strictly bounded memory delta (< 50MB)."""
    tenant_id = "tenant-benchmark-test"
    count = 10_000

    # Seed baseline in PostgreSQL
    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}';"))
        conn.commit()

    rows = []
    for i in range(1, count + 1):
        emp_id = f"EMP-SCALE-{i:05d}"
        fn = f"User{i}"
        ln = "Scale"
        em = f"user{i}@moveinsync.com"
        ph = f"+9198765{i % 100000:05d}"
        off = "BLR-CAMPUS-1" if i % 2 == 0 else "HYD-CAMPUS-2"
        h = compute_canonical_record_hash(emp_id, fn, ln, em, ph, off, "IT", None, "ACTIVE")
        rows.append({
            "employee_id": emp_id,
            "first_name": fn,
            "last_name": ln,
            "email": em,
            "phone": ph,
            "office_code": off,
            "department": "IT",
            "manager_id": "",
            "status": "ACTIVE",
            "source_version": 1,
        })

    csv_bytes = generate_roster_csv(rows)
    text_stream = io.StringIO(csv_bytes.getvalue().decode("utf-8"))

    proc = psutil.Process(os.getpid())
    rss_before = proc.memory_info().rss / (1024 * 1024)

    start_time = time.monotonic()
    async with async_session_factory() as session:
        service = ReconciliationService(session)
        run = await service.execute_reconciliation(
            tenant_id=tenant_id,
            filename="scale_test.csv",
            file_content=text_stream,
            threshold_percent=30.0,
        )
    elapsed = time.monotonic() - start_time
    rss_after = proc.memory_info().rss / (1024 * 1024)

    assert run.status == "COMPLETED"
    assert run.total_records_received == count
    assert run.records_created == count
    assert elapsed < 10.0, f"Diff took {elapsed:.2f}s, expected < 10s for {count} rows"

    # Memory bounded test
    memory_delta = rss_after - rss_before
    assert memory_delta < 50.0, f"Memory delta was {memory_delta:.2f} MB, exceeded 50MB bound"

    # Clean up connections
    await async_engine.dispose()
