"""Reproducible 200,000-Row Streaming Reconciliation Performance Benchmark

Evaluates time and space complexity against the official MoveInSync targets:
- Scale Target: Diff a 200,000-row snapshot in under 60 seconds.
- Memory Target: Strictly bounded RSS memory (< 256MB) without loading whole dataset.
- Measurement Rigor: Continuously samples peak RSS memory throughout execution.
- Workload Coverage: Benchmarks create, update, stale, unchanged, and missing/deactivation paths.
- Baseline Isolation: Runs on a clean, isolated tenant database baseline.
"""

import asyncio
import csv
import io
import os
import platform
import sys
import time
import psutil

# Ensure app root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.config import settings
from app.core.database import sync_engine, async_session_factory
from app.services.reconciliation_service import ReconciliationService
from app.utils.hashing import compute_canonical_record_hash


class MemorySampler:
    """Continuously polls process resident set size (RSS) to capture true peak memory."""
    def __init__(self, sample_interval: float = 0.05):
        self.sample_interval = sample_interval
        self.process = psutil.Process(os.getpid())
        self.initial_rss = self.get_rss()
        self.peak_rss = self.initial_rss
        self._running = False
        self._task = None

    def get_rss(self) -> float:
        return self.process.memory_info().rss / (1024 * 1024)

    async def _sample_loop(self):
        while self._running:
            current = self.get_rss()
            if current > self.peak_rss:
                self.peak_rss = current
            await asyncio.sleep(self.sample_interval)

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._sample_loop())

    async def stop(self) -> float:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        final = self.get_rss()
        if final > self.peak_rss:
            self.peak_rss = final
        return self.peak_rss


def generate_benchmark_workload_file(filepath: str, total_records: int = 200_000):
    """Generate realistic 200,000-row export with create, update, stale, and unchanged paths.
    
    Workload distribution for 200,000 rows:
    - 150,000 Unchanged (hash and version match baseline v2)
    - 20,000 Updates (version 3 with updated department/office)
    - 10,000 Stale (version 1, older than baseline v2)
    - 20,000 New Joiners (IDs 190,001 to 210,000, not in baseline)
    """
    print(f"Generating synthetic {total_records:,}-row workload export CSV at '{filepath}'...")
    gen_start = time.monotonic()
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
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
        ])

        # 1. 150,000 UNCHANGED: IDs 1 to 150,000 (v2, matches baseline)
        for i in range(1, 150_001):
            writer.writerow([
                f"EMP-BENCH-{i:06d}",
                f"FirstName{i % 500}",
                f"LastName{i % 500}",
                f"user{i}@company.com",
                f"+91987{i % 1000000:07d}",
                "BLR-CAMPUS-1" if i % 2 == 0 else "HYD-CAMPUS-2",
                "Engineering" if i % 3 == 0 else "Operations",
                f"MGR-{(i % 100):03d}",
                "ACTIVE",
                "2",
            ])

        # 2. 20,000 UPDATES: IDs 150,001 to 170,000 (v3, changed department)
        for i in range(150_001, 170_001):
            writer.writerow([
                f"EMP-BENCH-{i:06d}",
                f"FirstName{i % 500}",
                f"LastName{i % 500}",
                f"user{i}@company.com",
                f"+91987{i % 1000000:07d}",
                "HYD-CAMPUS-2",  # Transferred
                "AdvancedEngineering",  # Updated department
                f"MGR-{(i % 100):03d}",
                "ACTIVE",
                "3",  # Newer version
            ])

        # 3. 10,000 STALE: IDs 170,001 to 180,000 (v1, older than baseline v2)
        for i in range(170_001, 180_001):
            writer.writerow([
                f"EMP-BENCH-{i:06d}",
                f"FirstName{i % 500}",
                f"LastName{i % 500}",
                f"user{i}@company.com",
                f"+91987{i % 1000000:07d}",
                "BLR-CAMPUS-1",
                "OldDepartment",
                f"MGR-{(i % 100):03d}",
                "ACTIVE",
                "1",  # Stale version
            ])

        # 4. 20,000 NEW JOINS (CREATES): IDs 190,001 to 210,000
        for i in range(190_001, 210_001):
            writer.writerow([
                f"EMP-BENCH-{i:06d}",
                f"FirstName{i % 500}",
                f"LastName{i % 500}",
                f"user{i}@company.com",
                f"+91987{i % 1000000:07d}",
                "BLR-CAMPUS-1" if i % 2 == 0 else "HYD-CAMPUS-2",
                "Product",
                f"MGR-{(i % 100):03d}",
                "ACTIVE",
                "1",
            ])

    # Note: IDs 180,001 to 190,000 (10,000 active employees in baseline) are OMITTED from export!
    # They constitute the MISSING (DEACTIVATION) path (10,000 / 190,000 = 5.26% missing, well below 30% brake).

    gen_time = time.monotonic() - gen_start
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Generated {total_records:,} rows ({file_size_mb:.2f} MB) in {gen_time:.2f}s.")


def prepare_isolated_baseline(tenant_id: str, count: int = 190_000):
    """Seed clean isolated database baseline with 190,000 active employees at version 2."""
    print(f"Purging pre-existing data and preparing clean baseline for '{tenant_id}'...")
    seed_start = time.monotonic()

    with sync_engine.connect() as conn:
        conn.execute(text(f"DELETE FROM employees WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM outbox_events WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM reconciliation_staging WHERE tenant_id = '{tenant_id}'"))
        conn.execute(text(f"DELETE FROM reconciliation_runs WHERE tenant_id = '{tenant_id}'"))
        conn.commit()

        # Seed in chunks of 5000 using batch INSERT
        chunk_size = 5000
        for start_idx in range(1, count + 1, chunk_size):
            end_idx = min(start_idx + chunk_size, count + 1)
            params = []
            for i in range(start_idx, end_idx):
                emp_id = f"EMP-BENCH-{i:06d}"
                fn = f"FirstName{i % 500}"
                ln = f"LastName{i % 500}"
                em = f"user{i}@company.com"
                ph = f"+91987{i % 1000000:07d}"
                off = "BLR-CAMPUS-1" if i % 2 == 0 else "HYD-CAMPUS-2"
                dep = "Engineering" if i % 3 == 0 else "Operations"
                mgr = f"MGR-{(i % 100):03d}"
                h = compute_canonical_record_hash(emp_id, fn, ln, em, ph, off, dep, mgr, "ACTIVE")
                params.append({
                    "tenant_id": tenant_id,
                    "employee_id": emp_id,
                    "first_name": fn,
                    "last_name": ln,
                    "email": em,
                    "phone": ph,
                    "office_code": off,
                    "department": dep,
                    "manager_id": mgr,
                    "status": "ACTIVE",
                    "source_version": 2,
                    "record_hash": h,
                })

            conn.execute(
                text(
                    """
                    INSERT INTO employees (
                        id, tenant_id, employee_id, first_name, last_name, email, phone,
                        office_code, department, manager_id, status, source_version, source_timestamp,
                        record_hash, special_assistance_required, transport_review_required, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), :tenant_id, :employee_id, :first_name, :last_name, :email, :phone,
                        :office_code, :department, :manager_id, :status, :source_version, NOW(),
                        :record_hash, false, false, NOW(), NOW()
                    );
                    """
                ),
                params,
            )
            conn.commit()

    print(f"Clean baseline with {count:,} active employees prepared in {time.monotonic() - seed_start:.2f}s.")


async def run_benchmark(records: int = 200_000):
    print("=" * 70)
    print("MoveInSync Case Study 5: 200,000-Row Reconciliation Performance Benchmark")
    print("=" * 70)

    tenant_id = "bench-isolated-tenant"

    env_info = {
        "Platform": platform.platform(),
        "Processor": platform.processor(),
        "CPU Cores": os.cpu_count(),
        "Python Version": sys.version.split()[0],
        "PostgreSQL Version": "PostgreSQL 16 (Alpine Docker Container)",
        "Target Scale SLA": "< 60.00 seconds",
        "Target Memory SLA": "< 256.00 MB Peak RSS",
    }
    for k, v in env_info.items():
        print(f"  {k:<20}: {v}")
    print("-" * 70)

    os.makedirs("benchmark_data", exist_ok=True)
    bench_file = f"benchmark_data/roster_workload_{records}_records.csv"
    generate_benchmark_workload_file(bench_file, total_records=records)

    prepare_isolated_baseline(tenant_id, count=190_000)

    # Initialize continuous memory sampler
    sampler = MemorySampler(sample_interval=0.05)
    print(f"Initial Process RSS Memory: {sampler.initial_rss:.2f} MB")
    sampler.start()

    print(f"\nExecuting Streaming Reconciliation Engine on {records:,} records...")
    start_time = time.monotonic()

    with open(bench_file, "r", encoding="utf-8") as f:
        async with async_session_factory() as session:
            service = ReconciliationService(session)
            run = await service.execute_reconciliation(
                tenant_id=tenant_id,
                filename=os.path.basename(bench_file),
                file_content=f,
                threshold_percent=30.0,
            )

    elapsed = time.monotonic() - start_time
    peak_rss = await sampler.stop()
    final_rss = sampler.get_rss()
    throughput = records / elapsed if elapsed > 0 else 0

    print("=" * 70)
    print("BENCHMARK EXECUTION RESULTS")
    print("=" * 70)
    print(f"  Run ID                  : {run.run_id}")
    print(f"  Status                  : {run.status}")
    print(f"  Total Processed         : {run.total_records_received:,} records")
    print(f"  Created                 : {run.records_created:,}")
    print(f"  Updated                 : {run.records_updated:,}")
    print(f"  Unchanged (Hash Match)  : {run.records_unchanged:,}")
    print(f"  Stale (Ignored)         : {run.records_stale:,}")
    print(f"  Deactivated (Missing)   : {run.records_deactivated:,}")
    print(f"  Missing Active Records  : {run.missing_active_records:,} ({run.missing_percentage:.2f}%)")
    print(f"  Total Elapsed Time      : {elapsed:.2f} seconds")
    print(f"  Processing Throughput   : {throughput:,.0f} records/second")
    print(f"  Initial Process RSS     : {sampler.initial_rss:.2f} MB")
    print(f"  Peak Measured RSS       : {peak_rss:.2f} MB")
    print(f"  Final Process RSS       : {final_rss:.2f} MB")
    print(f"  Net Peak Memory Delta   : {(peak_rss - sampler.initial_rss):+.2f} MB")
    print(f"  Target Scale (< 60s)    : {'PASSED' if elapsed < 60.0 else 'FAILED'}")
    print(f"  Target Memory (< 256MB) : {'PASSED' if peak_rss < 256.0 else 'FAILED'}")
    print("=" * 70)

    return {
        "elapsed": elapsed,
        "records": records,
        "throughput": throughput,
        "initial_rss": sampler.initial_rss,
        "peak_rss": peak_rss,
        "final_rss": final_rss,
        "status": run.status,
    }


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    asyncio.run(run_benchmark(records=count))
