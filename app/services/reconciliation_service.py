"""Streaming Reconciliation Engine & Configurable Safety Brake

Processes nightly roster export files (up to 200,000 rows) in bounded streaming chunks.
Uses database staging tables and SQL anti-joins to strictly bound memory (< 256MB).
Enforces the 30% safety brake to protect against mass wrongful deactivations from truncated exports.
Provides concurrency-safe operator approval via the APPROVING state.
"""

import csv
import io
import time
import uuid
from typing import Dict, List, Tuple, Optional, Any
from sqlalchemy import select, update, func, exists, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status

from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import (
    RECONCILIATION_DURATION_SECONDS,
    SAFETY_BRAKE_TRIGGERS_TOTAL,
)
from app.models.base import utc_now
from app.models.employee import Employee
from app.models.outbox import OutboxEvent
from app.models.reconciliation import ReconciliationRun, ReconciliationStaging
from app.repositories.employee_repo import EmployeeRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.reconciliation_repo import ReconciliationRepository
from app.schemas.reconciliation import (
    ReconciliationRunResponse,
    ReconciliationReport,
)
from app.utils.hashing import compute_canonical_record_hash


class ReconciliationService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.employee_repo = EmployeeRepository(session)
        self.outbox_repo = OutboxRepository(session)
        self.recon_repo = ReconciliationRepository(session)

    async def execute_reconciliation(
        self,
        tenant_id: str,
        filename: str,
        file_content: Any,
        threshold_percent: Optional[float] = None,
    ) -> ReconciliationRun:
        """Stream and diff full nightly export file using database staging tables with bounded memory and safety brake."""
        start_time = time.monotonic()
        threshold = threshold_percent if threshold_percent is not None else settings.safety_brake_threshold_percent
        run_id = f"recon-{tenant_id}-{uuid.uuid4().hex[:8]}"

        # Initialize Reconciliation Audit Record
        recon_run = ReconciliationRun(
            run_id=run_id,
            tenant_id=tenant_id,
            filename=filename,
            status="STREAMING",
            started_at=utc_now(),
        )
        await self.recon_repo.create_run(recon_run)
        await self.session.commit()

        # 1. Total active employees before run (SQL COUNT, zero Python memory!)
        total_active_before = await self.employee_repo.count_active(tenant_id)

        # 2. Stream input CSV in bounded chunks directly into reconciliation_staging
        reader = csv.DictReader(file_content)
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or []) if f]
        header_valid = "employee_id" in fieldnames

        staging_buffer: List[ReconciliationStaging] = []
        chunk_seen_ids: Dict[str, int] = {}  # Scoped strictly to current chunk for duplicate collision prevention
        chunk_size = settings.reconciliation_chunk_size

        malformed_rows_count = 0
        duplicate_ids_count = 0
        duplicate_sample_ids: List[str] = []

        for row in reader:
            if not header_valid:
                malformed_rows_count += 1
                continue

            emp_id = (row.get("employee_id") or "").strip()
            first_name = (row.get("first_name") or "").strip()
            last_name = (row.get("last_name") or "").strip()
            email = (row.get("email") or "").strip().lower()
            office_code = (row.get("office_code") or "").strip().upper()

            # Required fields validation
            if not (emp_id and first_name and last_name and email and office_code):
                malformed_rows_count += 1
                continue

            # Email format validation
            if "@" not in email:
                malformed_rows_count += 1
                continue

            # Status validation
            status_val = (row.get("status") or "ACTIVE").strip().upper()
            if status_val not in ("ACTIVE", "INACTIVE"):
                malformed_rows_count += 1
                continue

            # Source version validation (must be positive integer, not defaulted)
            raw_version = (row.get("source_version") or "").strip()
            try:
                version = int(raw_version)
                if version < 1:
                    malformed_rows_count += 1
                    continue
            except (ValueError, TypeError):
                malformed_rows_count += 1
                continue

            phone = (row.get("phone") or "").strip() or None
            department = (row.get("department") or "").strip() or None
            manager_id = (row.get("manager_id") or "").strip() or None

            candidate_hash = compute_canonical_record_hash(
                employee_id=emp_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                phone=phone,
                office_code=office_code,
                department=department,
                manager_id=manager_id,
                status=status_val,
            )

            # Duplicate employee ID check within chunk
            if emp_id in chunk_seen_ids:
                duplicate_ids_count += 1
                if len(duplicate_sample_ids) < 10:
                    duplicate_sample_ids.append(emp_id)
                # Overwrite earlier row in staging buffer with latest
                idx = chunk_seen_ids[emp_id]
                staging_buffer[idx] = ReconciliationStaging(
                    id=uuid.uuid4(),
                    run_id=run_id,
                    tenant_id=tenant_id,
                    employee_id=emp_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone=phone,
                    office_code=office_code,
                    department=department,
                    manager_id=manager_id,
                    status=status_val,
                    source_version=version,
                    record_hash=candidate_hash,
                )
                continue

            chunk_seen_ids[emp_id] = len(staging_buffer)
            staging_buffer.append(
                ReconciliationStaging(
                    id=uuid.uuid4(),
                    run_id=run_id,
                    tenant_id=tenant_id,
                    employee_id=emp_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone=phone,
                    office_code=office_code,
                    department=department,
                    manager_id=manager_id,
                    status=status_val,
                    source_version=version,
                    record_hash=candidate_hash,
                )
            )

            if len(staging_buffer) >= chunk_size:
                self.session.add_all(staging_buffer)
                await self.session.commit()
                staging_buffer.clear()
                chunk_seen_ids.clear()

        # Flush remaining tail staging buffer
        if staging_buffer:
            self.session.add_all(staging_buffer)
            await self.session.commit()
            staging_buffer.clear()
            chunk_seen_ids.clear()

        # 3. Database-Driven Anti-Joins and Diff Counts
        stmt_count = select(func.count(distinct(ReconciliationStaging.employee_id))).where(
            ReconciliationStaging.run_id == run_id,
            ReconciliationStaging.tenant_id == tenant_id,
        )
        total_records_received = (await self.session.execute(stmt_count)).scalar() or 0

        # Anti-join to identify missing active employees
        stmt_missing = select(func.count()).select_from(Employee).where(
            Employee.tenant_id == tenant_id,
            Employee.status == "ACTIVE",
            ~exists().where(
                ReconciliationStaging.run_id == run_id,
                ReconciliationStaging.tenant_id == tenant_id,
                ReconciliationStaging.employee_id == Employee.employee_id,
            ),
        )
        missing_count = (await self.session.execute(stmt_missing)).scalar() or 0
        missing_percentage = (
            (missing_count / total_active_before) * 100.0 if total_active_before > 0 else 0.0
        )

        # Sample missing IDs for operator inspection (limit 20)
        sample_missing_ids: List[str] = []
        if missing_count > 0:
            stmt_sample = (
                select(Employee.employee_id)
                .where(
                    Employee.tenant_id == tenant_id,
                    Employee.status == "ACTIVE",
                    Employee.employee_id.not_in(
                        select(ReconciliationStaging.employee_id).where(
                            ReconciliationStaging.run_id == run_id,
                            ReconciliationStaging.tenant_id == tenant_id,
                        )
                    ),
                )
                .limit(20)
            )
            sample_missing_ids = list((await self.session.execute(stmt_sample)).scalars().all())

        elapsed_seconds = time.monotonic() - start_time

        run_record = await self.recon_repo.get_by_run_id(tenant_id, run_id, for_update=True)
        if not run_record:
            raise RuntimeError(f"Reconciliation run {run_id} lost")

        run_record.total_records_received = total_records_received
        run_record.missing_active_records = missing_count
        run_record.missing_percentage = round(missing_percentage, 2)

        anomaly_dict = {
            "threshold_percent": threshold,
            "total_active_before": total_active_before,
            "missing_count": missing_count,
            "missing_percentage": round(missing_percentage, 2),
            "sample_missing_ids": sample_missing_ids,
            "duplicate_ids_in_file_count": duplicate_ids_count,
            "duplicate_sample_ids": duplicate_sample_ids[:10],
            "malformed_rows_count": malformed_rows_count,
            "header_valid": header_valid,
            "elapsed_seconds": round(elapsed_seconds, 2),
        }
        run_record.anomaly_report = anomaly_dict

        # 4. SAFETY BRAKE CHECK
        if missing_percentage >= threshold:
            run_record.status = "SAFETY_BRAKE_TRIGGERED"
            run_record.operator_review_required = True
            await self.session.commit()

            SAFETY_BRAKE_TRIGGERS_TOTAL.labels(tenant=tenant_id).inc()
            RECONCILIATION_DURATION_SECONDS.labels(tenant=tenant_id, status="safety_brake_triggered").observe(elapsed_seconds)
            logger.warning(
                f"SAFETY BRAKE TRIGGERED for run {run_id}: {missing_count}/{total_active_before} ({missing_percentage:.1f}%) missing active employees. Deactivation halted.",
                extra={"tenant_id": tenant_id, "run_id": run_id},
            )
            return run_record

        # Flag review if anomalous duplicates or malformed rows detected even if below deactivation threshold
        if malformed_rows_count > 0 or duplicate_ids_count > 0:
            run_record.operator_review_required = True

        # 5. BELOW THRESHOLD: APPLY CREATES, UPDATES, UNCHANGED & DEACTIVATIONS
        # A. Count unchanged records via high-speed relational hash join in PostgreSQL (0.1s)
        stmt_unchanged = (
            select(func.count())
            .select_from(ReconciliationStaging)
            .join(
                Employee,
                (Employee.tenant_id == ReconciliationStaging.tenant_id)
                & (Employee.employee_id == ReconciliationStaging.employee_id),
            )
            .where(
                ReconciliationStaging.run_id == run_id,
                ReconciliationStaging.tenant_id == tenant_id,
                ReconciliationStaging.record_hash == Employee.record_hash,
                ReconciliationStaging.status == Employee.status,
                ReconciliationStaging.source_version == Employee.source_version,
            )
        )
        records_unchanged = (await self.session.execute(stmt_unchanged)).scalar() or 0

        # B. Process only real deltas (creates, field updates, version advances, and stale drops)
        records_created = 0
        records_updated = 0
        records_stale = 0

        last_staging_id = None
        while True:
            query_delta = (
                select(ReconciliationStaging)
                .outerjoin(
                    Employee,
                    (Employee.tenant_id == ReconciliationStaging.tenant_id)
                    & (Employee.employee_id == ReconciliationStaging.employee_id),
                )
                .where(
                    ReconciliationStaging.run_id == run_id,
                    ReconciliationStaging.tenant_id == tenant_id,
                    (
                        (Employee.id.is_(None))
                        | (ReconciliationStaging.record_hash != Employee.record_hash)
                        | (ReconciliationStaging.status != Employee.status)
                        | (ReconciliationStaging.source_version != Employee.source_version)
                    ),
                )
            )
            if last_staging_id is not None:
                query_delta = query_delta.where(ReconciliationStaging.id > last_staging_id)
            query_delta = query_delta.order_by(ReconciliationStaging.id).limit(chunk_size)

            staging_chunk = list((await self.session.execute(query_delta)).scalars().all())
            if not staging_chunk:
                break
            last_staging_id = staging_chunk[-1].id

            chunk_emp_ids = [s.employee_id for s in staging_chunk]
            stmt_emp = select(Employee).where(
                Employee.tenant_id == tenant_id,
                Employee.employee_id.in_(chunk_emp_ids),
            )
            res_emp = await self.session.execute(stmt_emp)
            existing_by_id = {e.employee_id: e for e in res_emp.scalars().all()}

            new_employees: List[Employee] = []
            outbox_events: List[OutboxEvent] = []

            for s in staging_chunk:
                if s.employee_id not in existing_by_id:
                    # CREATE
                    new_emp = Employee(
                        tenant_id=tenant_id,
                        employee_id=s.employee_id,
                        first_name=s.first_name,
                        last_name=s.last_name,
                        email=s.email,
                        phone=s.phone,
                        office_code=s.office_code,
                        department=s.department,
                        manager_id=s.manager_id,
                        status=s.status,
                        source_version=s.source_version,
                        record_hash=s.record_hash,
                        transport_review_required=False,
                    )
                    new_employees.append(new_emp)
                    existing_by_id[s.employee_id] = new_emp  # Prevent duplicate within same chunk
                    outbox_events.append(
                        OutboxEvent(
                            id=uuid.uuid4(),
                            tenant_id=tenant_id,
                            event_type="EMPLOYEE_CREATED",
                            aggregate_id=s.employee_id,
                            payload={"employee_id": s.employee_id, "status": s.status, "source": "reconciliation"},
                            status="PENDING",
                            retry_count=0,
                            next_retry_at=utc_now(),
                        )
                    )
                    records_created += 1
                else:
                    emp = existing_by_id[s.employee_id]
                    if s.source_version < emp.source_version:
                        records_stale += 1
                    elif s.record_hash == emp.record_hash and emp.status == s.status:
                        if s.source_version > emp.source_version:
                            # Advance source_version for newer snapshots even when business fields are unchanged!
                            emp.source_version = s.source_version
                            emp.updated_at = utc_now()
                            records_updated += 1
                    else:
                        # HR fields modified or status changed
                        office_changed = (emp.office_code != s.office_code)
                        emp.first_name = s.first_name
                        emp.last_name = s.last_name
                        emp.email = s.email
                        emp.phone = s.phone
                        emp.office_code = s.office_code
                        emp.department = s.department
                        emp.manager_id = s.manager_id
                        emp.status = s.status
                        emp.source_version = s.source_version
                        emp.record_hash = s.record_hash

                        if office_changed and (emp.preferred_pickup_point_id or emp.pickup_geofence):
                            emp.transport_review_required = True

                        event_type = "OFFICE_TRANSFERRED" if office_changed else "EMPLOYEE_UPDATED"
                        outbox_events.append(
                            OutboxEvent(
                                id=uuid.uuid4(),
                                tenant_id=tenant_id,
                                event_type=event_type,
                                aggregate_id=s.employee_id,
                                payload={"employee_id": s.employee_id, "status": s.status, "source": "reconciliation"},
                                status="PENDING",
                                retry_count=0,
                                next_retry_at=utc_now(),
                            )
                        )
                        records_updated += 1

            if new_employees:
                self.session.add_all(new_employees)
            if outbox_events:
                self.session.add_all(outbox_events)
            await self.session.commit()

        # Apply deactivations using database anti-join
        records_deactivated = 0
        if missing_count > 0:
            stmt_deact = (
                update(Employee)
                .where(
                    Employee.tenant_id == tenant_id,
                    Employee.status == "ACTIVE",
                    ~exists().where(
                        ReconciliationStaging.run_id == run_id,
                        ReconciliationStaging.tenant_id == tenant_id,
                        ReconciliationStaging.employee_id == Employee.employee_id,
                    ),
                )
                .values(status="INACTIVE", updated_at=utc_now())
                .returning(Employee.employee_id)
            )
            res_deact = await self.session.execute(stmt_deact)
            deactivated_ids = list(res_deact.scalars().all())

            for i in range(0, len(deactivated_ids), 1000):
                batch_deact = deactivated_ids[i:i + 1000]
                deact_events = [
                    OutboxEvent(
                        id=uuid.uuid4(),
                        tenant_id=tenant_id,
                        event_type="EMPLOYEE_DEACTIVATED",
                        aggregate_id=deact_id,
                        payload={"employee_id": deact_id, "reason": "NIGHTLY_RECONCILIATION_ABSENCE"},
                        status="PENDING",
                        retry_count=0,
                        next_retry_at=utc_now(),
                    )
                    for deact_id in batch_deact
                ]
                self.session.add_all(deact_events)
                await self.session.commit()
            records_deactivated = len(deactivated_ids)

        run_record.records_created = records_created
        run_record.records_updated = records_updated
        run_record.records_unchanged = records_unchanged
        run_record.records_stale = records_stale
        run_record.records_deactivated = records_deactivated
        run_record.status = "COMPLETED"
        run_record.completed_at = utc_now()
        await self.session.commit()

        RECONCILIATION_DURATION_SECONDS.labels(tenant=tenant_id, status="completed").observe(elapsed_seconds)
        logger.info(
            f"Reconciliation {run_id} completed: created={records_created}, updated={records_updated}, unchanged={records_unchanged}, deactivated={records_deactivated} in {elapsed_seconds:.2f}s",
            extra={"tenant_id": tenant_id, "run_id": run_id},
        )
        return run_record

    async def get_report(self, tenant_id: str, run_id: str) -> ReconciliationReport:
        """Fetch safety brake inspection report for operator review."""
        run = await self.recon_repo.get_by_run_id(tenant_id, run_id)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation run not found")

        report = run.anomaly_report or {}
        is_triggered = (run.status in ("SAFETY_BRAKE_TRIGGERED", "APPROVING"))

        return ReconciliationReport(
            run_id=run.run_id,
            tenant_id=run.tenant_id,
            status=run.status,
            safety_brake_triggered=is_triggered,
            safety_brake_threshold_percent=report.get("threshold_percent", settings.safety_brake_threshold_percent),
            total_active_before_run=report.get("total_active_before", run.total_records_received),
            snapshot_records_present=run.total_records_received,
            missing_active_count=run.missing_active_records,
            missing_percentage=run.missing_percentage,
            sample_missing_employee_ids=report.get("sample_missing_ids", []),
            anomaly_summary=report,
            recommendation=(
                "REJECT / ABORT: Possible truncated or corrupted HR file. Over 30% of workforce is absent."
                if is_triggered else
                "SAFE: Reconciliation within acceptable drift tolerance."
            ),
        )

    async def approve_run(
        self,
        tenant_id: str,
        run_id: str,
        operator_id: str,
        reason: str,
    ) -> ReconciliationRun:
        """Operator explicitly overrides safety brake and applies mass deactivations with concurrency safety."""
        # Atomic transition to APPROVING prevents concurrent approvals
        stmt = (
            update(ReconciliationRun)
            .where(
                ReconciliationRun.run_id == run_id,
                ReconciliationRun.tenant_id == tenant_id,
                ReconciliationRun.status == "SAFETY_BRAKE_TRIGGERED",
            )
            .values(
                status="APPROVING",
                operator_id=operator_id,
                operator_action_notes=reason,
            )
            .returning(ReconciliationRun)
        )
        res = await self.session.execute(stmt)
        run = res.scalar_one_or_none()
        await self.session.commit()

        if not run:
            existing = await self.recon_repo.get_by_run_id(tenant_id, run_id)
            if not existing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation run not found")
            if existing.status == "APPROVING":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Run is already being approved by another operator",
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Run cannot be approved; current status is '{existing.status}'",
            )

        # Deactivate ONLY employees that are currently ACTIVE (anti-join against staging)
        stmt_deact = (
            update(Employee)
            .where(
                Employee.tenant_id == tenant_id,
                Employee.status == "ACTIVE",
                ~exists().where(
                    ReconciliationStaging.run_id == run_id,
                    ReconciliationStaging.tenant_id == tenant_id,
                    ReconciliationStaging.employee_id == Employee.employee_id,
                ),
            )
            .values(status="INACTIVE", updated_at=utc_now())
            .returning(Employee.employee_id)
        )
        res_deact = await self.session.execute(stmt_deact)
        deactivated_ids = list(res_deact.scalars().all())

        # Only emit outbox events for employees that actually transitioned from ACTIVE to INACTIVE
        if deactivated_ids:
            for i in range(0, len(deactivated_ids), 1000):
                batch_deact = deactivated_ids[i:i + 1000]
                deact_events = [
                    OutboxEvent(
                        id=uuid.uuid4(),
                        tenant_id=tenant_id,
                        event_type="EMPLOYEE_DEACTIVATED",
                        aggregate_id=deact_id,
                        payload={"employee_id": deact_id, "reason": "OPERATOR_APPROVED_SAFETY_BRAKE_OVERRIDE"},
                        status="PENDING",
                        retry_count=0,
                        next_retry_at=utc_now(),
                    )
                    for deact_id in batch_deact
                ]
                self.session.add_all(deact_events)
                await self.session.commit()

        run_record = await self.recon_repo.get_by_run_id(tenant_id, run_id, for_update=True)
        if run_record:
            run_record.status = "COMPLETED"
            run_record.approved_by_operator = True
            run_record.records_deactivated = len(deactivated_ids)
            run_record.completed_at = utc_now()
            await self.session.commit()
            return run_record

        return run

    async def abort_run(
        self,
        tenant_id: str,
        run_id: str,
        operator_id: str,
        reason: str,
    ) -> ReconciliationRun:
        """Operator aborts a halted reconciliation run, preventing any deactivations."""
        stmt = (
            update(ReconciliationRun)
            .where(
                ReconciliationRun.run_id == run_id,
                ReconciliationRun.tenant_id == tenant_id,
                ReconciliationRun.status == "SAFETY_BRAKE_TRIGGERED",
            )
            .values(
                status="ABORTED",
                operator_id=operator_id,
                operator_action_notes=f"Aborted: {reason}",
                completed_at=utc_now(),
            )
            .returning(ReconciliationRun)
        )
        res = await self.session.execute(stmt)
        run = res.scalar_one_or_none()
        await self.session.commit()

        if not run:
            existing = await self.recon_repo.get_by_run_id(tenant_id, run_id)
            if not existing:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation run not found")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Run cannot be aborted; current status is '{existing.status}'",
            )

        logger.info(
            f"Reconciliation run {run_id} aborted by operator {operator_id}: {reason}",
            extra={"tenant_id": tenant_id, "run_id": run_id},
        )
        return run
