"""Streaming Reconciliation Engine & Configurable Safety Brake

Processes nightly roster export files (up to 200,000 rows) in bounded streaming chunks.
Compares canonical hashes against stored records without loading the entire dataset into memory.
Enforces the 30% safety brake to protect against mass wrongful deactivations from truncated exports.
"""

import csv
import io
import json
import time
import uuid
from typing import AsyncIterator, Dict, List, Set, Tuple, Optional, Any
from sqlalchemy import select, update, func
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
from app.models.reconciliation import ReconciliationRun
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
        file_content: io.TextIOBase,
        threshold_percent: Optional[float] = None,
    ) -> ReconciliationRun:
        """Stream and diff full nightly export file with bounded memory and safety brake."""
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

        # Fetch active employee map prior to run: {employee_id: record_hash}
        active_map_before: Dict[str, str] = await self.employee_repo.get_active_ids_and_hashes(tenant_id)
        total_active_before = len(active_map_before)

        seen_in_export: Set[str] = set()
        duplicate_ids_in_file: List[str] = []
        malformed_rows_count = 0

        records_created = 0
        records_updated = 0
        records_unchanged = 0
        records_stale = 0

        # Stream lines via csv.DictReader
        reader = csv.DictReader(file_content)
        fieldnames = [f.strip().lower() for f in (reader.fieldnames or []) if f]
        header_valid = "employee_id" in fieldnames

        chunk: List[Dict[str, str]] = []
        chunk_size = settings.reconciliation_chunk_size

        async def process_chunk(batch: List[Dict[str, str]]) -> Tuple[int, int, int, int]:
            c_created = 0
            c_updated = 0
            c_unchanged = 0
            c_stale = 0

            emp_ids = [row.get("employee_id", "").strip() for row in batch if row.get("employee_id")]
            if not emp_ids:
                return 0, 0, 0, 0

            # Query existing employees in this batch
            query = select(Employee).where(
                Employee.tenant_id == tenant_id,
                Employee.employee_id.in_(emp_ids),
            )
            result = await self.session.execute(query)
            existing_by_id = {emp.employee_id: emp for emp in result.scalars().all()}

            new_employees: List[Employee] = []
            outbox_events: List[OutboxEvent] = []

            for row in batch:
                emp_id = row.get("employee_id", "").strip()
                if not emp_id:
                    continue

                first_name = row.get("first_name", "").strip()
                last_name = row.get("last_name", "").strip()
                email = row.get("email", "").strip().lower()
                phone = row.get("phone", "").strip() or None
                office_code = row.get("office_code", "").strip().upper()
                department = row.get("department", "").strip() or None
                manager_id = row.get("manager_id", "").strip() or None
                status_val = row.get("status", "ACTIVE").strip().upper()
                try:
                    version = int(row.get("source_version", "1"))
                except ValueError:
                    version = 1

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

                if emp_id not in existing_by_id:
                    # CREATE
                    new_emp = Employee(
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
                        transport_review_required=False,
                    )
                    new_employees.append(new_emp)
                    outbox_events.append(
                        OutboxEvent(
                            id=uuid.uuid4(),
                            tenant_id=tenant_id,
                            event_type="EMPLOYEE_CREATED",
                            aggregate_id=emp_id,
                            payload={"employee_id": emp_id, "status": status_val, "source": "reconciliation"},
                            status="PENDING",
                            retry_count=0,
                            next_retry_at=utc_now(),
                        )
                    )
                    c_created += 1
                else:
                    emp = existing_by_id[emp_id]
                    if candidate_hash == emp.record_hash and emp.status == status_val:
                        # UNCHANGED: Stable hash match (Zero DB writes!)
                        c_unchanged += 1
                    elif version < emp.source_version:
                        # STALE: Lower version than current stored record
                        c_stale += 1
                    else:
                        # UPDATE: HR fields modified (Preserve Transport fields!)
                        office_changed = (emp.office_code != office_code)
                        emp.first_name = first_name
                        emp.last_name = last_name
                        emp.email = email
                        emp.phone = phone
                        emp.office_code = office_code
                        emp.department = department
                        emp.manager_id = manager_id
                        emp.status = status_val
                        emp.source_version = version
                        emp.record_hash = candidate_hash

                        if office_changed and (emp.preferred_pickup_point_id or emp.pickup_geofence):
                            emp.transport_review_required = True

                        event_type = "OFFICE_TRANSFERRED" if office_changed else "EMPLOYEE_UPDATED"
                        outbox_events.append(
                            OutboxEvent(
                                id=uuid.uuid4(),
                                tenant_id=tenant_id,
                                event_type=event_type,
                                aggregate_id=emp_id,
                                payload={"employee_id": emp_id, "status": status_val, "source": "reconciliation"},
                                status="PENDING",
                                retry_count=0,
                                next_retry_at=utc_now(),
                            )
                        )
                        c_updated += 1

            if new_employees:
                self.session.add_all(new_employees)
            if outbox_events:
                self.session.add_all(outbox_events)

            await self.session.commit()
            return c_created, c_updated, c_unchanged, c_stale

        # Process file stream in chunks
        for row in reader:
            if not header_valid:
                malformed_rows_count += 1
                continue

            emp_id = (row.get("employee_id") or "").strip()
            if not emp_id:
                malformed_rows_count += 1
                continue

            if emp_id in seen_in_export:
                duplicate_ids_in_file.append(emp_id)
            seen_in_export.add(emp_id)

            chunk.append(row)
            if len(chunk) >= chunk_size:
                c_cr, c_up, c_un, c_st = await process_chunk(chunk)
                records_created += c_cr
                records_updated += c_up
                records_unchanged += c_un
                records_stale += c_st
                chunk.clear()

        # Process remaining tail chunk
        if chunk:
            c_cr, c_up, c_un, c_st = await process_chunk(chunk)
            records_created += c_cr
            records_updated += c_up
            records_unchanged += c_un
            records_stale += c_st
            chunk.clear()

        # =====================================================================
        # SAFETY BRAKE & DEACTIVATION ANALYSIS
        # =====================================================================
        # Identify active employees missing from export
        missing_employee_ids = [
            emp_id for emp_id in active_map_before.keys()
            if emp_id not in seen_in_export
        ]
        missing_count = len(missing_employee_ids)
        missing_percentage = (
            (missing_count / total_active_before) * 100.0 if total_active_before > 0 else 0.0
        )

        total_records_received = len(seen_in_export)
        elapsed_seconds = time.monotonic() - start_time

        # Update run stats
        run_record = await self.recon_repo.get_by_run_id(tenant_id, run_id, for_update=True)
        if not run_record:
            raise RuntimeError(f"Reconciliation run {run_id} lost")

        run_record.total_records_received = total_records_received
        run_record.records_created = records_created
        run_record.records_updated = records_updated
        run_record.records_unchanged = records_unchanged
        run_record.records_stale = records_stale
        run_record.missing_active_records = missing_count
        run_record.missing_percentage = round(missing_percentage, 2)

        anomaly_dict = {
            "threshold_percent": threshold,
            "total_active_before": total_active_before,
            "missing_count": missing_count,
            "missing_percentage": round(missing_percentage, 2),
            "sample_missing_ids": missing_employee_ids[:20],
            "all_missing_ids": missing_employee_ids,
            "duplicate_ids_in_file_count": len(duplicate_ids_in_file),
            "duplicate_sample_ids": duplicate_ids_in_file[:10],
            "malformed_rows_count": malformed_rows_count,
            "header_valid": header_valid,
            "elapsed_seconds": round(elapsed_seconds, 2),
        }
        run_record.anomaly_report = anomaly_dict

        if missing_percentage >= threshold:
            # SAFETY BRAKE TRIGGERED: Halt deactivations and require human approval
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
        if malformed_rows_count > 0 or len(duplicate_ids_in_file) > 0:
            run_record.operator_review_required = True

        # NORMAL RUN: Missing percentage < threshold -> Apply deactivations
        records_deactivated = 0
        if missing_employee_ids:
            # Deactivate in chunks of 1000
            for i in range(0, len(missing_employee_ids), 1000):
                batch_deact = missing_employee_ids[i:i + 1000]
                await self.employee_repo.deactivate_batch(tenant_id, batch_deact)
                # Enqueue Outbox deactivation events
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
            records_deactivated = len(missing_employee_ids)

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
        is_triggered = (run.status == "SAFETY_BRAKE_TRIGGERED")

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
        """Operator explicitly overrides safety brake and applies mass deactivations."""
        run = await self.recon_repo.get_by_run_id(tenant_id, run_id, for_update=True)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation run not found")

        if run.status != "SAFETY_BRAKE_TRIGGERED":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Run cannot be approved; current status is '{run.status}'",
            )

        report = run.anomaly_report or {}
        all_missing: List[str] = report.get("all_missing_ids", [])

        if all_missing:
            for i in range(0, len(all_missing), 1000):
                batch_deact = all_missing[i:i + 1000]
                await self.employee_repo.deactivate_batch(tenant_id, batch_deact)
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

        run.status = "COMPLETED"
        run.approved_by_operator = True
        run.operator_id = operator_id
        run.operator_action_notes = reason
        run.records_deactivated = len(all_missing)
        run.completed_at = utc_now()
        await self.session.commit()

        logger.info(
            f"Safety brake overridden by operator {operator_id} for run {run_id}. Deactivated {len(all_missing)} employees.",
            extra={"tenant_id": tenant_id, "run_id": run_id},
        )
        return run

    async def abort_run(
        self,
        tenant_id: str,
        run_id: str,
        operator_id: str,
        reason: str,
    ) -> ReconciliationRun:
        """Operator aborts a halted reconciliation run, preventing any deactivations."""
        run = await self.recon_repo.get_by_run_id(tenant_id, run_id, for_update=True)
        if not run:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reconciliation run not found")

        if run.status != "SAFETY_BRAKE_TRIGGERED":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Run cannot be aborted; current status is '{run.status}'",
            )

        run.status = "ABORTED"
        run.operator_id = operator_id
        run.operator_action_notes = f"Aborted: {reason}"
        run.completed_at = utc_now()
        await self.session.commit()

        logger.info(
            f"Reconciliation run {run_id} aborted by operator {operator_id}: {reason}",
            extra={"tenant_id": tenant_id, "run_id": run_id},
        )
        return run
