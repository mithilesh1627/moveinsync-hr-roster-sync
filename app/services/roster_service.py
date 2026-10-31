"""Roster Domain Service

Orchestrates HR event ingestion, boundary validation, field ownership invariants,
version ordering, deterministic tie-breaking, and transactional outbox event emission.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.employee import Employee
from app.models.base import utc_now
from app.repositories.employee_repo import EmployeeRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.office_repo import OfficeRepository
from app.schemas.roster_events import (
    SingleRosterEventSchema,
    EventType,
    EmployeeStatus,
    RosterBatchEventRequest,
    RosterBatchEventResponse,
    RosterRecordResult,
    RosterRecordError,
)
from app.utils.hashing import compute_canonical_record_hash
from app.core.metrics import ROSTER_EVENTS_TOTAL
from app.core.logging import logger


class RosterService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.employee_repo = EmployeeRepository(session)
        self.outbox_repo = OutboxRepository(session)
        self.office_repo = OfficeRepository(session)

    async def process_batch_events(
        self,
        tenant_id: str,
        request: RosterBatchEventRequest,
    ) -> RosterBatchEventResponse:
        """Process a batch of HR events.
        
        Boundary rule: One bad record must never fail the entire batch.
        Each record is validated and applied individually in the transaction.
        """
        results: List[RosterRecordResult] = []
        errors: List[RosterRecordError] = []

        accepted_count = 0
        stale_count = 0
        duplicate_count = 0
        rejected_count = 0

        # Load valid office codes for tenant (cached)
        valid_offices = await self.office_repo.get_valid_office_codes(tenant_id)

        for idx, raw_event in enumerate(request.events):
            # 1. Boundary Validation per record
            try:
                event = SingleRosterEventSchema.model_validate(raw_event)
            except ValidationError as ve:
                rejected_count += 1
                first_err = ve.errors()[0]
                field_name = ".".join(str(loc) for loc in first_err.get("loc", []))
                err_msg = first_err.get("msg", "Validation error")
                emp_id = str(raw_event.get("employee_id", "UNKNOWN"))
                
                errors.append(
                    RosterRecordError(
                        record_index=idx,
                        employee_id=emp_id,
                        field=field_name,
                        error_code="INVALID_PAYLOAD",
                        message=err_msg,
                    )
                )
                ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="rejected").inc()
                continue

            # Validate office code against registered master if configured
            if event.office_code and valid_offices and event.office_code.upper() not in valid_offices:
                rejected_count += 1
                errors.append(
                    RosterRecordError(
                        record_index=idx,
                        employee_id=event.employee_id,
                        field="office_code",
                        error_code="UNKNOWN_OFFICE_CODE",
                        message=f"Office code '{event.office_code}' is not registered for tenant '{tenant_id}'",
                    )
                )
                ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="rejected").inc()
                continue

            # 2. Concurrency Safety: Lock employee row if existing
            emp = await self.employee_repo.get_by_id(
                tenant_id=tenant_id,
                employee_id=event.employee_id,
                for_update=True,
            )

            if emp is None:
                # Employee does not exist
                if event.event_type == EventType.DEACTIVATE:
                    # Deactivating non-existent employee is a no-op
                    duplicate_count += 1
                    results.append(
                        RosterRecordResult(
                            record_index=idx,
                            employee_id=event.employee_id,
                            status="duplicate_noop",
                            source_version=event.source_version,
                            message="Deactivation on non-existent employee is a no-op",
                        )
                    )
                    ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="duplicate_noop").inc()
                    continue

                if not (event.first_name and event.last_name and event.email and event.office_code):
                    rejected_count += 1
                    errors.append(
                        RosterRecordError(
                            record_index=idx,
                            employee_id=event.employee_id,
                            field="employee_id",
                            error_code="EMPLOYEE_NOT_FOUND",
                            message="Cannot create employee without mandatory first_name, last_name, email, and office_code",
                        )
                    )
                    ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="rejected").inc()
                    continue

                # CREATE NEW EMPLOYEE
                record_hash = compute_canonical_record_hash(
                    employee_id=event.employee_id,
                    first_name=event.first_name,
                    last_name=event.last_name,
                    email=event.email,
                    phone=event.phone,
                    office_code=event.office_code,
                    department=event.department,
                    manager_id=event.manager_id,
                    status="ACTIVE" if event.status != EmployeeStatus.INACTIVE else "INACTIVE",
                )

                new_emp = Employee(
                    tenant_id=tenant_id,
                    employee_id=event.employee_id,
                    first_name=event.first_name,
                    last_name=event.last_name,
                    email=event.email,
                    phone=event.phone,
                    office_code=event.office_code,
                    department=event.department,
                    manager_id=event.manager_id,
                    status="ACTIVE" if event.status != EmployeeStatus.INACTIVE else "INACTIVE",
                    source_version=event.source_version,
                    source_timestamp=event.source_timestamp,
                    record_hash=record_hash,
                    transport_review_required=False,
                    created_at=func.now(),
                    updated_at=func.now(),
                )
                await self.employee_repo.create(new_emp)

                # Atomically enqueue Outbox Event
                await self.outbox_repo.enqueue(
                    tenant_id=tenant_id,
                    event_type="EMPLOYEE_CREATED",
                    aggregate_id=event.employee_id,
                    payload={
                        "employee_id": event.employee_id,
                        "office_code": event.office_code,
                        "status": new_emp.status,
                        "source_version": event.source_version,
                    },
                )

                accepted_count += 1
                results.append(
                    RosterRecordResult(
                        record_index=idx,
                        employee_id=event.employee_id,
                        status="accepted",
                        source_version=event.source_version,
                        message="Employee created successfully",
                    )
                )
                ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="accepted").inc()
                continue

            # =================================================================
            # EMPLOYEE ALREADY EXISTS: VERSION ORDERING & TIE-BREAKING
            # =================================================================
            if event.source_version < emp.source_version:
                # STALE EVENT: Lower version than current stored version
                stale_count += 1
                results.append(
                    RosterRecordResult(
                        record_index=idx,
                        employee_id=event.employee_id,
                        status="stale_ignored",
                        source_version=event.source_version,
                        message=f"Stale version {event.source_version} ignored; current version is {emp.source_version}",
                    )
                )
                ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="stale_ignored").inc()
                continue

            if event.source_version == emp.source_version:
                # EQUAL VERSION: TIE-BREAKING PROTOCOL
                # Rule 1: Compare source timestamps
                if event.source_timestamp < emp.source_timestamp:
                    stale_count += 1
                    results.append(
                        RosterRecordResult(
                            record_index=idx,
                            employee_id=event.employee_id,
                            status="stale_ignored",
                            source_version=event.source_version,
                            message=f"Equal version {event.source_version} with older timestamp ignored as stale",
                        )
                    )
                    ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="stale_ignored").inc()
                    continue

                # Rule 2: Compare canonical hash to detect identical duplicate vs concurrent conflict
                candidate_hash = compute_canonical_record_hash(
                    employee_id=event.employee_id,
                    first_name=event.first_name or emp.first_name,
                    last_name=event.last_name or emp.last_name,
                    email=event.email or emp.email,
                    phone=event.phone or emp.phone,
                    office_code=event.office_code or emp.office_code,
                    department=event.department if event.department is not None else emp.department,
                    manager_id=event.manager_id if event.manager_id is not None else emp.manager_id,
                    status="INACTIVE" if event.event_type == EventType.DEACTIVATE else emp.status,
                )

                if candidate_hash == emp.record_hash:
                    # IDENTICAL DUPLICATE: No-op
                    duplicate_count += 1
                    results.append(
                        RosterRecordResult(
                            record_index=idx,
                            employee_id=event.employee_id,
                            status="duplicate_noop",
                            source_version=event.source_version,
                            message="Duplicate event with identical payload is a no-op",
                        )
                    )
                    ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="duplicate_noop").inc()
                    continue

                # Rule 3: Deterministic tie-breaking on equal timestamp (Lexicographical hash comparison)
                if event.source_timestamp == emp.source_timestamp and candidate_hash < emp.record_hash:
                    stale_count += 1
                    results.append(
                        RosterRecordResult(
                            record_index=idx,
                            employee_id=event.employee_id,
                            status="stale_ignored",
                            source_version=event.source_version,
                            message=f"Equal version {event.source_version} and timestamp with lower canonical hash ignored as stale",
                        )
                    )
                    ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="stale_ignored").inc()
                    continue

            # =================================================================
            # VALID NEWER VERSION: APPLY FIELD OWNERSHIP INVARIANTS
            # =================================================================
            # HR owns: first_name, last_name, email, phone, office_code, department, manager_id, status
            # Transport Admin owns: preferred_pickup_point_id, pickup_geofence, special_assistance_*, shift_timing
            # (Notice: transport fields are untouched!)

            # Detect Office Transfer Conflict
            office_changed = False
            if event.office_code and event.office_code != emp.office_code:
                office_changed = True
                emp.office_code = event.office_code
                # If employee has an active pickup point or geofence, flag transport operator review
                if emp.preferred_pickup_point_id or emp.pickup_geofence:
                    emp.transport_review_required = True

            # Apply HR Updates
            if event.first_name:
                emp.first_name = event.first_name
            if event.last_name:
                emp.last_name = event.last_name
            if event.email:
                emp.email = event.email
            if event.phone is not None:
                emp.phone = event.phone
            if event.department is not None:
                emp.department = event.department
            if event.manager_id is not None:
                emp.manager_id = event.manager_id

            previous_status = emp.status
            is_deactivation = False
            if event.event_type == EventType.DEACTIVATE or event.status == EmployeeStatus.INACTIVE:
                emp.status = "INACTIVE"
                # Emit EMPLOYEE_DEACTIVATED ONLY on actual ACTIVE -> INACTIVE transition
                is_deactivation = (previous_status == "ACTIVE")
            elif event.status == EmployeeStatus.ACTIVE:
                emp.status = "ACTIVE"

            emp.source_version = event.source_version
            emp.source_timestamp = event.source_timestamp
            emp.record_hash = compute_canonical_record_hash(
                employee_id=emp.employee_id,
                first_name=emp.first_name,
                last_name=emp.last_name,
                email=emp.email,
                phone=emp.phone,
                office_code=emp.office_code,
                department=emp.department,
                manager_id=emp.manager_id,
                status=emp.status,
            )
            emp.updated_at = func.now()

            # Determine Outbox Event Type
            if is_deactivation:
                outbox_event_type = "EMPLOYEE_DEACTIVATED"
            elif office_changed:
                outbox_event_type = "OFFICE_TRANSFERRED"
            else:
                outbox_event_type = "EMPLOYEE_UPDATED"

            # Atomically enqueue Outbox Event in same transaction
            await self.outbox_repo.enqueue(
                tenant_id=tenant_id,
                event_type=outbox_event_type,
                aggregate_id=event.employee_id,
                payload={
                    "employee_id": event.employee_id,
                    "office_code": emp.office_code,
                    "status": emp.status,
                    "source_version": event.source_version,
                    "transport_review_required": emp.transport_review_required,
                },
            )

            accepted_count += 1
            results.append(
                RosterRecordResult(
                    record_index=idx,
                    employee_id=event.employee_id,
                    status="accepted",
                    source_version=event.source_version,
                    message="Employee updated successfully",
                )
            )
            ROSTER_EVENTS_TOTAL.labels(tenant=tenant_id, status="accepted").inc()

        return RosterBatchEventResponse(
            tenant_id=tenant_id,
            total_received=len(request.events),
            accepted_count=accepted_count,
            stale_count=stale_count,
            duplicate_count=duplicate_count,
            rejected_count=rejected_count,
            results=results,
            errors=errors,
        )
