"""Nightly Reconciliation & Safety Brake Router"""

import io
from typing import Optional
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.auth import require_roles, AuthContext
from app.schemas.reconciliation import (
    ReconciliationRunResponse,
    ReconciliationReport,
    ReconciliationApprovalRequest,
)
from app.services.reconciliation_service import ReconciliationService

router = APIRouter(prefix="/reconciliation", tags=["Nightly Reconciliation"])


@router.post("/upload", response_model=ReconciliationRunResponse, status_code=status.HTTP_200_OK)
async def upload_reconciliation_file(
    file: UploadFile = File(...),
    threshold_percent: Optional[float] = Form(None),
    auth: AuthContext = Depends(require_roles(["hr_integration", "operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Upload and stream nightly full-roster export file (CSV up to 200,000 records).
    
    Streaming diff with bounded memory. If missing active employees >= threshold (default 30%),
    halts deactivation and triggers the Safety Brake.
    """
    tenant_id = auth.tenant_id
    filename = file.filename or "export.csv"

    # Read uploaded bytes and wrap in TextIOWrapper for streaming line processing
    contents = await file.read()
    text_stream = io.StringIO(contents.decode("utf-8"))

    service = ReconciliationService(session)
    run = await service.execute_reconciliation(
        tenant_id=tenant_id,
        filename=filename,
        file_content=text_stream,
        threshold_percent=threshold_percent,
    )

    return ReconciliationRunResponse(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        filename=run.filename,
        status=run.status,
        total_records_received=run.total_records_received,
        records_created=run.records_created,
        records_updated=run.records_updated,
        records_unchanged=run.records_unchanged,
        records_stale=run.records_stale,
        records_deactivated=run.records_deactivated,
        missing_active_records=run.missing_active_records,
        missing_percentage=run.missing_percentage,
        operator_review_required=run.operator_review_required,
        approved_by_operator=run.approved_by_operator,
        operator_id=run.operator_id,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )


@router.get("/{run_id}/report", response_model=ReconciliationReport)
async def get_reconciliation_report(
    run_id: str,
    auth: AuthContext = Depends(require_roles(["operator", "transport_admin", "hr_integration"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Inspect detailed safety brake report for a reconciliation run."""
    service = ReconciliationService(session)
    return await service.get_report(tenant_id=auth.tenant_id, run_id=run_id)


@router.post("/{run_id}/approve", response_model=ReconciliationRunResponse)
async def approve_safety_brake_run(
    run_id: str,
    request: ReconciliationApprovalRequest,
    auth: AuthContext = Depends(require_roles(["operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Operator explicitly overrides safety brake and authorizes mass employee deactivations."""
    service = ReconciliationService(session)
    run = await service.approve_run(
        tenant_id=auth.tenant_id,
        run_id=run_id,
        operator_id=request.operator_id,
        reason=request.reason,
    )
    return ReconciliationRunResponse(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        filename=run.filename,
        status=run.status,
        total_records_received=run.total_records_received,
        records_created=run.records_created,
        records_updated=run.records_updated,
        records_unchanged=run.records_unchanged,
        records_stale=run.records_stale,
        records_deactivated=run.records_deactivated,
        missing_active_records=run.missing_active_records,
        missing_percentage=run.missing_percentage,
        operator_review_required=run.operator_review_required,
        approved_by_operator=run.approved_by_operator,
        operator_id=run.operator_id,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )


@router.post("/{run_id}/abort", response_model=ReconciliationRunResponse)
async def abort_safety_brake_run(
    run_id: str,
    request: ReconciliationApprovalRequest,
    auth: AuthContext = Depends(require_roles(["operator"])),
    session: AsyncSession = Depends(get_db_session),
):
    """Operator aborts a halted reconciliation run without deactivating anyone."""
    service = ReconciliationService(session)
    run = await service.abort_run(
        tenant_id=auth.tenant_id,
        run_id=run_id,
        operator_id=request.operator_id,
        reason=request.reason,
    )
    return ReconciliationRunResponse(
        run_id=run.run_id,
        tenant_id=run.tenant_id,
        filename=run.filename,
        status=run.status,
        total_records_received=run.total_records_received,
        records_created=run.records_created,
        records_updated=run.records_updated,
        records_unchanged=run.records_unchanged,
        records_stale=run.records_stale,
        records_deactivated=run.records_deactivated,
        missing_active_records=run.missing_active_records,
        missing_percentage=run.missing_percentage,
        operator_review_required=run.operator_review_required,
        approved_by_operator=run.approved_by_operator,
        operator_id=run.operator_id,
        started_at=run.started_at,
        completed_at=run.completed_at,
    )
