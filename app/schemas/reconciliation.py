"""Schemas for Nightly Reconciliation & Safety Brake Workflows"""

from datetime import datetime
from typing import Optional, Any, Dict
from pydantic import BaseModel, Field


class ReconciliationRunResponse(BaseModel):
    """Status summary of a reconciliation run."""
    run_id: str
    tenant_id: str
    filename: str
    status: str
    total_records_received: int
    records_created: int
    records_updated: int
    records_unchanged: int
    records_stale: int
    records_deactivated: int
    missing_active_records: int
    missing_percentage: float
    operator_review_required: bool
    approved_by_operator: bool
    operator_id: Optional[str] = None
    started_at: datetime
    completed_at: Optional[datetime] = None


class ReconciliationReport(BaseModel):
    """Detailed inspection report for an operator reviewing an export halted by safety brake."""
    run_id: str
    tenant_id: str
    status: str
    safety_brake_triggered: bool
    safety_brake_threshold_percent: float
    total_active_before_run: int
    snapshot_records_present: int
    missing_active_count: int
    missing_percentage: float
    sample_missing_employee_ids: list[str] = Field(default_factory=list)
    anomaly_summary: Dict[str, Any] = Field(default_factory=dict)
    recommendation: str


class ReconciliationApprovalRequest(BaseModel):
    """Operator approval payload to override safety brake."""
    operator_id: str = Field(..., min_length=2, max_length=64)
    reason: str = Field(..., min_length=5, max_length=512, description="Mandatory reason for approving deactivations")
