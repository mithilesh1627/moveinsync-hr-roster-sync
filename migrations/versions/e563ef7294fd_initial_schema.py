"""initial_schema

Revision ID: e563ef7294fd
Revises: 
Create Date: 2026-10-30 10:28:53.161594

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'e563ef7294fd'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. employees
    op.create_table(
        'employees',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('tenant_id', sa.String(length=64), nullable=False),
        sa.Column('employee_id', sa.String(length=64), nullable=False),
        sa.Column('first_name', sa.String(length=128), nullable=False),
        sa.Column('last_name', sa.String(length=128), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('phone', sa.String(length=32), nullable=True),
        sa.Column('office_code', sa.String(length=64), nullable=False),
        sa.Column('department', sa.String(length=128), nullable=True),
        sa.Column('manager_id', sa.String(length=64), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('source_version', sa.Integer(), nullable=False),
        sa.Column('source_timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('record_hash', sa.String(length=128), nullable=False),
        sa.Column('preferred_pickup_point_id', sa.String(length=128), nullable=True),
        sa.Column('pickup_geofence', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('special_assistance_required', sa.Boolean(), nullable=False),
        sa.Column('special_assistance_notes', sa.Text(), nullable=True),
        sa.Column('shift_timing', sa.String(length=64), nullable=True),
        sa.Column('transport_review_required', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'employee_id', name='uq_tenant_employee'),
        if_not_exists=True,
    )
    op.create_index('idx_emp_tenant_status', 'employees', ['tenant_id', 'status'], if_not_exists=True)
    op.create_index('idx_emp_tenant_office', 'employees', ['tenant_id', 'office_code'], if_not_exists=True)
    op.create_index('idx_emp_review_required', 'employees', ['tenant_id', 'transport_review_required'], if_not_exists=True)

    # 2. idempotency_keys
    op.create_table(
        'idempotency_keys',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('tenant_id', sa.String(length=64), nullable=False),
        sa.Column('idempotency_key', sa.String(length=128), nullable=False),
        sa.Column('payload_hash', sa.String(length=128), nullable=False),
        sa.Column('response_code', sa.Integer(), nullable=False),
        sa.Column('response_body', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'idempotency_key', name='uq_tenant_idempotency_key'),
        if_not_exists=True,
    )
    op.create_index('idx_idempotency_expires', 'idempotency_keys', ['expires_at'], if_not_exists=True)

    # 3. outbox_events
    op.create_table(
        'outbox_events',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('tenant_id', sa.String(length=64), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('aggregate_id', sa.String(length=64), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('retry_count', sa.Integer(), nullable=False),
        sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )
    op.create_index('idx_outbox_poll', 'outbox_events', ['status', 'next_retry_at'], if_not_exists=True)
    op.create_index('idx_outbox_tenant_agg', 'outbox_events', ['tenant_id', 'aggregate_id'], if_not_exists=True)

    # 4. reconciliation_runs
    op.create_table(
        'reconciliation_runs',
        sa.Column('run_id', sa.String(length=64), nullable=False),
        sa.Column('tenant_id', sa.String(length=64), nullable=False),
        sa.Column('filename', sa.String(length=255), nullable=False),
        sa.Column('status', sa.String(length=64), nullable=False),
        sa.Column('total_records_received', sa.Integer(), nullable=False),
        sa.Column('records_created', sa.Integer(), nullable=False),
        sa.Column('records_updated', sa.Integer(), nullable=False),
        sa.Column('records_unchanged', sa.Integer(), nullable=False),
        sa.Column('records_stale', sa.Integer(), nullable=False),
        sa.Column('records_deactivated', sa.Integer(), nullable=False),
        sa.Column('missing_active_records', sa.Integer(), nullable=False),
        sa.Column('missing_percentage', sa.Float(), nullable=False),
        sa.Column('operator_review_required', sa.Boolean(), nullable=False),
        sa.Column('approved_by_operator', sa.Boolean(), nullable=False),
        sa.Column('operator_id', sa.String(length=64), nullable=True),
        sa.Column('operator_action_notes', sa.Text(), nullable=True),
        sa.Column('anomaly_report', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('run_id'),
        if_not_exists=True,
    )
    op.create_index('idx_recon_tenant_status', 'reconciliation_runs', ['tenant_id', 'status'], if_not_exists=True)
    op.create_index('idx_recon_created', 'reconciliation_runs', ['started_at'], if_not_exists=True)

    # 5. offices
    op.create_table(
        'offices',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('tenant_id', sa.String(length=64), nullable=False),
        sa.Column('office_code', sa.String(length=64), nullable=False),
        sa.Column('name', sa.String(length=128), nullable=False),
        sa.Column('city', sa.String(length=64), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'office_code', name='uq_tenant_office_code'),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table('offices')
    op.drop_table('reconciliation_runs')
    op.drop_table('outbox_events')
    op.drop_table('idempotency_keys')
    op.drop_table('employees')
