"""add_reconciliation_staging

Revision ID: f315041013be
Revises: e563ef7294fd
Create Date: 2026-10-30 11:18:12.500688

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f315041013be'
down_revision: Union[str, Sequence[str], None] = 'e563ef7294fd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'reconciliation_staging',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('run_id', sa.String(length=64), nullable=False),
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
        sa.Column('record_hash', sa.String(length=128), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )
    op.create_index('idx_staging_run_tenant_emp', 'reconciliation_staging', ['run_id', 'tenant_id', 'employee_id'], unique=False, if_not_exists=True)
    op.create_index('ix_reconciliation_staging_employee_id', 'reconciliation_staging', ['employee_id'], unique=False, if_not_exists=True)
    op.create_index('ix_reconciliation_staging_run_id', 'reconciliation_staging', ['run_id'], unique=False, if_not_exists=True)
    op.create_index('ix_reconciliation_staging_tenant_id', 'reconciliation_staging', ['tenant_id'], unique=False, if_not_exists=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_reconciliation_staging_tenant_id', table_name='reconciliation_staging')
    op.drop_index('ix_reconciliation_staging_run_id', table_name='reconciliation_staging')
    op.drop_index('ix_reconciliation_staging_employee_id', table_name='reconciliation_staging')
    op.drop_index('idx_staging_run_tenant_emp', table_name='reconciliation_staging')
    op.drop_table('reconciliation_staging')
