"""add_staging_unique_constraint

Revision ID: b741029c54e1
Revises: f315041013be
Create Date: 2026-10-30 19:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b741029c54e1'
down_revision: Union[str, Sequence[str], None] = 'f315041013be'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema: add unique constraint on (run_id, tenant_id, employee_id)."""
    op.create_unique_constraint(
        'uq_staging_run_tenant_emp',
        'reconciliation_staging',
        ['run_id', 'tenant_id', 'employee_id']
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_staging_run_tenant_emp', 'reconciliation_staging', type_='unique')
