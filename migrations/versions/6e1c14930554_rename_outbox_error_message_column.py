"""rename outbox error message column

Revision ID: 6e1c14930554
Revises: b741029c54e1
Create Date: 2026-10-31 03:58:46.197208

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '6e1c14930554'
down_revision: Union[str, Sequence[str], None] = 'b741029c54e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'outbox_events',
        'error_message',
        new_column_name='last_error',
    )


def downgrade() -> None:
    op.alter_column(
        'outbox_events',
        'last_error',
        new_column_name='error_message',
    )