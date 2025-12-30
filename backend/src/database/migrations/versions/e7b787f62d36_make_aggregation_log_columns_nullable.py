"""make_aggregation_log_columns_nullable

Revision ID: e7b787f62d36
Revises: 58ce33b2a457
Create Date: 2025-12-29 20:22:50.250733

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e7b787f62d36'
down_revision: Union[str, Sequence[str], None] = '58ce33b2a457'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Make completed_at and aggregated_until_ts nullable in aggregation_log.

    The ORM model has these columns as Optional, but the database has them
    as NOT NULL with DEFAULT CURRENT_TIMESTAMP. This causes issues when
    inserting 'running' status records that don't have a completed_at yet.
    """
    # Make completed_at nullable
    op.alter_column(
        'aggregation_log',
        'completed_at',
        existing_type=sa.TIMESTAMP(),
        nullable=True,
        server_default=None
    )

    # Make aggregated_until_ts nullable
    op.alter_column(
        'aggregation_log',
        'aggregated_until_ts',
        existing_type=sa.TIMESTAMP(),
        nullable=True,
        server_default=None
    )


def downgrade() -> None:
    """Restore NOT NULL constraints with DEFAULT CURRENT_TIMESTAMP."""
    # Restore completed_at to NOT NULL
    op.alter_column(
        'aggregation_log',
        'completed_at',
        existing_type=sa.TIMESTAMP(),
        nullable=False,
        server_default=sa.text('CURRENT_TIMESTAMP')
    )

    # Restore aggregated_until_ts to NOT NULL
    op.alter_column(
        'aggregation_log',
        'aggregated_until_ts',
        existing_type=sa.TIMESTAMP(),
        nullable=False,
        server_default=sa.text('CURRENT_TIMESTAMP')
    )
