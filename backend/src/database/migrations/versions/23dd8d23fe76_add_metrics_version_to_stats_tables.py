"""add_metrics_version_to_stats_tables

Revision ID: 23dd8d23fe76
Revises: 
Create Date: 2025-12-21 12:51:28.827535

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '23dd8d23fe76'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add metrics_version column to ride_daily_stats and park_daily_stats."""
    # Add metrics_version column to ride_daily_stats
    op.add_column(
        'ride_daily_stats',
        sa.Column(
            'metrics_version',
            sa.Integer(),
            nullable=False,
            server_default=sa.text('1'),
            comment='Calculation version for side-by-side comparison during bug fixes'
        )
    )

    # Add metrics_version column to park_daily_stats
    op.add_column(
        'park_daily_stats',
        sa.Column(
            'metrics_version',
            sa.Integer(),
            nullable=False,
            server_default=sa.text('1'),
            comment='Calculation version for side-by-side comparison during bug fixes'
        )
    )

    # Create indexes on metrics_version columns
    op.create_index(
        'idx_ride_daily_stats_version',
        'ride_daily_stats',
        ['metrics_version']
    )
    op.create_index(
        'idx_park_daily_stats_version',
        'park_daily_stats',
        ['metrics_version']
    )


def downgrade() -> None:
    """Remove metrics_version column from ride_daily_stats and park_daily_stats."""
    # Drop indexes first
    op.drop_index('idx_park_daily_stats_version', table_name='park_daily_stats')
    op.drop_index('idx_ride_daily_stats_version', table_name='ride_daily_stats')

    # Drop columns
    op.drop_column('park_daily_stats', 'metrics_version')
    op.drop_column('ride_daily_stats', 'metrics_version')
