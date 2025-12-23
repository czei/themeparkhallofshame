"""drop_ride_hourly_stats_table

Revision ID: 003_drop_hourly_stats
Revises: 23dd8d23fe76
Create Date: 2025-12-21 17:35:00.000000

Phase 4 (T025): Remove ride_hourly_stats table after ORM conversion.
All API endpoints now use on-the-fly aggregation from ride_status_snapshots.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '003_drop_hourly_stats'
down_revision: Union[str, Sequence[str], None] = '23dd8d23fe76'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Drop ride_hourly_stats table - no longer needed after ORM conversion."""
    from sqlalchemy import inspect

    connection = op.get_bind()
    inspector = inspect(connection)

    # Check if table exists before attempting to drop
    if 'ride_hourly_stats' not in inspector.get_table_names():
        return  # Table already dropped, nothing to do

    # Get existing indexes on the table
    existing_indexes = {idx['name'] for idx in inspector.get_indexes('ride_hourly_stats')}

    # Drop indexes first (only if they exist)
    if 'idx_ride_hourly_ride_date' in existing_indexes:
        op.drop_index('idx_ride_hourly_ride_date', table_name='ride_hourly_stats')
    if 'idx_ride_hourly_hour' in existing_indexes:
        op.drop_index('idx_ride_hourly_hour', table_name='ride_hourly_stats')
    if 'idx_ride_hourly_operated' in existing_indexes:
        op.drop_index('idx_ride_hourly_operated', table_name='ride_hourly_stats')

    # Drop the table
    op.drop_table('ride_hourly_stats')


def downgrade() -> None:
    """Recreate ride_hourly_stats table structure (data will be lost)."""

    # Recreate table structure
    op.create_table(
        'ride_hourly_stats',
        sa.Column('hourly_stat_id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('ride_id', sa.Integer(), nullable=False),
        sa.Column('hour_start_utc', sa.DateTime(), nullable=False),
        sa.Column('avg_wait_time_minutes', sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column('operating_snapshots', sa.Integer(), nullable=False, server_default=sa.text('0')),
        sa.Column('down_snapshots', sa.Integer(), nullable=False, server_default=sa.text('0')),
        sa.Column('downtime_hours', sa.Numeric(precision=5, scale=2), nullable=False, server_default=sa.text('0.00')),
        sa.Column('uptime_percentage', sa.Numeric(precision=5, scale=2), nullable=False, server_default=sa.text('0.00')),
        sa.Column('snapshot_count', sa.Integer(), nullable=False, server_default=sa.text('0')),
        sa.Column('ride_operated', sa.Boolean(), nullable=False, server_default=sa.text('0')),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('hourly_stat_id'),
        sa.ForeignKeyConstraint(['ride_id'], ['rides.ride_id'], ondelete='CASCADE'),
        mysql_charset='utf8mb4',
        mysql_collate='utf8mb4_unicode_ci'
    )

    # Recreate indexes
    op.create_index('idx_ride_hourly_ride_date', 'ride_hourly_stats', ['ride_id', 'hour_start_utc'])
    op.create_index('idx_ride_hourly_hour', 'ride_hourly_stats', ['hour_start_utc'])
    op.create_index('idx_ride_hourly_operated', 'ride_hourly_stats', ['ride_operated'])
