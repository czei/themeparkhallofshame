"""add_composite_indexes_for_time_series_queries

Revision ID: 58ce33b2a457
Revises: 23dd8d23fe76
Create Date: 2025-12-22 13:36:44.221249

T013: Add composite indexes for time-series query optimization.

These indexes optimize time-range queries on snapshots. The indexes are designed for:
- Time-range queries on snapshots (recorded_at)
- Entity-specific queries with time ordering (ride_id + recorded_at, park_id + recorded_at)

Note: This migration is idempotent - indexes may already exist on databases
that had them created manually before Alembic was adopted.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = '58ce33b2a457'
down_revision: Union[str, Sequence[str], None] = '23dd8d23fe76'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def index_exists(connection, table_name: str, index_name: str) -> bool:
    """Check if an index exists on a table."""
    inspector = inspect(connection)
    indexes = inspector.get_indexes(table_name)
    return any(idx['name'] == index_name for idx in indexes)


def upgrade() -> None:
    """Add composite indexes for time-series queries.

    Creates indexes on:
    - ride_status_snapshots: (ride_id, recorded_at) and (recorded_at)
    - park_activity_snapshots: (park_id, recorded_at) and (recorded_at)

    Idempotent: skips creation if index already exists.
    """
    connection = op.get_bind()

    # ride_status_snapshots indexes
    if not index_exists(connection, 'ride_status_snapshots', 'idx_ride_recorded'):
        op.create_index(
            'idx_ride_recorded',
            'ride_status_snapshots',
            ['ride_id', 'recorded_at']
        )

    if not index_exists(connection, 'ride_status_snapshots', 'idx_recorded_at'):
        op.create_index(
            'idx_recorded_at',
            'ride_status_snapshots',
            ['recorded_at']
        )

    # park_activity_snapshots indexes
    if not index_exists(connection, 'park_activity_snapshots', 'idx_park_recorded'):
        op.create_index(
            'idx_park_recorded',
            'park_activity_snapshots',
            ['park_id', 'recorded_at']
        )

    if not index_exists(connection, 'park_activity_snapshots', 'idx_recorded_at'):
        op.create_index(
            'idx_recorded_at',
            'park_activity_snapshots',
            ['recorded_at']
        )


def downgrade() -> None:
    """Remove composite indexes.

    Note: Only removes indexes this migration would have created.
    Does not affect other indexes on these tables.
    """
    connection = op.get_bind()

    # Only drop if exists (idempotent downgrade)
    if index_exists(connection, 'park_activity_snapshots', 'idx_recorded_at'):
        op.drop_index('idx_recorded_at', table_name='park_activity_snapshots')

    if index_exists(connection, 'park_activity_snapshots', 'idx_park_recorded'):
        op.drop_index('idx_park_recorded', table_name='park_activity_snapshots')

    if index_exists(connection, 'ride_status_snapshots', 'idx_recorded_at'):
        op.drop_index('idx_recorded_at', table_name='ride_status_snapshots')

    if index_exists(connection, 'ride_status_snapshots', 'idx_ride_recorded'):
        op.drop_index('idx_ride_recorded', table_name='ride_status_snapshots')
