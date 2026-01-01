"""Add data_source column to ride_status_snapshots

Revision ID: 004a_data_source
Revises: e7b787f62d36
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T006 - Add data_source ENUM column to distinguish LIVE vs ARCHIVE data
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004a_data_source'
down_revision = 'e7b787f62d36'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create the ENUM type first
    data_source_enum = sa.Enum('LIVE', 'ARCHIVE', name='data_source_enum')
    data_source_enum.create(op.get_bind(), checkfirst=True)

    # Add column with default value for existing data
    op.add_column(
        'ride_status_snapshots',
        sa.Column(
            'data_source',
            data_source_enum,
            nullable=False,
            server_default='LIVE',
            comment='Source of data: LIVE (collected) or ARCHIVE (imported)'
        )
    )

    # Add index for data_source queries (e.g., filtering by source)
    op.create_index(
        'idx_snapshots_data_source',
        'ride_status_snapshots',
        ['data_source', 'recorded_at']
    )


def downgrade() -> None:
    # Remove index first
    op.drop_index('idx_snapshots_data_source', table_name='ride_status_snapshots')

    # Remove column
    op.drop_column('ride_status_snapshots', 'data_source')

    # Drop the ENUM type
    sa.Enum('LIVE', 'ARCHIVE', name='data_source_enum').drop(op.get_bind(), checkfirst=True)
