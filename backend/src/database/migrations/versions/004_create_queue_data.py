"""Create queue_data table

Revision ID: 004d_queue_data
Revises: 004c_entity_metadata
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T009 - Create queue_data table for extended queue types (Lightning Lane, etc.)
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004d_queue_data'
down_revision = '004c_entity_metadata'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create queue type ENUM
    queue_type_enum = sa.Enum(
        'STANDBY', 'SINGLE_RIDER', 'RETURN_TIME', 'PAID_RETURN_TIME', 'BOARDING_GROUP',
        name='queue_type_enum'
    )
    queue_type_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'queue_data',
        sa.Column('queue_id', sa.BigInteger(), primary_key=True, autoincrement=True),
        # NOTE: No FK constraint - MySQL does not support FK references to partitioned tables
        # Application-level integrity enforced via import validation
        sa.Column(
            'snapshot_id',
            sa.BigInteger(),
            nullable=False,
            index=True,
            comment="Reference to ride_status_snapshots.snapshot_id (no FK due to partitioning)"
        ),
        sa.Column(
            'queue_type',
            queue_type_enum,
            nullable=False,
            comment="Type of queue: STANDBY, SINGLE_RIDER, RETURN_TIME, etc."
        ),
        sa.Column(
            'wait_time_minutes',
            sa.Integer(),
            nullable=True,
            comment="Wait time in minutes (NULL if not applicable)"
        ),
        sa.Column(
            'return_time_start',
            sa.DateTime(),
            nullable=True,
            comment="Start of return time window (for RETURN_TIME/PAID_RETURN_TIME)"
        ),
        sa.Column(
            'return_time_end',
            sa.DateTime(),
            nullable=True,
            comment="End of return time window"
        ),
        sa.Column(
            'price_amount',
            sa.Numeric(10, 2),
            nullable=True,
            comment="Price for paid queue types (PAID_RETURN_TIME)"
        ),
        sa.Column(
            'price_currency',
            sa.String(3),
            nullable=True,
            comment="ISO 4217 currency code (USD, EUR, etc.)"
        ),
        sa.Column(
            'boarding_group_status',
            sa.String(50),
            nullable=True,
            comment="Status for boarding groups (e.g., OPEN, CLOSED, DISTRIBUTING)"
        ),
        sa.Column(
            'boarding_group_current',
            sa.String(50),
            nullable=True,
            comment="Current boarding group being called"
        ),
        sa.Column(
            'recorded_at',
            sa.DateTime(),
            nullable=False,
            index=True,
            comment="Timestamp when queue data was recorded (denormalized for queries)"
        ),
        comment="Extended queue information beyond standby wait times"
    )

    # Create composite index for efficient queue type queries
    op.create_index(
        'idx_queue_type_time',
        'queue_data',
        ['queue_type', 'recorded_at']
    )


def downgrade() -> None:
    op.drop_table('queue_data')
    sa.Enum(
        'STANDBY', 'SINGLE_RIDER', 'RETURN_TIME', 'PAID_RETURN_TIME', 'BOARDING_GROUP',
        name='queue_type_enum'
    ).drop(op.get_bind(), checkfirst=True)
