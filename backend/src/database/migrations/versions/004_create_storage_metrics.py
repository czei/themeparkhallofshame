"""Create storage_metrics table

Revision ID: 004e_storage_metrics
Revises: 004d_queue_data
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T010 - Create storage_metrics table for capacity planning and monitoring
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004e_storage_metrics'
down_revision = '004d_queue_data'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'storage_metrics',
        sa.Column('metric_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            'table_name',
            sa.String(100),
            nullable=False,
            comment="Name of the table being measured"
        ),
        sa.Column(
            'measurement_date',
            sa.Date(),
            nullable=False,
            comment="Date of measurement"
        ),
        sa.Column(
            'row_count',
            sa.BigInteger(),
            nullable=False,
            comment="Number of rows in table"
        ),
        sa.Column(
            'data_size_mb',
            sa.Numeric(12, 2),
            nullable=False,
            comment="Data size in megabytes"
        ),
        sa.Column(
            'index_size_mb',
            sa.Numeric(12, 2),
            nullable=False,
            comment="Index size in megabytes"
        ),
        sa.Column(
            'total_size_mb',
            sa.Numeric(12, 2),
            nullable=False,
            comment="Total size (data + index) in megabytes"
        ),
        sa.Column(
            'growth_rate_mb_per_day',
            sa.Numeric(10, 4),
            nullable=True,
            comment="Calculated growth rate in MB per day (NULL if first measurement)"
        ),
        sa.Column(
            'partition_count',
            sa.Integer(),
            nullable=True,
            comment="Number of partitions (if table is partitioned)"
        ),
        sa.Column(
            'created_at',
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now()
        ),
        sa.UniqueConstraint('table_name', 'measurement_date', name='unique_table_date'),
        comment="Database storage tracking for capacity planning"
    )

    # Create index for time-series queries
    op.create_index(
        'idx_storage_date',
        'storage_metrics',
        ['measurement_date']
    )
    op.create_index(
        'idx_storage_table',
        'storage_metrics',
        ['table_name', 'measurement_date']
    )


def downgrade() -> None:
    op.drop_table('storage_metrics')
