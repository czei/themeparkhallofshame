"""Apply monthly partitioning to ride_status_snapshots

Revision ID: 004g_partition_snapshots
Revises: 004f_data_quality_log
Create Date: 2025-12-31

Feature: 004-themeparks-data-collection
Task: T012 - Apply monthly RANGE partitioning for permanent data retention

IMPORTANT: This migration requires special handling:
1. Must be run during maintenance window (locks table)
2. Creates partitions for 2024-2030 initially
3. Future partitions added via scheduled job

ROLLBACK: See docs/partitioning-rollback.md for recovery procedure
"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime, timedelta

# revision identifiers, used by Alembic.
revision = '004g_partition_snapshots'
down_revision = '004f_data_quality_log'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Apply monthly RANGE partitioning to ride_status_snapshots.

    MySQL requires the partition key to be part of all unique/primary keys.
    We need to:
    1. Drop the existing primary key
    2. Recreate with composite key including recorded_at
    3. Apply partitioning
    """
    connection = op.get_bind()

    # Check if table is already partitioned
    result = connection.execute(sa.text("""
        SELECT COUNT(*) as partition_count
        FROM information_schema.partitions
        WHERE table_schema = DATABASE()
        AND table_name = 'ride_status_snapshots'
        AND partition_name IS NOT NULL
    """))
    row = result.fetchone()
    if row and row[0] > 0:
        print("Table is already partitioned, skipping...")
        return

    # Step 1: Modify primary key to include partition key
    # MySQL requires partition key in all unique keys
    connection.execute(sa.text("""
        ALTER TABLE ride_status_snapshots
        DROP PRIMARY KEY,
        ADD PRIMARY KEY (snapshot_id, recorded_at)
    """))

    # Step 2: Generate partition DDL for 2024-2030
    # Using RANGE on YEAR(recorded_at) * 100 + MONTH(recorded_at) for monthly partitions
    partitions = []

    # Historical partition for data before 2024
    partitions.append("PARTITION p_before_2024 VALUES LESS THAN (202401)")

    # Monthly partitions for 2024-2030
    current_year = 2024
    end_year = 2031  # Create partitions through 2030

    for year in range(current_year, end_year):
        for month in range(1, 13):
            partition_name = f"p{year}{month:02d}"
            # Next month boundary
            next_month = month + 1
            next_year = year
            if next_month > 12:
                next_month = 1
                next_year = year + 1
            boundary = next_year * 100 + next_month
            partitions.append(f"PARTITION {partition_name} VALUES LESS THAN ({boundary})")

    # Future partition for data after 2030
    partitions.append("PARTITION p_future VALUES LESS THAN MAXVALUE")

    # Step 3: Apply partitioning
    partition_ddl = ",\n    ".join(partitions)
    connection.execute(sa.text(f"""
        ALTER TABLE ride_status_snapshots
        PARTITION BY RANGE (YEAR(recorded_at) * 100 + MONTH(recorded_at)) (
            {partition_ddl}
        )
    """))

    print(f"Created {len(partitions)} partitions for ride_status_snapshots")


def downgrade() -> None:
    """
    Remove partitioning from ride_status_snapshots.

    WARNING: This operation:
    1. Removes all partitions (data is preserved)
    2. Reverts to non-partitioned table
    3. May take significant time for large tables
    """
    connection = op.get_bind()

    # Check if table is partitioned
    result = connection.execute(sa.text("""
        SELECT COUNT(*) as partition_count
        FROM information_schema.partitions
        WHERE table_schema = DATABASE()
        AND table_name = 'ride_status_snapshots'
        AND partition_name IS NOT NULL
    """))
    row = result.fetchone()
    if not row or row[0] == 0:
        print("Table is not partitioned, skipping...")
        return

    # Remove partitioning
    connection.execute(sa.text("""
        ALTER TABLE ride_status_snapshots REMOVE PARTITIONING
    """))

    # Restore original primary key
    connection.execute(sa.text("""
        ALTER TABLE ride_status_snapshots
        DROP PRIMARY KEY,
        ADD PRIMARY KEY (snapshot_id)
    """))

    print("Removed partitioning from ride_status_snapshots")
