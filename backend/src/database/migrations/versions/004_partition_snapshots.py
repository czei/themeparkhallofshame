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
    MySQL also does NOT support foreign keys with partitioning.

    We need to:
    1. Drop the foreign key constraint to rides table
    2. Drop the existing primary key
    3. Recreate with composite key including recorded_at
    4. Apply partitioning

    Note: Foreign key is dropped permanently. Data integrity is maintained
    at application level. This is a common tradeoff for partitioned tables.
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

    # Step 1: Drop foreign key constraint (MySQL doesn't support FK with partitioning)
    # Find and drop any foreign keys on this table
    fk_result = connection.execute(sa.text("""
        SELECT CONSTRAINT_NAME
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'ride_status_snapshots'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    """))
    for fk_row in fk_result.fetchall():
        fk_name = fk_row[0]
        print(f"Dropping foreign key: {fk_name}")
        connection.execute(sa.text(f"""
            ALTER TABLE ride_status_snapshots
            DROP FOREIGN KEY {fk_name}
        """))

    # Step 2: Check if primary key already includes recorded_at
    pk_result = connection.execute(sa.text("""
        SELECT COUNT(*) as col_count
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'ride_status_snapshots'
        AND CONSTRAINT_NAME = 'PRIMARY'
        AND COLUMN_NAME = 'recorded_at'
    """))
    pk_row = pk_result.fetchone()
    pk_already_composite = pk_row and pk_row[0] > 0

    # Only modify PK if not already composite
    if not pk_already_composite:
        print("Modifying primary key to include recorded_at...")
        connection.execute(sa.text("""
            ALTER TABLE ride_status_snapshots
            DROP PRIMARY KEY,
            ADD PRIMARY KEY (snapshot_id, recorded_at)
        """))
    else:
        print("Primary key already includes recorded_at, skipping...")

    # Step 3: Generate partition DDL for 2024-2030
    # Using RANGE with UNIX_TIMESTAMP for TIMESTAMP columns
    # RANGE COLUMNS doesn't support TIMESTAMP type
    partitions = []

    # Calculate UNIX timestamps for partition boundaries
    # Unix timestamp for 2024-01-01 00:00:00 UTC = 1704067200
    import calendar
    from datetime import datetime as dt

    # Historical partition for data before 2024
    before_2024_ts = int(calendar.timegm(dt(2024, 1, 1, 0, 0, 0).timetuple()))
    partitions.append(f"PARTITION p_before_2024 VALUES LESS THAN ({before_2024_ts})")

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
            boundary_ts = int(calendar.timegm(dt(next_year, next_month, 1, 0, 0, 0).timetuple()))
            partitions.append(f"PARTITION {partition_name} VALUES LESS THAN ({boundary_ts})")

    # Future partition for data after 2030
    partitions.append("PARTITION p_future VALUES LESS THAN (MAXVALUE)")

    # Step 4: Apply partitioning
    partition_ddl = ",\n    ".join(partitions)
    connection.execute(sa.text(f"""
        ALTER TABLE ride_status_snapshots
        PARTITION BY RANGE (UNIX_TIMESTAMP(recorded_at)) (
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

    NOTE: The foreign key to rides table is NOT recreated.
    This was intentionally dropped to enable partitioning.
    Data integrity is maintained at the application level.
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
