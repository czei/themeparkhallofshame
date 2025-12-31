# Data Model: Theme Park Data Warehouse

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31

## Entity Relationship Diagram

```
┌─────────────────┐       ┌──────────────────────────────┐
│     parks       │       │    ride_status_snapshots     │
├─────────────────┤       │    (PARTITIONED BY MONTH)    │
│ park_id PK      │◄──┐   ├──────────────────────────────┤
│ park_name       │   │   │ snapshot_id PK               │
│ timezone        │   │   │ ride_id FK ───────────────────┼──┐
│ themeparks_id   │   │   │ recorded_at (PARTITION KEY)  │  │
└─────────────────┘   │   │ status                       │  │
                      │   │ wait_time                    │  │
┌─────────────────┐   │   │ computed_is_open             │  │
│     rides       │◄──┼───│ data_source (ARCHIVE/LIVE)   │  │
├─────────────────┤   │   └──────────────────────────────┘  │
│ ride_id PK      │───┘                                     │
│ park_id FK      │◄────────────────────────────────────────┘
│ ride_name       │       ┌──────────────────────────────┐
│ themeparks_id   │       │     import_checkpoints       │
│ tier            │       ├──────────────────────────────┤
└─────────────────┘       │ checkpoint_id PK             │
                          │ import_id UNIQUE             │
                          │ park_id FK                   │
┌─────────────────┐       │ last_processed_date          │
│ entity_metadata │       │ last_processed_file          │
├─────────────────┤       │ records_imported             │
│ metadata_id PK  │       │ status                       │
│ ride_id FK      │       └──────────────────────────────┘
│ latitude        │
│ longitude       │       ┌──────────────────────────────┐
│ indoor_outdoor  │       │     storage_metrics          │
│ height_min_cm   │       ├──────────────────────────────┤
│ height_max_cm   │       │ metric_id PK                 │
│ entity_type     │       │ table_name                   │
│ tags (JSON)     │       │ measurement_date             │
└─────────────────┘       │ row_count                    │
                          │ data_size_mb                 │
┌─────────────────┐       │ index_size_mb                │
│ queue_data      │       │ growth_rate_mb_per_day       │
├─────────────────┤       └──────────────────────────────┘
│ queue_id PK     │
│ snapshot_id *   │       ┌──────────────────────────────┐
│ (* no FK-partitioned)   │
│ queue_type      │       │   data_quality_log           │
│ wait_time_mins  │       ├──────────────────────────────┤
│ return_start    │       │ log_id PK                    │
│ return_end      │       │ issue_type                   │
│ price_amount    │       │ entity_id                    │
│ price_currency  │       │ timestamp_start              │
└─────────────────┘       │ timestamp_end                │
                          │ description                  │
                          │ resolution_status            │
                          └──────────────────────────────┘
```

## Schema Changes

### Modified Tables

#### rides (add column)
```python
# Add to orm_ride.py
themeparks_wiki_id: Mapped[Optional[str]] = mapped_column(
    String(36),
    index=True,
    comment="ThemeParks.wiki entity UUID for ID mapping"
)
```
**Migration**: Already exists in ORM, verify column exists in production.

#### ride_status_snapshots (add column + partitioning)
```python
# Add to orm_snapshots.py
data_source: Mapped[str] = mapped_column(
    Enum('LIVE', 'ARCHIVE', name='data_source_enum'),
    nullable=False,
    default='LIVE',
    comment="Source of data: LIVE (collected) or ARCHIVE (imported)"
)
```

**Partitioning DDL**:
```sql
-- Apply to existing table (requires data migration)
ALTER TABLE ride_status_snapshots
PARTITION BY RANGE (YEAR(recorded_at) * 100 + MONTH(recorded_at)) (
    PARTITION p_before_2025 VALUES LESS THAN (202501),
    PARTITION p202501 VALUES LESS THAN (202502),
    PARTITION p202502 VALUES LESS THAN (202503),
    -- Continue for future months...
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

### New Tables

#### import_checkpoints
```python
class ImportCheckpoint(Base):
    """Tracks historical import progress for resumable imports."""
    __tablename__ = "import_checkpoints"

    checkpoint_id: Mapped[int] = mapped_column(primary_key=True)
    # API-facing identifier (e.g., "imp_abc123") for external references
    import_id: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        nullable=False,
        comment="Public-facing import identifier (e.g., 'imp_abc123')"
    )
    park_id: Mapped[int] = mapped_column(ForeignKey("parks.park_id"))
    last_processed_date: Mapped[date] = mapped_column(Date)
    last_processed_file: Mapped[str] = mapped_column(String(255))
    records_imported: Mapped[int] = mapped_column(Integer, default=0)
    errors_encountered: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(
        Enum('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED', name='import_status_enum')
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, onupdate=func.now())
```

#### entity_metadata
```python
class EntityMetadata(Base):
    """Comprehensive attraction metadata from ThemeParks.wiki."""
    __tablename__ = "entity_metadata"

    metadata_id: Mapped[int] = mapped_column(primary_key=True)
    ride_id: Mapped[int] = mapped_column(ForeignKey("rides.ride_id"))
    themeparks_wiki_id: Mapped[str] = mapped_column(String(36), unique=True)
    latitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 7))
    longitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 7))
    indoor_outdoor: Mapped[Optional[str]] = mapped_column(
        Enum('INDOOR', 'OUTDOOR', 'HYBRID', name='indoor_outdoor_enum')
    )
    height_min_cm: Mapped[Optional[int]] = mapped_column(Integer)
    height_max_cm: Mapped[Optional[int]] = mapped_column(Integer)
    entity_type: Mapped[str] = mapped_column(String(50))  # ATTRACTION, SHOW, etc.
    tags: Mapped[Optional[str]] = mapped_column(JSON)
    last_updated: Mapped[datetime] = mapped_column(DateTime)
    version: Mapped[int] = mapped_column(Integer, default=1)
```

#### queue_data
```python
class QueueData(Base):
    """Extended queue information beyond standby wait times."""
    __tablename__ = "queue_data"

    queue_id: Mapped[int] = mapped_column(primary_key=True)
    # NOTE: No FK constraint - MySQL does not support FK references to partitioned tables
    # Application-level integrity enforced via import validation
    snapshot_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        index=True,
        comment="Reference to ride_status_snapshots.snapshot_id (no FK due to partitioning)"
    )
    queue_type: Mapped[str] = mapped_column(
        Enum('STANDBY', 'SINGLE_RIDER', 'RETURN_TIME',
             'PAID_RETURN_TIME', 'BOARDING_GROUP', name='queue_type_enum')
    )
    wait_time_minutes: Mapped[Optional[int]] = mapped_column(Integer)
    return_time_start: Mapped[Optional[datetime]] = mapped_column(DateTime)
    return_time_end: Mapped[Optional[datetime]] = mapped_column(DateTime)
    price_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    price_currency: Mapped[Optional[str]] = mapped_column(String(3))
    boarding_group_status: Mapped[Optional[str]] = mapped_column(String(50))
    boarding_group_current: Mapped[Optional[str]] = mapped_column(String(50))
```

#### storage_metrics
```python
class StorageMetrics(Base):
    """Database storage tracking for capacity planning."""
    __tablename__ = "storage_metrics"

    metric_id: Mapped[int] = mapped_column(primary_key=True)
    table_name: Mapped[str] = mapped_column(String(100))
    measurement_date: Mapped[date] = mapped_column(Date)
    row_count: Mapped[int] = mapped_column(BigInteger)
    data_size_mb: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    index_size_mb: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    growth_rate_mb_per_day: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))

    __table_args__ = (
        UniqueConstraint('table_name', 'measurement_date', name='unique_table_date'),
    )
```

#### data_quality_log
```python
class DataQualityLog(Base):
    """Data quality issues and gaps tracking."""
    __tablename__ = "data_quality_log"

    log_id: Mapped[int] = mapped_column(primary_key=True)
    issue_type: Mapped[str] = mapped_column(
        Enum('GAP', 'DUPLICATE', 'INVALID', 'MISSING_FIELD', name='issue_type_enum')
    )
    entity_type: Mapped[str] = mapped_column(String(50))  # 'ride', 'park', etc.
    entity_id: Mapped[Optional[int]] = mapped_column(Integer)
    timestamp_start: Mapped[datetime] = mapped_column(DateTime)
    timestamp_end: Mapped[Optional[datetime]] = mapped_column(DateTime)
    description: Mapped[str] = mapped_column(Text)
    resolution_status: Mapped[str] = mapped_column(
        Enum('OPEN', 'INVESTIGATING', 'RESOLVED', 'WONTFIX', name='resolution_enum'),
        default='OPEN'
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
```

## Validation Rules

| Entity | Field | Rule |
|--------|-------|------|
| RideStatusSnapshot | recorded_at | Must be UTC, not in future |
| RideStatusSnapshot | wait_time | Must be 0-300 or NULL |
| RideStatusSnapshot | status | Must be valid enum value |
| EntityMetadata | latitude | Must be -90 to 90 |
| EntityMetadata | longitude | Must be -180 to 180 |
| QueueData | price_amount | Must be >= 0 if not NULL |
| ImportCheckpoint | last_processed_date | Must not exceed today |

## Index Strategy

### Primary Indexes (partition-aware)
```sql
-- ride_status_snapshots (partitioned)
PRIMARY KEY (snapshot_id, recorded_at)  -- Include partition key
INDEX idx_ride_recorded (ride_id, recorded_at)  -- Most common query pattern

-- queue_data
PRIMARY KEY (queue_id)
INDEX idx_queue_snapshot (snapshot_id)

-- entity_metadata
PRIMARY KEY (metadata_id)
UNIQUE INDEX idx_themeparks_id (themeparks_wiki_id)
INDEX idx_metadata_ride (ride_id)
```

### Analytical Indexes
```sql
-- For year-over-year comparisons
INDEX idx_snapshots_date_ride (recorded_at, ride_id)

-- For park-level aggregations
INDEX idx_snapshots_park (ride_id, recorded_at)
-- (uses rides.park_id via join)
```

## State Transitions

### ImportCheckpoint.status
```
[Created] → PENDING → IN_PROGRESS → COMPLETED
                ↓           ↓
                └───────→ PAUSED → IN_PROGRESS (resume)
                            ↓
                          FAILED → IN_PROGRESS (retry)
```

### DataQualityLog.resolution_status
```
OPEN → INVESTIGATING → RESOLVED
  ↓         ↓
  └─────────┴──→ WONTFIX
```

## Migration Order

1. Add `data_source` column to `ride_status_snapshots`
2. Create `import_checkpoints` table
3. Create `entity_metadata` table
4. Create `queue_data` table
5. Create `storage_metrics` table
6. Create `data_quality_log` table
7. Apply partitioning to `ride_status_snapshots` (requires data migration)
8. Update ORM models with new relationships

## Partitioning Rollback Procedure

If partitioning causes issues after deployment, follow this rollback procedure:

### Step 1: Verify Current State
```sql
-- Check partitions exist
SELECT partition_name, table_rows, data_length
FROM information_schema.partitions
WHERE table_name = 'ride_status_snapshots';
```

### Step 2: Create Non-Partitioned Copy
```sql
-- Create table with identical schema but no partitions
CREATE TABLE ride_status_snapshots_unpartitioned LIKE ride_status_snapshots;
ALTER TABLE ride_status_snapshots_unpartitioned REMOVE PARTITIONING;

-- Copy all data (may take 10+ minutes for large datasets)
INSERT INTO ride_status_snapshots_unpartitioned
SELECT * FROM ride_status_snapshots;
```

### Step 3: Swap Tables
```sql
-- Rename in single transaction (minimal downtime)
RENAME TABLE
    ride_status_snapshots TO ride_status_snapshots_partitioned_backup,
    ride_status_snapshots_unpartitioned TO ride_status_snapshots;
```

### Step 4: Verify Application
- Run smoke tests against API
- Verify frontend loads correctly
- Check query performance is acceptable

### Step 5: Cleanup (after validation)
```sql
-- Only after confirming rollback success
DROP TABLE ride_status_snapshots_partitioned_backup;
```

**Estimated Rollback Time**: 10-15 minutes for current data volume (~135k rows/day)
