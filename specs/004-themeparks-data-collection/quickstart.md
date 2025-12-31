# Quickstart: Theme Park Data Warehouse Developer Guide

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31
**Purpose**: How to work with the historical import, permanent retention, and analytics schema

---

## Overview

This feature implements:
1. **Historical Import**: Import years of archive data from archive.themeparks.wiki
2. **Permanent Retention**: Keep all raw snapshots forever (no 24h deletion)
3. **Analytics Schema**: Partitioned tables optimized for time-series queries
4. **UUID Mapping**: Reconcile themeparks.wiki UUIDs with internal integer IDs

---

## Setup

### Prerequisites

```bash
# Ensure database migrations are applied
cd backend  # From project root, or use absolute path to your installation
DB_PASSWORD=<password> alembic upgrade head

# Verify new tables exist
mysql -u root -p themepark_tracker_dev -e "SHOW TABLES LIKE '%import%';"
mysql -u root -p themepark_tracker_dev -e "SHOW TABLES LIKE '%metadata%';"
mysql -u root -p themepark_tracker_dev -e "SHOW TABLES LIKE '%queue%';"
```

### Environment Variables

```bash
# .env additions
ARCHIVE_S3_BUCKET=archive.themeparks.wiki
ARCHIVE_S3_REGION=eu-west-2
IMPORT_BATCH_SIZE=10000
IMPORT_CHECKPOINT_INTERVAL=1000
```

---

## Historical Import

### Starting an Import

```python
from src.importer.archive_importer import ArchiveImporter
from src.database.connection import get_db_session

with get_db_session() as session:
    importer = ArchiveImporter(session)

    # Import a single park
    result = importer.import_park(
        park_id=134,  # Disneyland
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31)
    )

    print(f"Imported {result.records_imported} records")
    print(f"Errors: {result.errors_encountered}")
```

### Resuming After Interruption

```python
# Check for existing checkpoint
checkpoint = importer.get_checkpoint(park_id=134)
if checkpoint:
    print(f"Resuming from {checkpoint.last_processed_date}")
    result = importer.resume_import(checkpoint.checkpoint_id)
else:
    result = importer.import_park(park_id=134, ...)
```

### Monitoring Progress

```python
# Get import status
status = importer.get_import_status(import_id="imp_abc123")

print(f"Progress: {status.percent_complete}%")
print(f"Records: {status.records_imported} / {status.records_estimated}")
print(f"ETA: {status.eta_minutes} minutes")
```

### Command Line Usage

```bash
# Import all parks (runs sequentially with checkpointing)
PYTHONPATH=src python -m scripts.import_historical --all-parks

# Import specific park with date range
PYTHONPATH=src python -m scripts.import_historical \
    --park-id 134 \
    --start-date 2020-01-01 \
    --end-date 2025-12-31

# Resume interrupted import
PYTHONPATH=src python -m scripts.import_historical --resume
```

---

## UUID Reconciliation

### Matching Algorithm

The importer uses a multi-step matching algorithm to reconcile themeparks.wiki UUIDs with internal IDs:

```python
def reconcile_entity(themeparks_id: str, name: str, park_id: int) -> int:
    """
    Match themeparks.wiki entity to internal ride_id.

    Algorithm:
    1. If themeparks_wiki_id matches existing ride -> use existing ride_id
    2. Else fuzzy match on (park_id, ride_name) with Levenshtein distance < 3
    3. If confident match found -> populate themeparks_wiki_id, use existing ride_id
    4. Else create new ride record

    Returns:
        Internal ride_id (existing or newly created)
    """
```

### Manual ID Mapping

For rides that can't be auto-matched:

```python
from src.importer.id_mapper import IDMapper

mapper = IDMapper(session)

# Map a specific UUID to internal ID
mapper.map_entity(
    themeparks_wiki_id="abc123-uuid",
    internal_ride_id=1234
)

# Bulk mapping from CSV
mapper.import_mappings_from_csv("mappings/manual_ride_mappings.csv")
```

---

## Permanent Retention

### Key Change: No More Deletion

**Before (24h retention)**:
```python
# Old behavior - snapshots deleted after 24h
DELETE FROM ride_status_snapshots
WHERE recorded_at < NOW() - INTERVAL 24 HOUR;
```

**After (permanent retention)**:
```python
# New behavior - NOTHING is deleted
# All snapshots kept permanently
# Aggregation still runs for performance, but source data preserved
```

### Querying Historical Data

```python
from src.models.orm_snapshots import RideStatusSnapshot
from sqlalchemy import func

# Query year-over-year comparison (uses partition pruning)
last_year = session.query(
    func.avg(RideStatusSnapshot.wait_time).label('avg_wait')
).filter(
    RideStatusSnapshot.ride_id == 1234,
    RideStatusSnapshot.recorded_at.between(
        datetime(2024, 12, 1),
        datetime(2024, 12, 31)
    )
).scalar()

this_year = session.query(
    func.avg(RideStatusSnapshot.wait_time).label('avg_wait')
).filter(
    RideStatusSnapshot.ride_id == 1234,
    RideStatusSnapshot.recorded_at.between(
        datetime(2025, 12, 1),
        datetime(2025, 12, 31)
    )
).scalar()

print(f"YoY change: {((this_year - last_year) / last_year) * 100:.1f}%")
```

### Data Source Tracking

```python
# Query only live-collected data
live_data = session.query(RideStatusSnapshot).filter(
    RideStatusSnapshot.data_source == 'LIVE'
).all()

# Query only archive-imported data
archive_data = session.query(RideStatusSnapshot).filter(
    RideStatusSnapshot.data_source == 'ARCHIVE'
).all()

# Audit: count by source
counts = session.query(
    RideStatusSnapshot.data_source,
    func.count(RideStatusSnapshot.snapshot_id)
).group_by(RideStatusSnapshot.data_source).all()
```

---

## Partitioned Queries

### Partition-Aware Queries

The `ride_status_snapshots` table is partitioned by month. Always include `recorded_at` in WHERE clauses for partition pruning:

```python
# GOOD - Uses partition pruning (fast)
snapshots = session.query(RideStatusSnapshot).filter(
    RideStatusSnapshot.ride_id == 1234,
    RideStatusSnapshot.recorded_at >= datetime(2025, 12, 1),
    RideStatusSnapshot.recorded_at < datetime(2026, 1, 1)
).all()

# BAD - Scans all partitions (slow)
snapshots = session.query(RideStatusSnapshot).filter(
    RideStatusSnapshot.ride_id == 1234
).all()
```

### Verify Partition Pruning

```python
from sqlalchemy import text

# Check query plan
sql = """
EXPLAIN SELECT * FROM ride_status_snapshots
WHERE ride_id = 1234
AND recorded_at >= '2025-12-01'
AND recorded_at < '2026-01-01'
"""
result = session.execute(text(sql)).fetchall()

# Look for: partitions: p202512 (single partition)
# NOT: partitions: p202101,p202102,...,p202512 (all partitions)
for row in result:
    print(row)
```

---

## Entity Metadata

### Collecting Metadata

```python
from src.collector.metadata_collector import MetadataCollector

collector = MetadataCollector(session)

# Sync all entity metadata from themeparks.wiki
result = collector.sync_all_metadata()

print(f"Updated: {result.updated_count}")
print(f"Created: {result.created_count}")
print(f"Unchanged: {result.unchanged_count}")
```

### Querying Metadata

```python
from src.models.orm_metadata import EntityMetadata

# Get metadata for a ride
metadata = session.query(EntityMetadata).filter(
    EntityMetadata.ride_id == 1234
).first()

print(f"Location: ({metadata.latitude}, {metadata.longitude})")
print(f"Type: {metadata.entity_type}")
print(f"Indoor/Outdoor: {metadata.indoor_outdoor}")
print(f"Height: {metadata.height_min_cm} - {metadata.height_max_cm} cm")

# Find indoor rides near a location
from sqlalchemy import func

indoor_nearby = session.query(EntityMetadata).filter(
    EntityMetadata.indoor_outdoor == 'INDOOR',
    func.ST_Distance_Sphere(
        func.POINT(metadata.longitude, metadata.latitude),
        func.POINT(-117.9189, 33.8121)  # Disneyland
    ) < 5000  # 5km radius
).all()
```

---

## Queue Data

### Extended Queue Types

The new `queue_data` table captures all queue types beyond standby:

```python
from src.models.orm_queue import QueueData, QueueType

# Get all queue data for a snapshot
queues = session.query(QueueData).filter(
    QueueData.snapshot_id == snapshot.snapshot_id
).all()

for queue in queues:
    if queue.queue_type == QueueType.STANDBY:
        print(f"Standby: {queue.wait_time_minutes} min")
    elif queue.queue_type == QueueType.SINGLE_RIDER:
        print(f"Single Rider: {queue.wait_time_minutes} min")
    elif queue.queue_type == QueueType.PAID_RETURN_TIME:
        print(f"Lightning Lane: ${queue.price_amount} {queue.price_currency}")
        print(f"Return window: {queue.return_time_start} - {queue.return_time_end}")
    elif queue.queue_type == QueueType.BOARDING_GROUP:
        print(f"Virtual Queue: Group {queue.boarding_group_current}")
        print(f"Status: {queue.boarding_group_status}")
```

---

## Storage Monitoring

### Checking Storage Usage

```python
from src.database.repositories.storage_repository import StorageRepository

repo = StorageRepository(session)

# Get current storage summary
summary = repo.get_storage_summary()
print(f"Total size: {summary.total_size_gb} GB")
print(f"Capacity used: {summary.percent_used}%")

# Get growth projections
projections = repo.get_growth_projections(days=30)
print(f"Growth rate: {projections.growth_rate_gb_per_month} GB/month")
print(f"1 year projection: {projections.one_year_size_gb} GB")
print(f"Days until full: {projections.days_until_full}")
```

### Manual Storage Measurement

```bash
# Trigger immediate measurement (normally runs daily)
PYTHONPATH=src python -m scripts.measure_storage

# View partition sizes
mysql -u root -p themepark_tracker_dev -e "
SELECT
    partition_name,
    table_rows,
    ROUND(data_length / 1024 / 1024, 2) as data_mb,
    ROUND(index_length / 1024 / 1024, 2) as index_mb
FROM information_schema.partitions
WHERE table_name = 'ride_status_snapshots'
ORDER BY partition_name DESC
LIMIT 12;
"
```

---

## Data Quality Monitoring

### Checking Data Freshness

```python
from src.database.repositories.quality_repository import QualityRepository

repo = QualityRepository(session)

# Check freshness for all parks
freshness = repo.get_data_freshness()
for park in freshness:
    if park.minutes_since_last > 30:
        print(f"STALE: {park.park_name} - {park.minutes_since_last} min ago")
```

### Viewing Quality Issues

```python
# Get open issues
issues = repo.get_open_issues()
for issue in issues:
    print(f"[{issue.issue_type}] {issue.description}")
    print(f"  Entity: {issue.entity_type} #{issue.entity_id}")
    print(f"  Status: {issue.resolution_status}")

# Mark issue as resolved
repo.update_issue_status(
    log_id=1001,
    status='RESOLVED',
    notes='Fixed by deduplication job'
)
```

### Gap Detection

```python
# Find gaps in data
gaps = repo.detect_gaps(
    park_id=134,
    start_date=date(2020, 1, 1),
    end_date=date(2025, 12, 31),
    min_gap_hours=2
)

for gap in gaps:
    print(f"Gap: {gap.start_time} to {gap.end_time} ({gap.duration_hours}h)")
    print(f"  Type: {gap.gap_type}")
    print(f"  Source: {gap.data_source}")
```

---

## Migration Guide

### From 24h Retention to Permanent

1. **Apply migration**: `alembic upgrade head`
2. **Add data_source column**: Already included in migration
3. **Mark existing data**: All existing snapshots marked as `LIVE`
4. **Disable deletion cron**: Remove/comment out snapshot cleanup job
5. **Run historical import**: Import archive data marked as `ARCHIVE`

### ORM Model Updates

All existing ORM models continue to work. New additions:

```python
# New models to import
from src.models.orm_snapshots import RideStatusSnapshot  # Updated with data_source
from src.models.orm_metadata import EntityMetadata       # NEW
from src.models.orm_queue import QueueData               # NEW
from src.models.orm_import import ImportCheckpoint       # NEW
from src.models.orm_storage import StorageMetrics        # NEW
from src.models.orm_quality import DataQualityLog        # NEW
```

### API Compatibility

All existing API endpoints remain unchanged. New admin endpoints added:

- `GET /api/admin/import/*` - Import management
- `GET /api/admin/storage/*` - Storage monitoring
- `GET /api/admin/quality/*` - Data quality monitoring

---

## Testing

### Integration Test Pattern

```python
# tests/integration/test_historical_import.py
import pytest
from freezegun import freeze_time
from src.importer.archive_importer import ArchiveImporter

@freeze_time("2025-12-31 10:00:00")
def test_import_creates_checkpoint(mysql_connection):
    """Test that import creates checkpoint on interruption"""
    importer = ArchiveImporter(mysql_connection)

    # Start import with small batch for testing
    result = importer.import_park(
        park_id=134,
        start_date=date(2025, 12, 1),
        end_date=date(2025, 12, 31),
        batch_size=100
    )

    # Verify checkpoint exists
    checkpoint = importer.get_checkpoint(park_id=134)
    assert checkpoint is not None
    assert checkpoint.status == 'COMPLETED'
    assert checkpoint.records_imported > 0
```

### Unit Test Pattern

```python
# tests/unit/test_id_reconciliation.py
def test_fuzzy_match_finds_renamed_ride(mock_db_connection):
    """Test that fuzzy matching handles ride renames"""
    from src.importer.id_mapper import IDMapper

    mapper = IDMapper(mock_db_connection)

    # Simulate database with old name
    mock_db_connection.existing_rides = [
        {"ride_id": 1234, "name": "Space Mountain", "park_id": 134}
    ]

    # Try to match with slightly different name
    ride_id = mapper.reconcile_entity(
        themeparks_id="abc-123-uuid",
        name="Space Mountain - Ghost Galaxy",  # Seasonal overlay
        park_id=134
    )

    assert ride_id == 1234  # Should match existing ride
```

---

## Summary

**Key Changes**:
- ✅ Raw snapshots kept permanently (no 24h deletion)
- ✅ `data_source` column tracks LIVE vs ARCHIVE data
- ✅ Monthly partitioning for efficient historical queries
- ✅ UUID mapping via `themeparks_wiki_id` column
- ✅ Extended queue data (Lightning Lane, Virtual Queue, etc.)
- ✅ Entity metadata (coordinates, indoor/outdoor, height requirements)
- ✅ Storage monitoring and capacity planning
- ✅ Data quality tracking and gap detection

**Migration Checklist**:
1. [ ] Apply Alembic migrations
2. [ ] Verify partitioning is active
3. [ ] Disable snapshot deletion cron
4. [ ] Run historical import for all parks
5. [ ] Verify storage monitoring is working
6. [ ] Configure data quality alerts

**Need Help?**
- See `data-model.md` for complete schema
- See `research.md` for technical decisions
- See `contracts/` for API specifications
