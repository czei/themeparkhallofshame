# Research: Theme Park Data Warehouse

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31
**Status**: Complete

## Executive Summary

This research addresses the key technical decisions needed for implementing a permanent data warehouse for theme park wait time data. The primary challenges are:
1. Archive data structure and import strategy
2. Constitution conflict (Principle II vs permanent retention)
3. Schema design for 10+ years of data
4. UUID to internal ID reconciliation

## Research Findings

### 1. Archive.themeparks.wiki Data Structure

**VERIFIED**: Archive structure confirmed via direct S3 access.

#### Directory Structure
```
s3://archive.themeparks.wiki/
├── <destination-uuid>/           # ~90 destinations (resorts)
│   ├── 2021/
│   ├── 2022/
│   ├── 2023/
│   ├── 2024/
│   │   ├── 01/
│   │   ├── 02/
│   │   │   ├── 01.json.gz        # Day 1 of month
│   │   │   ├── 02.json.gz        # Day 2 of month
│   │   │   └── ...
│   │   └── 12/
│   └── 2025/
└── <another-destination-uuid>/
```

#### File Format
- **Compression**: zlib (NOT gzip despite `.gz` extension)
- **Content**: JSON array of events

**Decompression Code**:
```python
import zlib

def inflate_file(file):
    """Decompress zlib-compressed archive file."""
    new_filename = file.replace('.gz', '')
    with open(file, 'rb') as f:
        data = f.read()
    with open(new_filename, 'wb') as f:
        f.write(zlib.decompress(data))
```

**In-memory decompression**:
```python
import zlib
import json

def read_archive_file(file_path):
    """Read and parse zlib-compressed archive file."""
    with open(file_path, 'rb') as f:
        compressed = f.read()
    decompressed = zlib.decompress(compressed)
    return json.loads(decompressed)
```

#### Event Schema
```json
{
  "events": [
    {
      "entityId": "750939c5-a69e-408a-8d55-66c272fa265e",
      "internalId": "10877",
      "name": "TRANSFORMERS™: The Ride-3D",
      "data": {
        "status": "OPERATING",
        "queue": {
          "STANDBY": { "waitTime": 45 }
        },
        "destinationId": "89db5d43-c434-4097-b71f-f6869f495a22"
      },
      "eventTime": "2024-12-25T00:05:10.644Z",
      "localTime": "2024-12-24T19:05:10-05:00",
      "timezone": "America/New_York",
      "parkId": "eb3f4560-2383-4a36-9152-6b3e5ed6bc57",
      "parkSlug": "universalstudiosflorida"
    }
  ]
}
```

#### Key Observations
- **Event-based**: Each record is a status change/update, NOT periodic snapshots
- **Multiple events per ride per day**: Rides appear multiple times as status changes
- **Status values**: OPERATING, CLOSED, DOWN, REFURBISHMENT (matches our enum)
- **Queue data**: `data.queue.STANDBY.waitTime` contains wait time in minutes
- **Shows have showtimes**: `data.showtimes` array instead of queue data
- **Timezone-aware**: Both UTC (`eventTime`) and local time (`localTime`) provided
- **~90 destinations**: Top-level UUIDs represent resorts/destinations, not individual parks
- **Parks within destinations**: `parkId` and `parkSlug` identify specific parks

#### Destination UUID Mapping

The ~90 top-level UUIDs are **destinations** (resort groups). Each destination contains one or more parks. Key destinations:

**Disney Resorts:**
| UUID | Destination | Parks |
|------|-------------|-------|
| `e957da41-3552-4cf6-b636-5babc5cbc4e5` | Walt Disney World | Magic Kingdom, EPCOT, Hollywood Studios, Animal Kingdom, Typhoon Lagoon, Blizzard Beach |
| `bfc89fd6-314d-44b4-b89e-df1a89cf991e` | Disneyland Resort | Disneyland Park, Disney California Adventure |
| `e8d0207f-da8a-4048-bec8-117aa946b2c2` | Disneyland Paris | Disneyland Park, Walt Disney Studios Park |
| `faff60df-c766-4470-8adb-dee78e813f42` | Tokyo Disney Resort | Tokyo Disneyland, Tokyo DisneySea |
| `6e1464ca-1e9b-49c3-8937-c5c6f6675057` | Shanghai Disney Resort | Shanghai Disneyland |
| `abcfffe7-01f2-4f92-ae61-5093346f5a68` | Hong Kong Disneyland | Hong Kong Disneyland Park |

**Universal Resorts:**
| UUID | Destination | Parks |
|------|-------------|-------|
| `89db5d43-c434-4097-b71f-f6869f495a22` | Universal Orlando | Universal Studios Florida, Islands of Adventure, Volcano Bay, Epic Universe |
| `9fc68f1c-3f5e-4f09-89f2-aab2cf1a0741` | Universal Studios | Universal Studios Hollywood |
| `40ebecca-2221-4230-9814-6a00b3fbb558` | Universal Beijing | Universal Studios Beijing |

**Six Flags (15 destinations):** Six Flags Magic Mountain, Great Adventure, Over Texas, Great America, etc.

**Cedar Fair:** Cedar Point, Kings Island, Canada's Wonderland, Knott's Berry Farm, Carowinds, Kings Dominion, etc.

**SeaWorld:** Orlando (+ Aquatica), San Diego, San Antonio

**Others:** Busch Gardens (Tampa, Williamsburg), Dollywood, Hersheypark, Europa-Park, Efteling, PortAventura, LEGOLAND (8 locations), etc.

Full destination list available via: `curl -s "https://api.themeparks.wiki/v1/destinations"`

### 1b. Live vs Archive Data

Both live collection and archive data come from **themeparks.wiki**. The difference is:
- **LIVE**: Collected in real-time via themeparks.wiki API (ongoing, runs indefinitely)
- **ARCHIVE**: Historical data from archive.themeparks.wiki S3 bucket (one-time import)

#### Data Collection Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        themeparks.wiki                                  │
├─────────────────────────────────┬───────────────────────────────────────┤
│  archive.themeparks.wiki (S3)   │   api.themeparks.wiki (API)           │
│  ONE-TIME historical import     │   ONGOING live collection             │
│  Backfills years of past data   │   Runs every 5 min, forever           │
└─────────────────────────────────┴───────────────────────────────────────┘
                │                                   │
                ▼                                   ▼
        data_source='ARCHIVE'               data_source='LIVE'
```

**Key Point**: The archive import is a ONE-TIME operation to backfill historical data before our live collection started. All ongoing and future data collection uses the themeparks.wiki API.

#### Import Strategy

**Decision**: Archive data REPLACES existing live data for overlapping periods

**Rationale**:
- Archive data is the authoritative source (complete, validated)
- Our live collection may have gaps or errors
- Clean slate ensures data consistency
- Aggregation tables must be recalculated from archive data

**Implementation Steps**:

1. **Delete existing snapshot data**
   ```sql
   -- Clear existing snapshots (keep schema)
   TRUNCATE TABLE ride_status_snapshots;
   ```

2. **Import archive data** (one-time, all historical periods)
   - Mark all imported data with `data_source='ARCHIVE'`

3. **Recalculate ALL aggregation tables**
   - `ride_daily_stats` - recalculate from archive snapshots
   - `ride_hourly_stats` - recalculate from archive snapshots
   - `park_daily_stats` - recalculate from archive snapshots
   - This will take significant time for years of data

4. **Resume live collection**
   - New data marked as `data_source='LIVE'`
   - Aggregation jobs continue as normal

**WARNING**: This is a destructive operation. Ensure archive import is tested and validated before deleting production data.

### 2. Constitution Amendment: Permanent Retention

**Decision**: Amend Constitution Principle II to allow permanent snapshot retention for analytics-focused features

**Rationale**: User explicitly wants permanent retention because:
- Storage is cheap (~$0.02/GB/month for standard storage)
- 10 years = ~108 GB total (manageable)
- Raw data enables future correlations and ML features (005, 006)
- Aggregation loses granularity needed for pattern analysis

**Current Principle II States**:
> "Real-time data has a 24-hour retention window, after which it MUST be aggregated into permanent daily/weekly/monthly/yearly summaries and then deleted."

**Proposed Amendment**:
> "Real-time data is aggregated into permanent daily/weekly/monthly/yearly summaries. Raw snapshots MAY be retained permanently when explicitly required for analytics features. When permanent retention is enabled, table partitioning MUST be implemented to maintain query performance."

**Alternatives Considered**:
- Keep 24h deletion + hourly aggregates only (rejected: loses granularity for ML)
- Tiered retention (90 days raw, then hourly) (rejected: user prefers simplicity)

### 3. Partitioning Strategy

**Decision**: Implement monthly RANGE partitioning on `ride_status_snapshots.recorded_at`

**Rationale**:
- MySQL native partitioning supports RANGE on datetime columns
- Monthly partitions balance partition count vs data volume
- Partition pruning enables efficient date-range queries
- Old partitions can be individually archived/compressed if needed

**Implementation**:
```sql
ALTER TABLE ride_status_snapshots PARTITION BY RANGE (YEAR(recorded_at) * 100 + MONTH(recorded_at)) (
    PARTITION p202501 VALUES LESS THAN (202502),
    PARTITION p202502 VALUES LESS THAN (202503),
    -- etc.
);
```

**Alternatives Considered**:
- Yearly partitions (rejected: too large, ~49M rows/year)
- Quarterly partitions (acceptable alternative)
- No partitioning (rejected: query performance degrades with scale)

### 4. UUID to Internal ID Reconciliation

**Decision**: Add `themeparks_wiki_id` column to `rides` table, use fuzzy name matching for historical import

**Rationale**:
- `rides.themeparks_wiki_id` column already exists in ORM (line 61 in orm_ride.py)
- Historical import needs matching algorithm: first by UUID, then by fuzzy name
- Keep internal integer IDs for performance (joins, indexes)

**Matching Algorithm**:
1. If `themeparks_wiki_id` matches existing ride → use existing ride_id
2. Else fuzzy match on `(park_id, ride_name)` with Levenshtein distance < 3
3. If confident match found → populate `themeparks_wiki_id`, use existing ride_id
4. Else create new ride record

**Alternatives Considered**:
- Replace all IDs with UUIDs (rejected: breaking change, performance impact)
- External mapping table (acceptable alternative, more complex)

### 5. Timezone Handling

**Decision**: Use park-specific timezone from `parks` table for aggregation; store all raw data in UTC

**Rationale**:
- `parks.timezone` column exists (e.g., 'America/Los_Angeles', 'America/New_York')
- UTC storage is already standard (orm_snapshots.py line 32)
- Aggregation to `stat_date` must use park's local timezone
- Current code hardcodes Pacific timezone (needs fix)

**Risk Mitigated**: Incorrect day boundary calculations for non-Pacific parks

### 6. Data Source Tracking

**Decision**: Add `data_source` ENUM column ('LIVE', 'ARCHIVE') to ride_status_snapshots

**Rationale**:
- Enables audit of data provenance
- Allows separate quality analysis of archive vs live data
- Minimal storage overhead (1 byte per row)

## Open Questions Resolved

| Question | Answer |
|----------|--------|
| Archive file format? | JSON files organized by date |
| Timestamp format? | ISO 8601 UTC (e.g., "2025-12-31T16:05:33Z") |
| UUID vs integer IDs? | Keep integers, add UUID mapping column |
| Partitioning scheme? | Monthly RANGE on recorded_at |
| Permanent retention OK? | Yes, with constitution amendment |

## Dependencies Confirmed

- Feature 003 (ORM Refactoring): COMPLETE
- Alembic migrations: Available
- `themeparks_wiki_id` column: Already in ORM
- `parks.timezone` column: Already exists

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Archive bucket access denied | Test access before implementation; contact ThemeParks.wiki maintainers |
| Import takes too long | Implement resumable checkpoints; parallelize by park |
| Query performance degrades | Implement partitioning before import |
| Duplicate data from archive + live | Use (ride_id, recorded_at) as deduplication key |
| Storage exceeds projections | Monitor with StorageMetrics table; alert at 80% threshold |

## Edge Case Answers

Answers to edge cases from spec.md:

| Edge Case | Answer |
|-----------|--------|
| Archive unavailable/rate-limited | Use exponential backoff (tenacity); checkpoint progress for resume |
| Duplicate timestamps (archive + live) | Deduplicate on (ride_id, recorded_at); prefer LIVE over ARCHIVE if conflict |
| Park added/removed during import | Import all available data; mark new parks as discovered; skip removed parks gracefully |
| DST/timezone inconsistencies | All raw data stored as UTC; park timezone used only for aggregation day boundaries |
| Archive format changes between years | Normalize to consistent schema during import; log transformation decisions |
| Attractions renamed/merged/split | Fuzzy name matching (Levenshtein < 3); manual mapping CSV for edge cases |
| Storage approaching capacity | Alert at 80% threshold; capacity planning via StorageMetrics projections |
| Gaps in historical data | Log to data_quality_log with GAP issue_type; don't interpolate missing data |
| ID mismatch (archive UUID vs internal ID) | Add themeparks_wiki_id to rides table; fuzzy match on (park_id, name) |
| Corrupted/malformed archive files | Log error, skip file, continue with next; report in import quality report |
