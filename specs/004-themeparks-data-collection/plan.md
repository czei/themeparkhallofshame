# Implementation Plan: Theme Park Data Warehouse

**Branch**: `004-themeparks-data-collection` | **Date**: 2025-12-31 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/004-themeparks-data-collection/spec.md`

## Summary

Build a comprehensive data warehouse for theme park wait time analytics by:
1. **One-time historical import** from archive.themeparks.wiki (years of past data)
2. Transitioning from 24-hour retention to permanent snapshot storage
3. Redesigning schema with monthly partitioning for efficient time-series queries
4. Adding UUID mapping to reconcile themeparks.wiki IDs with internal integer IDs
5. Collecting extended queue data (Lightning Lane, Virtual Queue, etc.) and entity metadata

**Note**: All data comes from themeparks.wiki. The archive import is a one-time operation that REPLACES existing data, followed by recalculation of all aggregation tables. Live collection resumes after import completes.

## Technical Context

**Language/Version**: Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ ORM, Alembic 1.13+
**Primary Dependencies**: mysqlclient 2.2+, tenacity 8.2+, requests, boto3 (S3 access)
**Storage**: MySQL 5.7+ with RANGE partitioning by month on ride_status_snapshots
**MySQL Spatial**: Required for `ST_Distance_Sphere()` in entity_metadata queries (enabled by default in MySQL 5.7+)
**Testing**: pytest with freezegun, integration tests against themepark_test database
**Target Platform**: Linux server (AWS EC2)
**Project Type**: Web application (Flask backend + React frontend)
**Performance Goals**: <500ms for today/yesterday queries, <3s for year-over-year comparisons
**Constraints**: Partitioning required for queries spanning 10+ years of data
**Scale/Scope**: ~49M rows/year, 108 GB projected for 10 years with indexes

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Single Source of Truth | ✅ PASS | Raw snapshots are the source; aggregates derived on-demand |
| II. Data Retention | ⚠️ AMENDMENT REQUIRED | Constitution mandates 24h deletion; user wants permanent retention |
| III. ORM First | ✅ PASS | All new tables have ORM models |
| IV. TDD | ✅ PASS | Integration tests for import, unit tests for reconciliation |
| XI. Delegate to Zen/PAL | ✅ PASS | Used mcp__pal__analyze for architecture decisions |

**Amendment Required for Principle II**:
Current: "Real-time data has a 24-hour retention window, after which it MUST be aggregated...and then deleted."
Proposed: "Real-time data is aggregated into permanent summaries. Raw snapshots MAY be retained permanently when explicitly required for analytics features. When permanent retention is enabled, table partitioning MUST be implemented."

**ACTION REQUIRED**: Before Phase 3 implementation begins:
1. Run `/speckit.constitution` to formally amend Principle II
2. Document amendment in constitution changelog
3. Update CLAUDE.md "Canonical Business Rules" section if needed

## Project Structure

### Documentation (this feature)

```text
specs/004-themeparks-data-collection/
├── plan.md              # This file
├── research.md          # Phase 0 output (COMPLETE)
├── data-model.md        # Phase 1 output (COMPLETE)
├── quickstart.md        # Phase 1 output (COMPLETE)
├── contracts/           # Phase 1 output (COMPLETE)
│   ├── historical-import-api.md
│   ├── storage-metrics-api.md
│   └── data-quality-api.md
└── tasks.md             # Phase 2 output (PENDING - /speckit.tasks command)
```

### Source Code (repository root)

```text
backend/
├── src/
│   ├── api/
│   │   └── routes/
│   │       └── admin.py           # New admin endpoints for import/storage/quality
│   ├── collector/
│   │   └── metadata_collector.py  # Entity metadata sync from themeparks.wiki
│   ├── database/
│   │   ├── migrations/versions/   # Alembic migrations for new tables
│   │   └── repositories/
│   │       ├── quality_repository.py   # Data quality queries
│   │       └── storage_repository.py   # Storage metrics queries
│   ├── importer/
│   │   ├── archive_importer.py    # Historical import from S3
│   │   └── id_mapper.py           # UUID reconciliation
│   ├── models/
│   │   ├── orm_snapshots.py       # Updated: add data_source column
│   │   ├── orm_metadata.py        # NEW: EntityMetadata model
│   │   ├── orm_queue.py           # NEW: QueueData model
│   │   ├── orm_import.py          # NEW: ImportCheckpoint model
│   │   ├── orm_storage.py         # NEW: StorageMetrics model
│   │   └── orm_quality.py         # NEW: DataQualityLog model
│   └── scripts/
│       ├── import_historical.py   # CLI for historical import
│       └── measure_storage.py     # Storage measurement script
└── tests/
    ├── integration/
    │   ├── test_historical_import.py
    │   ├── test_uuid_reconciliation.py
    │   └── test_partitioned_queries.py
    └── unit/
        ├── test_id_mapper.py
        └── test_archive_parser.py
```

**Structure Decision**: Extends existing backend structure with new `importer/` module for historical import logic. All ORM models in `models/` following established patterns.

## Key Decisions

### 1. Permanent Retention (Constitution Amendment)
- **Decision**: Amend Principle II to allow permanent raw snapshot retention
- **Rationale**: User explicitly requested permanent retention; storage is cheap (~$0.02/GB/month); enables ML features in 005/006
- **Implementation**: Monthly RANGE partitioning on `recorded_at` for query performance

### 2. UUID Reconciliation Strategy
- **Decision**: Add `themeparks_wiki_id` column to `rides` table, use fuzzy name matching for historical import
- **Rationale**: Column already exists in ORM; fuzzy matching handles ride renames; keeps integer IDs for performance
- **Implementation**: Levenshtein distance < 3 for fuzzy matching; manual mapping CSV for edge cases

### 3. Data Source Tracking
- **Decision**: Add `data_source` ENUM ('LIVE', 'ARCHIVE') to ride_status_snapshots
- **Rationale**: Enables audit of data provenance; allows separate quality analysis
- **Implementation**: 1 byte per row overhead; existing data marked as 'LIVE'

### 4. Timezone Handling
- **Decision**: Use park-specific timezone from `parks.timezone` for aggregation
- **Rationale**: Current code hardcodes Pacific timezone; parks span multiple timezones
- **Implementation**: Update aggregation scripts to use park timezone for day boundary calculations

## Complexity Tracking

| Decision | Why Needed | Simpler Alternative Rejected Because |
|----------|------------|--------------------------------------|
| Monthly partitioning | 10+ years of data requires partition pruning | No partitioning: queries would scan all 49M+ rows/year |
| Import checkpoints | Multi-day imports need resume capability | Single transaction: would rollback on any failure |
| Extended queue_data table | Lightning Lane pricing, Virtual Queue status | Column additions: too many nullable columns on snapshots |

## Phase Summary

| Phase | Status | Artifacts |
|-------|--------|-----------|
| Phase 0: Research | ✅ COMPLETE | research.md |
| Phase 1: Design | ✅ COMPLETE | data-model.md, quickstart.md, contracts/ |
| Phase 2: Tasks | ⏳ PENDING | tasks.md (run `/speckit.tasks`) |
| Phase 3: Implementation | ⏳ PENDING | Source code, tests, migrations |

## Pre-Implementation Verification

**CRITICAL**: Before starting Phase 3, complete these verification steps:

### 1. Verify Archive Access and Format
```bash
aws s3 ls s3://archive.themeparks.wiki/ --no-sign-request
aws s3 cp s3://archive.themeparks.wiki/<park>/2024-01-01.json ./sample.json --no-sign-request
```

### 2. Verify Archive Data Compatibility
Sample archive files to confirm:
- JSON structure matches expected schema
- Timestamp format is consistent
- Status values match our enum

### 3. Park/Ride UUID Mapping
Determine which parks/rides in archive need UUID-to-internal-ID mapping

### 4. Plan Aggregation Recalculation
After archive import, ALL aggregation tables must be recalculated:
- `ride_daily_stats` (years of data)
- `ride_hourly_stats` (years of data)
- `park_daily_stats` (years of data)

Estimate time required and plan for downtime or staged rollout.

## Next Steps

1. Run `/speckit.tasks` to generate implementation tasks
2. Review and approve task list
3. Complete pre-implementation verification above
4. Begin implementation following TDD process
