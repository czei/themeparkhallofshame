# Tasks: Theme Park Data Warehouse

**Input**: Design documents from `/specs/004-themeparks-data-collection/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Organization**: Tasks grouped by user story for independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (US1-US5)
- Paths use `backend/` prefix per project structure

---

## Phase 1: Setup

**Purpose**: Project initialization and dependency configuration

- [x] T001 Create feature branch `004-themeparks-data-collection` from main
- [x] T002 [P] Add boto3 to requirements.txt for S3 archive access
- [x] T003 [P] Add Levenshtein library to requirements.txt for fuzzy matching
- [x] T004 [P] Create `backend/src/importer/` directory for import modules
- [x] T005 [P] Add environment variables to `.env.example`: `ARCHIVE_S3_BUCKET`, `ARCHIVE_S3_REGION`, `IMPORT_BATCH_SIZE`, `IMPORT_CHECKPOINT_INTERVAL`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database schema changes and ORM models that ALL user stories depend on

**CRITICAL**: Constitution amendment required before proceeding - run `/speckit.constitution` to amend Principle II for permanent retention

### Database Migrations

- [x] T006 Create Alembic migration for `data_source` ENUM column on `ride_status_snapshots` in `backend/src/database/migrations/versions/004_add_data_source_column.py`
- [x] T007 Create Alembic migration for `import_checkpoints` table in `backend/src/database/migrations/versions/004_create_import_checkpoints.py`
- [x] T008 [P] Create Alembic migration for `entity_metadata` table in `backend/src/database/migrations/versions/004_create_entity_metadata.py`
- [x] T009 [P] Create Alembic migration for `queue_data` table in `backend/src/database/migrations/versions/004_create_queue_data.py`
- [x] T010 [P] Create Alembic migration for `storage_metrics` table in `backend/src/database/migrations/versions/004_create_storage_metrics.py`
- [x] T011 [P] Create Alembic migration for `data_quality_log` table in `backend/src/database/migrations/versions/004_create_data_quality_log.py`
- [x] T012 Create Alembic migration for monthly RANGE partitioning on `ride_status_snapshots` in `backend/src/database/migrations/versions/004_partition_snapshots.py`

### ORM Models

- [x] T013 Update `backend/src/models/orm_snapshots.py` to add `data_source` column with ENUM ('LIVE', 'ARCHIVE')
- [x] T014 [P] Create `backend/src/models/orm_import.py` with ImportCheckpoint model per data-model.md
- [x] T015 [P] Create `backend/src/models/orm_metadata.py` with EntityMetadata model per data-model.md
- [x] T016 [P] Create `backend/src/models/orm_queue.py` with QueueData model and QueueType enum per data-model.md
- [x] T017 [P] Create `backend/src/models/orm_storage.py` with StorageMetrics model per data-model.md
- [x] T018 [P] Create `backend/src/models/orm_quality.py` with DataQualityLog model per data-model.md
- [x] T019 Update `backend/src/models/__init__.py` to export all new ORM models

### Core Infrastructure

- [x] T020 Create `backend/src/importer/__init__.py` with module exports
- [x] T021 [P] Create `backend/src/database/repositories/import_repository.py` with CRUD operations for ImportCheckpoint
- [x] T022 [P] Create `backend/src/database/repositories/quality_repository.py` with CRUD operations for DataQualityLog
- [x] T023 [P] Create `backend/src/database/repositories/storage_repository.py` with CRUD operations for StorageMetrics

**Checkpoint**: Foundation ready - run `alembic upgrade head` and verify all tables exist

---

## Phase 3: User Story 1 - Import Historical Wait Time Data (Priority: P1)

**Goal**: Import years of historical data from archive.themeparks.wiki with resumable checkpoints

**Independent Test**: Download sample archive files, parse into target schema, verify data integrity

### Implementation

- [x] T024 [US1] Create `backend/src/importer/archive_parser.py` with zlib decompression and JSON parsing per research.md
- [x] T025 [US1] Create `backend/src/importer/id_mapper.py` with UUID-to-internal-ID reconciliation using Levenshtein matching
- [x] T026 [US1] Create `backend/src/importer/archive_importer.py` with:
  - S3 file listing for destination UUIDs
  - Checkpoint creation/resumption
  - Batch processing with configurable size
  - Error logging to data_quality_log
- [x] T027 [US1] Create `backend/src/scripts/import_historical.py` CLI with argparse:
  - `--all-parks` flag for full import
  - `--park-id` and `--start-date`/`--end-date` for targeted import
  - `--resume` flag to continue from checkpoint
- [x] T028 [US1] Create `backend/src/api/routes/admin.py` with import management endpoints:
  - `POST /api/admin/import/start` - Start new import
  - `GET /api/admin/import/status/{import_id}` - Get import status
  - `POST /api/admin/import/resume/{import_id}` - Resume paused import
  - `POST /api/admin/import/pause/{import_id}` - Pause running import
  - `DELETE /api/admin/import/cancel/{import_id}` - Cancel import
  - `GET /api/admin/import/list` - List all imports with pagination
  - `GET /api/admin/import/quality/{import_id}` - Get quality report
- [x] T029 [US1] Register admin blueprint in `backend/src/api/app.py`
- [x] T030 [US1] Create integration test `backend/tests/integration/test_historical_import.py` verifying:
  - Checkpoint creation on interruption
  - Resume from checkpoint
  - Data integrity after import
- [x] T031 [US1] Create unit test `backend/tests/unit/test_archive_parser.py` for zlib decompression and JSON parsing
- [x] T032 [US1] Create unit test `backend/tests/unit/test_id_mapper.py` for UUID reconciliation and fuzzy matching

**Checkpoint**: Historical import functional - test with single park before full import

---

## Phase 4: User Story 2 - Collect Real-Time Data with Permanent Retention (Priority: P1)

**Goal**: Transition from 24h deletion to permanent retention; capture all queue types

**Independent Test**: Run collector for 24+ hours, verify data older than 24h is NOT deleted

### Implementation

- [x] T033 [US2] Update `backend/src/collector/themeparks_wiki_client.py` to extract all queue types:
  - STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP
- [x] T034 [US2] Create `backend/src/collector/queue_data_collector.py` to save extended queue data to queue_data table
- [x] T035 [US2] Update `backend/src/scripts/collect_snapshots.py` to:
  - Set `data_source='LIVE'` on new snapshots
  - Call queue_data_collector for extended queue types
- [x] T036 [US2] Remove or disable snapshot deletion cron job in `deployment/config/crontab.prod`
- [x] T037 [US2] Update `backend/src/api/routes/health.py` to include data freshness check (alert if >30 min stale)
- [x] T038 [US2] Create integration test `backend/tests/integration/test_permanent_retention.py` verifying data persists beyond 24h
- [x] T039 [US2] Create integration test `backend/tests/integration/test_queue_data_collection.py` for all queue types

**Checkpoint**: Live collection capturing all queue types with permanent retention

---

## Phase 5: User Story 3 - Optimize Schema for Analytics Queries (Priority: P1)

**Goal**: Ensure partitioned queries perform well; frontend continues working via updated ORM

**Independent Test**: Run year-over-year comparison query in <3 seconds; verify partition pruning in EXPLAIN

### Implementation

- [x] T040 [US3] Verify partition pruning works by creating `backend/tests/integration/test_partitioned_queries.py` with EXPLAIN analysis
- [x] T041 [US3] Update `backend/src/utils/query_helpers.py` to include partition-aware date range hints in common queries
- [x] T042 [US3] Update all date-range queries in `backend/src/database/queries/` to include `recorded_at` filter for partition pruning
- [x] T043 [US3] Create performance test `backend/tests/performance/test_partition_performance.py` for year-over-year queries
- [x] T044 [US3] Verify all existing frontend API endpoints work with partitioned table (manual testing checklist)
- [x] T045 [US3] Update aggregation scripts to use park-specific timezone from `parks.timezone` instead of hardcoded Pacific

**Checkpoint**: All queries use partition pruning; frontend works unchanged

---

## Phase 6: User Story 4 - Analyze and Report Storage Requirements (Priority: P2)

**Goal**: Provide storage monitoring, growth projections, and capacity alerts

**Independent Test**: Run storage analyzer, verify projections match actual growth over 7-day period

### Implementation

- [x] T046 [P] [US4] Create `backend/src/scripts/measure_storage.py` script to populate storage_metrics table from information_schema
- [x] T047 [US4] Add storage measurement to daily cron in `deployment/config/crontab.prod`
- [x] T048 [US4] Add storage endpoints to `backend/src/api/routes/admin.py`:
  - `GET /api/admin/storage/summary` - Current usage
  - `GET /api/admin/storage/growth` - Growth analysis and projections
  - `GET /api/admin/storage/partitions` - Partition-level details
  - `GET /api/admin/storage/retention-comparison` - Strategy comparison
  - `GET /api/admin/storage/alerts` - Active alerts
  - `POST /api/admin/storage/measure` - Trigger immediate measurement
- [x] T049 [US4] Implement alert generation in `backend/src/database/repositories/storage_repository.py`:
  - Capacity threshold alerts (60% warning, 80% critical)
  - Growth rate change alerts (>10% change)
  - Days-until-full projections
- [x] T050 [US4] Create integration test `backend/tests/integration/test_storage_metrics.py` for storage calculations

**Checkpoint**: Storage monitoring dashboard functional with projections

---

## Phase 7: User Story 5 - Collect Rich Entity Metadata (Priority: P2)

**Goal**: Collect coordinates, indoor/outdoor, height requirements from themeparks.wiki entity endpoints

**Independent Test**: Query indoor rides near coordinates; verify metadata completeness for tracked attractions

### Implementation

- [x] T051 [P] [US5] Create `backend/src/collector/metadata_collector.py` with:
  - themeparks.wiki entity API integration
  - Coordinate extraction (latitude, longitude)
  - Indoor/outdoor classification
  - Height requirement parsing
  - Tag extraction
- [x] T052 [US5] Create `backend/src/scripts/sync_metadata.py` CLI for manual metadata sync
- [x] T053 [US5] Add daily metadata sync to cron in `deployment/config/crontab.prod`
- [x] T054 [US5] Add data quality endpoints to `backend/src/api/routes/admin.py`:
  - `GET /api/admin/quality/summary` - Overall health
  - `GET /api/admin/quality/gaps` - Gap detection
  - `GET /api/admin/quality/issues` - Open issues
  - `PATCH /api/admin/quality/issues/{log_id}` - Update issue status
  - `GET /api/admin/quality/freshness` - Data freshness by park
  - `GET /api/admin/quality/coverage` - Coverage statistics
- [x] T055 [US5] Create integration test `backend/tests/integration/test_metadata_collection.py` for metadata sync
- [x] T056 [US5] Create integration test `backend/tests/integration/test_spatial_queries.py` for ST_Distance_Sphere queries

**Checkpoint**: Metadata complete for 90%+ of attractions; spatial queries functional

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, cleanup, and validation

- [x] T057 [P] Update CLAUDE.md with new ORM models and admin endpoints
- [x] T058 [P] Create destination UUID mapping reference file `backend/docs/destination-uuids.md`
- [x] T059 Run full test suite and fix any failures: `pytest`
- [x] T060 Run linting and fix issues: `ruff check .`
- [x] T061 Execute quickstart.md validation checklist manually
- [x] T062 Update deployment documentation with new cron jobs and environment variables
- [x] T063 Create rollback runbook in `backend/docs/partitioning-rollback.md` per data-model.md procedure

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1 (Setup) ─────────────────────────────────────────────┐
                                                              │
Phase 2 (Foundational) ◄─────────────────────────────────────┘
    │
    ├──► Phase 3 (US1: Historical Import) ──────────────────┐
    │                                                        │
    ├──► Phase 4 (US2: Permanent Retention) ────────────────┤
    │                                                        │
    ├──► Phase 5 (US3: Schema Optimization) ────────────────┤
    │                                                        │
    ├──► Phase 6 (US4: Storage Analysis) ───────────────────┤
    │                                                        │
    └──► Phase 7 (US5: Entity Metadata) ────────────────────┤
                                                              │
Phase 8 (Polish) ◄────────────────────────────────────────────┘
```

### User Story Dependencies

| Story | Can Start After | Dependencies on Other Stories |
|-------|-----------------|------------------------------|
| US1 (Import) | Phase 2 complete | None |
| US2 (Retention) | Phase 2 complete | None |
| US3 (Schema) | Phase 2 complete | US1 for partition testing with data |
| US4 (Storage) | Phase 2 complete | None |
| US5 (Metadata) | Phase 2 complete | None |

### Critical Path for MVP

**Minimum Viable Feature**: US1 + US2 + US3 (P1 stories)

1. Complete Setup (T001-T005)
2. Complete Foundational (T006-T023)
3. Complete US1: Historical Import (T024-T032)
4. Complete US2: Permanent Retention (T033-T039)
5. Complete US3: Schema Optimization (T040-T045)

---

## Parallel Opportunities

### Phase 2: Foundational Parallelization

```bash
# Parallel migrations (T008-T011):
Task: "Create migration for entity_metadata"
Task: "Create migration for queue_data"
Task: "Create migration for storage_metrics"
Task: "Create migration for data_quality_log"

# Parallel ORM models (T014-T018):
Task: "Create orm_import.py"
Task: "Create orm_metadata.py"
Task: "Create orm_queue.py"
Task: "Create orm_storage.py"
Task: "Create orm_quality.py"

# Parallel repositories (T021-T023):
Task: "Create import_repository.py"
Task: "Create quality_repository.py"
Task: "Create storage_repository.py"
```

### User Story Parallelization

Once Phase 2 is complete, different team members can work on different stories:

```bash
# Developer A: US1 (Historical Import)
# Developer B: US2 (Permanent Retention) + US4 (Storage)
# Developer C: US5 (Entity Metadata)
```

---

## Implementation Strategy

### MVP First (P1 Stories Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: US1 - Historical Import
4. Complete Phase 4: US2 - Permanent Retention
5. Complete Phase 5: US3 - Schema Optimization
6. **STOP and VALIDATE**: Test import with single park, verify retention
7. Deploy if ready

### Pre-Import Verification (CRITICAL)

Before running historical import on production:

1. Verify S3 access: `aws s3 ls s3://archive.themeparks.wiki/ --no-sign-request`
2. Test with single destination/day
3. Verify UUID matching works for sample park
4. Backup existing data
5. Plan aggregation recalculation time (may take hours for years of data)

### Incremental Delivery

1. Setup + Foundational → Schema ready
2. US1 (Import) → Can backfill historical data
3. US2 (Retention) → Live collection enhanced
4. US3 (Schema) → Performance validated
5. US4 (Storage) → Monitoring in place
6. US5 (Metadata) → Rich entity data available

---

## Notes

- [P] = different files, no dependencies between tasks
- [Story] = maps task to user story for traceability
- Constitution amendment required before Phase 2 (Principle II: permanent retention)
- Partitioning migration (T012) must run AFTER data_source column (T006) is applied
- Historical import is ONE-TIME operation that REPLACES existing data
- After import: must recalculate ALL aggregation tables (ride_daily_stats, ride_hourly_stats, park_daily_stats)

---

## Task Summary

| Phase | Tasks | Parallel Tasks | Story |
|-------|-------|----------------|-------|
| Phase 1: Setup | 5 | 4 | - |
| Phase 2: Foundational | 18 | 11 | - |
| Phase 3: US1 Import | 9 | 0 | US1 |
| Phase 4: US2 Retention | 7 | 0 | US2 |
| Phase 5: US3 Schema | 6 | 0 | US3 |
| Phase 6: US4 Storage | 5 | 1 | US4 |
| Phase 7: US5 Metadata | 6 | 1 | US5 |
| Phase 8: Polish | 7 | 2 | - |
| **Total** | **63** | **19** | - |

**MVP Scope**: Phases 1-5 (45 tasks) deliver P1 functionality
**Full Scope**: All 8 phases (63 tasks) deliver complete feature
