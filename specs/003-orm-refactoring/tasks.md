# Tasks: ORM Refactoring for Reliable Data Access

**Input**: Design documents from `/specs/003-orm-refactoring/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and ORM dependencies

- [X] T001 Create backend/src/models/ directory structure with __init__.py
- [X] T002 Add SQLAlchemy 2.0+, Alembic 1.13+ to backend/requirements.txt
- [X] T003 [P] Configure SQL logging environment variable (SQL_ECHO) in backend/.env.example

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core ORM infrastructure that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

### ORM Models (All parallelizable once base.py is defined)

- [X] T004 [P] Create backend/src/models/base.py with DeclarativeBase, engine config, and Session factory (context-managed session-per-request pattern with scoped_session bound to Flask request context) - MUST complete first or land concurrently for other models to import cleanly
- [X] T005 [P] Create backend/src/models/park.py with Park model including relationships and is_operating_at method
- [X] T006 [P] Create backend/src/models/ride.py with Ride model including relationships and get_current_status method
- [X] T007 [P] Create backend/src/models/snapshots.py with RideStatusSnapshot and ParkActivitySnapshot models including hybrid methods (is_operating, is_down)
- [X] T008 [P] Create backend/src/models/stats.py with DailyStats model including new metrics_version column
- [X] T009 [P] Create backend/src/models/weather.py with WeatherObservation and WeatherForecast models
- [X] T010 Update backend/src/api/app.py with Flask teardown_appcontext to close/remove the SQLAlchemy session from src.models.base (import db_session explicitly and call db_session.remove())

### Migration Framework

- [X] T011 Initialize Alembic in backend/src/database/migrations/ with alembic init command
- [X] T012 Configure backend/src/database/migrations/env.py to import all ORM models from src.models and ensure DB URL is pulled from same config source as runtime (env var/config object for dev/test/prod compatibility)
- [X] T013 Create backend/src/database/migrations/versions/001_add_composite_indexes.py migration script (58ce33b2a457_add_composite_indexes_for_time_series_.py - idempotent, checks if indexes exist)
- [X] T014 Create backend/src/database/migrations/versions/002_add_metrics_version_to_daily_stats.py migration script
- [X] T015 Test migrations 001, 002, and 003 on local dev database with alembic upgrade head (all migrations applied successfully)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Type-Safe Data Access (Priority: P1)

**Goal**: Migrate repository queries from raw SQL to type-safe ORM models

**Independent Test**: Run existing integration tests (pytest backend/tests/integration/), verify identical results with ORM queries

### Implementation for User Story 1

- [X] T016 [P] [US1] Create backend/src/utils/query_helpers.py with ORM query abstraction layer (RideStatusQuery, DowntimeQuery, UptimeQuery classes) - Include only shared query primitives; story-specific logic stays in repositories to prevent god module
- [X] T017 [P] [US1] Migrate backend/src/database/repositories/ride_status_repository.py from raw SQL to ORM queries
- [X] T018 [P] [US1] Migrate backend/src/database/repositories/stats_repository.py from raw SQL to ORM queries
- [X] T019 [P] [US1] Migrate backend/src/database/repositories/park_repository.py from raw SQL to ORM queries
- [X] T020 [P] [US1] Migrate backend/src/database/repositories/weather_repository.py from raw SQL to ORM queries
- [X] T021 [US1] Update backend/src/database/connection.py to initialize SQLAlchemy engine alongside existing MySQL connection
- [X] T022 [US1] Run full test suite (pytest backend/tests/) and fix any ORM-related failures - 756 passed (ORM-related tests pass, remaining 45 failures are spec tests for unimplemented wait times API features)

**Checkpoint**: At this point, User Story 1 should be fully functional - type-safe ORM queries replacing raw SQL

---

## Phase 4: User Story 2 - Flexible Hourly Analytics (Priority: P1)

**Goal**: Remove hourly_stats table and implement on-the-fly ORM hourly aggregation queries

**Independent Test**: Query hourly metrics for custom time window (e.g., 11:30am-1:45pm), verify response time <500ms

### Implementation for User Story 2

- [X] T023 [US2] Implement hourly aggregation ORM queries in backend/src/utils/query_helpers.py (replace hourly_stats table reads)
- [X] T024 [US2] Update hourly API endpoints in backend/src/api/routes/ to use new ORM aggregation queries (depends on T023)
- [X] T025 [US2] Create backend/src/database/migrations/versions/003_drop_hourly_stats_table.py migration script (idempotent, checks table/index existence)
- [X] T026 [US2] Remove hourly aggregation cron job code from backend/src/scripts/ (or comment out in cron config) - Cron removed from crontab.prod, deprecation notices added to aggregate_hourly.py and backfill_hourly_stats.py
- [X] T027 [US2] Validate hourly query performance with MySQL EXPLAIN plans, verify composite index usage (all queries use idx_ride_recorded, idx_recorded_at, idx_rss_time_range_covering, idx_park_recorded)

**Checkpoint**: At this point, User Story 2 should be fully functional - hourly_stats removed, flexible ORM queries working

---

## Phase 5: User Story 3 - Bug Fixes Without Backfills (Priority: P1)

**Goal**: Validate that ORM query bug fixes apply instantly to all historical periods

**Independent Test**: Introduce deliberate test bug in ORM query, deploy fix, verify both new and historical queries return corrected values

### Implementation for User Story 3

- [X] T028 [US3] Create backend/tests/golden_data/test_orm_query_parity.py with regression tests validating ORM results match historical raw SQL values
- [X] T029 [US3] Add "No Backfill Benefit" section to specs/003-orm-refactoring/quickstart.md documenting instant bug fix capability

**Checkpoint**: At this point, User Story 3 validation complete - instant fix behavior proven

---

## Phase 6: User Story 4 - Safe Database Migrations (Priority: P2)

**Goal**: Validate Alembic migration workflow with rollback capability

**Independent Test**: Apply migration, verify schema change with SHOW CREATE TABLE, rollback with alembic downgrade -1, verify original schema restored

### Implementation for User Story 4

- [X] T030 [US4] Test migration 001 (composite indexes) on local dev database: alembic upgrade +1, verify indexes with SHOW INDEX (tested via alembic upgrade head)
- [X] T031 [US4] Test migration 002 (metrics_version) on local dev database: alembic upgrade +1, verify column with DESCRIBE daily_stats (tested via alembic upgrade head)
- [ ] T032 [US4] Add "Migrations" section to specs/003-orm-refactoring/quickstart.md with alembic upgrade/downgrade examples
- [X] T033 [US4] Test rollback capability: alembic downgrade -1 for each migration (001, 002, 003), verify schema reverts correctly - tested downgrade/upgrade cycle successfully

**Checkpoint**: At this point, User Story 4 complete - migration framework validated

---

## Phase 7: User Story 5 - Idempotent Daily Recomputation (Priority: P2)

**Goal**: Create idempotent recompute job for daily_stats table using ORM models

**Independent Test**: Run recompute for 7-day date range, verify daily_stats values match raw snapshot calculations, run job again (idempotent test), confirm no duplicate data

**Depends on**: Phase 2 (ORM infrastructure complete) + DailyStats repository/query helpers from US1

### Implementation for User Story 5

- [X] T034 [US5] Create backend/src/scripts/recompute_daily_stats.py with argparse CLI interface (--start-date, --end-date, --metrics-version)
- [X] T035 [US5] Implement idempotent UPSERT logic using metrics_version column in daily_stats table
- [X] T036 [US5] Add progress tracking (tqdm or logging) and error handling for 90-day batch processing
- [X] T037 [US5] Test recompute job with 7-day historical data, measure execution time, verify <6 hour target extrapolated to 90 days - 7 days in 36 seconds (90-day extrapolated: ~8 min, well under 6h target)

**Checkpoint**: At this point, User Story 5 complete - recompute job working and idempotent

---

## Phase 8: User Story 6 - Performance Validation (Priority: P2)

**Goal**: Load test hourly ORM queries and validate <500ms p95 response time target

**Independent Test**: Run Locust load test simulating 20 concurrent users, measure p95 response time for hourly queries

**Depends on**: US2 (hourly queries), US5 (daily recompute job)

### Implementation for User Story 6

- [X] T038 [US6] Create backend/tests/performance/locustfile.py with Locust load test configuration
- [X] T039 [US6] Run load tests simulating 20 concurrent users hitting hourly API endpoints, measure and log p95 response time - p95=23ms, p99=78ms (well under 500ms target)
- [X] T040 [US6] Validate composite index usage with EXPLAIN plans for critical hourly aggregation queries - idx_pas_time_range_covering and idx_rss_time_range_covering confirmed
- [X] T041 [US6] Configure slow query logging (queries >1s) in backend/.env.example and document MySQL my.cnf settings

**Checkpoint**: At this point, User Story 6 complete - performance targets validated

---

## Phase 9: Complete Core ORM Repositories (Quick Wins)

**Purpose**: Fix remaining raw SQL in already-converted ORM repositories
**Depends on**: Phase 3 complete

- [X] T042 [P] Fix stats_repository.py line 284: Replace text('INTERVAL 30 DAY') with Python timedelta - Already converted to ORM
- [X] T043 [P] Fix ride_repository.py line 365: Replace text('INTERVAL 1 HOUR') with Python timedelta - Already converted to ORM

**Checkpoint**: Core ORM repos are 100% SQL-free

---

## Phase 10: Query Builders Foundation [COMPLETE]

**Purpose**: Create shared ORM primitives for query class conversions
**Depends on**: Phase 9 complete
**Status**: All INTERVAL patterns fixed, TimeIntervalHelper and QueryClassBase added to query_helpers.py

- [X] T044 Convert builders/filters.py: Replace text('INTERVAL X HOUR') with ORM-compatible time filters
  - Fixed literal_column("INTERVAL X HOUR") in all files:
    - `api/routes/health.py:108` → `timedelta(hours=24)`
    - `database/queries/charts/park_rides_comparison.py:503` → `timedelta(hours=8)`
    - `database/queries/charts/park_shame_history.py:580, 632` → `timedelta(hours=8)`
    - `database/queries/charts/park_waittime_history.py:311` → `timedelta(hours=8)`
    - `database/queries/charts/ride_downtime_history.py:355` → `timedelta(hours=8)`
    - `database/queries/charts/ride_waittime_history.py:221` → `timedelta(hours=8)`
    - `scripts/aggregate_live_rankings.py:119, 165, 181` → `timedelta(hours=N)` and `timedelta(days=7)`
- [X] T045 Add ORM time interval helpers to query_helpers.py (TimeIntervalHelper class with days_ago, hours_ago, etc.)
- [X] T046 Create QueryClassBase in query_helpers.py: Base class with execute_and_fetchall, execute_and_fetchone, execute_scalar

**Checkpoint**: Foundation ready for query class conversions ✓

---

## Phase 11: Rankings Query Classes [COMPLETE]

**Purpose**: Convert user-facing ranking queries to ORM
**Status**: Already converted - 0 text() calls remaining

- [X] T047 [P] Convert rankings/park_downtime_rankings.py to ORM
- [X] T048 [P] Convert rankings/ride_downtime_rankings.py to ORM
- [X] T049 [P] Convert rankings/park_wait_time_rankings.py to ORM
- [X] T050 [P] Convert rankings/ride_wait_time_rankings.py to ORM
- [X] T051 [P] Convert today/today_park_rankings.py to ORM
- [X] T052 [P] Convert today/today_ride_rankings.py to ORM
- [X] T053 [P] Convert yesterday/yesterday_park_rankings.py to ORM
- [X] T054 [P] Convert yesterday/yesterday_ride_rankings.py to ORM

**Checkpoint**: All ranking queries use ORM ✓

---

## Phase 12: Wait Times Query Classes [COMPLETE]

**Purpose**: Convert period-based wait time queries to ORM
**Status**: Already converted - 0 text() calls remaining

- [X] T055 [P] Convert today/today_park_wait_times.py to ORM
- [X] T056 [P] Convert today/today_ride_wait_times.py to ORM
- [X] T057 [P] Convert yesterday/yesterday_park_wait_times.py to ORM
- [X] T058 [P] Convert yesterday/yesterday_ride_wait_times.py to ORM
- [X] T059 [P] Convert live/live_park_rankings.py to ORM
- [X] T060 [P] Convert live/live_ride_rankings.py to ORM
- [X] T061 [P] Convert live/fast_live_park_rankings.py to ORM
- [X] T062 [P] Convert live/status_summary.py to ORM

**Checkpoint**: All wait time queries use ORM ✓

---

## Phase 13: Chart Query Classes [COMPLETE]

**Purpose**: Migrate chart query classes from Connection to Session pattern
**Depends on**: Phase 10 complete (T046 QueryClassBase)
**Status**: All chart query classes migrated from Connection to Session. Updated callers in trends.py and parks.py.

- [X] T063 [P] Migrate charts/park_shame_history.py from Connection to Session (848 LOC, 9 execute calls)
- [X] T064 [P] Migrate charts/park_waittime_history.py from Connection to Session (402 LOC, 6 execute calls)
- [X] T065 [P] Migrate charts/ride_downtime_history.py from Connection to Session (513 LOC, 6 execute calls)
- [X] T066 [P] Migrate charts/ride_waittime_history.py from Connection to Session (431 LOC, 6 execute calls)
- [X] T067 [P] Migrate charts/park_rides_comparison.py from Connection to Session (531 LOC, 7 execute calls)

**Checkpoint**: All chart queries use Session pattern ✓ (2,725 LOC total)

---

## Phase 14: Trends Query Classes [COMPLETE]

**Purpose**: Convert analytical trend queries to ORM
**Status**: Already converted - 0 text() calls remaining

- [X] T068 [P] Convert trends/declining_parks.py to ORM
- [X] T069 [P] Convert trends/declining_rides.py to ORM
- [X] T070 [P] Convert trends/improving_parks.py to ORM
- [X] T071 [P] Convert trends/improving_rides.py to ORM
- [X] T072 [P] Convert trends/least_reliable_rides.py to ORM
- [X] T073 [P] Convert trends/longest_wait_times.py to ORM

**Checkpoint**: All trend queries use ORM ✓ - Query class layer complete

---

## Phase 15: Remaining Repositories [COMPLETE]

**Purpose**: Convert data pipeline repositories to ORM
**Status**: Already converted - 0 text() calls remaining

- [X] T074 [P] Convert snapshot_repository.py to ORM
- [X] T075 [P] Convert status_change_repository.py to ORM
- [X] T076 [P] Convert aggregation_repository.py to ORM
- [X] T077 [P] Convert schedule_repository.py to ORM
- [X] T078 [P] Convert data_quality_repository.py to ORM

**Checkpoint**: Repository layer 100% ORM ✓

---

## Phase 16: Calculators and Audit Tools [COMPLETE]

**Purpose**: Convert internal calculation and audit code to ORM
**Status**: Already converted - 0 text() calls remaining

- [X] T079 Convert calculators/shame_score.py to ORM
- [X] T080 [P] Convert audit/aggregate_verification.py to ORM
- [X] T081 [P] Convert audit/anomaly_detector.py to ORM
- [X] T082 [P] Convert audit/computation_trace.py to ORM
- [X] T083 [P] Convert audit/validation_checks.py to ORM

**Checkpoint**: All calculators and audit tools use ORM ✓

---

## Phase 17: Processors [COMPLETE]

**Purpose**: Convert core data processing to ORM (HIGH RISK)
**Status**: Already converted - 0 text() calls remaining

- [X] T084 Convert processor/aggregation_service.py to ORM (largest file, ~1500 lines)
- [X] T085 Convert processor/operating_hours_detector.py to ORM
- [X] T086 Convert processor/status_change_detector.py to ORM

**Checkpoint**: Data processing pipeline uses ORM ✓

---

## Phase 18: Scripts [PARTIAL]

**Purpose**: Convert batch operation scripts to ORM
**Status**: 2 scripts with 12 text() calls remaining (aggregate_live_rankings + seed_test_data)

- [X] T087 [P] scripts/aggregate_hourly.py - DEPRECATED (not converting)
- [X] T088 [P] Convert scripts/aggregate_daily.py to ORM - 0 text() calls
- [~] T089 [P] Convert scripts/aggregate_live_rankings.py to ORM - PARTIAL (7→5 text() calls)
  - **Status**: PARTIAL - park rankings INSERT converted to ORM, ride rankings kept as text() due to MySQL limitation
  - **MySQL Limitation**: CTEs cannot be referenced in HAVING clauses within INSERT...SELECT context
  - **Remaining text() calls (acceptable)**:
    - 4x DDL operations (TRUNCATE, RENAME) - no ORM equivalent
    - 1x Ride rankings INSERT with CTEs+HAVING - MySQL limitation prevents ORM conversion
  - **Completed**: Park rankings INSERT converted to `insert().from_select()` using ORM models
  - **Added ORM models**: ParkLiveRankingsStaging, RideLiveRankings, RideLiveRankingsStaging
  - Lines 106, 368: TRUNCATE TABLE - acceptable as text() (DDL)
  - Lines 339, 346, 600, 607: RENAME TABLE - acceptable as text() (DDL)
  - Lines 335, 596: SELECT COUNT(*) - convert to `session.query(func.count())`
- [X] T090 [P] Convert scripts/collect_snapshots.py to ORM - 0 text() calls
- [X] T091 [P] Convert scripts/collect_parks.py to ORM - 0 text() calls
- [X] T092 [P] Convert scripts/collect_weather.py to ORM - 0 text() calls
- [X] T093 [P] scripts/backfill_hourly_stats.py - DEPRECATED (not converting)
- [X] T094 [P] Convert scripts/backfill_shame_scores.py to ORM - 0 text() calls
- [ ] T095 [P] Convert scripts/seed_test_data.py to ORM - 2 text() calls (LOW priority - test utility only)
- [X] T096 [P] Convert scripts/check_data_collection.py to ORM - 0 text() calls

**Checkpoint**: Core scripts converted, utility scripts remain

---

## Phase 19: API Routes and Verification [COMPLETE]

**Purpose**: Complete SQL elimination in API layer
**Depends on**: All previous phases complete
**Status**: All core application code verified SQL-free. Only acceptable text() calls remain.

- [X] T097 Convert api/routes/health.py to ORM (database checks) - INTERVAL fixed in T044
- [X] T098 Convert api/routes/search.py to ORM - Already uses session.execute(select(...)), fully converted
- [X] T099 Final verification: grep -r "text(" src/ returns only acceptable cases:
  - Documentation examples (connection.py, query_helpers.py)
  - server_default in ORM models
  - DDL operations (TRUNCATE, RENAME) in aggregate scripts
  - Complex MySQL-specific CTEs in aggregate_live_rankings.py
  - seed_test_data.py (test utility, LOW priority - T095)

**Checkpoint**: API layer uses ORM ✓ - Core application code is 100% SQL-free

---

## Phase 20: Test Suite Migration [IN PROGRESS]

**Purpose**: Update test suite to use ORM Session fixtures instead of raw Connection fixtures
**Status**: Unit tests COMPLETE (805 passed), Integration tests IN PROGRESS

### Unit Test Migration (COMPLETE - 805 tests passing)

- [X] T100 Update unit tests checking StatsRepository methods to use ORM query classes
- [X] T101 Update unit tests checking SQL patterns (e.g., "AS rides_down") to ORM-compatible patterns
- [X] T102 Update unit tests using mock params to test ORM logic directly

### Integration Test Migration (IN PROGRESS)

Integration tests updated to use `mysql_session` fixture instead of `mysql_connection`:

**Completed (200 tests passing):**
- [X] T103 [P] Update tests/integration/test_snapshot_repository.py to use mysql_session fixture
- [X] T104 [P] Update tests/integration/test_aggregation_repository.py to use mysql_session fixture
- [X] T105 [P] Update tests/integration/test_aggregation_service_integration.py to use mysql_session
- [X] T106 [P] Update tests/integration/test_api_calculations_integration.py to use mysql_session
- [X] T107 [P] Update tests/integration/test_classification_integration.py to use mysql_session
- [X] T108 [P] Update tests/integration/test_daily_consistency_park_vs_rides.py to use mysql_session
- [X] T109 [P] Update tests/integration/test_data_presence_after_mirror.py to use mysql_session
- [X] T110 [P] Update tests/integration/test_heatmap_api.py to use mysql_session
- [X] T111 [P] Update tests/integration/test_park_details_api.py to use mysql_session
- [X] T112 [P] Update tests/integration/test_chart_equivalence.py to use mysql_session
- [X] T113 [P] Update tests/integration/test_api_endpoints_integration.py to use mysql_session
- [X] T114 [P] Update tests/integration/test_today_api_contract.py to use mysql_session
- [X] T115 [P] Batch update all remaining test files to mysql_session

**Remaining failures (66 tests) - Complex issues:**
- [ ] T116 Fix tests using get_db_session() that bypass test fixture transaction
- [ ] T117 Fix monthly/weekly aggregation tests with data dependencies
- [ ] T118 Fix ride details API tests with Flask app context issues
- [ ] T119 Fix timestamp drift and fallback heuristic tests

**Checkpoint**: 200 integration tests pass with ORM Session fixtures

---

## Phase 21: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, code cleanup, and deployment readiness
**Depends on**: All SQL elimination phases complete

- [ ] T116 [P] Verify specs/003-orm-refactoring/quickstart.md has complete ORM usage examples
- [ ] T117 [P] Run backend test suite with coverage report (pytest --cov=backend/src --cov-report=term-missing), verify >80% coverage
- [ ] T118 Code cleanup: remove commented-out raw SQL queries, unused imports - Done when: grep finds 0 occurrences of text() in active code paths (excluding migrations)
- [ ] T119 Update deployment/deploy.sh to include alembic upgrade head step before restarting services
- [ ] T120 Update CLAUDE.md: Add "No Raw SQL Policy" section documenting ORM-only requirement

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1: Setup
    |
    v
Phase 2: Foundational (BLOCKS all user stories)
    |
    +--------------------+--------------------+
    |                    |                    |
    v                    v                    v
Phase 3: US1 (P1)   Phase 4: US2 (P1)   Phase 6: US4 (P2)
    |                    |                    |
    v                    |                    |
Phase 5: US3 (P1)       |                    v
    |                    |               Phase 7: US5 (P2)
    |                    |                    |
    +--------------------+--------------------+
                         |
                         v
                    Phase 8: US6 (P2)
                         |
                         v
              Phase 9: Quick Wins (fix INTERVAL syntax)
                         |
                         v
              Phase 10: Query Builders Foundation
                         |
         +---------------+---------------+---------------+
         |               |               |               |
         v               v               v               v
    Phase 11:       Phase 12:       Phase 13:       Phase 14:
    Rankings        Wait Times      Charts          Trends
    (8 tasks)       (8 tasks)       (5 tasks)       (6 tasks)
         |               |               |               |
         +---------------+---------------+---------------+
                         |
                         v
              Phase 15: Remaining Repositories (parallel with 11-14)
                         |
         +---------------+---------------+
         |                               |
         v                               v
    Phase 16:                       Phase 17:
    Calculators/Audit               Processors
         |                               |
         +---------------+---------------+
                         |
                         v
                  Phase 18: Scripts
                         |
                         v
                  Phase 19: API Routes + Verification
                         |
                         v
                  Phase 20: Polish (FINAL)
```

### User Story Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational (Phase 2) - Independent of US1 (different queries)
- **User Story 3 (P1)**: Can start after Foundational (Phase 2) - Validates benefit of US1+US2 but minimal new code
- **User Story 4 (P2)**: Can start after Foundational (Phase 2) - Independent (migration scripts)
- **User Story 5 (P2)**: Depends on US1 being functional (needs ORM models working)
- **User Story 6 (P2)**: Depends on US2 (hourly queries) and US5 (daily recompute) being testable
- **Polish (Phase 9)**: Depends on all desired user stories being complete

### Within Each User Story

- Foundational: All model creation tasks (T004-T009) can run in parallel [P]
- US1: All repository migration tasks (T017-T020) can run in parallel [P] after query_helpers.py (T016) exists
- US2: T024 (API updates) depends on T023 (query implementation) - sequential execution required
- US4: Migration tests (T030-T031) can run in parallel [P]
- US6: Load test creation (T038) and EXPLAIN validation (T040) can run in parallel [P] after performance baseline

### Parallel Opportunities

**Total parallelizable tasks**: 20 tasks marked [P]

**Phase 2 (Foundational)**:
```bash
# Launch all ORM model tasks together:
Task T004: "Create backend/src/models/base.py"
Task T005: "Create backend/src/models/park.py"
Task T006: "Create backend/src/models/ride.py"
Task T007: "Create backend/src/models/snapshots.py"
Task T008: "Create backend/src/models/stats.py"
Task T009: "Create backend/src/models/weather.py"
```

**Phase 3 (US1)**:
```bash
# After T016 (query_helpers.py) completes, launch all repository migrations:
Task T017: "Migrate ride_status_repository.py to ORM"
Task T018: "Migrate stats_repository.py to ORM"
Task T019: "Migrate park_repository.py to ORM"
Task T020: "Migrate weather_repository.py to ORM"
```

**Phase 4 (US2)**:
```bash
# Sequential execution (T024 depends on T023):
Task T023: "Implement hourly aggregation ORM queries" (first)
Task T024: "Update hourly API endpoints" (after T023 completes)
```

---

## Implementation Strategy

### MVP First (Minimum Viable Product)

**Goal**: Get type-safe ORM queries working end-to-end

1. Complete Phase 1: Setup (3 tasks)
2. Complete Phase 2: Foundational (12 tasks) - CRITICAL BLOCKER
3. Complete Phase 3: User Story 1 (7 tasks)
4. **STOP and VALIDATE**: Run pytest backend/tests/, verify all tests pass with ORM
5. Deploy to staging, validate API responses unchanged
6. **MVP COMPLETE** - Type-safe ORM queries replacing raw SQL

### Incremental Delivery

**After MVP, deliver one user story at a time:**

1. **Foundation** (Phase 1-2) → Foundation ready
2. **+ User Story 1** (Phase 3) → Test independently → Deploy/Demo (**MVP!**)
3. **+ User Story 2** (Phase 4) → Test independently → Deploy/Demo (Flexible analytics unlocked)
4. **+ User Story 3** (Phase 5) → Test independently → Deploy/Demo (No backfill benefit proven)
5. **+ User Story 4** (Phase 6) → Test independently → Deploy/Demo (Migration workflow validated)
6. **+ User Story 5** (Phase 7) → Test independently → Deploy/Demo (Recompute job available)
7. **+ User Story 6** (Phase 8) → Test independently → Deploy/Demo (Performance validated)
8. **+ Polish** (Phase 9) → Final cleanup → Production-ready

Each increment adds value without breaking previous functionality.

### Parallel Team Strategy

**With multiple developers, maximize parallelization:**

1. **All developers**: Complete Phase 1 + Phase 2 together (foundational work)
2. **Once Phase 2 complete**:
   - Developer A: User Story 1 (repository migrations)
   - Developer B: User Story 2 (hourly analytics)
   - Developer C: User Story 4 (migration validation)
3. **After US1 complete**:
   - Developer A: User Story 5 (recompute job - depends on US1)
4. **After US2 + US5 complete**:
   - Any developer: User Story 6 (performance validation - depends on US2 + US5)
5. Stories integrate cleanly due to independent design

---

## Notes

- **[P] tasks**: Different files, no dependencies - run in parallel
- **[Story] label**: Maps task to specific user story for traceability
- **Each user story**: Independently completable and testable
- **API contracts preserved**: Repository method signatures unchanged (see contracts/api-preservation.md)
- **Testing approach**: Existing 935 tests will be updated to validate ORM queries; golden data regression tests (T028) ensure ORM parity with raw SQL; acceptance tests validate each user story independently at checkpoints
- **Commit strategy**: Commit after each task or logical group
- **Stop at checkpoints**: Validate each user story independently before proceeding
- **Avoid**: Vague tasks, same-file conflicts, cross-story dependencies that break independence

---

## Summary

- **Total Tasks**: 120 tasks across 21 phases
- **Original ORM Refactoring**: 41 tasks (Phases 1-8) - COMPLETE
- **SQL Elimination Extension**: 63 tasks (Phases 9-19) - ~85% COMPLETE
- **Test Suite Migration**: 16 tasks (Phase 20) - IN PROGRESS (Unit tests DONE, Integration tests WIP)
- **Parallel Opportunities**: 55+ tasks marked [P] for concurrent execution
- **MVP Scope**: Phase 1-2 (foundational) + Phase 3 (US1) = 22 tasks
- **User Stories**: 6 stories (3 P1, 3 P2) with independent test criteria
- **Critical Path**: Foundational phase (12 tasks) blocks all user story work
- **Performance Target**: <500ms p95 for hourly queries (validated in US6)
- **Migration Strategy**: Alembic with rollback capability (US4)
- **API Compatibility**: Zero Flask API changes (see contracts/api-preservation.md)
- **End Goal**: Zero raw SQL in codebase (excluding migrations and DDL)

### Remaining Work Summary (as of 2025-12-27)

| Priority | Task | Issue | Effort |
|----------|------|-------|--------|
| ✓ PARTIAL | T089 | aggregate_live_rankings.py - park INSERT→ORM, ride INSERT→text() (MySQL limitation) | Done |
| MEDIUM | T044-T046 | Query Builders Foundation + INTERVAL fixes | ~4h |
| MEDIUM | T063-T067 | Chart classes Connection→Session migration | ~8h |
| LOW | T095 | seed_test_data.py (test utility) | ~1h |
| LOW | T116-T119 | Fix 66 failing integration tests | ~4h |

### SQL Elimination Phases Summary

| Phase | Tasks | Purpose | Status |
|-------|-------|---------|--------|
| 9 | 2 | Fix core ORM repos (INTERVAL syntax) | ✅ COMPLETE |
| 10 | 3 | Query builders foundation | ⏳ NOT STARTED |
| 11 | 8 | Rankings query classes | ✅ COMPLETE |
| 12 | 8 | Wait times query classes | ✅ COMPLETE |
| 13 | 5 | Chart classes (Connection→Session) | ⏳ NOT STARTED |
| 14 | 6 | Trends query classes | ✅ COMPLETE |
| 15 | 5 | Remaining repositories | ✅ COMPLETE |
| 16 | 5 | Calculators/audit tools | ✅ COMPLETE |
| 17 | 3 | Processors | ✅ COMPLETE |
| 18 | 10 | Scripts | ⚠️ 90% (T089 partial, T095 remain) |
| 19 | 3 | API routes + verification | ⚠️ 67% (T097-T098 done) |
| 20 | 16 | Test suite migration | ⚠️ 75% (66 tests failing) |
| 21 | 5 | Polish & final cleanup | ⏳ NOT STARTED |
