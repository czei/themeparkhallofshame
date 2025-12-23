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
- [ ] T026 [US2] Remove hourly aggregation cron job code from backend/src/scripts/ (or comment out in cron config)
- [X] T027 [US2] Validate hourly query performance with MySQL EXPLAIN plans, verify composite index usage (all queries use idx_ride_recorded, idx_recorded_at, idx_rss_time_range_covering, idx_park_recorded)

**Checkpoint**: At this point, User Story 2 should be fully functional - hourly_stats removed, flexible ORM queries working

---

## Phase 5: User Story 3 - Bug Fixes Without Backfills (Priority: P1)

**Goal**: Validate that ORM query bug fixes apply instantly to all historical periods

**Independent Test**: Introduce deliberate test bug in ORM query, deploy fix, verify both new and historical queries return corrected values

### Implementation for User Story 3

- [ ] T028 [US3] Create backend/tests/golden_data/test_orm_query_parity.py with regression tests validating ORM results match historical raw SQL values
- [ ] T029 [US3] Add "No Backfill Benefit" section to specs/003-orm-refactoring/quickstart.md documenting instant bug fix capability

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

- [ ] T034 [US5] Create backend/src/scripts/recompute_daily_stats.py with argparse CLI interface (--start-date, --end-date, --metrics-version)
- [ ] T035 [US5] Implement idempotent UPSERT logic using metrics_version column in daily_stats table
- [ ] T036 [US5] Add progress tracking (tqdm or logging) and error handling for 90-day batch processing
- [ ] T037 [US5] Test recompute job with 7-day historical data, measure execution time, verify <6 hour target extrapolated to 90 days

**Checkpoint**: At this point, User Story 5 complete - recompute job working and idempotent

---

## Phase 8: User Story 6 - Performance Validation (Priority: P2)

**Goal**: Load test hourly ORM queries and validate <500ms p95 response time target

**Independent Test**: Run Locust load test simulating 20 concurrent users, measure p95 response time for hourly queries

**Depends on**: US2 (hourly queries), US5 (daily recompute job)

### Implementation for User Story 6

- [ ] T038 [US6] Create backend/tests/performance/test_hourly_query_performance.py with Locust load test configuration
- [ ] T039 [US6] Run load tests simulating 20 concurrent users hitting hourly API endpoints, measure and log p95 response time
- [ ] T040 [US6] Validate composite index usage with EXPLAIN plans for critical hourly aggregation queries
- [ ] T041 [US6] Configure slow query logging (queries >1s) in backend/.env and document in deployment config

**Checkpoint**: At this point, User Story 6 complete - performance targets validated

---

## Phase 9: Complete Core ORM Repositories (Quick Wins)

**Purpose**: Fix remaining raw SQL in already-converted ORM repositories
**Depends on**: Phase 3 complete

- [ ] T042 [P] Fix stats_repository.py line 284: Replace text('INTERVAL 30 DAY') with Python timedelta
- [ ] T043 [P] Fix ride_repository.py line 365: Replace text('INTERVAL 1 HOUR') with Python timedelta

**Checkpoint**: Core ORM repos are 100% SQL-free

---

## Phase 10: Query Builders Foundation

**Purpose**: Create shared ORM primitives for query class conversions
**Depends on**: Phase 9 complete

- [ ] T044 Convert builders/filters.py: Replace text('INTERVAL X HOUR') with ORM-compatible time filters
- [ ] T045 Add ORM time interval helpers to query_helpers.py (timedelta-based date math)
- [ ] T046 Create QueryClassBase in query_helpers.py: Base class accepting Session instead of Connection

**Checkpoint**: Foundation ready for query class conversions

---

## Phase 11: Rankings Query Classes

**Purpose**: Convert user-facing ranking queries to ORM
**Depends on**: Phase 10 complete

- [ ] T047 [P] Convert rankings/park_downtime_rankings.py to ORM
- [ ] T048 [P] Convert rankings/ride_downtime_rankings.py to ORM
- [ ] T049 [P] Convert rankings/park_wait_time_rankings.py to ORM
- [ ] T050 [P] Convert rankings/ride_wait_time_rankings.py to ORM
- [ ] T051 [P] Convert today/today_park_rankings.py to ORM
- [ ] T052 [P] Convert today/today_ride_rankings.py to ORM
- [ ] T053 [P] Convert yesterday/yesterday_park_rankings.py to ORM
- [ ] T054 [P] Convert yesterday/yesterday_ride_rankings.py to ORM

**Checkpoint**: All ranking queries use ORM

---

## Phase 12: Wait Times Query Classes

**Purpose**: Convert period-based wait time queries to ORM
**Depends on**: Phase 10 complete (can run parallel with Phase 11)

- [ ] T055 [P] Convert today/today_park_wait_times.py to ORM
- [ ] T056 [P] Convert today/today_ride_wait_times.py to ORM
- [ ] T057 [P] Convert yesterday/yesterday_park_wait_times.py to ORM
- [ ] T058 [P] Convert yesterday/yesterday_ride_wait_times.py to ORM
- [ ] T059 [P] Convert live/live_park_rankings.py to ORM
- [ ] T060 [P] Convert live/live_ride_rankings.py to ORM
- [ ] T061 [P] Convert live/fast_live_park_rankings.py to ORM
- [ ] T062 [P] Convert live/status_summary.py to ORM

**Checkpoint**: All wait time queries use ORM

---

## Phase 13: Chart Query Classes

**Purpose**: Convert visualization queries to ORM
**Depends on**: Phase 10 complete (can run parallel with Phases 11-12)

- [ ] T063 [P] Convert charts/park_shame_history.py to ORM
- [ ] T064 [P] Convert charts/park_waittime_history.py to ORM
- [ ] T065 [P] Convert charts/ride_downtime_history.py to ORM
- [ ] T066 [P] Convert charts/ride_waittime_history.py to ORM
- [ ] T067 [P] Convert charts/park_rides_comparison.py to ORM

**Checkpoint**: All chart queries use ORM

---

## Phase 14: Trends Query Classes

**Purpose**: Convert analytical trend queries to ORM
**Depends on**: Phase 10 complete (can run parallel with Phases 11-13)

- [ ] T068 [P] Convert trends/declining_parks.py to ORM
- [ ] T069 [P] Convert trends/declining_rides.py to ORM
- [ ] T070 [P] Convert trends/improving_parks.py to ORM
- [ ] T071 [P] Convert trends/improving_rides.py to ORM
- [ ] T072 [P] Convert trends/least_reliable_rides.py to ORM
- [ ] T073 [P] Convert trends/longest_wait_times.py to ORM

**Checkpoint**: All trend queries use ORM - Query class layer complete

---

## Phase 15: Remaining Repositories

**Purpose**: Convert data pipeline repositories to ORM
**Depends on**: Phase 3 complete (can run parallel with Phases 11-14)

- [ ] T074 [P] Convert snapshot_repository.py to ORM
- [ ] T075 [P] Convert status_change_repository.py to ORM
- [ ] T076 [P] Convert aggregation_repository.py to ORM
- [ ] T077 [P] Convert schedule_repository.py to ORM
- [ ] T078 [P] Convert data_quality_repository.py to ORM

**Checkpoint**: Repository layer 100% ORM

---

## Phase 16: Calculators and Audit Tools

**Purpose**: Convert internal calculation and audit code to ORM
**Depends on**: Phase 15 complete

- [ ] T079 Convert calculators/shame_score.py to ORM
- [ ] T080 [P] Convert audit/aggregate_verification.py to ORM
- [ ] T081 [P] Convert audit/anomaly_detector.py to ORM
- [ ] T082 [P] Convert audit/computation_trace.py to ORM
- [ ] T083 [P] Convert audit/validation_checks.py to ORM

**Checkpoint**: All calculators and audit tools use ORM

---

## Phase 17: Processors

**Purpose**: Convert core data processing to ORM (HIGH RISK)
**Depends on**: Phase 15 complete

- [ ] T084 Convert processor/aggregation_service.py to ORM (largest file, ~1500 lines)
- [ ] T085 Convert processor/operating_hours_detector.py to ORM
- [ ] T086 Convert processor/status_change_detector.py to ORM

**Checkpoint**: Data processing pipeline uses ORM

---

## Phase 18: Scripts

**Purpose**: Convert batch operation scripts to ORM
**Depends on**: Phase 17 complete

- [ ] T087 [P] Convert scripts/aggregate_hourly.py to ORM
- [ ] T088 [P] Convert scripts/aggregate_daily.py to ORM
- [ ] T089 [P] Convert scripts/aggregate_live_rankings.py to ORM
- [ ] T090 [P] Convert scripts/collect_snapshots.py to ORM
- [ ] T091 [P] Convert scripts/collect_parks.py to ORM
- [ ] T092 [P] Convert scripts/collect_weather.py to ORM
- [ ] T093 [P] Convert scripts/backfill_hourly_stats.py to ORM
- [ ] T094 [P] Convert scripts/backfill_shame_scores.py to ORM
- [ ] T095 [P] Convert scripts/seed_test_data.py to ORM
- [ ] T096 [P] Convert scripts/check_data_collection.py to ORM

**Checkpoint**: All scripts use ORM

---

## Phase 19: API Routes and Verification

**Purpose**: Complete SQL elimination in API layer
**Depends on**: All previous phases complete

- [ ] T097 Convert api/routes/health.py to ORM (database checks)
- [ ] T098 Convert api/routes/search.py to ORM
- [ ] T099 Final verification: grep -r "text(" src/ returns only migrations and test mocks

**Checkpoint**: API layer 100% ORM

---

## Phase 20: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, code cleanup, and deployment readiness
**Depends on**: All SQL elimination phases complete

- [ ] T100 [P] Verify specs/003-orm-refactoring/quickstart.md has complete ORM usage examples
- [ ] T101 [P] Run backend test suite with coverage report (pytest --cov=backend/src --cov-report=term-missing), verify >80% coverage
- [ ] T102 Code cleanup: remove commented-out raw SQL queries, unused imports - Done when: grep finds 0 occurrences of text() in active code paths (excluding migrations)
- [ ] T103 Update deployment/deploy.sh to include alembic upgrade head step before restarting services
- [ ] T104 Update CLAUDE.md: Add "No Raw SQL Policy" section documenting ORM-only requirement

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

- **Total Tasks**: 104 tasks across 20 phases
- **Original ORM Refactoring**: 41 tasks (Phases 1-8) - MOSTLY COMPLETE
- **SQL Elimination Extension**: 63 tasks (Phases 9-20) - NEW
- **Parallel Opportunities**: 55+ tasks marked [P] for concurrent execution
- **MVP Scope**: Phase 1-2 (foundational) + Phase 3 (US1) = 22 tasks
- **User Stories**: 6 stories (3 P1, 3 P2) with independent test criteria
- **Critical Path**: Foundational phase (12 tasks) blocks all user story work
- **Performance Target**: <500ms p95 for hourly queries (validated in US6)
- **Migration Strategy**: Alembic with rollback capability (US4)
- **API Compatibility**: Zero Flask API changes (see contracts/api-preservation.md)
- **End Goal**: Zero raw SQL in codebase (excluding migrations)

### SQL Elimination Phases Summary

| Phase | Tasks | Purpose | Risk |
|-------|-------|---------|------|
| 9 | 2 | Fix core ORM repos (INTERVAL syntax) | Very Low |
| 10 | 3 | Query builders foundation | Medium |
| 11 | 8 | Rankings query classes | Medium |
| 12 | 8 | Wait times query classes | Medium |
| 13 | 5 | Chart query classes | Medium |
| 14 | 6 | Trends query classes | Low |
| 15 | 5 | Remaining repositories | Medium |
| 16 | 5 | Calculators/audit tools | Low |
| 17 | 3 | Processors (HIGH RISK) | High |
| 18 | 10 | Scripts | Medium |
| 19 | 3 | API routes + verification | Low |
| 20 | 5 | Polish & final cleanup | Low |
