# Tasks: Pre-Computed Time-Series Aggregation Tables

**Input**: Design documents from `/specs/001-aggregation-tables/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api/parks.yaml

**Tests**: This project follows Test-Driven Development (TDD) per CLAUDE.md constitutional principles. All test tasks are MANDATORY and must be written BEFORE implementation.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `- [ ] [ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

This project uses web application structure:
- Backend: `backend/src/`, `backend/tests/`
- Frontend: `frontend/` (no changes in this refactoring - API contract maintained)
- Database: `backend/src/database/migrations/`
- Scripts: `backend/src/scripts/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Prepare local development environment per quickstart.md

- [ ] T001 Mirror production database to local dev environment using `deployment/scripts/mirror-production-db.sh --days=7`
- [ ] T002 [P] Verify mirrored data completeness in `themepark_tracker_dev` database (check snapshot counts, date ranges)
- [ ] T003 [P] Review existing aggregation infrastructure in `backend/src/scripts/aggregate_daily.py` to understand proven patterns

**Checkpoint**: Local environment ready with production data for testing

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database schema and shared infrastructure that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T004 Create database migration `backend/src/database/migrations/012_add_hourly_stats.sql` with `park_hourly_stats` and `ride_hourly_stats` tables per data-model.md schema
- [ ] T005 Apply migration to local dev database and verify tables created with correct indexes
- [ ] T006 [P] Implement hourly aggregation script `backend/src/scripts/aggregate_hourly.py` (reuses `ShameScoreCalculator`, `RideStatusSQL`, `DowntimeSQL` from existing code)
- [ ] T007 [P] Add `USE_HOURLY_TABLES` feature flag to `backend/src/utils/metrics.py` (defaults to `false` for safe rollback)
- [ ] T008 Implement backfill script `backend/src/scripts/backfill_hourly_stats.py` for populating historical hourly data from raw snapshots

**Checkpoint**: Foundation ready - hourly aggregation infrastructure complete, user story implementation can now begin

---

## Phase 3: User Story 1 - Fast TODAY Rankings (Priority: P1) 🎯 MVP

**Goal**: Users viewing the "Today" rankings page experience instant load times (sub-1-second response) via hybrid query combining hourly tables with live rankings.

**Independent Test**: Load TODAY rankings page with 7 days of mirrored production data and verify sub-second response times. Rankings match current GROUP BY implementation results within tolerance (0.1 shame score difference).

**Implementation Strategy**: Per research.md Finding 7, TODAY period uses hybrid approach:
1. Query completed hours from `park_hourly_stats` (fast lookup)
2. Query current partial hour from `park_live_rankings` (10-min refresh)
3. Merge with weighted averaging by hour count

### Tests for User Story 1 (TDD - MANDATORY)

> **TDD RED PHASE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T009 [P] [US1] Contract test for `/api/parks/downtime?period=today` in `backend/tests/integration/test_today_chart_data.py` - validates response format matches contracts/api/parks.yaml
- [ ] T010 [P] [US1] Integration test for TODAY hybrid query logic in `backend/tests/integration/test_today_chart_data.py` - verifies weighted averaging of completed hours + current hour
- [ ] T011 [P] [US1] Chart query equivalence test in `backend/tests/integration/test_chart_query_equivalence.py` - compares hourly table results vs GROUP BY HOUR on same data (within 0.1 tolerance)

**Checkpoint**: Tests written and FAILING (RED) - ready for GREEN phase

### Implementation for User Story 1 (TDD GREEN/REFACTOR)

- [ ] T012 [US1] Refactor `backend/src/database/queries/charts/park_shame_history.py` to use single query class with `use_hourly_tables` parameter (per research.md Finding 4)
- [ ] T013 [US1] Add `_query_hourly_tables()` method to `ParkShameHistoryQuery` class - implements fast SELECT from `park_hourly_stats` with date range filter
- [ ] T014 [US1] Add `_query_raw_snapshots()` method to `ParkShameHistoryQuery` class - preserves existing GROUP BY HOUR logic for rollback path
- [ ] T015 [US1] Implement TODAY hybrid query in `backend/src/database/queries/today/today_park_rankings.py` - combines completed hours from hourly tables + current hour from `park_live_rankings` with weighted averaging
- [ ] T016 [US1] Update `backend/src/database/repositories/stats_repository.py` to add `get_hourly_stats()` method for querying `park_hourly_stats` table
- [ ] T017 [US1] Update Flask route `/api/parks/downtime?period=today` in `backend/src/api/routes/parks.py` to use new hybrid query (controlled by `USE_HOURLY_TABLES` env var)
- [ ] T018 [US1] Run tests - verify all US1 tests now PASS (TDD GREEN phase)
- [ ] T019 [US1] Code cleanup and refactoring while keeping tests green (TDD REFACTOR phase)
- [ ] T020 [P] [US1] Create hourly job health check in `backend/src/database/queries/monitoring/hourly_job_health.py` - queries `aggregation_log` for last successful 'hourly' run, alerts if > 2 hours ago (CRITICAL: proactive failure detection for new cron job)
- [ ] T021 [P] [US1] Integration test for `USE_HOURLY_TABLES=false` rollback path in `backend/tests/integration/test_feature_flag_rollback.py` - validates system uses `_query_raw_snapshots()` when feature flag disabled

**Checkpoint**: TODAY rankings work with sub-1-second response times. Tests pass. API contract maintained. Feature flag allows instant rollback to GROUP BY approach. **Monitoring in place to detect aggregation job failures.**

---

## Phase 4: User Story 2 - Fast Historical Period Views (Priority: P2)

**Goal**: Users viewing YESTERDAY, last week, and last month rankings experience the same instant load times as TODAY (sub-1-second response).

**Independent Test**: Load each historical period page (YESTERDAY, last_week, last_month) with production data and verify sub-second response times. Response format matches current implementation (API contract maintained).

**Implementation Strategy**: Extends US1 pattern to other periods. YESTERDAY/last_week/last_month query `park_hourly_stats` directly (no hybrid needed - periods are complete). Charts for park details modal also use hourly tables.

### Tests for User Story 2 (TDD - MANDATORY)

> **TDD RED PHASE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T022 [P] [US2] Contract test for `/api/parks/downtime?period=yesterday` in `backend/tests/integration/test_yesterday_api.py` - validates response format
- [ ] T023 [P] [US2] Contract test for `/api/parks/downtime?period=last_week` in `backend/tests/integration/test_weekly_api.py` - validates response format
- [ ] T024 [P] [US2] Contract test for `/api/parks/downtime?period=last_month` in `backend/tests/integration/test_monthly_api.py` - validates response format
- [ ] T025 [P] [US2] Integration test for park details chart data in `backend/tests/integration/test_hourly_chart_data.py` - verifies `/api/parks/{park_id}/details?period=today` returns 24 hourly data points

**Checkpoint**: Tests written and FAILING (RED) - ready for GREEN phase

### Implementation for User Story 2 (TDD GREEN/REFACTOR)

- [ ] T026 [P] [US2] Update YESTERDAY query in `backend/src/database/queries/yesterday/yesterday_park_rankings.py` to use `park_hourly_stats` (controlled by `USE_HOURLY_TABLES` env var)
- [ ] T027 [P] [US2] Update last_week query logic to aggregate from `park_hourly_stats` instead of GROUP BY on raw snapshots
- [ ] T028 [P] [US2] Update last_month query logic to aggregate from `park_hourly_stats` instead of GROUP BY on raw snapshots
- [ ] T029 [US2] Update park details chart query in `backend/src/database/queries/charts/park_shame_history.py` to use hourly tables for all periods (TODAY, YESTERDAY, last_week, last_month)
- [ ] T030 [US2] Update Flask routes in `backend/src/api/routes/parks.py` for YESTERDAY/last_week/last_month periods to use new queries
- [ ] T031 [US2] Run tests - verify all US2 tests now PASS (TDD GREEN phase)
- [ ] T032 [US2] Code cleanup and refactoring while keeping tests green (TDD REFACTOR phase)

**Checkpoint**: All historical periods (YESTERDAY, last_week, last_month) load in under 1 second. Park details modal charts display instantly. Tests pass. API contract maintained across all periods.

---

## Phase 5: User Story 3 - Yearly Aggregation Capability (Priority: P3)

**Goal**: The system architecture supports aggregating full calendar years of data. This validates the aggregation pattern can scale to year-long timeframes for future yearly awards functionality (UI NOT implemented in this phase per Out of Scope).

**Independent Test**: Run yearly aggregation on completed calendar year (e.g., 2024) and verify aggregate created within required timeframe (<2 days). Query the yearly aggregate and confirm sub-second response. This proves architecture can handle year-scale data volumes.

**Implementation Strategy**: Extends existing `aggregate_daily.py` pattern. Yearly aggregates use `park_yearly_stats` table (already exists from migration 003). No UI changes - this is data infrastructure only.

### Tests for User Story 3 (TDD - MANDATORY)

> **TDD RED PHASE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T033 [P] [US3] Unit test for yearly aggregation logic in `backend/tests/unit/test_yearly_aggregation.py` - verifies aggregation from daily stats produces correct yearly averages
- [ ] T034 [P] [US3] Integration test for yearly query performance in `backend/tests/integration/test_yearly_query_performance.py` - validates sub-1-second response for year-long aggregate

**Checkpoint**: Tests written and FAILING (RED) - ready for GREEN phase

### Implementation for User Story 3 (TDD GREEN/REFACTOR)

- [ ] T035 [US3] Verify `park_yearly_stats` table exists (created in migration 003) - check schema matches requirements (park_id, year, avg_wait_time, avg_shame_score, sample_count)
- [ ] T036 [US3] Add yearly aggregation method to `backend/src/scripts/aggregate_daily.py` (reuses existing `DailyAggregator` pattern)
- [ ] T037 [US3] Implement yearly aggregation job that runs after Dec 31 (queries `park_daily_stats` for previous year, writes to `park_yearly_stats`)
- [ ] T038 [US3] Add yearly aggregation logging to `aggregation_log` table with execution metrics (records processed, processing time, success/failure status)
- [ ] T039 [US3] Run tests - verify all US3 tests now PASS (TDD GREEN phase)
- [ ] T040 [US3] Code cleanup and refactoring while keeping tests green (TDD REFACTOR phase)

**Checkpoint**: Yearly aggregation infrastructure complete. Architecture validated for year-scale data. Tests prove sub-second query performance. Yearly awards UI (out of scope) can be built on this foundation in future phase.

---

## Phase 6: User Story 4 - Automated Continuous Aggregation (Priority: P4)

**Goal**: The system automatically maintains aggregated data as new snapshots are collected, without manual intervention. Hourly job runs at :05 past each hour, daily job runs at 2 AM Pacific, yearly job runs after Dec 31.

**Independent Test**: Run data collection for 24 hours, verify aggregation jobs execute on schedule, and confirm all ranking views show up-to-date data. Check `aggregation_log` table for success status and processing times.

**Implementation Strategy**: Configure cron jobs or systemd timers for scheduled execution. Jobs are idempotent (use `INSERT ... ON DUPLICATE KEY UPDATE`). Error handling logs to `aggregation_log` and alerts operators.

### Tests for User Story 4 (TDD - MANDATORY)

> **TDD RED PHASE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T041 [P] [US4] Unit test for hourly job idempotency in `backend/tests/unit/test_hourly_aggregation.py` - verifies running same hour twice produces identical results (no duplicate rows)
- [ ] T042 [P] [US4] Unit test for aggregation error handling in `backend/tests/unit/test_hourly_aggregation.py` - verifies failures logged to `aggregation_log` with error details
- [ ] T043 [P] [US4] Integration test for continuous aggregation in `backend/tests/integration/test_continuous_aggregation.py` - simulates 24 hours of collection, verifies hourly aggregates created within 15 minutes of hour completion

**Checkpoint**: Tests written and FAILING (RED) - ready for GREEN phase

### Implementation for User Story 4 (TDD GREEN/REFACTOR)

- [ ] T044 [US4] Add error handling to `backend/src/scripts/aggregate_hourly.py` - catches exceptions, logs to `aggregation_log` with status='failure' and error message
- [ ] T045 [US4] Add retry logic to `backend/src/scripts/aggregate_hourly.py` - retries failed aggregations with exponential backoff (max 3 attempts)
- [ ] T046 [US4] Implement aggregation_log writes in `backend/src/scripts/aggregate_hourly.py` - logs every execution with aggregation_type='hourly', target_period, parks_processed, rides_processed, processing_time_seconds
- [ ] T047 [US4] Create cron job configuration in `deployment/cron.d/aggregate_hourly` - runs at :05 past every hour with proper environment variables (DATABASE_NAME, USE_HOURLY_TABLES, etc.)
- [ ] T048 [US4] Update daily aggregation job in `backend/src/scripts/aggregate_daily.py` to add cleanup task - deletes `park_hourly_stats` older than 3 years (per data-model.md retention policy)
- [ ] T049 [US4] Create monitoring dashboard query in `backend/src/database/queries/monitoring/aggregation_health.py` - queries `aggregation_log` for recent failures, processing time trends
- [ ] T050 [US4] Run tests - verify all US4 tests now PASS (TDD GREEN phase)
- [ ] T051 [US4] Code cleanup and refactoring while keeping tests green (TDD REFACTOR phase)

**Checkpoint**: Continuous aggregation system operational. Jobs run on schedule, handle errors gracefully, retry failures, log execution metrics. System maintains up-to-date aggregates automatically.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories, final validation before production deployment

- [ ] T052 [P] Run full local testing workflow per `specs/001-aggregation-tables/quickstart.md` - complete all 10 steps (mirror DB, migration, backfill, API testing, browser verification)
- [ ] T053 [P] Performance benchmarking per quickstart.md Step 7 - compare GROUP BY HOUR vs hourly table queries, verify 100-700x improvement
- [ ] T054 [P] Run complete test suite with `pytest backend/tests/ -v` - verify all 882+ tests pass with 0 failures
- [ ] T055 [P] Run linting with `ruff check backend/` - verify no lint errors
- [ ] T056 Update CLAUDE.md with any new canonical business rules discovered during implementation
- [ ] T057 [P] Update `specs/001-aggregation-tables/quickstart.md` with any deviations from original plan (document actual vs planned)
- [ ] T058 Manual browser verification (MANDATORY per CLAUDE.md) - open http://localhost:8080, test all periods (TODAY, YESTERDAY, last_week, last_month), verify shame scores match between Rankings table and Details modal
- [ ] T059 Security review - verify no SQL injection vulnerabilities in new queries, confirm proper input validation on API parameters (period, park_id)
- [ ] T060 [P] Add data quality monitoring queries in `backend/src/database/queries/monitoring/hourly_stats_quality.py` - checks for missing hours, low snapshot counts, shame score anomalies per research.md Finding 3 data quality warnings
- [ ] T061 Production deployment checklist - verify migration tested on mirrored DB, feature flag in place for rollback, monitoring configured, cron jobs ready

**Checkpoint**: Feature complete, all tests passing, manual verification complete, ready for production deployment

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User Story 1 (P1): Can start after Foundational - MVP target
  - User Story 2 (P2): Extends US1, but independently testable (different periods)
  - User Story 3 (P3): Independent of US1/US2 (yearly aggregation separate concern)
  - User Story 4 (P4): Integrates all stories (automation layer)
- **Polish (Phase 7)**: Depends on all user stories being complete

### User Story Dependencies

```
Foundational (Phase 2) - MUST COMPLETE FIRST
    │
    ├─> User Story 1 (P1) 🎯 MVP - TODAY rankings
    │       └─> Feature flag controls rollback path
    │
    ├─> User Story 2 (P2) - YESTERDAY/last_week/last_month
    │       └─> Extends US1 pattern to other periods
    │
    ├─> User Story 3 (P3) - Yearly aggregation
    │       └─> Independent (validates architecture scalability)
    │
    └─> User Story 4 (P4) - Continuous aggregation automation
            └─> Depends on US1-US3 being complete (orchestrates all jobs)
```

### Within Each User Story (TDD Workflow)

1. **RED**: Write tests FIRST, ensure they FAIL
2. **GREEN**: Write minimum code to make tests PASS
3. **REFACTOR**: Clean up code while keeping tests green
4. Story complete when all tests pass and manual verification succeeds

### Parallel Opportunities

- **Phase 1 (Setup)**: T001-T003 can run in parallel (different concerns)
- **Phase 2 (Foundational)**: T006-T007 can run in parallel (migration must complete first)
- **Phase 3 (US1)**: T009-T011 tests can run in parallel; T013-T014 query methods can run in parallel
- **Phase 4 (US2)**: T022-T025 tests can run in parallel; T026-T028 period queries can run in parallel
- **Phase 5 (US3)**: T033-T034 tests can run in parallel
- **Phase 6 (US4)**: T041-T043 tests can run in parallel
- **Phase 7 (Polish)**: T052-T055 validation tasks can run in parallel

---

## Parallel Example: User Story 1 (TODAY Rankings)

```bash
# TDD RED PHASE - Launch all tests together, verify they FAIL:
Task T009: "Contract test for /api/parks/downtime?period=today"
Task T010: "Integration test for TODAY hybrid query logic"
Task T011: "Chart query equivalence test"

# TDD GREEN PHASE - After tests fail, implement in dependency order:
# First, query methods (can run in parallel):
Task T013: "Add _query_hourly_tables() method"
Task T014: "Add _query_raw_snapshots() method"

# Then, integrate (sequential - depends on T013/T014):
Task T015: "Implement TODAY hybrid query"
Task T016: "Update stats_repository.py"
Task T017: "Update Flask route"

# TDD REFACTOR PHASE:
Task T018: "Run tests - verify all PASS"
Task T019: "Code cleanup while keeping tests green"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only) - Recommended Approach

1. **Complete Phase 1**: Setup - Mirror production DB, verify data (**1 hour**)
2. **Complete Phase 2**: Foundational - Migration, aggregation script, backfill (**6-8 hours**)
   - T006 (aggregate_hourly.py): 4-6 hours alone - must replicate DailyAggregator pattern
   - T008 (backfill script): 2-3 hours - depends on T006 completion
3. **Complete Phase 3**: User Story 1 - TODAY rankings with hybrid query + monitoring (**12-14 hours**)
   - T012-T014 (refactor park_shame_history.py): 4-5 hours - 467-line file with complex CTEs
   - T015 (TODAY hybrid query): 3-4 hours - weighted averaging with time boundary handling
   - T020-T021 (monitoring + rollback test): 2 hours - CRITICAL for MVP observability
   - Tests + validation: 3-5 hours
4. **STOP and VALIDATE**:
   - Run tests - all US1 tests must pass
   - Manual browser verification - TODAY rankings load < 1 second
   - Performance benchmark - verify 100-700x improvement
   - Monitoring health check - verify hourly job alerts working
5. **Deploy/Demo MVP**: TODAY rankings feature complete and production-ready

**Estimated MVP Time**: **19-23 hours** with rigorous TDD workflow (revised from 11h based on code complexity analysis)

**Why the Revision**: Original estimate allocated ~35 min/task. Code examination revealed:
- Existing query files are 230-467 lines with complex CTEs
- DailyAggregator pattern is 150+ lines just for setup/error handling
- TDD workflow (RED-GREEN-REFACTOR) adds necessary overhead
- Manual browser testing requires substantial time per CLAUDE.md

### Incremental Delivery (Full Feature)

1. **Foundation** (Setup + Foundational): **7-9 hours** → Database ready with hourly tables
2. **+US1** (TODAY): **12-14 hours** → Test independently → Deploy MVP ✅
3. **+US2** (YESTERDAY/last_week/last_month): **6-8 hours** → Test independently → Deploy
4. **+US3** (Yearly): **4-5 hours** → Test independently → Deploy
5. **+US4** (Automation): **6-7 hours** → Test independently → Deploy
6. **+Polish**: **5-6 hours** → Final validation → Production deployment

**Total Estimated Time**: **40-49 hours** for complete feature (all 4 user stories, revised from 25h)

Each story adds value without breaking previous stories. Feature flag (`USE_HOURLY_TABLES`) allows instant rollback at any stage.

### Parallel Team Strategy

With multiple developers (after Foundational phase complete):

1. **Team completes Setup + Foundational together** (**7-9 hours**)
2. **Once Foundational done, stories proceed in parallel**:
   - Developer A: User Story 1 (TODAY) - **12-14 hours**
   - Developer B: User Story 2 (YESTERDAY/last_week/last_month) - **6-8 hours**
   - Developer C: User Story 3 (Yearly) + User Story 4 (Automation) - **10-12 hours**
3. Stories complete and integrate independently via feature flag

**Total Time with 3 Developers**: **21-23 hours** (vs 40-49 hours sequential, 47% time savings)

---

## Notes

- **[P] tasks** = different files, no dependencies, safe to parallelize
- **[Story] label** maps task to specific user story for traceability (US1, US2, US3, US4)
- Each user story should be independently completable and testable
- **TDD MANDATORY**: Verify tests fail before implementing (RED), make tests pass (GREEN), then refactor
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- **Feature Flag**: USE_HOURLY_TABLES env var allows instant rollback to GROUP BY approach if issues discovered
- **Manual Verification Required**: Per CLAUDE.md, browser testing is mandatory before marking complete
- **Out of Scope Reminder**:
  - aggregate_daily.py refactoring deferred to Phase 2 (separate feature)
  - Yearly awards UI not implemented (data infrastructure only)
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

---

## Task Counts

- **Phase 1 (Setup)**: 3 tasks
- **Phase 2 (Foundational)**: 5 tasks
- **Phase 3 (US1 - TODAY)**: **13 tasks** (3 tests + 8 implementation + **2 monitoring/validation** - revised from 11)
- **Phase 4 (US2 - Historical)**: 11 tasks (4 tests + 7 implementation)
- **Phase 5 (US3 - Yearly)**: 8 tasks (2 tests + 6 implementation)
- **Phase 6 (US4 - Automation)**: 11 tasks (3 tests + 8 implementation)
- **Phase 7 (Polish)**: 10 tasks
- **Total**: **61 tasks** (revised from 59)

**Parallel Opportunities**: **25 tasks** marked [P] (**41% parallelizable**, revised from 23 tasks)

**MVP Scope (US1 only)**: **21 tasks** (Phase 1: 3 + Phase 2: 5 + Phase 3: 13, revised from 19)

**Time Estimates (Revised)**:
- **MVP**: 19-23 hours (vs original 11h - 2x adjustment based on code complexity)
- **Full Feature**: 40-49 hours (vs original 25h - 1.8x adjustment)

**Independent Test Criteria**:
- US1: Load TODAY rankings with production data, verify < 1 second response
- US2: Load YESTERDAY/last_week/last_month, verify < 1 second each
- US3: Run yearly aggregation on completed year, verify < 1 second query
- US4: Run 24-hour collection cycle, verify jobs execute on schedule

**Constitutional Compliance**:
- ✅ TDD Workflow (Principle VI): Tests written before implementation (RED-GREEN-REFACTOR)
- ✅ DRY Principles (Principle VII): Reuses ShameScoreCalculator, sql_helpers, single query class pattern
- ✅ Production Integrity (Principle IX): Local testing workflow with mirrored DB mandatory
- ✅ Mandatory Human Verification: Browser testing required before task completion (CLAUDE.md)
