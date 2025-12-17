# Tasks: Weather Data Collection

**Input**: Design documents from `/specs/002-weather-collection/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/openmeteo-api.yaml

**Tests**: Following TDD (Constitution Principle VI) - tests written FIRST, verify they FAIL before implementation

**Organization**: Tasks organized by implementation phase (user stories) from spec.md

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (phase) this task belongs to (e.g., US1, US2, etc.)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `backend/src/`, `backend/tests/`
- Paths assume web application structure from plan.md

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and dependency management

- [X] T001 Add new Python dependencies to backend/requirements.txt (requests>=2.31.0, tenacity>=8.2.3)
- [X] T002 Install Python dependencies via pip install -r backend/requirements.txt
- [X] T003 [P] Verify existing utilities exist (backend/src/utils/timezone.py, backend/src/utils/config.py)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database schema and core infrastructure that MUST be complete before ANY user story

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Database Migration

- [X] T004 Create migration file backend/src/database/migrations/018_weather_schema.sql
- [X] T005 Define weather_observations table with all fields, indexes, and foreign keys in 018_weather_schema.sql
- [X] T006 Define weather_forecasts table with all fields, indexes, and foreign keys in 018_weather_schema.sql
- [ ] T007 Test migration on local dev database (mysql < 018_weather_schema.sql) [USER ACTION REQUIRED]
- [ ] T008 Verify tables created and indexes exist (SHOW CREATE TABLE weather_observations;) [USER ACTION REQUIRED]

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Database Schema (Priority: P1) 🎯 MVP FOUNDATION

**Goal**: Deploy weather_observations and weather_forecasts tables to production database

**Independent Test**: Run migration on mirrored production DB, verify tables exist with correct schema

### Tests for US1 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T009 [P] [US1] Unit test for migration rollback in backend/tests/unit/test_weather_migration.py
- [ ] T010 [P] [US1] Integration test for table schema validation in backend/tests/integration/test_weather_schema.py
- [ ] T011 [P] [US1] Integration test for foreign key constraints in backend/tests/integration/test_weather_schema.py

### Implementation for US1

- [ ] T012 [US1] Run migration on local test database and verify schema
- [ ] T013 [US1] Create rollback script (DROP TABLE IF EXISTS weather_forecasts; DROP TABLE IF EXISTS weather_observations;)
- [ ] T014 [US1] Document migration in quickstart.md (already done)
- [ ] T015 [US1] Verify unique constraints work (test duplicate insert)

**Checkpoint**: At this point, database schema should be deployed and testable independently

---

## Phase 4: User Story 2 - Weather API Client (Priority: P1) 🎯 MVP

**Goal**: Implement Open-Meteo API client with rate limiting and error handling

**Independent Test**: Call fetch_weather(lat, lon) and verify JSON response structure

### Tests for US2 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T016 [P] [US2] Contract test for Open-Meteo API response schema in backend/tests/contract/test_openmeteo_contract.py
- [ ] T017 [P] [US2] Unit test for TokenBucket rate limiter in backend/tests/unit/test_token_bucket.py
- [ ] T018 [P] [US2] Unit test for OpenMeteo client fetch_weather() in backend/tests/unit/test_openmeteo_client.py
- [ ] T019 [P] [US2] Unit test for API response parsing in backend/tests/unit/test_openmeteo_client.py
- [ ] T020 [P] [US2] Unit test for API error handling (timeout, 400, 500) in backend/tests/unit/test_openmeteo_client.py

### Implementation for US2

- [ ] T021 [P] [US2] Create TokenBucket rate limiter class in backend/src/utils/rate_limiter.py
- [ ] T022 [US2] Create OpenMeteoClient class (singleton pattern) in backend/src/api/openmeteo_client.py
- [ ] T023 [US2] Implement fetch_weather() method with tenacity @retry decorator in backend/src/api/openmeteo_client.py
- [ ] T024 [US2] Implement API response parsing (_parse_weather_data) with validation in backend/src/api/openmeteo_client.py
- [ ] T025 [US2] Implement unit conversion methods (Celsius→Fahrenheit, km/h→mph) in backend/src/api/openmeteo_client.py
- [ ] T026 [US2] Add global client instance and getter function in backend/src/api/openmeteo_client.py
- [ ] T027 [US2] Add structured JSON logging for API calls in backend/src/api/openmeteo_client.py

**Checkpoint**: At this point, API client should fetch and parse weather data successfully

---

## Phase 5: User Story 3 - Repository Layer (Priority: P1) 🎯 MVP

**Goal**: Implement repositories for idempotent inserts/queries of weather data

**Independent Test**: Insert observation, query it back, insert duplicate (verify ON DUPLICATE KEY UPDATE)

### Tests for US3 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T028 [P] [US3] Unit test for WeatherObservationRepository insert in backend/tests/unit/test_weather_repository.py
- [ ] T029 [P] [US3] Unit test for WeatherForecastRepository insert in backend/tests/unit/test_weather_repository.py
- [ ] T030 [P] [US3] Integration test for idempotent observation insert in backend/tests/integration/test_weather_repository.py
- [ ] T031 [P] [US3] Integration test for idempotent forecast insert in backend/tests/integration/test_weather_repository.py
- [ ] T032 [P] [US3] Integration test for batch insert performance in backend/tests/integration/test_weather_repository.py

### Implementation for US3

- [ ] T033 [P] [US3] Create WeatherObservationRepository class in backend/src/database/repositories/weather_repository.py
- [ ] T034 [P] [US3] Create WeatherForecastRepository class in backend/src/database/repositories/weather_repository.py
- [ ] T035 [US3] Implement insert_observation() with ON DUPLICATE KEY UPDATE in backend/src/database/repositories/weather_repository.py
- [ ] T036 [US3] Implement insert_forecast() with ON DUPLICATE KEY UPDATE in backend/src/database/repositories/weather_repository.py
- [ ] T037 [US3] Implement batch_insert_observations() using executemany() in backend/src/database/repositories/weather_repository.py
- [ ] T038 [US3] Implement batch_insert_forecasts() using executemany() in backend/src/database/repositories/weather_repository.py
- [ ] T039 [US3] Implement get_latest_observation(park_id) query method in backend/src/database/repositories/weather_repository.py
- [ ] T040 [US3] Add database error logging in backend/src/database/repositories/weather_repository.py

**Checkpoint**: At this point, repositories should insert/query weather data idempotently

---

## Phase 6: User Story 4 - Collection Script (Priority: P1) 🎯 MVP

**Goal**: Implement weather collection script with concurrent execution and failure threshold

**Independent Test**: Run collect_weather.py --test, verify 150 observations inserted

### Tests for US4 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T041 [P] [US4] Unit test for WeatherCollector._collect_for_park() in backend/tests/unit/test_weather_collector.py
- [ ] T042 [P] [US4] Unit test for failure threshold logic (>50% fail = abort) in backend/tests/unit/test_weather_collector.py
- [ ] T043 [P] [US4] Integration test for concurrent collection with 10 workers in backend/tests/integration/test_weather_collection.py
- [ ] T044 [P] [US4] Integration test for rate limiting (1 req/sec) in backend/tests/integration/test_weather_collection.py
- [ ] T045 [P] [US4] Integration test for graceful park failure handling in backend/tests/integration/test_weather_collection.py

### Implementation for US4

- [ ] T046 [US4] Create WeatherCollector class in backend/src/scripts/collect_weather.py
- [ ] T047 [US4] Implement run() method with ThreadPoolExecutor (10 workers) in backend/src/scripts/collect_weather.py
- [ ] T048 [US4] Implement _collect_for_park() method with API client call in backend/src/scripts/collect_weather.py
- [ ] T049 [US4] Implement failure threshold check (>50% fail = raise RuntimeError) in backend/src/scripts/collect_weather.py
- [ ] T050 [US4] Implement --current flag for hourly observations in backend/src/scripts/collect_weather.py
- [ ] T051 [US4] Implement --forecast flag for 6-hourly forecasts in backend/src/scripts/collect_weather.py
- [ ] T052 [US4] Implement --test flag for manual testing in backend/src/scripts/collect_weather.py
- [ ] T053 [US4] Add structured JSON logging (start/end times, success/fail counts) in backend/src/scripts/collect_weather.py
- [ ] T054 [US4] Add main() function with argparse CLI in backend/src/scripts/collect_weather.py
- [ ] T055 [US4] Test script manually: PYTHONPATH=backend/src python3 backend/src/scripts/collect_weather.py --test

**Checkpoint**: At this point, collection script should fetch and store weather for all parks

---

## Phase 7: User Story 5 - Scheduled Jobs (Priority: P2)

**Goal**: Schedule hourly current weather and 6-hourly forecast collection via cron

**Independent Test**: Check crontab -l shows scheduled jobs, monitor logs for hourly runs

### Tests for US5 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T056 [P] [US5] Integration test for cron schedule (verify job runs on the hour) in backend/tests/integration/test_weather_cron.py

### Implementation for US5

- [ ] T057 [US5] Add hourly current weather cron job to crontab (0 * * * *)
- [ ] T058 [US5] Add 6-hourly forecast cron job to crontab (0 */6 * * *)
- [ ] T059 [US5] Wrap cron jobs with backend/src/utils/cron_wrapper.py for failure alerting
- [ ] T060 [US5] Create log rotation for /var/log/weather_collection.log
- [ ] T061 [US5] Document cron setup in deployment/README.md
- [ ] T062 [US5] Verify jobs run successfully (tail -f /var/log/weather_collection.log)

**Checkpoint**: At this point, weather collection should run automatically every hour

---

## Phase 8: User Story 6 - Cleanup Job (Priority: P2)

**Goal**: Implement daily cleanup job to delete old observations (>2 years) and forecasts (>90 days)

**Independent Test**: Insert old data, run cleanup script, verify old data deleted

### Tests for US6 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T063 [P] [US6] Unit test for cleanup_old_observations() in backend/tests/unit/test_weather_cleanup.py
- [ ] T064 [P] [US6] Unit test for cleanup_old_forecasts() in backend/tests/unit/test_weather_cleanup.py
- [ ] T065 [P] [US6] Integration test for 2-year observation retention in backend/tests/integration/test_weather_cleanup.py
- [ ] T066 [P] [US6] Integration test for 90-day forecast retention in backend/tests/integration/test_weather_cleanup.py

### Implementation for US6

- [ ] T067 [P] [US6] Create cleanup_old_observations() function in backend/src/scripts/cleanup_weather.py
- [ ] T068 [P] [US6] Create cleanup_old_forecasts() function in backend/src/scripts/cleanup_weather.py
- [ ] T069 [US6] Implement DELETE query for observations older than 730 days in backend/src/scripts/cleanup_weather.py
- [ ] T070 [US6] Implement DELETE query for forecasts issued > 90 days ago in backend/src/scripts/cleanup_weather.py
- [ ] T071 [US6] Add logging for cleanup results (rows deleted) in backend/src/scripts/cleanup_weather.py
- [ ] T072 [US6] Add main() function with argparse CLI in backend/src/scripts/cleanup_weather.py
- [ ] T073 [US6] Add daily cron job for cleanup (0 4 * * * - runs at 4am UTC)
- [ ] T074 [US6] Test cleanup script manually: PYTHONPATH=backend/src python3 backend/src/scripts/cleanup_weather.py

**Checkpoint**: At this point, old weather data should be automatically deleted daily

---

## Phase 9: User Story 7 - Monitoring (Priority: P3)

**Goal**: Configure CloudWatch alarms and metrics for collection failures

**Independent Test**: Trigger failure condition, verify CloudWatch alarm fires

### Tests for US7 (TDD)

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T075 [P] [US7] Integration test for CloudWatch metric emission in backend/tests/integration/test_weather_monitoring.py

### Implementation for US7

- [ ] T076 [P] [US7] Create CloudWatch metric WeatherCollectionFailures in monitoring config
- [ ] T077 [P] [US7] Create CloudWatch alarm for parks with data >3 hours old in monitoring config
- [ ] T078 [P] [US7] Create CloudWatch dashboard for collection metrics in monitoring config
- [ ] T079 [US7] Add metric emission to collection script (emit failures count) in backend/src/scripts/collect_weather.py
- [ ] T080 [US7] Add SQL query to check data freshness (SELECT COUNT(*) WHERE observation_time < NOW() - INTERVAL 3 HOUR)
- [ ] T081 [US7] Document monitoring setup in deployment/MONITORING.md
- [ ] T082 [US7] Test alarm by stopping collection and verifying alert

**Checkpoint**: At this point, monitoring should alert on collection failures

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation, and Zen code review

- [ ] T083 [P] Run full test suite: pytest backend/tests/ -v
- [ ] T084 [P] Verify test coverage >80%: pytest --cov=backend/src --cov-report=term-missing
- [ ] T085 [P] Run linting: ruff check backend/src/
- [ ] T086 Run quickstart.md validation (manual test all steps)
- [ ] T087 **Mandatory Zen Code Review**: Run mcp__pal__codereview on backend/src/scripts/collect_weather.py
- [ ] T088 **Implement Zen Recommendations**: Apply all code review findings
- [ ] T089 Mirror production database: ./deployment/scripts/mirror-production-db.sh --days=7
- [ ] T090 Test collection with production data locally
- [ ] T091 Manual browser verification (no frontend changes, but verify backend APIs work)
- [ ] T092 Update CLAUDE.md with final weather collection patterns (if needed)
- [ ] T093 Prepare deployment checklist for production

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-9)**: All depend on Foundational phase completion
  - US1 (Schema): Can start after Foundational
  - US2 (API Client): Can start after Foundational, parallel to US1
  - US3 (Repository): Depends on US1 (schema must exist)
  - US4 (Collection): Depends on US2, US3
  - US5 (Cron): Depends on US4
  - US6 (Cleanup): Depends on US3 (repositories)
  - US7 (Monitoring): Depends on US4
- **Polish (Phase 10)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (Schema)**: No dependencies after Foundational
- **US2 (API Client)**: No dependencies after Foundational (parallel to US1)
- **US3 (Repository)**: Depends on US1 (tables must exist)
- **US4 (Collection)**: Depends on US2 (API client) and US3 (repositories)
- **US5 (Cron)**: Depends on US4 (collection script)
- **US6 (Cleanup)**: Depends on US3 (repositories), parallel to US4/US5
- **US7 (Monitoring)**: Depends on US4 (collection script)

### Within Each User Story

- Tests MUST be written and FAIL before implementation (TDD)
- Unit tests before implementation
- Implementation follows research.md patterns
- Integration tests verify end-to-end functionality
- Story complete before moving to next priority

### Parallel Opportunities

- **Phase 1**: All Setup tasks can run in parallel
- **Phase 2**: Tasks T004-T006 can run in parallel (different sections of migration file)
- **US1**: Tests T009-T011 can run in parallel
- **US2**: Tests T016-T020 can run in parallel; Implementation T021, T022 can run in parallel
- **US3**: Tests T028-T032 can run in parallel; Repositories T033, T034 can run in parallel
- **US4**: Tests T041-T045 can run in parallel
- **US5**: T057-T059 can run in parallel
- **US6**: Tests T063-T066 can run in parallel; Implementation T067, T068 can run in parallel
- **US7**: T076-T078 can run in parallel
- **Polish**: T083-T085 can run in parallel

---

## Parallel Example: User Story 2 (API Client)

```bash
# Launch all tests for User Story 2 together (TDD - write tests first):
Task T016: "Contract test for Open-Meteo API response schema in backend/tests/contract/test_openmeteo_contract.py"
Task T017: "Unit test for TokenBucket rate limiter in backend/tests/unit/test_token_bucket.py"
Task T018: "Unit test for OpenMeteo client fetch_weather() in backend/tests/unit/test_openmeteo_client.py"
Task T019: "Unit test for API response parsing in backend/tests/unit/test_openmeteo_client.py"
Task T020: "Unit test for API error handling in backend/tests/unit/test_openmeteo_client.py"

# After tests FAIL, launch parallel implementation:
Task T021: "Create TokenBucket rate limiter class in backend/src/utils/rate_limiter.py"
Task T022: "Create OpenMeteoClient class in backend/src/api/openmeteo_client.py"
```

---

## Implementation Strategy

### MVP First (US1-US4 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - creates database schema)
3. Complete Phase 3: US1 (Database Schema) → VALIDATE schema deployed
4. Complete Phase 4: US2 (API Client) → VALIDATE API calls work
5. Complete Phase 5: US3 (Repository) → VALIDATE data inserts work
6. Complete Phase 6: US4 (Collection Script) → VALIDATE end-to-end collection
7. **STOP and VALIDATE**: Run collection manually, verify data in database
8. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Database schema ready
2. Add US1 (Schema) → Test independently → Deploy schema
3. Add US2 (API Client) → Test independently → Verify API calls
4. Add US3 (Repository) → Test independently → Verify data inserts
5. Add US4 (Collection) → Test independently → Deploy/Demo (MVP!)
6. Add US5 (Cron) → Test independently → Schedule jobs
7. Add US6 (Cleanup) → Test independently → Ensure retention policy
8. Add US7 (Monitoring) → Test independently → Complete system
9. Each phase adds value without breaking previous phases

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: US1 (Schema) → US3 (Repository) → US6 (Cleanup)
   - Developer B: US2 (API Client) → US4 (Collection) → US5 (Cron)
   - Developer C: US7 (Monitoring) + Testing support
3. Stories complete and integrate at US4 (Collection Script)

---

## Notes

- **[P] tasks** = different files, no dependencies, can run in parallel
- **[Story] label** maps task to specific user story for traceability
- **TDD Mandatory** (Constitution Principle VI): Write tests FIRST, verify FAIL, then implement
- **Zen Code Review Mandatory** (Constitution Principle X): T087-T088 required before deployment
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Follow research.md patterns for all implementations (TokenBucket, failure threshold, API validation)
- **Critical**: TokenBucket MUST release lock before sleep (research.md fix)
- Verify tests fail before implementing
- Use existing timezone utilities (utils/timezone.py) - do NOT duplicate
- Use existing Config class (utils/config.py) - do NOT duplicate

---

## Total Task Count: 93 tasks

- **Setup**: 3 tasks
- **Foundational**: 5 tasks
- **US1 (Schema)**: 7 tasks (3 tests + 4 implementation)
- **US2 (API Client)**: 12 tasks (5 tests + 7 implementation)
- **US3 (Repository)**: 13 tasks (5 tests + 8 implementation)
- **US4 (Collection)**: 15 tasks (5 tests + 10 implementation)
- **US5 (Cron)**: 7 tasks (1 test + 6 implementation)
- **US6 (Cleanup)**: 12 tasks (4 tests + 8 implementation)
- **US7 (Monitoring)**: 8 tasks (1 test + 7 implementation)
- **Polish**: 11 tasks

**Parallel Opportunities**: 43 tasks marked [P] can run concurrently (46% of total)

**MVP Scope**: US1-US4 (42 tasks) = Minimum viable weather collection system

**Independent Test Criteria**:
- US1: Migration succeeds, tables exist, schema valid
- US2: API client fetches weather, rate limiting works
- US3: Repositories insert data idempotently
- US4: Collection script runs, 150 parks collected
- US5: Cron jobs execute on schedule
- US6: Old data deleted per retention policy
- US7: Alarms fire on failures
