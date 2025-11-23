# Tasks: Theme Park Downtime Tracker

**Input**: Design documents from `/specs/001-theme-park-tracker/`
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/api.yaml ✓

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

Based on plan.md project structure:
- **Backend**: `backend/src/`, `backend/tests/`
- **Frontend**: `frontend/`
- **Deployment**: `deployment/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create project directory structure per plan.md (backend/, frontend/, deployment/, specs/)
- [X] T002 Initialize Python project with backend/requirements.txt (Flask 3.0+, SQLAlchemy, mysqlclient, tenacity, python-json-logger, python-dotenv, pytest, pytest-cov)
- [X] T003 Create backend/requirements-dev.txt (pytest, pytest-mock, pytest-flask, pytest-cov, black, flake8, mypy)
- [X] T004 [P] Create .gitignore for Python/Node/MySQL (exclude .env, __pycache__, venv/, node_modules/, *.pyc, *.log)
- [X] T005 [P] Create backend/.env.example with template environment variables (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, QUEUE_TIMES_API_BASE_URL, ENVIRONMENT)
- [X] T006 [P] Create backend/pytest.ini with test configuration (testpaths, python_files, python_classes, python_functions, coverage settings)
- [X] T007 [P] Create deployment/scripts/setup-database.sh for MySQL schema initialization
- [X] T008 Initialize frontend structure (index.html, css/, js/, assets/)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Database Schema & Infrastructure

- [X] T009 Create MySQL schema migration backend/src/database/migrations/001_initial_schema.sql (parks, rides, ride_classifications tables from data-model.md)
- [X] T010 Create MySQL schema migration backend/src/database/migrations/002_raw_data_tables.sql (ride_status_snapshots, ride_status_changes, park_activity_snapshots tables)
- [X] T011 Create MySQL schema migration backend/src/database/migrations/003_aggregates_tables.sql (park_operating_sessions, aggregation_log, ride_daily/weekly/monthly/yearly_stats, park_daily/weekly/monthly/yearly_stats)
- [X] T012 Create MySQL schema migration backend/src/database/migrations/004_indexes.sql (all indexes from data-model.md: time-series, composite, filter indexes)
- [X] T013 Create MySQL cleanup event script backend/src/database/migrations/005_cleanup_events.sql (automated 24-hour retention cleanup using aggregation_log verification)

### Core Infrastructure

- [X] T014 Implement backend/src/utils/config.py for environment config management (AWS SSM Parameter Store for production, python-dotenv for local)
- [X] T015 Implement backend/src/utils/logger.py with python-json-logger for structured CloudWatch logging
- [X] T016 Implement backend/src/database/connection.py with SQLAlchemy Core connection pooling (pool_size=10, max_overflow=20, pool_recycle=3600, pool_pre_ping=True)
- [X] T017 Create backend/src/models/park.py entity model with fields from data-model.md (park_id, name, city, state_province, country, timezone, operator, is_disney, is_universal, is_active)
- [X] T018 Create backend/src/models/ride.py entity model with fields from data-model.md (ride_id, park_id, name, land_area, tier, is_active)
- [X] T019 Create backend/src/models/statistics.py entity models for aggregate stats (ride_daily_stats, park_daily_stats, ride_weekly_stats, etc.)

### Data Collection Infrastructure

- [ ] T020 Implement backend/src/collector/queue_times_client.py with tenacity retry logic (3 attempts, exponential backoff, handle Timeout/ConnectionError)
- [ ] T021 Implement backend/src/collector/status_calculator.py for computed_is_open logic (wait_time > 0 OR (is_open = true AND wait_time = 0))
- [ ] T022 Implement backend/src/collector/data_collection_service.py main collection orchestrator (fetch all parks, insert snapshots, detect status changes)
- [ ] T023 Implement backend/src/database/repositories/park_repository.py for park CRUD operations
- [ ] T024 Implement backend/src/database/repositories/ride_repository.py for ride CRUD operations
- [ ] T025 Implement backend/src/database/repositories/stats_repository.py for statistics queries

### Ride Classification System (FR-022 to FR-032)

- [ ] T026 Create data/manual_overrides.csv schema (park_id, ride_id, override_tier, reason, date_added) with documentation
- [ ] T027 Create data/exact_matches.json schema (cache_key format: {park_id}:{ride_id}, tier, confidence, reasoning, research_sources, schema_version)
- [ ] T028 Implement backend/src/classifier/pattern_matcher.py with keyword rules (Tier 3: "kiddie", "carousel", "theater"; Tier 1: "coaster", "mountain", "space")
- [ ] T029 Implement backend/src/classifier/ai_classifier.py using mcp__zen__chat with Gemini-2.5-pro and web search capability (FR-029)
- [ ] T030 Implement backend/src/classifier/classification_service.py with 4-tier hierarchical logic (manual_overrides → exact_matches.json → pattern_matcher → ai_agent)
- [ ] T031 Implement caching logic in classification_service.py (confidence > 0.85 → exact_matches.json, cache_key: {park_id}:{ride_id}, schema versioning)
- [ ] T032 Implement parallel processing in classification_service.py using ThreadPoolExecutor for batch AI classification (FR-032)
- [ ] T033 Implement classification confidence scoring and flagging for human review (confidence < 0.5 → manual review required, FR-031)
- [ ] T034 Create backend/scripts/classify_rides.py CLI script to run classification on all unclassified rides

### Aggregation & Processing

- [ ] T035 Implement backend/src/processor/operating_hours_detector.py to detect park open/close from ride activity in local timezone (FR-003, uses parks.timezone field)
- [ ] T036 Implement backend/src/processor/status_change_detector.py to detect ride status transitions (open↔closed) and calculate duration
- [ ] T037 Implement backend/src/processor/aggregation_service.py for daily/weekly/monthly/yearly stats calculation with aggregation_log tracking (FR-007, FR-045)
- [ ] T038 Implement timezone-aware aggregation logic in aggregation_service.py (iterate through distinct park timezones, FR-045)
- [ ] T039 Implement retry logic for aggregation with 3 attempts at 12:10 AM, 1:10 AM, 2:10 AM (FR-007)
- [ ] T040 Create backend/scripts/aggregate_daily.py CLI script for daily aggregation job
- [ ] T041 Create backend/scripts/cleanup_raw_data.py CLI script with safe cleanup using aggregation_log verification

### API Framework

- [ ] T042 Create backend/src/api/app.py Flask application with Blueprints structure and Flask-CORS middleware
- [ ] T043 Implement API key authentication middleware in backend/src/api/middleware/auth.py (X-API-Key header validation, FR-041, FR-043)
- [ ] T044 Implement rate limiting middleware in backend/src/api/middleware/rate_limiter.py (100 req/hour, 1000 req/day per API key, FR-042, FR-044)
- [ ] T045 Implement backend/src/api/routes/health.py with GET /api/health endpoint (database connectivity, last collection timestamp)
- [ ] T046 Create backend/src/api/middleware/error_handler.py for standardized error responses (400, 401, 404, 429, 500)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - View Park Downtime Rankings (Priority: P1) 🎯 MVP

**Goal**: Users can view parks ranked by total downtime hours for selected time periods (Today, 7 Days, 30 Days) with filtering by park operator

**Independent Test**: Call GET /api/parks/downtime?period=today&filter=all-parks and verify ranked park list with downtime hours

### Implementation for User Story 1

- [ ] T047 [US1] Implement GET /api/parks/downtime endpoint in backend/src/api/routes/parks.py (FR-010, FR-011, FR-012, FR-013)
- [ ] T048 [US1] Implement query logic for today's park rankings in stats_repository.py using park_daily_stats table (data-model.md Query 1)
- [ ] T049 [US1] Implement query logic for 7-day park rankings using park_weekly_stats table with trend calculation
- [ ] T050 [US1] Implement query logic for 30-day park rankings using park_monthly_stats table with trend calculation
- [ ] T051 [US1] Implement Disney & Universal filter logic (is_disney = TRUE OR is_universal = TRUE, FR-020, FR-021)
- [ ] T052 [US1] Implement aggregate statistics calculation (total parks tracked, peak downtime, currently down rides, FR-013)
- [ ] T053 [US1] Add Queue-Times.com attribution to response (FR-033, FR-035)
- [ ] T054 [US1] Add queue_times_url field to park responses linking to Queue-Times.com park pages (FR-036)
- [ ] T055 [US1] Create frontend/js/components/park-rankings.js to fetch and render park rankings table
- [ ] T056 [US1] Implement time period selector (Today, 7 Days, 30 Days) in park-rankings.js
- [ ] T057 [US1] Implement park filter toggle (Disney & Universal / All Parks) in park-rankings.js
- [ ] T058 [US1] Display aggregate statistics at top of page (total parks, peak downtime, currently down rides) in park-rankings.js
- [ ] T059 [US1] Add trend indicators (percentage change arrows) to park rankings display
- [ ] T060 [US1] Make park names clickable links to Queue-Times.com (FR-036)
- [ ] T061 [US1] Add "Data powered by Queue-Times.com" footer with link to queue-times.com (FR-033)

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - View Individual Ride Performance (Priority: P2)

**Goal**: Users can view individual rides ranked by downtime hours with current status badges and 7-day averages

**Independent Test**: Call GET /api/rides/downtime?period=7days and verify ranked ride list with downtime hours and status badges

### Implementation for User Story 2

- [ ] T062 [US2] Implement GET /api/rides/downtime endpoint in backend/src/api/routes/rides.py (FR-014, FR-015, FR-016)
- [ ] T063 [US2] Implement query logic for ride performance rankings using ride_weekly_stats table (data-model.md Query 2)
- [ ] T064 [US2] Implement current status badge logic by querying most recent ride_status_snapshots (FR-016)
- [ ] T065 [US2] Add ride tier display (1/2/3) alongside ride name in response (FR-027)
- [ ] T066 [US2] Add Queue-Times.com attribution and ride detail links to response (FR-033, FR-036)
- [ ] T067 [US2] Create frontend/js/components/ride-performance.js to fetch and render ride performance table
- [ ] T068 [US2] Implement status badges (Down/Running) with color coding in ride-performance.js
- [ ] T069 [US2] Display ride tier badges (Tier 1/2/3) alongside ride names
- [ ] T070 [US2] Make ride names clickable links to Queue-Times.com ride pages (FR-036)
- [ ] T071 [US2] Add time period selector (Today, 7 Days, 30 Days) to ride performance view

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Monitor Real-Time Wait Times (Priority: P3)

**Goal**: Users can view current wait times sorted by longest waits with 7-day averages and peak times

**Independent Test**: Call GET /api/rides/waittimes?mode=live and verify wait times sorted descending with trend percentages

### Implementation for User Story 3

- [ ] T072 [US3] Implement GET /api/rides/waittimes endpoint in backend/src/api/routes/rides.py (FR-017, FR-018, FR-019)
- [ ] T073 [US3] Implement Live mode query (current wait times from most recent snapshots, data-model.md Query 3)
- [ ] T074 [US3] Implement 7 Day Average mode query (average wait times from ride_weekly_stats)
- [ ] T075 [US3] Implement Peak Times mode query (max wait times from ride_weekly_stats.peak_wait_time)
- [ ] T076 [US3] Add trend percentage calculation (current vs 7-day average)
- [ ] T077 [US3] Create frontend/js/components/wait-times.js to fetch and render wait times table
- [ ] T078 [US3] Implement mode selector (Live, 7 Day Average, Peak Times) in wait-times.js
- [ ] T079 [US3] Display wait times sorted by longest waits descending
- [ ] T080 [US3] Add trend percentage indicators (current vs average)

**Checkpoint**: All core user stories (US1, US2, US3) should now be independently functional

---

## Phase 6: User Story 4 - View Weighted Downtime Rankings (Priority: P4)

**Goal**: Users can view park rankings weighted by ride tier importance (Tier 1: 3x, Tier 2: 2x, Tier 3: 1x)

**Independent Test**: Call GET /api/parks/downtime with weighted=true parameter and verify weighted scores account for tier distribution

### Implementation for User Story 4

- [ ] T081 [US4] Implement weighted downtime scoring logic in stats_repository.py (FR-024: Park Score = Σ(downtime_hours × tier_weight) / Σ(all_ride_weights))
- [ ] T082 [US4] Add weighted scoring query from data-model.md Query 1b (park_weights CTE, weighted_downtime CTE)
- [ ] T083 [US4] Add weighted=true query parameter to GET /api/parks/downtime endpoint
- [ ] T084 [US4] Update frontend park-rankings.js to display weighted scores when enabled
- [ ] T085 [US4] Add weighted ranking toggle switch in park-rankings.js UI

**Checkpoint**: Weighted downtime scoring functional

---

## Phase 7: User Story 5 - Filter by Park Type (Priority: P5)

**Goal**: Users can filter park and ride views by "Disney & Universal" or "All Parks"

**Independent Test**: Call GET /api/parks/downtime?filter=disney-universal and verify only Disney/Universal parks returned

### Implementation for User Story 5

- [ ] T086 [US5] Verify filter parameter implemented in GET /api/parks/downtime (already added in T051)
- [ ] T087 [US5] Add filter parameter to GET /api/rides/downtime endpoint
- [ ] T088 [US5] Add filter parameter to GET /api/rides/waittimes endpoint
- [ ] T089 [US5] Update all frontend components to pass filter parameter to API calls
- [ ] T090 [US5] Implement global filter toggle in frontend/js/app.js that updates all views

**Checkpoint**: Filtering works across all views

---

## Phase 8: User Story 6 - Access Detailed Statistics (Priority: P6)

**Goal**: Users can click park names to view detailed park information including tier distribution and operating hours

**Independent Test**: Call GET /api/parks/{parkId}/details and verify detailed park info with tier breakdown

### Implementation for User Story 6

- [ ] T091 [US6] Implement GET /api/parks/{parkId}/details endpoint in backend/src/api/routes/parks.py (contracts/api.yaml endpoint 2)
- [ ] T092 [US6] Implement query logic for park detail view (tier distribution, current status, recent operating hours)
- [ ] T093 [US6] Add tier distribution calculation (count of Tier 1/2/3 rides, FR-028)
- [ ] T094 [US6] Add recent operating sessions from park_operating_sessions table
- [ ] T095 [US6] Create frontend modal/detail view for park details
- [ ] T096 [US6] Display tier distribution chart/breakdown (FR-028)
- [ ] T097 [US6] Display recent operating hours and session information

**Checkpoint**: Park detail views functional with tier transparency

---

## Phase 9: User Story 7 - Learn About Project Mission (Priority: P7)

**Goal**: Users can access an "About This Project" modal explaining the mission and methodology

**Independent Test**: Click "About This Project" link and verify modal displays project mission with respect for maintenance professionals

### Implementation for User Story 7

- [ ] T098 [US7] Create frontend/js/components/about-modal.js with modal overlay implementation
- [ ] T099 [US7] Add modal content explaining project mission emphasizing respect for maintenance professionals (FR-034)
- [ ] T100 [US7] Add methodology explanation (data source, update frequency, attribution)
- [ ] T101 [US7] Implement modal open/close logic (click link, close button, outside click, Escape key)
- [ ] T102 [US7] Add "About This Project" link to main navigation in frontend/index.html
- [ ] T103 [US7] Style modal with Mary Blair-inspired design matching overall site aesthetic

**Checkpoint**: All user stories (US1-US7) fully implemented

---

## Phase 10: User Story 8 - View Performance Trends (Priority: P8)

**Goal**: Users can view parks and rides showing significant uptime percentage changes (≥5% improvement or decline) comparing current period to previous period

**Independent Test**: Click "Trends" navigation tab and verify four trend tables appear showing parks/rides with ≥5% uptime changes, properly filtered and sorted

### Implementation for User Story 8

- [ ] T104 [US8] Implement GET /api/trends endpoint in backend/src/api/routes/trends.py (FR-046, FR-047, FR-054)
- [ ] T105 [US8] Implement query logic for parks-improving category using data-model.md Query 8 (park_daily_stats, park_weekly_stats, park_monthly_stats tables)
- [ ] T106 [US8] Implement query logic for parks-declining category using data-model.md Query 9
- [ ] T107 [US8] Implement query logic for rides-improving category using data-model.md Query 10 (ride_daily_stats, ride_weekly_stats, ride_monthly_stats tables)
- [ ] T108 [US8] Implement query logic for rides-declining category using data-model.md Query 11
- [ ] T109 [US8] Implement period comparison logic (today vs yesterday, 7days vs previous 7days, 30days vs previous 30days) (FR-047)
- [ ] T110 [US8] Implement ≥5% threshold filtering for uptime percentage changes (FR-048)
- [ ] T111 [US8] Implement sorting by improvement percentage (FR-049) and decline percentage (FR-050)
- [ ] T112 [US8] Add park filter application to trends endpoint (Disney & Universal / All Parks) (FR-053)
- [ ] T113 [US8] Create frontend/js/components/trends.js to fetch and render all four trend tables
- [ ] T114 [US8] Implement trend table display with uptime percentage comparisons showing previous period %, current period %, and change % (FR-051, FR-052)

**Checkpoint**: All user stories (US1-US8) fully implemented

---

## Phase 11: Scheduled Jobs & Automation

**Purpose**: Implement cron jobs for data collection and aggregation

- [ ] T115 Create backend/scripts/collect.py CLI entry point for data collection (calls data_collection_service.py)
- [ ] T116 Add error handling and logging to collect.py (log to CloudWatch with structured JSON)
- [ ] T117 Add error handling and logging to aggregate_daily.py
- [ ] T118 Add error handling and logging to cleanup_raw_data.py
- [ ] T119 Create deployment/systemd/collector.service systemd service definition for data collector
- [ ] T120 Create deployment/systemd/api.service systemd service definition for Flask API
- [ ] T121 Create crontab configuration in deployment/scripts/setup-cron.sh (*/10 collect.py, 10 0 aggregate_daily.py with flock)
- [ ] T122 Add CloudWatch "dead man's switch" monitoring alarm for collection failures (alert if no collection in 15 minutes)

---

## Phase 12: Frontend Polish & Integration

**Purpose**: Complete frontend implementation with navigation and design

- [ ] T123 Create frontend/index.html main structure with navigation tabs (Park Rankings, Ride Performance, Wait Times, Trends)
- [ ] T124 Create frontend/css/styles.css with Mary Blair-inspired design from mockup
- [ ] T125 Implement frontend/js/app.js main application controller with tab switching logic
- [ ] T126 Implement frontend/js/api-client.js REST API client wrapper with error handling
- [ ] T127 Add frontend loading states and error messages for API calls
- [ ] T128 Add frontend responsive design for mobile/tablet viewing
- [ ] T129 Add frontend/assets/images/led-display.gif ThemeParkWaits.com sponsorship graphic
- [ ] T130 Implement data freshness indicator showing last update timestamp (FR-040)
- [ ] T131 Add Queue-Times.com attribution footer to all pages (FR-033)

---

## Phase 13: Deployment & Infrastructure

**Purpose**: Deploy to webperformance.com server (co-located deployment)

- [ ] T132 Assess current webperformance.com server resources (CPU, memory, disk, MySQL version)
- [ ] T133 Create deployment/systemd/themepark-collector.service with resource limits (CPUQuota=25%, MemoryMax=512M, IOWeight=50)
- [ ] T134 Create deployment/systemd/themepark-api.service with resource limits (CPUQuota=30%, MemoryMax=512M) - optional if using mod_wsgi
- [ ] T135 Create deployment/apache/themeparkwaits.conf VirtualHost configuration for api.themeparkwaits.com with mod_wsgi
- [ ] T136 Create deployment/scripts/deploy-backend.sh script (rsync or git pull, install dependencies, restart services)
- [ ] T137 Create deployment/scripts/deploy-frontend.sh script (copy static files to /var/www/themeparkwaits/frontend)
- [ ] T138 Create production .env file with secure credentials (DB_PASSWORD, QUEUE_TIMES_API_KEY, FLASK_ENV=production)
- [ ] T139 Set up production MySQL database on existing server (CREATE DATABASE themepark_tracker_prod)
- [ ] T140 Run database migrations on production (001-005 migration scripts)
- [ ] T141 Configure CloudWatch monitoring with migration triggers (CPU >60%, traffic >1000 req/day)
- [ ] T142 Set up Let's Encrypt SSL certificate for api.themeparkwaits.com
- [ ] T143 Test production deployment end-to-end (API health, data collection, frontend)

---

## Phase 14: Testing & Validation

**Purpose**: Comprehensive testing across all components (primarily local development)

- [ ] T144 [P] Create backend/tests/unit/test_status_calculator.py (test computed_is_open logic with all edge cases)
- [ ] T145 [P] Create backend/tests/unit/test_operating_hours.py (test park operating hours detection with timezone handling)
- [ ] T146 [P] Create backend/tests/unit/test_aggregation.py (test daily/weekly/monthly stat calculations)
- [ ] T147 [P] Create backend/tests/unit/test_classification.py (test 4-tier classification hierarchy)
- [ ] T148 [P] Create backend/tests/integration/test_collection_pipeline.py (test API → DB → aggregation flow)
- [ ] T149 [P] Create backend/tests/integration/test_api_endpoints.py (test all API endpoints with mock data, including /api/trends)
- [ ] T150 [P] Create backend/tests/contract/test_api_contract.py (validate responses against contracts/api.yaml OpenAPI schema)
- [ ] T151 Run pytest with coverage report locally (target >80% code coverage)
- [ ] T152 Validate API response times meet requirements locally (current status <50ms, aggregates <100ms, trends <100ms, FR-037, FR-038)
- [ ] T153 Validate data collection cycle completes in <5 minutes for all parks locally (FR-039)
- [ ] T154 Test API authentication and rate limiting locally (FR-041, FR-042, FR-043, FR-044)
- [ ] T155 Test timezone-aware aggregation for parks across multiple timezones locally (FR-045)
- [ ] T156 Test aggregation failure recovery and retry logic locally (FR-007)
- [ ] T157 Validate storage growth remains under 500 MB projection (SC-009)

---

## Phase 15: Polish & Documentation

**Purpose**: Final polish and documentation

- [ ] T158 [P] Add inline code documentation and docstrings to all Python modules
- [ ] T159 [P] Create backend/src/database/migrations/README.md with migration instructions
- [ ] T160 [P] Add API usage examples to contracts/api.yaml OpenAPI documentation
- [ ] T161 Code cleanup: Remove debug prints, commented code, unused imports
- [ ] T162 Run black code formatter on all Python files
- [ ] T163 Run flake8 linter and fix warnings
- [ ] T164 Run mypy type checker and add type hints where missing
- [ ] T165 Security review: Check for SQL injection, XSS, CSRF vulnerabilities
- [ ] T166 Performance optimization: Review database indexes, query EXPLAIN plans
- [ ] T167 Create deployment runbook with troubleshooting steps for co-located setup
- [ ] T168 Final validation: Run through all user stories end-to-end locally (US1-US8)
- [ ] T169 Final validation: Run through all user stories end-to-end in production (US1-US8)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-10)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3 → P4 → P5 → P6 → P7 → P8)
- **Scheduled Jobs (Phase 11)**: Depends on Foundational (data collection/aggregation infrastructure)
- **Frontend Polish (Phase 12)**: Depends on at least US1, US2, US3 being complete
- **Deployment (Phase 13)**: Depends on all desired user stories being complete
- **Testing (Phase 14)**: Can start after Foundational, expand as user stories complete
- **Polish (Phase 15)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - Independently testable
- **User Story 4 (P4)**: Depends on US1 (extends park rankings) but independently testable
- **User Story 5 (P5)**: Depends on US1, US2, US3 (adds filtering to existing views)
- **User Story 6 (P6)**: Depends on US1 (park detail view from rankings) - Independently testable
- **User Story 7 (P7)**: No dependencies (standalone modal) - Can start after Frontend structure exists
- **User Story 8 (P8)**: Depends on US1, US2 (uses same aggregation infrastructure) - Independently testable

### Within Each User Story

- Backend endpoints before frontend components
- Repository queries before endpoint implementation
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- All Foundational database migrations (T009-T013) can run in parallel
- All Foundational entity models (T017-T019) can run in parallel
- All Foundational repositories (T023-T025) can run in parallel
- All classification components (T026-T033) can be developed in parallel
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- All unit tests marked [P] can run in parallel
- Different user stories can be worked on in parallel by different team members

---

## Implementation Strategy

### MVP First (User Story 1-3 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Park Rankings)
4. Complete Phase 4: User Story 2 (Ride Performance)
5. Complete Phase 5: User Story 3 (Wait Times)
6. Complete Phase 10: Scheduled Jobs (enable data collection)
7. Complete Phase 11: Frontend Polish
8. **STOP and VALIDATE**: Test US1-3 independently
9. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (Park Rankings!)
3. Add User Story 2 → Test independently → Deploy/Demo (+ Ride Performance!)
4. Add User Story 3 → Test independently → Deploy/Demo (+ Wait Times!)
5. Add User Story 4 → Test independently → Deploy/Demo (+ Weighted Scoring!)
6. Add User Story 5 → Test independently → Deploy/Demo (+ Filtering!)
7. Add User Story 6 → Test independently → Deploy/Demo (+ Park Details!)
8. Add User Story 7 → Test independently → Deploy/Demo (+ About Mission!)
9. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (Park Rankings)
   - Developer B: User Story 2 (Ride Performance)
   - Developer C: User Story 3 (Wait Times)
   - Developer D: Classification System + Scheduled Jobs
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies, can run in parallel
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- File paths follow plan.md structure (backend/src/, frontend/, deployment/)
- All tasks reference specific functional requirements (FR-XXX) and success criteria (SC-XXX) from spec.md
- Database queries reference data-model.md sample queries
- API endpoints reference contracts/api.yaml OpenAPI specification
- Technology decisions reference research.md
