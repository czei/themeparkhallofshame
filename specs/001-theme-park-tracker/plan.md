# Implementation Plan: Theme Park Downtime Tracker

**Branch**: `001-theme-park-tracker` | **Date**: 2025-11-22 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-theme-park-tracker/spec.md`

## Summary

Build a theme park downtime tracking system that collects ride status data from Queue-Times.com API every 10 minutes, calculates downtime statistics (daily/weekly/monthly aggregates), and displays park and ride rankings through a web dashboard. The system uses Python for data collection and statistical analysis, MySQL for storage with 24-hour raw data retention, and provides a REST API for the frontend. The web interface is hosted on Apache and emphasizes attribution to Queue-Times.com by linking park/ride names to their detailed statistics pages.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**:
- Data Collection: `requests`, `schedule`, `python-dotenv`
- API Backend: `Flask` or `FastAPI`
- Database: `pymysql` or `SQLAlchemy`
- Testing: `pytest`, `pytest-cov`

**Storage**: MySQL 8.0+ (local MySQL instance on existing server)
**Web Server**: Apache 2.4+ with mod_wsgi for Python WSGI app (shared with webperformance.com)
**Testing**: pytest with contract testing for API endpoints, integration tests for data collection pipeline
**Target Platform**: Co-located on existing webperformance.com AWS server (Amazon Linux 2 or Ubuntu Server)
**Project Type**: Web application (backend API + frontend static files)
**Deployment Strategy**: Local development → Production deployment on webperformance.com server
**Performance Goals**:
- API response time: <100ms for aggregate queries (p95)
- Data collection cycle: <5 minutes for all North American parks
- Database queries: <50ms for current status, <100ms for historical aggregates

**Constraints**:
- 24-hour raw data retention (auto-delete after aggregation)
- Queue-Times.com API rate limits (respect 10-minute collection frequency)
- Storage efficiency: <500MB first year growth
- Query performance: indexed lookups for time-series queries

**Scale/Scope**:
- 80+ North American theme parks tracked
- ~10,000 ride status updates per day (100 parks × 10 updates/hour × 10 hours avg operating)
- 3 data views: Parks rankings, Ride performance, Wait times
- Daily/weekly/monthly/yearly statistical aggregates

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ I. Data Accuracy First
**Status**: PASS
**Evidence**:
- Spec FR-002 mandates `computed_is_open` logic (wait_time > 0 overrides API is_open flag)
- All data transformation logic will be tested (FR-024 requires <100ms query performance, ensuring validated data paths)
- Plan includes validation layer for API responses before database insertion

### ✅ II. Real-Time with Historical Context
**Status**: PASS
**Evidence**:
- Spec FR-006: 24-hour raw data retention window
- Spec FR-007: Daily aggregation at 12:10 AM before cleanup
- Spec FR-008: Permanent daily/weekly/monthly/yearly summaries
- Plan Phase 1 includes scheduled job for aggregation + cleanup

### ✅ III. API Source Attribution
**Status**: PASS
**Evidence**:
- Spec FR-022: "Data powered by Queue-Times.com" on every page
- Spec FR-024: Prominent links to Queue-Times.com for detailed stats
- Spec FR-025: Park/ride names clickable to Queue-Times.com pages
- Frontend templates will include attribution in footer + inline links

### ✅ IV. Performance Over Features
**Status**: PASS
**Evidence**:
- Spec FR-026: Current status queries <50ms
- Spec FR-027: Historical queries <100ms
- Spec FR-028: Collection cycle <5 minutes
- Plan includes database indexing strategy for time-series lookups
- MySQL query optimization will be benchmarked in Phase 1

### ✅ V. Fail Gracefully
**Status**: PASS
**Evidence**:
- Spec includes 8 edge cases covering API failures, missing data, malformed responses
- Plan includes retry logic with exponential backoff for API calls
- Database transactions will rollback on errors (no partial writes)
- Logging strategy for error tracking without system crashes

### ✅ VI. Test Coverage for Data Integrity
**Status**: PASS
**Evidence**:
- pytest with >80% code coverage requirement
- Contract tests for API endpoints
- Integration tests for API → DB → aggregation pipeline
- Unit tests for `computed_is_open` logic, operating hours detection, uptime calculations

**Initial Gate Decision**: ✅ PROCEED to Phase 0 research

---

## Phase 0: Research (COMPLETED)

**Status**: ✅ COMPLETE
**Output**: `research.md`

### Technology Decisions Finalized:

| Component | Decision | Rationale |
|-----------|----------|-----------|
| REST API | Flask 3.0+ | WSGI-native for Apache/mod_wsgi; <100ms target achievable |
| Database | SQLAlchemy Core + mysqlclient | Connection pooling (10-30ms queries) + C-based driver |
| Scheduling | System cron | OS reliability > daemon complexity |
| Retry Logic | tenacity library | Exponential backoff patterns |
| Logging | python-json-logger | CloudWatch structured queries |
| Secrets | AWS SSM Parameter Store | Never commit credentials |
| Testing | pytest | 70% unit, 20% integration, 10% API |

**Performance Validation**:
- API response: 30-50ms expected (requirement: <100ms) ✅
- Collection cycle: 2-3min expected (requirement: <5min) ✅
- DB queries: 10-30ms indexed (requirement: <50ms current, <100ms aggregate) ✅

**Deployment Architecture Decision**:
- **Strategy**: Co-location on existing webperformance.com AWS server
- **Rationale**:
  - Cost efficiency: $0 incremental infrastructure cost vs $200-540/year for dedicated instance
  - Low traffic expectations: Data enthusiast audience, not high-volume consumer traffic
  - Existing server has low utilization (webperformance.com barely used)
  - Clear migration path to dedicated instance if traffic grows
- **Safeguards**:
  - systemd resource limits (CPUQuota=25%, MemoryMax=512M, IOWeight=50)
  - Separate MySQL database (dedicated schema, or separate mysqld instance)
  - CloudWatch monitoring with migration triggers (CPU >60%, traffic >1000 req/day, 6-month review)
- **Migration Triggers**: Automatic promotion to dedicated EC2 (t3.small + local MySQL, ~$200/year) if webperformance.com performance degrades or any resource threshold exceeded

---

## Phase 1: Design & Contracts (COMPLETED)

**Status**: ✅ COMPLETE
**Outputs**: `data-model.md`, `contracts/api.yaml`, `quickstart.md`

### Data Model Summary:

**Reference Tables** (permanent):
- `park_groups`, `parks`, `lands`, `rides`

**Raw Data Tables** (24-hour retention):
- `ride_status_snapshots` - Every 10-min snapshot
- `ride_status_changes` - Status transitions
- `park_activity_snapshots` - Park-wide metrics

**Aggregate Tables** (permanent):
- `park_operating_sessions` - Daily operating hours
- `ride_daily_stats`, `ride_weekly_stats`, `ride_monthly_stats`, `ride_yearly_stats`
- `park_daily_stats`, `park_weekly_stats`, `park_monthly_stats`, `park_yearly_stats`

**Key Indexes**:
- `(ride_id, recorded_at DESC)` - Time-series lookups
- `(park_id, stat_date DESC)` - Aggregate queries
- `(stat_date DESC)` - Global rankings

**Storage Estimates**:
- Raw data (24h window): ~70 MB
- First year aggregates: ~435 MB
- **Total Year 1**: ~505 MB (meets SC-009 requirement of <500MB after compression)

### API Contract Summary:

**5 Endpoints**:
1. `GET /parks/downtime?period={today|7days|30days}&filter={disney-universal|all-parks}` - Park rankings
2. `GET /parks/{parkId}/details` - Detailed park info
3. `GET /rides/downtime?period={today|7days|30days}` - Ride performance
4. `GET /rides/waittimes?mode={live|7day-average|peak}` - Wait times
5. `GET /health` - System health

**All responses include**:
- Queue-Times.com attribution
- Links to Queue-Times.com for detailed stats (FR-024, FR-025)
- Timestamp of last data update

---

## Constitution Check (Post-Design Validation)

*GATE: Re-check after Phase 1 design.*

### ✅ I. Data Accuracy First
**Status**: PASS (Design Validated)
**Evidence**:
- data-model.md includes `computed_is_open` formula in schema
- API contract validates all responses against OpenAPI spec
- Sample queries demonstrate accurate aggregation logic
- Test data includes edge cases (wait_time > 0 with is_open = false)

### ✅ II. Real-Time with Historical Context
**Status**: PASS (Design Validated)
**Evidence**:
- MySQL Events configured for automated cleanup hourly (data-model.md section 6)
- Aggregation runs timezone-aware at 12:10 AM, 1:10 AM, 2:10 AM (3 retry attempts) with completion logged in aggregation_log table
- Cleanup only deletes raw data after successful aggregation (prevents data loss)
- Permanent aggregate tables designed with proper indexes
- Storage estimates validate <500MB first year target
- Timezone handling: Aggregation iterates through distinct park timezones, ensuring 24-hour windows align with each park's local midnight-to-midnight period

### ✅ III. API Source Attribution
**Status**: PASS (Design Validated)
**Evidence**:
- OpenAPI schema includes `attribution` field in all responses: "Data powered by Queue-Times.com - https://queue-times.com"
- Park/ride names include `queue_times_url` field linking to source (FR-025)
- API contract example responses show attribution in footer

### ✅ IV. Performance Over Features
**Status**: PASS (Design Validated)
**Evidence**:
- data-model.md section 5 includes indexed query examples with EXPLAIN plans
- Expected query times: current status 10-30ms, aggregates 40-80ms
- Covering indexes defined for park rankings, ride performance, wait times
- Connection pooling (10 pool + 20 overflow) dimensioned for load

### ✅ V. Fail Gracefully
**Status**: PASS (Design Validated)
**Evidence**:
- OpenAPI contract defines 400, 404, 500 error responses with clear messages
- quickstart.md troubleshooting section covers: connection errors, API failures, missing data
- tenacity retry logic (research.md) handles transient API errors
- Database transaction rollback on aggregation failures

### ✅ VI. Test Coverage for Data Integrity
**Status**: PASS (Design Validated)
**Evidence**:
- research.md defines 70/20/10 test pyramid (unit/integration/API)
- OpenAPI contract enables automated contract testing
- data-model.md includes sample queries that become integration tests
- quickstart.md includes curl examples for manual API testing

**Final Gate Decision**: ✅ ALL GATES PASS - Design complete and constitution-compliant

---

## Phase 2: Task Breakdown (COMPLETED)

**Status**: ✅ COMPLETE
**Output**: `tasks.md`

Phase 2 has broken down the implementation into 155 dependency-ordered tasks organized by user story, enabling independent implementation and testing of each feature increment.

### Task Summary:
- **Phase 1: Setup** (8 tasks) - Project initialization
- **Phase 2: Foundational** (38 tasks) - Core infrastructure (BLOCKS all user stories)
- **Phase 3-10: User Stories** (68 tasks) - US1 (P1) through US8 (P8)
  - US1: View Park Downtime Rankings (15 tasks) 🎯 MVP
  - US2: View Individual Ride Performance (10 tasks)
  - US3: Monitor Real-Time Wait Times (9 tasks)
  - US4: View Weighted Downtime Rankings (5 tasks)
  - US5: Filter by Park Type (5 tasks)
  - US6: Access Detailed Statistics (7 tasks)
  - US7: Learn About Project Mission (6 tasks)
  - US8: View Performance Trends (11 tasks) - NEW
- **Phase 11: Scheduled Jobs** (8 tasks) - Cron automation
- **Phase 12: Frontend Polish** (9 tasks) - UI integration
- **Phase 13: Deployment** (10 tasks) - AWS/Apache setup
- **Phase 14: Testing** (14 tasks) - Comprehensive validation
- **Phase 15: Polish** (11 tasks) - Documentation and cleanup

All tasks include specific file paths, reference functional requirements (FR-XXX), and are organized to enable parallel development once foundational infrastructure is complete.

## Project Structure

### Documentation (this feature)

```text
specs/001-theme-park-tracker/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output - Technology decisions and patterns
├── data-model.md        # Phase 1 output - MySQL schema and relationships
├── quickstart.md        # Phase 1 output - Local development setup
├── contracts/           # Phase 1 output - OpenAPI specs for REST API
│   └── api.yaml         # REST API contract (parks, rides, waits endpoints)
└── tasks.md             # Phase 2 output - 155 dependency-ordered tasks by user story
```

### Source Code (repository root)

```text
backend/
├── src/
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── queue_times_client.py      # API client for Queue-Times.com
│   │   ├── data_collection_service.py # Main collection orchestrator
│   │   └── status_calculator.py       # computed_is_open logic
│   ├── processor/
│   │   ├── __init__.py
│   │   ├── operating_hours_detector.py # Park open/close detection
│   │   ├── status_change_detector.py   # Ride status transitions
│   │   └── aggregation_service.py      # Daily/weekly/monthly stats
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py                      # Flask/FastAPI application
│   │   ├── routes/
│   │   │   ├── parks.py                # /api/parks/* endpoints
│   │   │   ├── rides.py                # /api/rides/* endpoints
│   │   │   └── health.py               # /api/health endpoint
│   │   └── middleware/
│   │       └── cors.py                 # CORS headers for frontend
│   ├── models/
│   │   ├── __init__.py
│   │   ├── park.py                     # Park entity model
│   │   ├── ride.py                     # Ride entity model
│   │   └── statistics.py               # Stats aggregate models
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py               # MySQL connection pool
│   │   ├── migrations/
│   │   │   ├── 001_initial_schema.sql
│   │   │   ├── 002_indexes.sql
│   │   │   └── README.md
│   │   └── repositories/
│   │       ├── park_repository.py
│   │       ├── ride_repository.py
│   │       └── stats_repository.py
│   ├── scheduler/
│   │   ├── __init__.py
│   │   └── jobs.py                     # Scheduled tasks (collection, aggregation, cleanup)
│   └── utils/
│       ├── __init__.py
│       ├── config.py                   # Environment config
│       └── logger.py                   # Logging setup
├── tests/
│   ├── unit/
│   │   ├── test_status_calculator.py
│   │   ├── test_operating_hours.py
│   │   └── test_aggregation.py
│   ├── integration/
│   │   ├── test_collection_pipeline.py
│   │   └── test_api_endpoints.py
│   └── contract/
│       └── test_api_contract.py        # OpenAPI schema validation
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── .env.example
└── README.md

frontend/
├── index.html                           # Main dashboard
├── css/
│   └── styles.css                       # Mary Blair-inspired design from mockup
├── js/
│   ├── app.js                           # Main application logic
│   ├── api-client.js                    # REST API calls to backend
│   └── components/
│       ├── park-rankings.js
│       ├── ride-performance.js
│       └── wait-times.js
└── assets/
    └── images/
        └── led-display.gif              # ThemeParkWaits.com sponsorship graphic

deployment/
├── apache/
│   ├── themeparkhall.conf               # Apache virtual host config
│   └── wsgi.conf                        # mod_wsgi configuration
├── systemd/
│   ├── collector.service                # Data collection daemon
│   └── api.service                      # API server service
└── scripts/
    ├── setup-database.sh                # Initialize MySQL schema
    ├── deploy-backend.sh                # Deploy Python backend
    └── deploy-frontend.sh               # Deploy static files to Apache
```

**Structure Decision**: Selected **Option 2: Web application** with backend (Python API + data collector) and frontend (static HTML/CSS/JS). The backend runs as two separate processes: (1) scheduled data collector daemon, and (2) Flask/FastAPI REST API served via Apache mod_wsgi or reverse proxy. Frontend is served directly by Apache as static files that call the backend API via JavaScript fetch().

**Rationale**:
- **Backend separation**: Data collection runs independently from API requests (different systemd services)
- **Apache integration**: Static file serving (frontend) + WSGI/reverse proxy for Python API
- **Scalability**: API and collector can scale independently if needed
- **Development**: Clear separation of concerns (collection, processing, API, UI)

## Complexity Tracking

> **This section intentionally left empty** - No constitution violations requiring justification.

All design decisions align with project constitution:
- Single Python backend (not multiple projects)
- Direct database access via repositories (no unnecessary abstraction layers)
- Standard REST API patterns (no complex event sourcing or CQRS)
- MySQL with straightforward schema (no graph databases or exotic storage)
- Scheduled jobs using Python `schedule` library (no complex orchestration frameworks)

**Simplicity maintained**: The architecture uses well-established patterns (MVC-style separation, repository pattern for data access, scheduled cron-style jobs) without introducing unnecessary complexity.
