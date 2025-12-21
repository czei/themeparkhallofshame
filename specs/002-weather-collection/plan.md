# Implementation Plan: Weather Data Collection

**Branch**: `002-weather-collection` | **Date**: 2025-12-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-weather-collection/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Add comprehensive weather data collection for ~150 theme parks using Open-Meteo API (free, no API key). Collect hourly current weather observations and 6-hourly 7-day forecasts to enable machine learning correlation analysis between weather conditions (particularly thunderstorms via WMO codes 95/96/99) and ride downtime patterns. Weather collection runs as parallel process to existing ride data collection, with 2-year retention for hourly observations and 90-day retention for forecasts.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: Flask 3.0+, SQLAlchemy 2.0+ (Core only), mysqlclient 2.2+, tenacity, requests
**Storage**: MySQL/MariaDB (existing database, new tables: weather_observations, weather_forecasts)
**Testing**: pytest with freezegun for time-based tests, mock for API responses
**Target Platform**: Linux server (AWS EC2, same as existing backend)
**Project Type**: Web application (backend only, no frontend changes in this feature)
**Performance Goals**: Collection cycle completes within 5 minutes (150 parks @ 1 req/sec), database queries <200ms
**Constraints**: 1 request/second to Open-Meteo API (respectful usage), zero impact on existing ride collection, idempotent inserts for safe re-runs
**Scale/Scope**: ~150 parks, 2.6M hourly observation rows (2 years), 9M forecast rows (90 days rolling), concurrent collection with 10 workers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ I. Data Accuracy First
- Weather data from Open-Meteo API validated before insertion
- WMO weather codes provide structured thunderstorm detection (95/96/99)
- TIMESTAMP fields ensure UTC storage (no DST ambiguity)
- **PASS**: Weather data accuracy supports correlation analysis goals

### ✅ II. Real-Time with Historical Context
- Hourly observations: 2-year retention (730 days)
- Forecasts: 90-day retention
- Daily cleanup job removes old data
- **PASS**: Retention policy balances historical analysis with storage efficiency

### ✅ III. API Source Attribution
- No frontend changes in this feature (backend data collection only)
- Future weather display would require Open-Meteo attribution
- **PASS**: Attribution required only when data is displayed to users

### ✅ IV. Performance Over Features
- Collection cycle: <5 minutes (150 parks @ 1 req/sec = 2.5 min + margin)
- Concurrent collection with ThreadPoolExecutor (10 workers)
- TokenBucket rate limiter ensures 1 req/sec compliance
- Database inserts via batching (not 1 row at a time)
- **PASS**: Performance targets met

### ✅ V. Fail Gracefully
- Single park API failure does NOT block other parks
- Tenacity @retry with exponential backoff
- Structured JSON logging for CloudWatch
- Graceful degradation (partial data better than no data)
- **PASS**: Resilient error handling

### ✅ VI. Test-Driven Development (TDD)
- Unit tests: Mock Open-Meteo API responses
- Integration tests: Real database with transaction rollback
- Contract tests: Validate API response schema
- freezegun for deterministic time-based testing
- **PASS**: TDD workflow followed, >80% coverage target

### ✅ VII. DRY Principles & Single Source of Truth
- Weather API client: Singleton pattern (single instance)
- Repository pattern: Centralized database logic
- Timezone utilities: Use existing `utils/timezone.py` (not duplicate)
- **PASS**: No duplicated business logic

### ✅ VIII. Architecture Stability
- Uses existing repository pattern (SQLAlchemy Core)
- Uses existing Config class for environment variables
- Uses existing timezone utilities
- No architectural changes, just new tables + collection script
- **PASS**: Extends existing architecture, no changes

### ✅ IX. Production Integrity & Local-First Development
- Development on feature branch `002-weather-collection`
- Local testing with mirrored production DB required before deployment
- Migration tested locally before production
- **PASS**: Local-first workflow enforced

### ✅ X. Mandatory AI-Assisted Expert Review
- **REQUIRED**: Zen review after Phase 1 design (data-model.md, contracts)
- **REQUIRED**: Zen codereview after Phase 4 implementation (collection script)
- **REQUIRED**: All recommendations implemented before proceeding
- **PASS**: Review gates established

**Constitution Check Result**: ✅ ALL GATES PASS - Proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/002-weather-collection/
├── spec.md              # Feature specification (completed)
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   └── openmeteo-api.yaml  # Open-Meteo API contract
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
backend/
├── src/
│   ├── api/
│   │   └── openmeteo_client.py         # NEW: Open-Meteo API client (singleton)
│   ├── database/
│   │   ├── migrations/
│   │   │   └── 018_weather_schema.sql  # NEW: Weather tables migration
│   │   └── repositories/
│   │       └── weather_repository.py   # NEW: Weather observation/forecast repos
│   ├── scripts/
│   │   ├── collect_weather.py          # NEW: Hourly weather + forecast collector
│   │   └── cleanup_weather.py          # NEW: Delete old weather data (2yr/90day)
│   └── utils/
│       ├── timezone.py                 # EXISTING: Pacific Time utilities (reuse)
│       └── config.py                   # EXISTING: Config class (reuse)
└── tests/
    ├── unit/
    │   ├── test_openmeteo_client.py    # NEW: Mock API responses
    │   ├── test_weather_repository.py  # NEW: Mock DB connection
    │   └── test_token_bucket.py        # NEW: Rate limiter logic
    ├── integration/
    │   ├── test_weather_collection.py  # NEW: Real DB, test transaction
    │   └── test_weather_cleanup.py     # NEW: Verify retention policy
    └── contract/
        └── test_openmeteo_contract.py  # NEW: Validate API schema
```

**Structure Decision**: Web application (Option 2). This feature is backend-only (no frontend changes). New files follow existing repository pattern:
- API clients in `src/api/`
- Repositories in `src/database/repositories/`
- Collection scripts in `src/scripts/`
- Migrations in `src/database/migrations/`
- Tests mirror source structure (unit/integration/contract)

## Complexity Tracking

**N/A** - No constitution violations. All gates pass.

---

## Phase Completion Status

### ✅ Phase 0: Research (COMPLETE)

**Deliverables**:
- ✅ `research.md` - All technical decisions documented
- ✅ TokenBucket rate limiter implementation
- ✅ ThreadPoolExecutor configuration (10 workers)
- ✅ Batch insert pattern (executemany + ON DUPLICATE KEY)
- ✅ Open-Meteo API reliability analysis
- ✅ TIMESTAMP vs DATETIME comparison

**Expert Review Findings**:
- ❌ **CRITICAL BUG FOUND**: TokenBucket sleeps inside lock (all workers blocked)
- ✅ **FIXED**: Lock released during sleep (concurrency enabled)
- ✅ **ADDED**: Failure threshold (>50% fail = abort)
- ✅ **ADDED**: API response validation (structure check before parse)

### ✅ Phase 1: Design & Contracts (COMPLETE)

**Deliverables**:
- ✅ `data-model.md` - Entity definitions, validation rules, indexes
- ✅ `contracts/openmeteo-api.yaml` - API contract specification
- ✅ `quickstart.md` - Developer setup instructions
- ✅ Agent context updated (CLAUDE.md)

**Zen Expert Review** (Constitution Principle X):
- ✅ Review conducted via `mcp__pal__thinkdeep` (gemini-2.5-pro)
- ✅ Architecture: Sound - extends existing patterns
- ✅ Schema: Correct - TIMESTAMP, indexes, foreign keys
- ✅ Performance: Meets <200ms query target
- ✅ Constitution: All 10 principles compliant
- ✅ Critical bug identified and fixed
- ✅ All recommendations implemented

**Constitution Check Re-Evaluation** (Post-Design):
- ✅ I. Data Accuracy: WMO codes, comprehensive validation
- ✅ II. Real-Time + Historical: 2yr/90d retention
- ✅ III. API Attribution: Backend only (no frontend)
- ✅ IV. Performance: <5 min collection, <200ms queries
- ✅ V. Fail Gracefully: Retry + per-park error handling + failure threshold
- ✅ VI. TDD: Tests defined, TDD cycle mandatory
- ✅ VII. DRY: No duplicated logic
- ✅ VIII. Architecture Stability: Extends existing patterns
- ✅ IX. Local-First: Development on feature branch
- ✅ X. Mandatory Review: ✅ COMPLETE

**Ready for Phase 2**: Implementation (via `/speckit.tasks` command)

---

## Implementation Notes

**Key Design Decisions** (From Expert Review):

1. **TokenBucket Concurrency**: Lock MUST be released during sleep
   - Bug: `with self.lock: ... time.sleep(wait_time)` blocks all workers
   - Fix: Calculate wait_time inside lock, sleep outside lock

2. **Failure Threshold**: >50% park failures = abort collection
   - Prevents silent systemic failure (e.g., API down)
   - Tolerates individual park failures (bad coordinates)

3. **API Response Validation**: Check structure before parsing
   - Validates time/temp arrays are lists and same length
   - Catches API contract changes early
   - Prevents data corruption from misaligned timestamps

**Next Command**: `/speckit.tasks` to generate implementation task list
