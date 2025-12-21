# Implementation Plan: ORM Refactoring for Reliable Data Access

**Branch**: `003-orm-refactoring` | **Date**: 2025-12-21 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/003-orm-refactoring/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Replace raw SQL strings with type-safe ORM models using SQLAlchemy 2.0+, remove hourly_stats aggregation table (serve hourly metrics via indexed on-the-fly queries), retain daily_stats table with improved recomputation tooling, and implement database migration framework (Alembic). This addresses infrastructure fragility from scattered SQL logic while enabling flexible analytics queries for ML/pattern analysis features. **CRITICAL CONSTRAINT**: Maintain existing Flask REST API contracts without changes - refactoring is internal only, frontend remains unaffected.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: Flask 3.0+, SQLAlchemy 2.0+ Core (migrating to ORM models), Alembic 1.13+ (new - migration framework), mysqlclient 2.2+, tenacity 8.2+
**Storage**: MySQL 5.7+ or MariaDB 10.3+ (existing production database - schema modifications only)
**Testing**: pytest 7.4+ with pytest-cov 4.1+, freezegun for time-based determinism
**Target Platform**: Linux production server (current: ec2-user@webperformance.com), macOS development (local testing)
**Project Type**: Web application (Flask backend API + separate frontend - backend refactoring only)
**Performance Goals**: Hourly queries <500ms @ 95th percentile (20 concurrent users), daily queries <10% regression vs. current baseline
**Constraints**:
- Zero Flask API contract changes (same endpoints, same JSON responses, same query params)
- Daily recompute job completes 90 days in <6 hours
- Migration downtime <5 minutes
- 24-hour snapshot retention maintained (no extension in this feature)
- Repository pattern preserves existing function signatures

**Scale/Scope**:
- ~935 existing tests to maintain/update
- 6 database tables for ORM models (ride_status_snapshots, park_activity_snapshots, daily_stats, parks, rides, weather_*)
- 1 table to drop (hourly_stats)
- ~20 repository files to migrate from raw SQL to ORM
- Production database: ~10GB, <100 concurrent users

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ PASS: Principle I - Data Accuracy First
ORM refactoring maintains existing data accuracy by preserving business logic in centralized helpers (src/utils/sql_helpers.py migrated to ORM methods). Query results validated via golden data regression tests (FR-027) to ensure identical values before/after migration.

### ✅ PASS: Principle II - Real-Time with Historical Context
No change to retention policy (24-hour snapshots, permanent daily aggregations). ORM layer simply replaces query mechanism while preserving existing aggregation jobs and retention cleanup.

### ✅ PASS: Principle III - API Source Attribution
Frontend unchanged, attribution display unchanged. This is a backend-only refactoring with zero API contract changes.

### ✅ PASS: Principle IV - Performance Over Features
Explicit performance targets defined (FR-010: <500ms @ 95th percentile for hourly queries, SC-008: <10% regression for daily queries). Load testing required before deployment (FR-028). Composite indexes added (FR-009, FR-021) to meet performance goals. If targets not met, materialized views can be added without reverting to full hourly_stats table.

### ✅ PASS: Principle V - Fail Gracefully
ORM framework provides better error handling than raw SQL (type-safe queries catch schema mismatches at startup vs. runtime). Migration rollback capability (FR-018) ensures safe recovery from failed schema changes. Recompute job handles missing data gracefully (FR-014).

### ✅ PASS: Principle VI - Test-Driven Development (TDD)
This implementation will follow strict TDD cycle:
1. RED: Write tests validating ORM query returns same results as current raw SQL (golden data tests)
2. GREEN: Implement ORM models and queries to pass tests
3. REFACTOR: Consolidate business logic into ORM model methods

All 33 functional requirements have testable acceptance criteria (FR-025: >80% coverage, FR-026: regression tests, FR-027: golden data validation).

### ✅ PASS: Principle VII - DRY Principles & Single Source of Truth
**CRITICAL ALIGNMENT**: This refactoring directly addresses the root cause identified in Constitution - duplicated logic across 20+ query files. ORM migration will:
- Centralize business logic into ORM model methods (e.g., `RideSnapshot.is_operating()`, `ParkSnapshot.appears_open()`)
- Move shared calculations to query abstraction layer (FR-003)
- Eliminate string-based SQL duplication (FR-032)
- Migrate src/utils/sql_helpers.py to ORM query builder methods (FR-031)

Each business rule will have exactly one canonical implementation as ORM model method or repository helper.

### ✅ PASS: Principle VIII - Architecture Stability
This architectural change is justified by documented root cause analysis:
1. **Root cause**: Raw SQL strings cause fragility (typos → runtime errors, schema changes break silently, maintaining SQL across 64 test files creates tech debt, duplicated logic causes regression cycles)
2. **Cost-benefit analysis**: Hybrid approach (remove hourly_stats, keep daily_stats) balances flexibility gains with performance risk mitigation
3. **Staged migration plan**: Phase 0 research, Phase 1 design, Phase 2 implementation with independent validation via golden data tests and load testing
4. **Production lessons**: Decision informed by Kennywood YESTERDAY bug, backfill pain points, need for flexible analytics queries (Feature 005)

### ✅ PASS: Principle IX - Production Integrity & Local-First Development
Implementation plan includes:
- Local development with mirrored production data (deployment/scripts/mirror-production-db.sh)
- Migration testing on local copy before production deployment
- Full test suite execution with production data patterns
- Load testing with production-like concurrency
- Zero direct production server code modifications

### ✅ PASS: Principle X - Mandatory AI-Assisted Expert Review
**MANDATORY REVIEW GATES**:
- **Phase 0 (Research)**: Zen `thinkdeep` to validate ORM strategy and migration approach
- **After data model design**: Zen `analyze` to validate ORM model structure and relationships
- **After repository migration**: Zen `codereview` to validate code quality, DRY compliance, and business logic centralization
- **Before deployment**: Zen `secaudit` for SQL injection prevention and security validation

All recommendations MUST be implemented before proceeding to next phase.

### No Violations - No Complexity Justification Required

All constitutional principles align with ORM refactoring goals. This change directly supports Principles VI, VII, VIII (TDD, DRY, Architecture Stability).

## Project Structure

### Documentation (this feature)

```text
specs/003-orm-refactoring/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output: ORM strategy, migration patterns, index design
├── data-model.md        # Phase 1 output: ORM models, relationships, validation rules
├── quickstart.md        # Phase 1 output: Developer guide for using ORM layer
├── contracts/           # Phase 1 output: (N/A - maintaining existing Flask API contracts)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Web application structure (backend refactoring only, frontend unchanged)

backend/
├── src/
│   ├── models/          # ORM model definitions (NEW - Phase 1)
│   │   ├── __init__.py
│   │   ├── base.py     # SQLAlchemy declarative base, engine config
│   │   ├── ride.py     # Ride ORM model
│   │   ├── park.py     # Park ORM model
│   │   ├── snapshots.py # RideStatusSnapshot, ParkActivitySnapshot models
│   │   ├── stats.py    # DailyStats ORM model (hourly_stats REMOVED)
│   │   └── weather.py  # WeatherObservation, WeatherForecast models
│   │
│   ├── database/        # MIGRATE: Raw SQL → ORM queries
│   │   ├── connection.py     # Database connection (UPDATE: add SQLAlchemy engine)
│   │   ├── repositories/     # MIGRATE: All SQL strings → ORM
│   │   │   ├── ride_status_repository.py  # MIGRATE to ORM
│   │   │   ├── stats_repository.py        # MIGRATE to ORM
│   │   │   ├── park_repository.py         # MIGRATE to ORM
│   │   │   └── weather_repository.py      # MIGRATE to ORM
│   │   └── migrations/       # NEW: Alembic migration scripts
│   │       ├── env.py
│   │       ├── alembic.ini
│   │       └── versions/
│   │           ├── 001_add_composite_indexes.py
│   │           ├── 002_add_metrics_version_to_daily_stats.py
│   │           └── 003_drop_hourly_stats_table.py
│   │
│   ├── utils/
│   │   ├── sql_helpers.py    # MIGRATE: SQL builder → ORM query methods
│   │   └── query_helpers.py  # NEW: ORM query abstraction layer
│   │
│   ├── api/             # NO CHANGES to routes/contracts (maintain API compatibility)
│   │   ├── routes/      # Function signatures preserved, internal queries use ORM
│   │   └── app.py       # Flask app setup unchanged
│   │
│   ├── scripts/
│   │   ├── aggregate_daily.py      # UPDATE: Use ORM models for aggregation
│   │   └── recompute_daily_stats.py # NEW: Idempotent recompute job (FR-013)
│   │
│   └── (collector/, classifier/, processor/ - NO CHANGES in this feature)
│
└── tests/
    ├── unit/            # NEW: ORM model tests, query method tests
    │   ├── test_orm_models.py
    │   └── test_query_helpers.py
    ├── integration/     # UPDATE: Validate ORM queries match raw SQL results
    │   ├── test_orm_query_parity.py  # NEW: Golden data regression tests
    │   └── (existing integration tests - update to use ORM)
    ├── contract/        # NO CHANGES (API contracts unchanged)
    └── golden_data/     # UPDATE: Add ORM query validation datasets

frontend/            # NO CHANGES (API contracts preserved)
deployment/          # UPDATE: Add migration step to deployment scripts
```

**Structure Decision**: Web application (backend + frontend). Backend-only refactoring maintains strict API contract compatibility. ORM models added to `src/models/`, database layer migrated to use ORM queries, Flask API routes unchanged.

## Complexity Tracking

> **No violations - this section intentionally empty per Constitution Check results.**

---

## Phase 0: Research & Strategy

**Purpose**: Resolve all NEEDS CLARIFICATION items and establish technical approach before implementation.

### Research Tasks

1. **SQLAlchemy 2.0 ORM Strategy**
   - Decision needed: Declarative models vs. imperative mapping
   - Decision needed: Session management pattern (scoped_session vs. context-based)
   - Decision needed: Relationship loading strategy (lazy vs. eager vs. select in load)
   - Research: SQLAlchemy 2.0 best practices for Flask integration
   - Research: Type annotation support in SQLAlchemy 2.0 models

2. **Migration Framework Selection**
   - Decision needed: Alembic configuration for existing production database
   - Research: Zero-downtime migration patterns for dropping hourly_stats table
   - Research: Rollback strategy for schema changes
   - Research: Migration testing patterns with production data copies

3. **Index Design for Performance**
   - Research: Composite index design for time-series queries (ride_id, snapshot_time)
   - Research: MySQL/MariaDB index optimization for GROUP BY queries
   - Research: EXPLAIN plan analysis patterns for validating index usage
   - Decision needed: Index naming conventions and documentation

4. **Query Migration Patterns**
   - Research: Translating raw SQL CTEs (Common Table Expressions) to SQLAlchemy ORM
   - Research: Timezone handling in SQLAlchemy (UTC storage, Pacific time queries)
   - Research: NULL handling patterns in ORM aggregations (AVG, COUNT, SUM)
   - Decision needed: Query abstraction layer architecture (repository pattern vs. model methods)

5. **Testing Strategy**
   - Research: Golden data test patterns for query parity validation
   - Research: Load testing tools for concurrent query performance
   - Research: Integration test patterns with automatic transaction rollback
   - Decision needed: Test data generation strategy for ORM models

6. **Recompute Job Design**
   - Research: Idempotent batch processing patterns
   - Research: metrics_version column design and usage
   - Decision needed: Recompute job CLI interface and scheduling
   - Research: Progress tracking and error handling for 90-day recompute

**Output**: `research.md` with decisions, rationale, and alternatives considered for each area.

---

## Phase 1: Design & Contracts

**Prerequisites**: `research.md` complete, all NEEDS CLARIFICATION resolved

### Deliverables

#### 1. `data-model.md` - ORM Model Definitions

Extract entities from spec and define ORM models:

**Core Models**:
- **Ride**: park_id (FK), name, ride_type, is_active, created_at, updated_at
  - Relationship: belongs_to Park
  - Relationship: has_many RideStatusSnapshot
  - Methods: `get_current_status()`, `calculate_uptime(period)`

- **Park**: name, park_type (disney/universal/other), timezone, created_at, updated_at
  - Relationship: has_many Ride
  - Relationship: has_many ParkActivitySnapshot
  - Methods: `is_operating_at(timestamp)`, `get_operating_hours(date)`

- **RideStatusSnapshot**: ride_id (FK), snapshot_time, status, wait_time, computed_is_open, park_appears_open
  - Relationship: belongs_to Ride
  - Indexes: composite(ride_id, snapshot_time), snapshot_time
  - Methods: `is_down()`, `is_operating()`
  - Validation: snapshot_time in UTC, status in ENUM

- **ParkActivitySnapshot**: park_id (FK), snapshot_time, park_appears_open, first_activity_time, last_activity_time
  - Relationship: belongs_to Park
  - Indexes: composite(park_id, snapshot_time), snapshot_time
  - Methods: `is_within_operating_hours(timestamp)`

- **DailyStats**: date, ride_id (FK), park_id (FK), total_downtime_minutes, shame_score, uptime_percentage, metrics_version (NEW)
  - Relationship: belongs_to Ride, belongs_to Park
  - Indexes: composite(date, ride_id), date, metrics_version
  - Validation: date is Pacific timezone date, metrics_version tracks calculation version

- **WeatherObservation, WeatherForecast**: (existing models - ORM definitions added)

**Dropped Tables**:
- ~~hourly_stats~~ - REMOVED (served via on-the-fly ORM queries)

**Query Abstraction Layer**:
- `RideStatusSQL` → `RideStatusQuery` (ORM-based)
  - `rides_that_operated_cte()` → `rides_that_operated_query()`
  - `is_down()` → ORM filter method
- `DowntimeSQL` → `DowntimeQuery` (ORM-based)
- `UptimeSQL` → `UptimeQuery` (ORM-based)

#### 2. `contracts/` - API Contract Validation

**CRITICAL**: This refactoring maintains existing Flask API contracts. No new contracts generated.

Instead, create:
- `contracts/api-preservation.md`: Documents that all existing endpoints remain unchanged
- `contracts/repository-interfaces.md`: Documents that repository method signatures are preserved
- Test suite validates API response schemas remain identical (contract tests)

#### 3. `quickstart.md` - Developer Guide

**Purpose**: How to use the new ORM layer while maintaining existing API contracts.

**Sections**:
1. **Setup**: Database connection with SQLAlchemy engine
2. **Querying**: Using ORM models vs. old raw SQL patterns
3. **Migrations**: Running Alembic migrations (up/down)
4. **Testing**: Writing ORM model tests, using fixtures
5. **Performance**: EXPLAIN plan analysis, index validation
6. **Debugging**: SQL logging, query profiling

**Example Code**:
```python
# OLD (raw SQL - to be migrated)
cursor.execute("SELECT * FROM rides WHERE park_id = %s", (park_id,))

# NEW (ORM)
session.query(Ride).filter(Ride.park_id == park_id).all()
# OR using repository pattern
ride_repo.find_by_park(park_id)
```

#### 4. Agent Context Update

Run `.specify/scripts/bash/update-agent-context.sh claude` to add:
- SQLAlchemy 2.0+ ORM to Active Technologies
- Alembic 1.13+ to Active Technologies
- ORM model structure to Project Structure
- Migration commands to Commands section

---

## Phase 2: Implementation Planning

**Output**: Dependency-ordered `tasks.md` (created by `/speckit.tasks` command, NOT this command)

This planning document ends here. Next step: `/speckit.tasks` to generate implementation tasks.

---

## Success Criteria Validation

**From Spec** (verify plan addresses all success criteria):

- ✅ **SC-001**: Plan includes migration of all repository SQL → ORM (src/database/repositories/)
- ✅ **SC-002**: Plan includes composite indexes (FR-009) and load testing (FR-028) for <500ms target
- ✅ **SC-003**: Plan includes golden data regression tests (research.md, data-model.md testing section)
- ✅ **SC-004**: Plan requires >80% ORM test coverage (FR-025)
- ✅ **SC-005**: Plan includes recompute job design (research task #6) with 6-hour target (FR-015)
- ✅ **SC-006**: Plan uses ORM parameterized queries (inherent to SQLAlchemy, validated by secaudit)
- ✅ **SC-007**: Plan includes Alembic migration framework (research task #2, migrations/ directory)
- ✅ **SC-008**: Plan includes performance baseline comparison (FR-024, load testing)
- ✅ **SC-009**: Plan includes slow query monitoring (FR-023)
- ✅ **SC-010**: Plan tracks development velocity improvement (qualitative - easier ORM queries vs. SQL strings)

**Additional Validations**:
- ✅ Flask API contracts preserved (no changes to frontend)
- ✅ Repository pattern maintains existing function signatures (FR-033)
- ✅ TDD workflow enforced (Constitution Principle VI)
- ✅ DRY principles enforced (Constitution Principle VII) - business logic centralized in ORM models
- ✅ Zen review gates defined (Constitution Principle X)
- ✅ Local-first development workflow (Constitution Principle IX)

---

## Next Steps

1. **Execute Phase 0**: Generate `research.md` by researching all 6 technical areas
2. **Zen Review (thinkdeep)**: Validate ORM strategy and migration approach (Constitutional requirement)
3. **Implement recommendations**: Apply Zen feedback before proceeding
4. **Execute Phase 1**: Generate `data-model.md`, `quickstart.md`, and contract preservation docs
5. **Zen Review (analyze)**: Validate ORM model structure (Constitutional requirement)
6. **Update agent context**: Run update script to add SQLAlchemy/Alembic to Claude context
7. **Execute Phase 2**: Run `/speckit.tasks` to generate dependency-ordered implementation tasks
