# Implementation Plan: Pre-Computed Time-Series Aggregation Tables

**Branch**: `001-aggregation-tables` | **Date**: 2025-12-05 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-aggregation-tables/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

**Refactoring Focus**: Add hourly aggregation tables to eliminate slow GROUP BY HOUR queries on raw snapshots. The project already has daily/weekly/monthly/yearly aggregation infrastructure with atomic swap patterns. This refactoring extends that pattern to hourly bucketing to fix chart performance issues (currently 5-10 seconds) and TODAY period queries that scan 1,440+ snapshots per park per day.

**Key Insight**: Infrastructure already exists - we're extending a proven pattern, not building from scratch.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: Flask 3.0+, SQLAlchemy 2.0+ (Core only, no ORM models), mysqlclient 2.2+
**Storage**: MySQL/MariaDB with existing schema (park_activity_snapshots, ride_status_snapshots, park_daily_stats, etc.)
**Testing**: pytest 7.4+ with unit/integration test structure (882 tests in suite)
**Target Platform**: Linux server (single-server deployment for 500 concurrent users)
**Project Type**: Web application (Flask backend + static frontend)
**Performance Goals**: Sub-1-second response times for all ranking queries (currently 5-10 seconds for charts)
**Constraints**:
- Maintain Flask API response format (frontend depends on JSON structure)
- Use SQLAlchemy Core with text() queries (no ORM migration)
- Follow atomic swap pattern for zero-downtime updates (proven in park_live_rankings)
- Respect 7-day hybrid denominator for shame score consistency
- Support 90-day raw retention, 3-year hourly retention, indefinite daily/yearly retention

**Scale/Scope**:
- 80 parks × 12 snapshots/hour × 24 hours = 23,040 snapshots/day
- 8.4M snapshots/year
- 500 concurrent users target
- Charts need hourly buckets for last 24 hours (TODAY), last 7 days (WEEK), last 30 days (MONTH)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ Data Accuracy First (Principle I)
- Shame score calculation uses centralized `ShameScoreCalculator` class (single source of truth)
- Park status precedence logic enforced via `RideStatusSQL.rides_that_operated_cte()`
- 7-day hybrid denominator prevents seasonal closure false positives
- **Compliance**: Hourly aggregates will store pre-computed shame scores using same calculator

### ✅ Real-Time with Historical Context (Principle II)
- Raw snapshots retained 90 days (meets requirement)
- Hourly aggregates retained 3 years
- Daily/yearly aggregates retained indefinitely
- **Compliance**: Extends existing retention architecture to hourly granularity

### ✅ Performance Over Features (Principle IV)
- Current: Charts take 5-10 seconds (GROUP BY HOUR on 1,440+ snapshots)
- Target: Sub-1-second response via pre-aggregated hourly tables
- **Compliance**: Pre-computation moves work to batch jobs, queries become simple SELECT

### ✅ Test-Driven Development (Principle VI)
- 882 tests in suite demonstrate TDD culture
- Integration tests validate cross-feature consistency with real data
- **Compliance**: Plan includes TDD workflow for hourly aggregation (test aggregation logic, then implement)

### ✅ DRY Principles & Single Source of Truth (Principle VII)
- Existing: `ShameScoreCalculator`, `sql_helpers.py` (RideStatusSQL, DowntimeSQL, UptimeSQL)
- Centralized metrics in `metrics.py` (SNAPSHOT_INTERVAL_MINUTES, SHAME_SCORE_MULTIPLIER)
- **Compliance**: Hourly aggregation will reuse existing calculators, not duplicate logic

### ✅ Production Integrity & Local-First Development (Principle IX)
- Local dev workflow: `mirror-production-db.sh` → test locally → deploy
- **Compliance**: Migration tested on mirrored DB before production deployment

### ✅ Mandatory AI-Assisted Expert Review (Principle X)
- **Compliance**: This plan will undergo Zen codereview after Phase 1 design completion

**Gate Status**: ✅ PASSED - All constitutional principles aligned

## Project Structure

### Documentation (this feature)

```text
specs/001-aggregation-tables/
├── spec.md              # Feature specification (✅ complete)
├── checklists/
│   └── requirements.md  # Spec quality validation (✅ complete)
├── plan.md              # This file (/speckit.plan output)
├── research.md          # Phase 0: Technology choices & patterns
├── data-model.md        # Phase 1: Hourly aggregate schema
├── quickstart.md        # Phase 1: How to test/run locally
├── contracts/           # Phase 1: API response formats
│   └── api/
│       └── parks.yaml   # OpenAPI spec for /api/parks/* endpoints
└── tasks.md             # Phase 2: Implementation tasks (/speckit.tasks)
```

### Source Code (repository root)

```text
backend/
├── src/
│   ├── api/
│   │   ├── app.py                          # Flask app (existing)
│   │   └── routes/
│   │       ├── parks.py                    # Park rankings endpoints (modify for hourly data)
│   │       └── rides.py                    # Ride rankings endpoints (modify for hourly data)
│   ├── database/
│   │   ├── calculators/
│   │   │   └── shame_score.py              # Centralized shame score logic (reuse)
│   │   ├── migrations/
│   │   │   └── 012_add_hourly_stats.sql    # NEW: Hourly aggregate tables
│   │   ├── queries/
│   │   │   ├── charts/
│   │   │   │   ├── park_shame_history.py   # MODIFY: Use hourly tables instead of GROUP BY
│   │   │   │   └── ride_downtime_history.py # MODIFY: Use hourly tables
│   │   │   ├── today/
│   │   │   │   ├── today_park_rankings.py  # MODIFY: Use hourly tables for TODAY
│   │   │   │   └── today_ride_rankings.py  # MODIFY: Use hourly tables
│   │   │   └── live/
│   │   │       └── live_park_rankings.py   # EXISTING: Already uses park_live_rankings cache
│   │   └── repositories/
│   │       └── stats_repository.py         # MODIFY: Add hourly stats queries
│   ├── scripts/
│   │   ├── aggregate_hourly.py             # NEW: Hourly aggregation job (runs every hour)
│   │   ├── aggregate_daily.py              # MODIFY: Read from hourly tables instead of snapshots
│   │   └── collect_snapshots.py            # EXISTING: No changes (continues 5-min collection)
│   └── utils/
│       ├── sql_helpers.py                  # EXISTING: Reuse RideStatusSQL, DowntimeSQL
│       └── metrics.py                      # EXISTING: Reuse SNAPSHOT_INTERVAL_MINUTES
└── tests/
    ├── unit/
    │   ├── test_hourly_aggregation.py      # NEW: Test aggregation logic
    │   └── test_shame_score_hourly.py      # NEW: Test hourly shame score consistency
    └── integration/
        └── test_hourly_chart_data.py       # NEW: Test chart queries with hourly data

frontend/
├── src/
│   ├── components/
│   │   └── park-details-modal.js          # EXISTING: No changes (API contract maintained)
│   └── pages/
│       └── rankings.html                  # EXISTING: No changes (API contract maintained)
└── tests/ (no changes needed)

deployment/
└── scripts/
    └── mirror-production-db.sh            # EXISTING: Use for local testing
```

**Structure Decision**: Web application structure with Flask backend + static frontend. Backend uses SQLAlchemy Core (no ORM models). This refactoring adds hourly aggregation tables and migrates chart/TODAY queries to use them. Frontend unchanged (API contract maintained).

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

*No violations - all constitutional principles aligned.*

