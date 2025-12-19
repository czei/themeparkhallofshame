# Testing Plan: Cross-Period Metric Consistency (No Mocks)

## Goal
Catch mismatches where metrics (shame score, rides down/affected, avg wait time, etc.) show different values across reports/endpoints for the same period (LIVE, TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH). All tests run against MySQL (no new mocks). DB should be refreshed before the suite when needed.

## Current Gaps
- Period consistency is only spot-checked; no contract that rankings/details/heatmaps align.
- Cross-period drift not exercised on the same data slice.
- Weekly/monthly/yearly stats tables are empty and unvalidated in prod/mirror.
- Live caches vs snapshots/stats not reconciled.
- Weather observations not asserted end-to-end.

## Data Strategy (No Mocks)
- Use MySQL fixtures/seeds (small, deterministic) that cover:
  - Multi-day snapshots per park/ride (open/closed/down) spanning day/week/month boundaries.
  - At least one status-change sequence (open → down → open) for a ride.
  - >=2 weeks of data to populate weekly/monthly rollups.
  - Weather observations for a few parks.
- Optional: `run-all-tests.sh` respects `REFRESH_TEST_DB=1` to refresh/mirror/seed before running tests.

## New Integration Tests to Add

### 1) Period Consistency Matrix (`tests/integration/test_metric_consistency_by_period.py`)
- For each period: LIVE, TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH:
  - Fetch via API: park rankings, ride rankings, park details, ride details, heatmap (parks/rides).
  - Fetch via SQL: corresponding stats tables (daily/weekly/monthly or latest snapshots for LIVE).
  - Assert equality (or tight tolerance) for shame_score, affected/ rides_down, avg_wait_time, total_rides_tracked.
  - Fail on any mismatch.

### 2) Cross-Period Sanity (`tests/integration/test_metric_consistency_by_period.py`)
- For 2–3 parks spanning >30 days, assert reasonable relationships (e.g., last_month downtime ≥ last_week unless filtered; LIVE differs from TODAY when park is closed).

### 3) Live Cache vs Snapshots (`tests/integration/test_live_cache_consistency.py`)
- Recompute live metrics from latest ride_status_snapshots/park_activity_snapshots.
- Compare to park_live_rankings*/ride_live_rankings* and LIVE API responses.
- Catch stale or missing cache rows.

### 4) Weekly/Monthly Rollups (`tests/integration/test_weekly_monthly_aggregation.py`)
- Seed a small multi-day dataset.
- Run aggregation entrypoints (hourly → daily → weekly/monthly).
- Assert weekly/monthly/yearly stats match rolled-up daily stats; idempotent on rerun (no dupes).

### 5) Status Change Flow (`tests/integration/test_status_change_flow.py`)
- Run collector twice (open→down) against seeded data.
- Assert ride_status_changes populated and daily stats reflect counts/longest_downtime.
- Cross-check against API details/rankings for that day.

### 6) Boundary Dates (`tests/integration/test_metric_consistency_by_period.py`)
- Seed snapshots across PST midnight; assert TODAY vs YESTERDAY splits are correct for shame_score/rides_down/avg_wait.
- Do the same for week/month boundaries.

### 7) Weather Observations (`tests/integration/test_weather_observations_end_to_end.py`)
- With real weather_observations in DB, assert counts and sampled rows match what collector would produce; ensure availability doesn’t differ across periods.

## Hard Fail Criteria
- Any metric mismatch between API and SQL for the same period.
- Any critical table empty locally when prod has rows (mirror audit already enforces for weather/snapshots/stats).
- Rollup tables diverge from summed daily stats after aggregation run.

## Execution Notes
- No new mocks; all tests hit MySQL.
- Keep seeds small to maintain speed; use transactions/cleanup in fixtures.
- Prefer `PYTHONPATH=src pytest tests/integration/... --no-cov` for targeted runs; full suite via `run-all-tests.sh` after optional `REFRESH_TEST_DB=1` refresh.

