# Quickstart: Local Testing for Hourly Aggregation

**Feature**: 001-aggregation-tables
**Phase**: 1 (Design & Contracts)
**Date**: 2025-12-05

## Overview

This guide explains how to test the hourly aggregation refactoring locally before deploying to production. Following constitutional Principle IX (Production Integrity & Local-First Development), **ALL testing MUST happen locally with mirrored production data before any production deployment**.

## Prerequisites

- Python 3.11+
- MySQL/MariaDB running locally
- Local dev database configured (`themepark_tracker_dev`)
- SSH access to production server (for mirroring data)

##Step 1: Mirror Production Database

**MANDATORY**: Mirror production data to local dev environment before starting work.

```bash
# From repository root
cd deployment/scripts

# Option A: Mirror last 7 days (sufficient for hourly aggregation testing)
./mirror-production-db.sh --days=7

# Option B: Full mirror (if testing historical data or backfill)
./mirror-production-db.sh --full

# Verify mirror completed
mysql -u root themepark_tracker_dev -e "
SELECT
    'snapshots' as tbl,
    COUNT(*) as rows,
    MIN(recorded_at) as earliest,
    MAX(recorded_at) as latest
FROM park_activity_snapshots
UNION ALL
SELECT
    'daily_stats',
    COUNT(*),
    MIN(stat_date),
    MAX(stat_date)
FROM park_daily_stats;"
```

**Expected Output**:
```
+--------------+--------+---------------------+---------------------+
| tbl          | rows   | earliest            | latest              |
+--------------+--------+---------------------+---------------------+
| snapshots    | 161280 | 2025-11-28 08:00:00 | 2025-12-05 08:00:00 |
| daily_stats  | 560    | 2025-11-28          | 2025-12-04          |
+--------------+--------+---------------------+---------------------+
```

## Step 2: Run Database Migration

Apply the migration to add hourly stats tables:

```bash
cd backend

# Run migration
mysql -u root themepark_tracker_dev < src/database/migrations/012_add_hourly_stats.sql

# Verify tables created
mysql -u root themepark_tracker_dev -e "
SHOW TABLES LIKE '%hourly%';
DESCRIBE park_hourly_stats;
DESCRIBE ride_hourly_stats;"
```

**Expected Output**:
```
Tables_in_themepark_tracker_dev (%hourly%)
+-------------------+
| park_hourly_stats |
| ride_hourly_stats |
+-------------------+

park_hourly_stats:
+-------------------------+--------------+------+
| Field                   | Type         | Null |
+-------------------------+--------------+------+
| id                      | bigint       | NO   |
| park_id                 | int          | NO   |
| hour_start_utc          | datetime     | NO   |
| shame_score             | decimal(3,1) | YES  |
| avg_wait_time_minutes   | decimal(6,2) | YES  |
| ...                     | ...          | ...  |
+-------------------------+--------------+------+
```

## Step 3: Backfill Historical Hourly Data

Populate hourly stats tables with historical data from mirrored snapshots:

```bash
# Backfill last 7 days (process newest first)
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m scripts.backfill_hourly_stats --days=7

# Monitor progress (script logs to stdout)
# Expected: ~168 hours × 80 parks = 13,440 rows in park_hourly_stats
```

**Expected Output**:
```
2025-12-05 08:05:12 INFO Starting hourly backfill for last 7 days
2025-12-05 08:05:12 INFO Backfilling hours 2025-12-04 00:00:00 to 2025-12-05 00:00:00
2025-12-05 08:05:14 INFO Hour 2025-12-04 23:00:00: 80 parks, 4200 rides processed
2025-12-05 08:05:16 INFO Hour 2025-12-04 22:00:00: 80 parks, 4200 rides processed
...
2025-12-05 08:08:42 INFO Backfill complete: 13,440 park hours, 705,600 ride hours
```

**Verification**:
```bash
mysql -u root themepark_tracker_dev -e "
SELECT
    COUNT(*) as total_hours,
    MIN(hour_start_utc) as earliest_hour,
    MAX(hour_start_utc) as latest_hour,
    AVG(snapshot_count) as avg_snapshots_per_hour
FROM park_hourly_stats;"
```

**Expected**:
```
+-------------+---------------------+---------------------+------------------------+
| total_hours | earliest_hour       | latest_hour         | avg_snapshots_per_hour |
+-------------+---------------------+---------------------+------------------------+
|       13440 | 2025-11-28 08:00:00 | 2025-12-05 07:00:00 |                  11.8  |
+-------------+---------------------+---------------------+------------------------+
```

## Step 4: Test Chart Queries (TDD Approach)

**TDD Step 1 - RED**: Write failing tests that define expected behavior.

```bash
# Run existing chart tests (should pass with old GROUP BY implementation)
pytest tests/integration/test_chart_queries.py -v

# Run new hourly chart tests (should FAIL - not implemented yet)
pytest tests/integration/test_hourly_chart_data.py -v
```

**Expected**: New tests fail because query classes haven't been updated yet.

**TDD Step 2 - GREEN**: Implement new query classes using hourly tables.

```bash
# After implementing ParkShameHistoryQueryV2 (see data-model.md)
pytest tests/integration/test_hourly_chart_data.py -v

# Verify results match original GROUP BY approach
pytest tests/integration/test_chart_query_equivalence.py -v
```

**Expected**: Tests pass, confirming hourly queries return same results as GROUP BY.

**TDD Step 3 - REFACTOR**: Clean up code while keeping tests green.

## Step 5: Test Flask API Locally

Start local backend server and test endpoints:

```bash
# Terminal 1: Start Flask backend
cd backend
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m flask --app src.api.app run --port 5001

# Terminal 2: Test API endpoints
# Test TODAY rankings (should use hourly tables after refactoring)
curl -s "http://localhost:5001/api/parks/downtime?period=today&limit=10" | jq '.data[0]'

# Test park details with chart data
curl -s "http://localhost:5001/api/parks/196/details?period=today" | jq '.chart_data | length'

# Expected: 24 hourly data points (last 24 hours)
```

**Expected Response**:
```json
{
  "rank": 1,
  "park_id": 196,
  "park_name": "Six Flags Magic Mountain",
  "shame_score": 6.8,
  "total_downtime_hours": 12.5,
  "rides_operating": 48,
  "rides_down": 5,
  ...
}

24  # 24 hourly data points in chart_data array
```

## Step 6: Manual Browser Testing

**MANDATORY** (per CLAUDE.md): Manual verification required before marking task complete.

```bash
# Terminal 1: Backend already running (from Step 5)

# Terminal 2: Start frontend (if static files)
cd frontend
python3 -m http.server 8080

# Or if using live-server:
npx live-server --port=8080
```

**Browser Checklist**:

1. Open http://localhost:8080
2. Click "TODAY" tab → Rankings load < 1 second ✅
3. Click "YESTERDAY" tab → Rankings load < 1 second ✅
4. Click "Details" on Six Flags Magic Mountain
   - Shame score chart displays 24 hourly points ✅
   - Hover over data points shows correct values ✅
   - Chart renders smoothly (no lag) ✅
5. Switch between "TODAY", "YESTERDAY", "LAST WEEK" periods
   - Charts load instantly ✅
   - No JavaScript errors in console ✅
6. Verify shame scores match between Rankings table and Details modal ✅

**Do NOT mark task as complete until human explicitly confirms browser testing passed.**

## Step 7: Performance Benchmarking

Compare query performance before/after refactoring:

```bash
# Benchmark original GROUP BY query
mysql -u root themepark_tracker_dev -e "
SET profiling = 1;
SELECT
    HOUR(recorded_at) as hour,
    AVG(shame_score) as avg_shame_score
FROM park_activity_snapshots
WHERE park_id = 196
  AND recorded_at >= '2025-12-04 00:00:00'
  AND recorded_at < '2025-12-05 00:00:00'
GROUP BY HOUR(recorded_at);
SHOW PROFILE;
"

# Benchmark new hourly table query
mysql -u root themepark_tracker_dev -e "
SET profiling = 1;
SELECT
    hour_start_utc,
    shame_score
FROM park_hourly_stats
WHERE park_id = 196
  AND hour_start_utc >= '2025-12-04 00:00:00'
  AND hour_start_utc < '2025-12-05 00:00:00'
ORDER BY hour_start_utc;
SHOW PROFILE;
"
```

**Expected Performance**:
- Original GROUP BY: 3-7 seconds (scanning 1,440 snapshots)
- Hourly table: 0.01-0.05 seconds (indexed lookup on 24 rows)
- **Improvement**: 100-700x faster ✅

## Step 8: Aggregation Job Testing

Test hourly aggregation job runs correctly:

```bash
# Run aggregation for a specific hour (idempotent - safe to re-run)
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m scripts.aggregate_hourly --hour="2025-12-05 07:00:00"

# Verify aggregation completed successfully
mysql -u root themepark_tracker_dev -e "
SELECT * FROM aggregation_log
WHERE aggregation_type = 'hourly'
ORDER BY created_at DESC LIMIT 5;"
```

**Expected Output**:
```
+------+-----------------+---------------------+---------+-----------------+-----------------+-----------------------+
| id   | aggregation_type| target_period       | status  | parks_processed | rides_processed | processing_time_sec   |
+------+-----------------+---------------------+---------+-----------------+-----------------+-----------------------+
| 9876 | hourly          | 2025-12-05 07:00:00 | success |              80 |            4200 |                  8.3  |
+------+-----------------+---------------------+---------+-----------------+-----------------+-----------------------+
```

**Test Idempotency** (run same hour twice):
```bash
# Run twice - second run should update existing rows (ON DUPLICATE KEY UPDATE)
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m scripts.aggregate_hourly --hour="2025-12-05 07:00:00"
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m scripts.aggregate_hourly --hour="2025-12-05 07:00:00"

# Verify no duplicate rows created
mysql -u root themepark_tracker_dev -e "
SELECT COUNT(*) as rows_for_hour
FROM park_hourly_stats
WHERE hour_start_utc = '2025-12-05 07:00:00';"
```

**Expected**: 80 rows (one per park), not 160 ✅

## Step 9: Integration Test Suite

Run full test suite to verify no regressions:

```bash
cd backend

# Run all tests
pytest tests/ -v

# Run only aggregation-related tests
pytest tests/unit/test_hourly_aggregation.py -v
pytest tests/integration/test_hourly_chart_data.py -v
pytest tests/integration/test_today_chart_data.py -v

# Run constitution-critical tests (shame score consistency)
pytest tests/unit/test_shame_score_hourly.py -v
```

**Expected**: All tests pass (882+ tests, 0 failures) ✅

## Step 10: Ready for Production Deployment

After completing all steps above and receiving explicit human verification (Step 6), proceed to production deployment:

```bash
# Commit changes
git add .
git commit -m "feat(aggregation): implement hourly stats tables for chart performance

- Add hourly aggregation tables (park_hourly_stats, ride_hourly_stats)
- Migrate chart queries from GROUP BY HOUR to pre-aggregated tables
- Add backfill script for historical data
- Add aggregate_hourly.py job for continuous aggregation
- Performance: Chart queries improved from 5-10s to <100ms

Tested locally with mirrored production data:
- 882 tests pass
- Browser verification complete
- API contract maintained (no breaking changes)
- Constitutional principles validated (TDD, DRY, Production Integrity)

Closes #<issue>"

# Push to feature branch
git push origin 001-aggregation-tables

# Create PR (will be reviewed before merge)
gh pr create --title "Hourly aggregation tables for chart performance" \
  --body "See commit message for details. Ready for production deployment after review."
```

## Troubleshooting

### Issue: Backfill script fails with "No snapshots found"

**Cause**: Production data not mirrored or date range outside available data.

**Solution**:
```bash
# Check available snapshot date range
mysql -u root themepark_tracker_dev -e "
SELECT MIN(recorded_at), MAX(recorded_at)
FROM park_activity_snapshots;"

# Re-run mirror script with correct date range
./deployment/scripts/mirror-production-db.sh --days=7
```

### Issue: Chart queries return empty results

**Cause**: Hourly tables not populated or query filtering wrong date range.

**Solution**:
```bash
# Verify hourly data exists for query date range
mysql -u root themepark_tracker_dev -e "
SELECT COUNT(*), MIN(hour_start_utc), MAX(hour_start_utc)
FROM park_hourly_stats;"

# If empty, run backfill
DATABASE_NAME=themepark_tracker_dev PYTHONPATH=src python3 -m scripts.backfill_hourly_stats --days=7
```

### Issue: Shame scores don't match between old and new queries

**Cause**: Aggregation logic differs from ShameScoreCalculator.

**Solution**:
```bash
# Run equivalence test to find mismatch
pytest tests/integration/test_chart_query_equivalence.py -v -s

# Fix aggregation logic to use centralized calculator
# Re-run backfill after fixing
```

### Issue: Frontend charts not updating after hourly job runs

**Cause**: Cache not invalidated or aggregation job not completing.

**Solution**:
```bash
# Check aggregation_log for failures
mysql -u root themepark_tracker_dev -e "
SELECT * FROM aggregation_log
WHERE aggregation_type = 'hourly'
  AND status = 'failure'
ORDER BY created_at DESC LIMIT 10;"

# Clear query cache manually
# (cache auto-invalidates after 5 minutes)
```

## Summary

Following this quickstart ensures:
- ✅ Local testing with production data (Principle IX)
- ✅ TDD workflow (Principle VI)
- ✅ Human verification before deployment (CLAUDE.md mandatory)
- ✅ Performance benchmarks validated
- ✅ API contract maintained (frontend unchanged)
- ✅ Constitutional compliance verified

**Next Step**: After all testing passes and human verifies, proceed to code review and production deployment per deployment/scripts/deploy.sh.

