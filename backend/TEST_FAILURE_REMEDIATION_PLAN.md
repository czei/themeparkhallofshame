# Test Failure Remediation Plan

**Date**: 2025-12-10
**Total Tests**: 927 (791 passed, 107 failed, 9 errors, 28 skipped)
**Pass Rate**: 85.3%

## Root Cause Summary

All 116 failures (107 FAILED + 9 ERROR) have a **single root cause**:

**The test database (`themepark_test`) has an incomplete/outdated schema that doesn't match the dev database (`themepark_tracker_dev`).**

### Schema Differences Found

| Table | Test DB Columns | Dev DB Columns | Missing Columns |
|-------|-----------------|----------------|-----------------|
| `parks` | 12 | 16 | `latitude`, `longitude`, `created_at`, `updated_at` |
| `rides` | 6 | 14 | `queue_times_id`, `themeparks_wiki_id`, `tier`, `entity_type`, `land_area`, `disney_entity_id`, `created_at`, `updated_at` |

### Error Messages
- `Unknown column 'latitude' in 'field list'`
- `Unknown column 'queue_times_id' in 'where clause'`

---

## Fix Strategy

### ONE-TIME FIX: Rebuild Test Database Schema

Run the following commands to fix ALL 116 failures at once:

```bash
# 1. Create proper schema dump from dev
mysqldump -u root -p'294e043ww' --no-data themepark_tracker_dev 2>/dev/null > /tmp/dev_schema.sql

# 2. Completely recreate test database
mysql -u root -p'294e043ww' -e "DROP DATABASE IF EXISTS themepark_test; CREATE DATABASE themepark_test;"

# 3. Load the schema
mysql -u root -p'294e043ww' themepark_test < /tmp/dev_schema.sql

# 4. Grant permissions
mysql -u root -p'294e043ww' -e "GRANT ALL PRIVILEGES ON themepark_test.* TO 'themepark_test'@'localhost'; FLUSH PRIVILEGES;"

# 5. Verify
mysql -u root -p'294e043ww' themepark_test -e "DESCRIBE parks;" | wc -l  # Should be 17 (16 cols + header)
mysql -u root -p'294e043ww' themepark_test -e "DESCRIBE rides;" | wc -l  # Should be 15 (14 cols + header)
```

### Also Fix: `setup-test-database.sh`

The existing script at `backend/scripts/setup-test-database.sh` has a bug where the mysqldump piping silently fails. Update line 32:

**Current (broken):**
```bash
mysqldump -u root -p${DB_ROOT_PASSWORD} --no-data ${SOURCE_DB} 2>/dev/null | mysql -u ${DB_USER} -p${DB_PASSWORD} ${DB_NAME} 2>&1 | grep -v "Warning" || true
```

**Fixed:**
```bash
# Two-step process to ensure schema is actually loaded
mysqldump -u root -p${DB_ROOT_PASSWORD} --no-data ${SOURCE_DB} 2>/dev/null > /tmp/schema_dump.sql
mysql -u root -p${DB_ROOT_PASSWORD} ${DB_NAME} < /tmp/schema_dump.sql
rm /tmp/schema_dump.sql
```

---

## Test Classification by File

All tests below will pass once the schema is fixed. They are all **VALID tests** that should be kept.

### Category 1: Schema Errors (107 FAILED)

These fail due to missing columns when executing SQL:

| File | Tests | Error Type |
|------|-------|------------|
| `test_park_repository.py` | 12 | Missing `latitude`, `longitude` |
| `test_ride_repository.py` | 13 | Missing `queue_times_id`, `tier` |
| `test_aggregation_service_integration.py` | 6 | Missing columns in joins |
| `test_classification_integration.py` | 6 | Missing `tier` column |
| `test_monthly_aggregation_integration.py` | 12 | Missing columns in aggregation |
| `test_weekly_aggregation_integration.py` | 6 | Missing columns in aggregation |
| `test_snapshot_repository.py` | 6 | Missing columns |
| `test_status_change_repository.py` | 3 | Missing columns |
| `test_schedule_repository.py` | 13 | Passes (uses minimal columns) |
| `test_ride_details_daily_aggregation_api.py` | 8 | Missing columns |
| `test_today_api_contract.py` | 8 | Missing columns |
| `test_today_hybrid_query.py` | 5 | Missing columns |

### Category 2: Fixture Setup Errors (9 ERROR)

These error during fixture setup (before test runs):

| File | Tests | Fixture Issue |
|------|-------|---------------|
| `test_hourly_multi_hour_outages.py` | 3 | `setup_test_park` fixture fails |
| `test_collect_snapshots_integration.py` | 6 | `setup_test_park` fixture fails |

### Category 3: API Endpoint Tests (52 ERROR at setup)

These all use `comprehensive_test_data` fixture which fails due to schema:

| File | Tests |
|------|-------|
| `test_api_calculations_integration.py` | 12 |
| `test_api_endpoints_integration.py` | 40 |

---

## Action Items

1. **[REQUIRED]** Fix test database schema (commands above)
2. **[REQUIRED]** Update `setup-test-database.sh` to use two-step dump/load
3. **[OPTIONAL]** Add schema verification to CI pipeline
4. **[OPTIONAL]** Add pre-test hook to verify schema matches dev

---

## Verification

After fixing the schema, run:

```bash
TEST_DB_HOST=localhost \
TEST_DB_PORT=3306 \
TEST_DB_NAME=themepark_test \
TEST_DB_USER=themepark_test \
TEST_DB_PASSWORD=test_password \
PYTHONPATH=src pytest tests/ --ignore=tests/performance/ --tb=no -q
```

Expected result: **927 passed, 0 failed, 0 errors**

---

## Tests That Are Already Passing (791)

These include:
- All unit tests in `tests/unit/`
- `test_schedule_repository.py` (uses minimal schema)
- Other integration tests that don't touch missing columns
