# Production Migration Fixes - 2025-12-08

## Critical Issues Fixed

Based on Zen AI code review, the following critical issues were identified and fixed before production deployment:

### Issue #1: Migration 013 Not Idempotent ✅ FIXED
**Severity**: HIGH - Would cause FATAL ERROR on re-run
**Location**: `src/database/migrations/013_add_hourly_stats.sql`

**Problem**:
- Used `CREATE TABLE` without `IF NOT EXISTS`
- Included `SHOW INDEX` statements in transaction (not production-safe)

**Fix Applied**:
```sql
-- Changed from:
CREATE TABLE park_hourly_stats (

-- To:
CREATE TABLE IF NOT EXISTS park_hourly_stats (
```

Also removed `SHOW INDEX` statements and moved to comment for manual verification.

**Impact**: Migration can now be run multiple times safely (idempotent).

---

### Issue #2: aggregation_type Enum Mismatch ✅ FIXED
**Severity**: HIGH - Would cause RUNTIME ERROR
**Location**: `src/scripts/aggregate_hourly.py:169`

**Problem**:
- Script inserted `'hourly_14'` (for 2PM)
- Enum only accepts `'hourly'`
- Would fail at runtime when cron job runs

**Fix Applied**:
```python
# Changed from:
aggregation_type = f"hourly_{self.target_hour.hour:02d}"

# To:
aggregation_type = "hourly"
```

**Impact**: Script now matches migration 014 enum definition exactly.

---

### Issue #3: UPSERT Pattern Hides History ✅ FIXED
**Severity**: MEDIUM - Would prevent monitoring individual hours
**Location**: `src/scripts/aggregate_hourly.py:188`

**Problem**:
- Used `ON DUPLICATE KEY UPDATE`
- Overwrote previous hourly runs on same day
- Couldn't track which specific hours succeeded/failed

**Fix Applied**:
```python
# Removed ON DUPLICATE KEY UPDATE
# Added timestamp range tracking:
INSERT INTO aggregation_log (
    aggregation_date,
    aggregation_type,
    aggregated_from_ts,   # NEW: Hour start
    aggregated_until_ts,  # NEW: Hour end
    started_at,
    status,
    parks_processed,
    rides_processed
) VALUES (...)
```

**Impact**: Each hourly run now gets its own log entry, enabling proper monitoring.

---

## Verification Steps

Before deploying to production:

1. **Test Migration Idempotency**:
   ```bash
   # Run migration twice on test database
   mysql -u root -p themepark_tracker_test < src/database/migrations/013_add_hourly_stats.sql
   mysql -u root -p themepark_tracker_test < src/database/migrations/013_add_hourly_stats.sql
   # Should succeed both times (no error)
   ```

2. **Test Hourly Aggregation**:
   ```bash
   # Run hourly script on test database
   PYTHONPATH=src python3 -m scripts.aggregate_hourly --hour 2025-12-08-14
   # Should create entry in aggregation_log with aggregation_type='hourly'
   ```

3. **Verify Schema**:
   ```sql
   -- Check tables created
   SHOW TABLES LIKE '%hourly%';

   -- Check aggregation_log has timestamp columns
   DESCRIBE aggregation_log;
   ```

---

## Remaining Issues (Non-Blocking)

These issues were identified but are not blocking production deployment:

- **Medium**: avg_wait_time_minutes reference in aggregate_hourly.py:438 (already fixed in stats_repository.py)
- **Low**: Rollback documentation could be more prescriptive

---

## Production Deployment Checklist

- [x] Migration 013 made idempotent (IF NOT EXISTS)
- [x] SHOW INDEX statements removed from migration
- [x] aggregation_type fixed to use 'hourly' enum value
- [x] UPSERT removed, plain INSERT with timestamps
- [ ] Test migrations on mirrored production database
- [ ] Verify hourly aggregation script runs successfully
- [ ] Deploy to production
- [ ] Monitor first hourly run

---

---

### Issue #4: UNIQUE Constraint Blocks Multiple Hourly Entries ✅ FIXED
**Severity**: CRITICAL - Would cause PRIMARY KEY VIOLATION
**Location**: `src/database/migrations/003_aggregates_tables.sql:19`

**Problem**:
- Table has `UNIQUE KEY unique_aggregation (aggregation_date, aggregation_type)`
- Prevents multiple hourly entries per day
- First hourly run succeeds, second hourly run FAILS with duplicate key error

**Fix Applied**:
Created new migration `015_relax_aggregation_log_unique_constraint.sql`:
```sql
ALTER TABLE aggregation_log
DROP INDEX unique_aggregation;

CREATE INDEX idx_aggregation_date_type
ON aggregation_log(aggregation_date, aggregation_type);

CREATE INDEX idx_aggregated_until_ts
ON aggregation_log(aggregated_until_ts);
```

**Impact**: Multiple hourly aggregations per day now supported.

---

## Migration Execution Order

**CRITICAL**: Migrations must be run in this exact order:

1. `013_add_hourly_stats.sql` - Creates park_hourly_stats and ride_hourly_stats tables
2. `014_add_hourly_aggregation_type.sql` - Adds 'hourly' to aggregation_type enum
3. `015_relax_aggregation_log_unique_constraint.sql` - Drops unique constraint for hourly entries

---

## Files Modified

1. `src/database/migrations/013_add_hourly_stats.sql` - Added IF NOT EXISTS, removed SHOW INDEX
2. `src/database/migrations/014_add_hourly_aggregation_type.sql` - No changes (already correct)
3. `src/database/migrations/015_relax_aggregation_log_unique_constraint.sql` - NEW migration created
4. `src/scripts/aggregate_hourly.py` - Fixed aggregation_type, removed UPSERT

---

*Generated: 2025-12-08 by Zen AI Code Review*
