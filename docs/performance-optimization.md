# Performance Optimization Guide

This document describes the performance optimizations implemented in Theme Park Hall of Shame to ensure fast page loads and responsive UI.

## Overview

The app tracks ~3,000 rides across ~200 parks with snapshots every 5 minutes, generating millions of rows in the snapshot tables. Without optimization, "TODAY" queries that aggregate this data took 20-30 seconds. After optimization, responses are under 10ms for cached requests.

## Optimization Layers

### 1. Database Indexes (Migration 005)

**File**: `backend/src/database/migrations/005_performance_indexes.sql`

Covering indexes allow MySQL to satisfy queries entirely from the index without accessing the main table data.

```sql
-- Covering index for time-range aggregations on ride snapshots
CREATE INDEX idx_rss_time_range_covering
ON ride_status_snapshots (recorded_at, ride_id, computed_is_open, wait_time);

-- Covering index for park status joins
CREATE INDEX idx_pas_time_range_covering
ON park_activity_snapshots (recorded_at, park_id, park_appears_open);
```

**Why these columns in this order**:
- `recorded_at` first: Most selective for time-range WHERE clauses
- `ride_id`/`park_id`: GROUP BY column
- Remaining columns: Covered in SELECT/WHERE to avoid table access

**Impact**: Queries that previously did full table scans (20-30s) now use index scans (~3-7s for cold queries).

### 2. Query Result Caching

**File**: `backend/src/utils/cache.py`

In-memory cache with 5-minute TTL for API responses.

```python
from utils.cache import get_query_cache, generate_cache_key

cache = get_query_cache()
cache_key = generate_cache_key("parks_downtime", period="today", filter="all-parks")

# Check cache first
cached = cache.get(cache_key)
if cached is not None:
    return jsonify(cached), 200

# Compute and cache
result = expensive_query()
cache.set(cache_key, result)
```

**Cached Endpoints**:
| Endpoint | Periods Cached |
|----------|----------------|
| `/parks/downtime` | today, 7days, 30days |
| `/parks/waittimes` | today, 7days, 30days |
| `/rides/downtime` | today, 7days, 30days |
| `/rides/waittimes` | today, 7days, 30days |
| `/trends/longest-wait-times` | today, 7days, 30days |
| `/trends/least-reliable` | today, 7days, 30days |

**Not Cached**: `period=live` queries (need real-time data)

**Cache Key Generation**:
```python
# Keys are deterministic - same params always produce same key
generate_cache_key("parks_downtime", period="today", filter="all-parks")
# Returns: "parks_downtime:a1b2c3d4"
```

**Impact**:
- First request: 3-7 seconds (database query)
- Subsequent requests: ~8 milliseconds (cache hit)

### 3. InnoDB Buffer Pool

**Production Config**: `/etc/my.cnf.d/mariadb-server.cnf`

```ini
[mysqld]
innodb_buffer_pool_size = 512M
```

The buffer pool caches table and index data in memory. Increased from 128MB to 512MB to fit more of the frequently-accessed snapshot tables.

**Verify current setting**:
```sql
SHOW VARIABLES LIKE 'innodb_buffer_pool_size';
```

### 4. Query Refactoring (CTEs vs Correlated Subqueries)

**File**: `backend/src/database/queries/today/today_ride_wait_times.py`

Replaced correlated subqueries with Common Table Expressions (CTEs) that run once.

**Before** (correlated subquery - runs per row):
```sql
SELECT
    r.name,
    (SELECT wait_time FROM ride_status_snapshots
     WHERE ride_id = r.ride_id
     ORDER BY recorded_at DESC LIMIT 1) as current_wait
FROM rides r
```

**After** (CTE - runs once):
```sql
WITH latest_snapshots AS (
    SELECT ride_id, wait_time as current_wait_time
    FROM ride_status_snapshots rss
    INNER JOIN (
        SELECT ride_id, MAX(recorded_at) as max_recorded_at
        FROM ride_status_snapshots
        GROUP BY ride_id
    ) latest ON rss.ride_id = latest.ride_id
        AND rss.recorded_at = latest.max_recorded_at
)
SELECT r.name, ls.current_wait_time
FROM rides r
LEFT JOIN latest_snapshots ls ON r.ride_id = ls.ride_id
```

**Impact**: Reduced per-row subquery execution from O(n) to O(1).

### 5. Frontend Serialization (Awards)

**File**: `frontend/js/components/awards.js`

Changed Awards component from parallel to sequential API calls to avoid overwhelming the server on cold cache.

**Before**:
```javascript
// 4 simultaneous requests can timeout on cold cache
const [waitPark, waitRide, reliablePark, reliableRide] = await Promise.all([
    this.apiClient.get('/trends/longest-wait-times', {...}),
    this.apiClient.get('/trends/longest-wait-times', {...}),
    this.apiClient.get('/trends/least-reliable', {...}),
    this.apiClient.get('/trends/least-reliable', {...})
]);
```

**After**:
```javascript
// Sequential calls - each caches result for subsequent requests
const waitPark = await this.apiClient.get('/trends/longest-wait-times', {...});
const waitRide = await this.apiClient.get('/trends/longest-wait-times', {...});
const reliablePark = await this.apiClient.get('/trends/least-reliable', {...});
const reliableRide = await this.apiClient.get('/trends/least-reliable', {...});
```

## Performance Summary

| Query Type | Before | After (Cold) | After (Cached) |
|------------|--------|--------------|----------------|
| TODAY rankings | 20-30s | 3-7s | ~8ms |
| 7-day rankings | 2-5s | 2-5s | ~8ms |
| 30-day rankings | 2-5s | 2-5s | ~8ms |
| LIVE rankings | <1s | <1s | N/A (not cached) |
| Awards page | 90-120s | 15-30s | ~40ms |

## Gunicorn Worker Considerations

The app runs with 2 gunicorn workers, each with its own in-memory cache. This means:

1. First request to worker 1: Cache miss, queries database, caches result
2. First request to worker 2: Cache miss, queries database, caches result
3. Subsequent requests: Cache hits (either worker)

After both workers' caches are primed (typically after first few page loads), all requests are instant.

For true shared caching, consider Redis in the future.

## Monitoring

### Check Cache Statistics
```python
from utils.cache import get_query_cache
cache = get_query_cache()
print(cache.get_stats())
# {"total_entries": 12, "valid_entries": 10, "ttl_seconds": 300}
```

### Check Index Usage
```sql
EXPLAIN SELECT ... FROM ride_status_snapshots ...
-- Look for "Using index" in Extra column (covering index)
-- Look for idx_rss_time_range_covering in key column
```

### Verify Buffer Pool
```sql
SHOW ENGINE INNODB STATUS;
-- Look for "Buffer pool hit rate" - should be > 99%
```

## Future Optimizations

1. **Redis Cache**: Shared cache across workers for consistent cache hits
2. **Pre-aggregation Jobs**: Nightly jobs to pre-compute TODAY stats at midnight
3. **Query Result Materialized Views**: For complex aggregations
4. **Connection Pooling**: If connection overhead becomes significant

## Files Modified

| File | Change |
|------|--------|
| `src/utils/cache.py` | Query cache implementation |
| `src/api/routes/parks.py` | Added caching to downtime/waittimes |
| `src/api/routes/rides.py` | Added caching to downtime/waittimes |
| `src/api/routes/trends.py` | Added caching to Awards endpoints |
| `src/database/queries/today/today_ride_wait_times.py` | CTEs instead of subqueries |
| `src/database/migrations/005_performance_indexes.sql` | Covering indexes |
| `frontend/js/components/awards.js` | Sequential API calls |

## Testing

```bash
# Run cache unit tests
PYTHONPATH=src pytest tests/unit/test_query_cache.py -v

# Run index coverage tests (requires database)
PYTHONPATH=src pytest tests/unit/test_index_coverage.py -v
```
