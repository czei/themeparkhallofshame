# Precalculated Table Views Implementation Plan

## Overview

Currently, API endpoints recalculate rankings and statistics on every request, even though the underlying data only changes every 10 minutes (when snapshots are collected). This plan introduces a caching layer that precalculates all table views after each data collection, reducing redundant computation and improving API response times.

## Problem Statement

```
Current Flow (Inefficient):
+----------+     +----------+     +----------+
|  User 1  |---->|   API    |---->| Database |
+----------+     | (calc)   |     | (query)  |
                 +----------+     +----------+
+----------+     +----------+     +----------+
|  User 2  |---->|   API    |---->| Database |
+----------+     | (calc)   |     | (query)  |  <- Same calculation repeated!
                 +----------+     +----------+
```

- Data updates every 10 minutes
- Each user request triggers full calculation
- Hundreds of identical calculations between updates

## Solution Architecture

```
New Flow (Efficient):
                                  +------------------+
+-------------------+             |  cached_api_views |
| collect_snapshots |------------>|  (precalculated) |
+-------------------+   refresh   +------------------+
                                          |
+----------+     +----------+             |
|  User 1  |---->|   API    |<------------+ (read only)
+----------+     +----------+             |
+----------+          |                   |
|  User 2  |----------+-------------------+
+----------+
```

- Cache refreshed once after each snapshot collection
- All users read from precalculated cache
- Fallback to live calculation if cache is stale

---

## Design Decisions

### Storage: Database Cache Tables (vs Redis/In-Memory)

**Chosen: Database Cache Tables**

| Criteria | Database | Redis/Memory |
|----------|----------|--------------|
| Persistence | Yes | No (lost on restart) |
| Multi-instance | Works | Needs shared Redis |
| Debugging | Queryable | Harder to inspect |
| Infrastructure | Already have MySQL | New dependency |
| Performance | Fast enough (<10ms reads) | Faster |

### Cache Freshness Strategy

- Cache considered fresh if < 15 minutes old
- Fallback to live calculation if stale
- Config flag to disable caching: `ENABLE_VIEW_CACHE=false`

---

## Database Schema

### Table: `cached_api_views`

```sql
CREATE TABLE cached_api_views (
    cache_id INT AUTO_INCREMENT PRIMARY KEY,
    cache_key VARCHAR(100) NOT NULL UNIQUE,
    view_type ENUM('park_rankings', 'ride_rankings', 'trends', 'status_summary', 'chart_data'),
    period ENUM('today', '7days', '30days', 'live'),
    filter_type VARCHAR(50),
    result_json LONGTEXT NOT NULL,
    record_count INT,
    generated_at DATETIME NOT NULL,
    generation_time_ms INT,
    INDEX idx_cache_key (cache_key),
    INDEX idx_generated_at (generated_at)
);
```

### Table: `cache_refresh_log`

```sql
CREATE TABLE cache_refresh_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    refresh_started_at DATETIME NOT NULL,
    refresh_completed_at DATETIME,
    status ENUM('running', 'success', 'partial', 'failed'),
    views_refreshed INT DEFAULT 0,
    views_failed INT DEFAULT 0,
    total_time_ms INT,
    error_message TEXT
);
```

### Cache Key Naming Convention

Format: `{entity}_{metric}_{period}_{filter}`

Examples:
- `park_downtime_today_disney-universal`
- `park_downtime_today_all-parks`
- `ride_waittimes_7days_all-parks`
- `trends_parks-improving_7days_disney-universal`
- `status_summary_live`
- `chart_park_shame_today_disney-universal`

---

## Cache Keys Inventory

| Category | Keys | Formula |
|----------|------|---------|
| Park downtime rankings | 6 | 3 periods x 2 filters |
| Park wait time rankings | 6 | 3 periods x 2 filters |
| Ride downtime rankings | 6 | 3 periods x 2 filters |
| Ride wait time rankings | 6 | 3 periods x 2 filters |
| Trends: parks improving | 6 | 3 periods x 2 filters |
| Trends: parks declining | 6 | 3 periods x 2 filters |
| Trends: rides improving | 6 | 3 periods x 2 filters |
| Trends: rides declining | 6 | 3 periods x 2 filters |
| Live status summary | 1 | Single global view |
| Chart: park shame history | 6 | 3 periods x 2 filters |
| Chart: ride downtime history | 6 | 3 periods x 2 filters |
| Chart: wait time history | 6 | 3 periods x 2 filters |
| **TOTAL** | **~67** | |

---

## Implementation Tasks

### Phase 1: Database Schema

```
[Task 1] ---> [Task 2] ---> [Task 3]
 Schema       Migration     Repository
```

#### Task 1: Create cache table schema
- **File:** `backend/src/database/schema/cache_tables.py`
- Define `CachedApiView` SQLAlchemy model
- Define `CacheRefreshLog` SQLAlchemy model
- Add appropriate indexes for cache_key lookups

#### Task 2: Create database migration
- **File:** `backend/src/database/migrations/add_cache_tables.sql`
- SQL script to create both tables
- Include indexes and constraints

#### Task 3: Create cache repository
- **File:** `backend/src/database/repositories/cache_repository.py`
- Methods:
  - `get_by_key(cache_key: str) -> CachedApiView`
  - `upsert(cache_key: str, result_json: str, metadata: dict)`
  - `delete_stale(max_age_minutes: int)`
  - `get_cache_stats() -> dict`

---

### Phase 2: Cache Refresh Service

```
[Task 4] ---> [Task 5] ---> [Task 6]
 Service       Script        Tests
```

#### Task 4: Create cache refresh service
- **File:** `backend/src/services/cache_refresh_service.py`
- Define `CACHE_DEFINITIONS` list with all ~67 cache configurations
- Implement `refresh_all_views()`:
  ```python
  def refresh_all_views(self) -> CacheRefreshLog:
      log = CacheRefreshLog(started_at=now(), status='running')
      for defn in CACHE_DEFINITIONS:
          try:
              result = defn['query_class'](self.db).execute(**defn['params'])
              self.cache_repo.upsert(defn['key'], json.dumps(result))
              log.views_refreshed += 1
          except Exception as e:
              log.views_failed += 1
              logger.error(f"Failed: {defn['key']}: {e}")
      log.status = 'success' if log.views_failed == 0 else 'partial'
      return log
  ```
- Implement `refresh_single_view(cache_key: str)` for targeted refresh

#### Task 5: Create refresh script
- **File:** `backend/src/scripts/refresh_cache.py`
- Standalone script callable from cron or manually
- Command line options:
  - `--all` - Refresh all views
  - `--key <cache_key>` - Refresh single view
  - `--if-stale` - Only refresh if cache is stale
- Add timing metrics and structured logging

#### Task 6: Add unit tests for cache service
- **File:** `backend/tests/services/test_cache_refresh_service.py`
- Test cases:
  - Successful refresh of all views
  - Partial failure handling
  - Single view refresh
  - Stale cache detection
  - Logging verification

---

### Phase 3: API Integration

```
[Task 7] ---> [Task 8] ---> [Task 9] ---> [Task 10]
 Helper        Parks         Rides         Trends
```

#### Task 7: Create cache helper decorator/function
- **File:** `backend/src/api/utils/cache_helper.py`
- Reusable function to check cache and fallback:
  ```python
  def get_cached_or_compute(cache_key: str, compute_fn: Callable,
                            max_age_minutes: int = 15) -> dict:
      cached = cache_repo.get_by_key(cache_key)
      if cached and cached.is_fresh(max_age_minutes):
          return json.loads(cached.result_json)
      # Fallback to live computation
      return compute_fn()
  ```
- Add response headers for cache status (X-Cache-Hit, X-Cache-Age)

#### Task 8: Update parks routes to use cache
- **File:** `backend/src/api/routes/parks.py`
- Modify endpoints:
  - `GET /api/parks/downtime`
  - `GET /api/parks/waittimes`
- Pattern:
  ```python
  @parks_bp.route('/downtime')
  def get_downtime():
      period = request.args.get('period', 'today')
      filter_type = request.args.get('filter', 'disney-universal')
      cache_key = f'park_downtime_{period}_{filter_type}'

      def compute():
          return ParkDowntimeRankings(db).execute(period=period, filter=filter_type)

      return jsonify(get_cached_or_compute(cache_key, compute))
  ```

#### Task 9: Update rides routes to use cache
- **File:** `backend/src/api/routes/rides.py`
- Modify endpoints:
  - `GET /api/rides/downtime`
  - `GET /api/rides/waittimes`

#### Task 10: Update trends routes to use cache
- **File:** `backend/src/api/routes/trends.py`
- Modify endpoints:
  - `GET /api/trends`
  - `GET /api/trends/chart-data`

---

### Phase 4: Integration & Deployment

```
[Task 11] ---> [Task 12] ---> [Task 13] ---> [Task 14]
 Snapshot       Monitor        Cron          Int Tests
```

#### Task 11: Integrate with snapshot collection
- **File:** `backend/src/scripts/collect_snapshots.py`
- After successful snapshot collection, trigger cache refresh:
  ```python
  def main():
      collector = SnapshotCollector()
      success = collector.collect()

      if success:
          cache_service = CacheRefreshService(db_session)
          refresh_log = cache_service.refresh_all_views()
          logger.info(f"Cache: {refresh_log.views_refreshed} refreshed, "
                      f"{refresh_log.views_failed} failed")
  ```

#### Task 12: Add monitoring endpoint
- **File:** `backend/src/api/routes/admin.py`
- New endpoint: `GET /api/admin/cache-status`
- Response:
  ```json
  {
    "cache_enabled": true,
    "total_views": 67,
    "fresh_views": 65,
    "stale_views": 2,
    "last_refresh": "2024-01-15T10:30:00Z",
    "last_refresh_duration_ms": 4500,
    "last_refresh_status": "success"
  }
  ```

#### Task 13: Update deployment configuration
- **File:** `deployment/config/crontab.prod`
- Add fallback cron job (runs if collection fails):
  ```bash
  # Fallback cache refresh - every 15 minutes, only if stale
  */15 * * * * cd /opt/themeparkhallofshame/backend && \
      python -m src.scripts.refresh_cache --if-stale >> logs/cache_refresh.log 2>&1
  ```

#### Task 14: Write integration tests
- **File:** `backend/tests/integration/test_cache_integration.py`
- Test full flow:
  1. Trigger snapshot collection
  2. Verify cache tables populated
  3. Call API endpoint
  4. Verify response comes from cache (check headers)
  5. Verify cache fallback when stale

---

### Phase 5: Documentation & Rollout

#### Task 15: Update documentation
- Update `CLAUDE.md` with new commands:
  ```
  python -m src.scripts.refresh_cache --all    # Refresh all cache
  python -m src.scripts.refresh_cache --if-stale  # Refresh only if stale
  ```
- Document cache key naming convention
- Add troubleshooting section for cache issues

---

## Dependency Graph

```
Phase 1                Phase 2              Phase 3              Phase 4           Phase 5
+-------+             +-------+            +-------+            +--------+         +-------+
|Task 1 |------------>|Task 4 |----------->|Task 7 |----------->|Task 11 |-------->|Task 15|
+-------+             +-------+            +-------+            +--------+         +-------+
    |                     |                    |                    |
    v                     v                    v                    v
+-------+             +-------+            +-------+            +--------+
|Task 2 |             |Task 5 |            |Task 8 |            |Task 12 |
+-------+             +-------+            +-------+            +--------+
    |                     |                    |                    |
    v                     v                    v                    v
+-------+             +-------+            +-------+            +--------+
|Task 3 |             |Task 6 |            |Task 9 |            |Task 13 |
+-------+             +-------+            +-------+            +--------+
                                               |                    |
                                               v                    v
                                           +-------+            +--------+
                                           |Task 10|            |Task 14 |
                                           +-------+            +--------+
```

---

## Rollback Plan

If issues arise after deployment:

1. **Immediate:** Set `ENABLE_VIEW_CACHE=false` in config
   - API routes fall back to live calculation automatically

2. **If cache causes errors:**
   ```sql
   TRUNCATE TABLE cached_api_views;
   TRUNCATE TABLE cache_refresh_log;
   ```

3. **Full rollback:** Revert API route changes
   - Cache tables can remain (no harm if unused)

---

## Success Metrics

| Metric | Before | Target |
|--------|--------|--------|
| Avg API response time | ~200ms | <50ms |
| Database queries/request | 3-5 | 1 |
| CPU usage during traffic | High | Minimal |
| Cache hit rate | N/A | >95% |

---

## Files Summary

### New Files
| File | Purpose |
|------|---------|
| `backend/src/database/schema/cache_tables.py` | SQLAlchemy models |
| `backend/src/database/migrations/add_cache_tables.sql` | Database migration |
| `backend/src/database/repositories/cache_repository.py` | Data access layer |
| `backend/src/services/cache_refresh_service.py` | Cache refresh logic |
| `backend/src/scripts/refresh_cache.py` | Standalone refresh script |
| `backend/src/api/utils/cache_helper.py` | API helper functions |
| `backend/src/api/routes/admin.py` | Monitoring endpoint |
| `backend/tests/services/test_cache_refresh_service.py` | Unit tests |
| `backend/tests/integration/test_cache_integration.py` | Integration tests |

### Modified Files
| File | Changes |
|------|---------|
| `backend/src/api/routes/parks.py` | Read from cache |
| `backend/src/api/routes/rides.py` | Read from cache |
| `backend/src/api/routes/trends.py` | Read from cache |
| `backend/src/scripts/collect_snapshots.py` | Trigger cache refresh |
| `deployment/config/crontab.prod` | Add fallback cron |
| `CLAUDE.md` | Document new commands |
