# Quickstart Guide: Weather Data Collection

**Feature**: 002-weather-collection
**Date**: 2025-12-17
**Audience**: Developers setting up local weather collection system

## Prerequisites

- Python 3.11+
- MySQL/MariaDB 8.0+ (local dev database)
- Production database mirror (via `deployment/scripts/mirror-production-db.sh`)
- Virtual environment activated

## Setup Steps

### 1. Database Migration

Run the weather schema migration:

```bash
# Navigate to backend directory
cd backend

# Apply migration to local dev database
mysql -h localhost -u themepark_dev -p themepark_dev < src/database/migrations/018_weather_schema.sql

# Verify tables created
mysql -h localhost -u themepark_dev -p -e "SHOW TABLES LIKE 'weather_%';" themepark_dev
# Expected output:
# +--------------------------------+
# | Tables_in_themepark_dev (weather_%) |
# +--------------------------------+
# | weather_forecasts              |
# | weather_observations           |
# +--------------------------------+
```

**Rollback** (if needed):
```bash
mysql -h localhost -u themepark_dev -p themepark_dev -e "DROP TABLE IF EXISTS weather_forecasts; DROP TABLE IF EXISTS weather_observations;"
```

### 2. Install Python Dependencies

Add new dependencies to `requirements.txt` (if not already present):

```text
# requirements.txt
requests>=2.31.0
tenacity>=8.2.3
```

Install:
```bash
pip install -r requirements.txt
```

### 3. Verify Parks Have Coordinates

Check that parks table has latitude/longitude data:

```bash
mysql -h localhost -u themepark_dev -p themepark_dev -e "
SELECT park_id, name, latitude, longitude, timezone
FROM parks
WHERE is_active = TRUE
LIMIT 5;
"
```

**Expected output** (example):
```
+---------+---------------------+-----------+------------+------------------+
| park_id | name                | latitude  | longitude  | timezone         |
+---------+---------------------+-----------+------------+------------------+
|       1 | Magic Kingdom       |  28.41777 |  -81.58116 | America/New_York |
|       2 | EPCOT               |  28.37444 |  -81.54937 | America/New_York |
|       3 | Hollywood Studios   |  28.35750 |  -81.55895 | America/New_York |
|       4 | Animal Kingdom      |  28.35876 |  -81.59059 | America/New_York |
|       5 | Universal Studios   |  28.47944 |  -81.46855 | America/New_York |
+---------+---------------------+-----------+------------+------------------+
```

**If coordinates are missing**, populate from external source (not in scope for this quickstart).

### 4. Test Open-Meteo API Connection

Quick test to ensure Open-Meteo API is accessible:

```bash
# Test API call for Magic Kingdom coordinates
curl "https://api.open-meteo.com/v1/forecast?latitude=28.41777&longitude=-81.58116&hourly=temperature_2m,weather_code&temperature_unit=fahrenheit&timezone=UTC&forecast_days=1"
```

**Expected output** (truncated):
```json
{
  "latitude": 28.416
}
  "longitude": -81.58125,
  "hourly": {
    "time": ["2025-12-17T00:00", "2025-12-17T01:00", ...],
    "temperature_2m": [72.5, 71.8, ...],
    "weather_code": [0, 0, ...]
  }
}
```

**If connection fails**, check:
- Internet connectivity
- Firewall rules (HTTPS outbound traffic allowed)
- DNS resolution for `api.open-meteo.com`

### 5. Run Weather Collection Script (Manual Test)

First manual test of weather collection:

```bash
# From backend/ directory
PYTHONPATH=src python3 src/scripts/collect_weather.py --test
```

**Expected output**:
```
2025-12-17 12:00:00 [INFO] Starting weather collection...
2025-12-17 12:00:00 [INFO] Found 150 parks with coordinates
2025-12-17 12:00:01 [INFO] Collected weather for Magic Kingdom (28.41777, -81.58116)
2025-12-17 12:00:02 [INFO] Collected weather for EPCOT (28.37444, -81.54937)
...
2025-12-17 12:02:30 [INFO] Collection complete: 150 successful, 0 failed
2025-12-17 12:02:30 [INFO] Inserted 150 observations
```

**If errors occur**:
- Check database connection (credentials in environment variables)
- Verify `parks` table has `latitude`, `longitude`, `timezone` columns
- Check API rate limiting (should see 1 req/sec pacing)

### 6. Verify Data Inserted

Check that weather observations were inserted:

```bash
mysql -h localhost -u themepark_dev -p themepark_dev -e "
SELECT
    wo.observation_id,
    p.name AS park_name,
    wo.observation_time,
    wo.temperature_f,
    wo.weather_code,
    wo.collected_at
FROM weather_observations wo
INNER JOIN parks p ON wo.park_id = p.park_id
ORDER BY wo.collected_at DESC
LIMIT 5;
"
```

**Expected output** (example):
```
+----------------+---------------------+---------------------+--------------+--------------+---------------------+
| observation_id | park_name           | observation_time    | temperature_f | weather_code | collected_at        |
+----------------+---------------------+---------------------+--------------+--------------+---------------------+
|            745 | Universal Studios   | 2025-12-17 12:00:00 |         73.2 |            0 | 2025-12-17 12:02:28 |
|            744 | Animal Kingdom      | 2025-12-17 12:00:00 |         74.1 |            0 | 2025-12-17 12:02:27 |
|            743 | Hollywood Studios   | 2025-12-17 12:00:00 |         72.8 |            0 | 2025-12-17 12:02:26 |
|            742 | EPCOT               | 2025-12-17 12:00:00 |         71.9 |            0 | 2025-12-17 12:00:02 |
|            741 | Magic Kingdom       | 2025-12-17 12:00:00 |         72.5 |            0 | 2025-12-17 12:00:01 |
+----------------+---------------------+---------------------+--------------+--------------+---------------------+
```

### 7. Run Tests

Run unit tests for weather collection:

```bash
# From backend/ directory
pytest tests/unit/test_openmeteo_client.py -v
pytest tests/unit/test_weather_repository.py -v
pytest tests/unit/test_token_bucket.py -v
```

**Expected output**:
```
tests/unit/test_openmeteo_client.py::test_fetch_weather_success PASSED
tests/unit/test_openmeteo_client.py::test_fetch_weather_timeout PASSED
tests/unit/test_openmeteo_client.py::test_fetch_weather_invalid_coordinates PASSED
...
===================== 15 passed in 2.34s =====================
```

Run integration tests (requires test database):

```bash
pytest tests/integration/test_weather_collection.py -v
```

**Expected output**:
```
tests/integration/test_weather_collection.py::test_collect_weather_for_park PASSED
tests/integration/test_weather_collection.py::test_idempotent_insert PASSED
tests/integration/test_weather_collection.py::test_cleanup_old_observations PASSED
...
===================== 8 passed in 5.12s =====================
```

### 8. Schedule Collection (Cron)

Add hourly weather collection to crontab:

```bash
# Edit crontab
crontab -e

# Add these lines:
# Hourly current weather collection (every hour at :00)
0 * * * * cd /path/to/backend && PYTHONPATH=src python3 src/scripts/collect_weather.py --current 2>&1 | tee -a /var/log/weather_collection.log

# 6-hourly forecast collection (00:00, 06:00, 12:00, 18:00 UTC)
0 */6 * * * cd /path/to/backend && PYTHONPATH=src python3 src/scripts/collect_weather.py --forecast 2>&1 | tee -a /var/log/weather_collection.log

# Daily cleanup (04:00 UTC)
0 4 * * * cd /path/to/backend && PYTHONPATH=src python3 src/scripts/cleanup_weather.py 2>&1 | tee -a /var/log/weather_cleanup.log
```

**Verify cron schedule**:
```bash
crontab -l | grep weather
```

**Wrap with cron_wrapper.py** (for failure alerting):
```bash
# Modify crontab to use wrapper
0 * * * * cd /path/to/backend && python3 src/utils/cron_wrapper.py "Weather Collection" "PYTHONPATH=src python3 src/scripts/collect_weather.py --current"
```

### 9. Monitor Collection

Check logs for collection success:

```bash
# View recent collection log
tail -f /var/log/weather_collection.log

# Check for errors
grep ERROR /var/log/weather_collection.log
```

**Healthy log output**:
```
2025-12-17 13:00:00 [INFO] Starting hourly weather collection...
2025-12-17 13:00:00 [INFO] Found 150 parks with coordinates
2025-12-17 13:02:30 [INFO] Collection complete: 150 successful, 0 failed
2025-12-17 13:02:30 [INFO] Inserted 150 observations
```

**Error indicators**:
- `[ERROR] Failed to fetch weather for park_id=X`: API timeout or invalid coordinates
- `[ERROR] Database connection failed`: Check MySQL credentials
- `[ERROR] Rate limit exceeded`: Token bucket implementation issue

### 10. Query Weather Data

Example queries for development/debugging:

**Get latest weather for all parks**:
```sql
SELECT
    p.name,
    wo.observation_time,
    wo.temperature_f,
    wo.wind_speed_mph,
    wo.precipitation_mm,
    CASE
        WHEN wo.weather_code IN (95, 96, 99) THEN 'THUNDERSTORM'
        WHEN wo.weather_code >= 61 THEN 'RAIN'
        ELSE 'CLEAR'
    END AS conditions
FROM weather_observations wo
INNER JOIN parks p ON wo.park_id = p.park_id
WHERE wo.observation_time = (
    SELECT MAX(observation_time)
    FROM weather_observations
)
ORDER BY p.name;
```

**Find parks with thunderstorms in last 24 hours**:
```sql
SELECT
    p.name,
    wo.observation_time,
    wo.weather_code
FROM weather_observations wo
INNER JOIN parks p ON wo.park_id = p.park_id
WHERE wo.weather_code IN (95, 96, 99)
  AND wo.observation_time >= NOW() - INTERVAL 24 HOUR
ORDER BY wo.observation_time DESC;
```

**Check forecast accuracy** (compare forecasted vs actual):
```sql
SELECT
    p.name,
    f.forecast_time,
    f.temperature_f AS forecasted_temp,
    o.temperature_f AS actual_temp,
    ABS(f.temperature_f - o.temperature_f) AS error_degrees
FROM weather_forecasts f
INNER JOIN weather_observations o
    ON f.park_id = o.park_id
    AND f.forecast_time = o.observation_time
INNER JOIN parks p ON f.park_id = p.park_id
WHERE f.issued_at >= NOW() - INTERVAL 7 DAY
ORDER BY error_degrees DESC
LIMIT 10;
```

## Troubleshooting

### Issue: "No module named 'tenacity'"

**Solution**: Install missing dependency
```bash
pip install tenacity
```

### Issue: "Table 'weather_observations' doesn't exist"

**Solution**: Run migration
```bash
mysql -h localhost -u themepark_dev -p themepark_dev < src/database/migrations/018_weather_schema.sql
```

### Issue: "API timeout after 30 seconds"

**Cause**: Open-Meteo API slow response

**Solution**: Tenacity will retry automatically (exponential backoff). Check API status at https://status.open-meteo.com

### Issue: "Foreign key constraint fails (park_id=999)"

**Cause**: Park doesn't exist in `parks` table

**Solution**: Verify park exists:
```bash
mysql -h localhost -u themepark_dev -p themepark_dev -e "SELECT * FROM parks WHERE park_id = 999;"
```

### Issue: "Rate limit: Too many requests"

**Cause**: Token bucket rate limiter not working correctly

**Solution**: Check `TokenBucket` implementation in `src/utils/rate_limiter.py`:
- Verify `time.sleep()` is called when tokens unavailable
- Check thread-safety (lock acquired before token check)

### Issue: "Duplicate key error on (park_id, observation_time)"

**Cause**: Re-running collection for same hour

**Solution**: This is expected! `ON DUPLICATE KEY UPDATE` should handle gracefully. If error persists, check repository implementation uses correct SQL.

## Development Workflow

### Adding New Weather Variables

1. Update `openmeteo_client.py`:
   - Add variable to `hourly` parameter string
   - Add field to parsing logic

2. Update `018_weather_schema.sql`:
   - Add column to `weather_observations` table
   - Add column to `weather_forecasts` table

3. Update `weather_repository.py`:
   - Add field to INSERT/UPDATE queries

4. Add tests:
   - Unit test: Mock API response includes new variable
   - Integration test: Verify new column populated

5. Run tests before committing:
```bash
pytest tests/unit/test_openmeteo_client.py -v
pytest tests/integration/test_weather_collection.py -v
```

### Testing with Mirrored Production Data

Before deploying changes:

```bash
# Mirror production database
deployment/scripts/mirror-production-db.sh --days=7

# Run collection script against mirrored data
PYTHONPATH=src python3 src/scripts/collect_weather.py --test

# Verify data looks correct
mysql -h localhost -u themepark_dev -p themepark_dev -e "
SELECT COUNT(*) FROM weather_observations;
"
```

## Next Steps

✅ **Quickstart Complete**

After local setup and testing:
1. Update agent context: `.specify/scripts/bash/update-agent-context.sh claude`
2. Run Zen review on data model and contracts
3. Implement recommendations
4. Proceed to Phase 2: Implementation (via `/speckit.tasks` command)
