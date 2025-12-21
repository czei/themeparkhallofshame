# Data Model: Weather Data Collection

**Feature**: 002-weather-collection
**Phase**: 1 - Design & Contracts
**Date**: 2025-12-17

## Overview

This document defines the data entities, relationships, and validation rules for weather data collection. The weather system extends the existing Theme Park database with two new tables that maintain referential integrity with the `parks` table.

## Entity Relationship Diagram

```
┌─────────────────┐
│     parks       │ (EXISTING)
├─────────────────┤
│ park_id (PK)    │
│ name            │
│ latitude        │◄──────┐
│ longitude       │       │
│ timezone        │       │
│ is_active       │       │
└─────────────────┘       │
                          │
                 ┌────────┴────────────────┐
                 │                         │
      ┌──────────▼───────────┐  ┌─────────▼──────────┐
      │ weather_observations │  │ weather_forecasts  │
      ├──────────────────────┤  ├────────────────────┤
      │ observation_id (PK)  │  │ forecast_id (PK)   │
      │ park_id (FK)         │  │ park_id (FK)       │
      │ observation_time     │  │ issued_at          │
      │ collected_at         │  │ forecast_time      │
      │ temperature_c        │  │ temperature_c      │
      │ temperature_f        │  │ temperature_f      │
      │ wind_speed_kmh       │  │ wind_speed_kmh     │
      │ wind_speed_mph       │  │ wind_speed_mph     │
      │ precipitation_mm     │  │ precipitation_mm   │
      │ weather_code         │  │ weather_code       │
      │ ...                  │  │ ...                │
      └──────────────────────┘  └────────────────────┘
```

## Entity Definitions

### Entity: WeatherObservation

**Description**: Represents a single hourly weather observation for a specific park at a specific time.

**Fields**:

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `observation_id` | BIGINT | PRIMARY KEY, AUTO_INCREMENT | Unique identifier for each observation |
| `park_id` | INT | NOT NULL, FOREIGN KEY → parks(park_id) | Park this observation belongs to |
| `observation_time` | TIMESTAMP | NOT NULL | UTC datetime when weather was observed |
| `collected_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | UTC datetime when we fetched this data |
| `temperature_c` | DECIMAL(5,2) | NULL | Temperature in Celsius (-99.99 to 999.99) |
| `temperature_f` | DECIMAL(5,2) | NULL | Temperature in Fahrenheit (-99.99 to 999.99) |
| `apparent_temperature_c` | DECIMAL(5,2) | NULL | "Feels like" temperature in Celsius |
| `apparent_temperature_f` | DECIMAL(5,2) | NULL | "Feels like" temperature in Fahrenheit |
| `wind_speed_kmh` | DECIMAL(5,2) | NULL | Wind speed in km/h (0.00 to 999.99) |
| `wind_speed_mph` | DECIMAL(5,2) | NULL | Wind speed in mph (0.00 to 999.99) |
| `wind_gusts_kmh` | DECIMAL(5,2) | NULL | Wind gust speed in km/h |
| `wind_gusts_mph` | DECIMAL(5,2) | NULL | Wind gust speed in mph |
| `wind_direction_degrees` | SMALLINT | NULL | Wind direction (0-360 degrees, 0=North) |
| `precipitation_mm` | DECIMAL(6,2) | NULL | Total precipitation in mm (0.00 to 9999.99) |
| `precipitation_probability` | TINYINT | NULL | Probability of precipitation (0-100%) |
| `rain_mm` | DECIMAL(6,2) | NULL | Rainfall amount in mm |
| `snowfall_mm` | DECIMAL(6,2) | NULL | Snowfall amount in mm (water equivalent) |
| `cloud_cover_percent` | TINYINT | NULL | Cloud coverage (0-100%) |
| `visibility_meters` | INT | NULL | Visibility in meters (0 to 999999) |
| `humidity_percent` | TINYINT | NULL | Relative humidity (0-100%) |
| `pressure_hpa` | DECIMAL(6,2) | NULL | Barometric pressure in hPa (0.00 to 9999.99) |
| `weather_code` | SMALLINT | NULL | WMO weather code (0-99, see WMO Codes below) |

**Indexes**:
- `PRIMARY KEY (observation_id)`: Fast lookups by ID
- `INDEX idx_park_time (park_id, observation_time)`: Fast queries for specific park+time
- `INDEX idx_weather_code (weather_code)`: Fast thunderstorm filtering (WHERE weather_code IN (95,96,99))
- `UNIQUE KEY unique_observation (park_id, observation_time)`: Prevent duplicate observations

**Validation Rules**:
1. **Temperature Range**: -99.99°C to 999.99°C (-99.99°F to 999.99°F)
   - Rationale: Covers Earth's temperature extremes with margin
2. **Wind Speed Range**: 0.00 to 999.99 km/h (0.00 to 999.99 mph)
   - Rationale: Covers hurricane-force winds (>250 mph)
3. **Wind Direction**: 0 to 360 degrees
   - Rationale: 0 = North, 90 = East, 180 = South, 270 = West, 360 wraps to 0
4. **Precipitation**: 0.00 to 9999.99 mm
   - Rationale: Covers extreme rainfall events (world record: 1,825mm in 24h)
5. **Percentages**: 0 to 100
   - Fields: `precipitation_probability`, `cloud_cover_percent`, `humidity_percent`
6. **Weather Code**: 0 to 99
   - Rationale: WMO standard codes (see below)
7. **Timestamps**: Must be timezone-aware UTC
   - Application code must use `datetime.now(timezone.utc)`

**Business Rules**:
- `observation_time` MUST be on the hour (minutes=0, seconds=0)
- Duplicate (park_id, observation_time) pairs are updated, not rejected (idempotent)
- NULL values allowed for all weather fields (API may not provide all data)
- Foreign key constraint: Park must exist in `parks` table
- ON DELETE CASCADE: If park is deleted, all observations are deleted

---

### Entity: WeatherForecast

**Description**: Represents a single hourly forecast for a specific park, issued at a specific time, predicting weather at a future time.

**Fields**:

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `forecast_id` | BIGINT | PRIMARY KEY, AUTO_INCREMENT | Unique identifier for each forecast |
| `park_id` | INT | NOT NULL, FOREIGN KEY → parks(park_id) | Park this forecast belongs to |
| `issued_at` | TIMESTAMP | NOT NULL | UTC datetime when forecast was generated |
| `forecast_time` | TIMESTAMP | NOT NULL | UTC datetime that this forecast predicts |
| `temperature_c` | DECIMAL(5,2) | NULL | Forecasted temperature in Celsius |
| `temperature_f` | DECIMAL(5,2) | NULL | Forecasted temperature in Fahrenheit |
| `apparent_temperature_c` | DECIMAL(5,2) | NULL | Forecasted "feels like" temperature in Celsius |
| `apparent_temperature_f` | DECIMAL(5,2) | NULL | Forecasted "feels like" temperature in Fahrenheit |
| `wind_speed_kmh` | DECIMAL(5,2) | NULL | Forecasted wind speed in km/h |
| `wind_speed_mph` | DECIMAL(5,2) | NULL | Forecasted wind speed in mph |
| `wind_gusts_kmh` | DECIMAL(5,2) | NULL | Forecasted wind gust speed in km/h |
| `wind_gusts_mph` | DECIMAL(5,2) | NULL | Forecasted wind gust speed in mph |
| `wind_direction_degrees` | SMALLINT | NULL | Forecasted wind direction (0-360 degrees) |
| `precipitation_mm` | DECIMAL(6,2) | NULL | Forecasted precipitation in mm |
| `precipitation_probability` | TINYINT | NULL | Forecasted probability of precipitation (0-100%) |
| `rain_mm` | DECIMAL(6,2) | NULL | Forecasted rainfall amount in mm |
| `snowfall_mm` | DECIMAL(6,2) | NULL | Forecasted snowfall amount in mm |
| `cloud_cover_percent` | TINYINT | NULL | Forecasted cloud coverage (0-100%) |
| `visibility_meters` | INT | NULL | Forecasted visibility in meters |
| `humidity_percent` | TINYINT | NULL | Forecasted relative humidity (0-100%) |
| `pressure_hpa` | DECIMAL(6,2) | NULL | Forecasted barometric pressure in hPa |
| `weather_code` | SMALLINT | NULL | Forecasted WMO weather code (0-99) |

**Indexes**:
- `PRIMARY KEY (forecast_id)`: Fast lookups by ID
- `INDEX idx_park_issued (park_id, issued_at)`: Find all forecasts issued at a specific time for a park
- `INDEX idx_park_forecast_time (park_id, forecast_time)`: Find all forecasts predicting a specific future time
- `INDEX idx_weather_code (weather_code)`: Fast thunderstorm filtering
- `UNIQUE KEY unique_forecast (park_id, issued_at, forecast_time)`: Prevent duplicate forecasts

**Validation Rules**:
- Same as `WeatherObservation` (temperature, wind, precipitation ranges)
- **Forecast Time Constraint**: `forecast_time` MUST be > `issued_at`
  - Rationale: Can't forecast the past
- **Forecast Horizon**: `forecast_time` typically ≤ `issued_at` + 7 days
  - Rationale: Open-Meteo provides 16-day forecasts, but we only store 7 days

**Business Rules**:
- Forecasts issued every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)
- Each forecast run inserts 168 rows per park (7 days × 24 hours)
- Duplicate (park_id, issued_at, forecast_time) tuples are updated, not rejected (idempotent)
- Forecasts older than 90 days (issued_at < NOW() - INTERVAL 90 DAY) are deleted daily
- Foreign key constraint: Park must exist in `parks` table
- ON DELETE CASCADE: If park is deleted, all forecasts are deleted

---

### Entity: Park (EXISTING - Extended Usage)

**Description**: Existing `parks` table. Weather collection uses `latitude`, `longitude`, and `timezone` fields.

**Fields Used for Weather Collection**:

| Field | Type | Usage |
|-------|------|-------|
| `park_id` | INT | Foreign key for weather_observations and weather_forecasts |
| `latitude` | DECIMAL(10,8) | Passed to Open-Meteo API (required) |
| `longitude` | DECIMAL(11,8) | Passed to Open-Meteo API (required) |
| `timezone` | VARCHAR(50) | Used for logging (e.g., "America/New_York") |
| `is_active` | TINYINT | Only collect weather for active parks |

**Validation for Weather Collection**:
- Parks with `latitude IS NULL OR longitude IS NULL` are skipped (logged as warning)
- Parks with `is_active = 0` are skipped
- Latitude range: -90 to 90 (enforced by Open-Meteo API)
- Longitude range: -180 to 180 (enforced by Open-Meteo API)

**Business Rules**:
- Weather collection queries: `SELECT * FROM parks WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND is_active = TRUE`
- If park coordinates change, next collection will fetch weather for new location (no migration needed)

---

## WMO Weather Codes

The `weather_code` field uses World Meteorological Organization (WMO) standard codes:

| Code | Description | Significance |
|------|-------------|--------------|
| 0 | Clear sky | Safe for all rides |
| 1-3 | Mainly clear, partly cloudy, overcast | Safe for all rides |
| 45, 48 | Fog, depositing rime fog | Visibility reduced |
| 51-55 | Drizzle (light, moderate, dense) | Minor impact |
| 56-57 | Freezing drizzle | Ice on surfaces, ride closures possible |
| 61-65 | Rain (slight, moderate, heavy) | Outdoor rides may close (heavy rain) |
| 66-67 | Freezing rain | Ice on surfaces, ride closures likely |
| 71-75 | Snowfall (slight, moderate, heavy) | Outdoor rides closed (heavy snow) |
| 77 | Snow grains | Minor impact |
| 80-82 | Rain showers (slight, moderate, violent) | Outdoor rides may close |
| 85-86 | Snow showers (slight, heavy) | Outdoor rides may close |
| **95** | **Thunderstorm (slight or moderate)** | **Lightning policy: Outdoor rides CLOSED** |
| **96** | **Thunderstorm with slight hail** | **Lightning policy: Outdoor rides CLOSED** |
| **99** | **Thunderstorm with heavy hail** | **Lightning policy: Outdoor rides CLOSED** |

**Thunderstorm Detection**:
- Codes 95, 96, 99 indicate thunderstorm conditions
- Used as proxy for lightning (direct lightning data requires commercial API)
- Query for thunderstorm correlation: `WHERE weather_code IN (95, 96, 99)`

**Sources**:
- WMO Code Table 4677: https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM
- Open-Meteo Weather Codes: https://open-meteo.com/en/docs

---

## Data Retention & Cleanup

### Retention Policies

| Table | Retention Period | Rationale |
|-------|------------------|-----------|
| `weather_observations` | 2 years (730 days) | Park visitors need hourly granularity for visit correlation |
| `weather_forecasts` | 90 days (from `issued_at`) | Sufficient for forecast accuracy analysis |

### Cleanup Queries

**Delete Old Observations**:
```sql
DELETE FROM weather_observations
WHERE observation_time < NOW() - INTERVAL 730 DAY;
```

**Delete Old Forecasts**:
```sql
DELETE FROM weather_forecasts
WHERE issued_at < NOW() - INTERVAL 90 DAY;
```

**Cleanup Schedule**: Daily at 04:00 UTC (low-traffic period)

---

## State Transitions

Weather observations and forecasts do not have explicit state transitions (no status field). However, the data lifecycle follows this pattern:

```
[Collection] → [Storage] → [Analysis] → [Cleanup]
     ↓             ↓            ↓            ↓
  API Call      INSERT      JOIN with       DELETE
              (idempotent)  ride data    (after 2yr/90d)
```

**Collection State**:
- Hourly observations: Collected every hour at :00
- Forecasts: Collected every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)

**Storage State**:
- `collected_at` timestamp records when data was fetched
- `ON DUPLICATE KEY UPDATE` handles re-collection (idempotent)

**Analysis State** (future feature):
- Weather data joined with ride_status_snapshots on (park_id, timestamp)
- Machine learning model correlates weather conditions with ride closures

**Cleanup State**:
- Observations deleted after 730 days
- Forecasts deleted 90 days after `issued_at`
- Deletion is permanent (no archive in this feature)

---

## Relationships

### One-to-Many: parks → weather_observations
- One park has many weather observations (hourly over 2 years = 17,520 rows per park)
- Foreign key: `weather_observations.park_id → parks.park_id`
- Cascade: ON DELETE CASCADE (if park deleted, delete observations)

### One-to-Many: parks → weather_forecasts
- One park has many weather forecasts (168 hours × 4 collections/day × 90 days = 60,480 rows per park)
- Foreign key: `weather_forecasts.park_id → parks.park_id`
- Cascade: ON DELETE CASCADE (if park deleted, delete forecasts)

### No Direct Relationship: weather_observations ↔ weather_forecasts
- Forecasts and observations are independent entities
- Forecast accuracy analysis joins on (park_id, forecast_time = observation_time)

---

## Performance Considerations

### Query Patterns

**Common Queries**:

1. **Get latest observation for a park**:
   ```sql
   SELECT * FROM weather_observations
   WHERE park_id = ?
   ORDER BY observation_time DESC
   LIMIT 1;
   ```
   - Uses index: `idx_park_time`

2. **Get observations for time range**:
   ```sql
   SELECT * FROM weather_observations
   WHERE park_id = ? AND observation_time BETWEEN ? AND ?;
   ```
   - Uses index: `idx_park_time` (compound index on park_id + observation_time)

3. **Find thunderstorm periods**:
   ```sql
   SELECT park_id, observation_time
   FROM weather_observations
   WHERE weather_code IN (95, 96, 99);
   ```
   - Uses index: `idx_weather_code`

4. **Forecast accuracy analysis** (future):
   ```sql
   SELECT
       f.park_id,
       f.forecast_time,
       f.temperature_c AS forecasted_temp,
       o.temperature_c AS actual_temp,
       ABS(f.temperature_c - o.temperature_c) AS error
   FROM weather_forecasts f
   INNER JOIN weather_observations o
       ON f.park_id = o.park_id
       AND f.forecast_time = o.observation_time
   WHERE f.issued_at = ?;
   ```
   - Uses indexes: `idx_park_forecast_time`, `idx_park_time`

### Storage Estimates

**Observations**:
- Row size: ~200 bytes (20 fields × 10 bytes average)
- 150 parks × 24 hours/day × 730 days = 2,628,000 rows
- Storage: 2,628,000 × 200 bytes = 526 MB

**Forecasts**:
- Row size: ~200 bytes
- 150 parks × 168 forecast hours × 4 collections/day × 90 days = 9,072,000 rows
- Storage: 9,072,000 × 200 bytes = 1,814 MB

**Total**: ~2.3 GB for weather data (small compared to ride snapshot data)

---

## Validation Rules Summary

### Field-Level Validation

| Field | Rule | Enforcement |
|-------|------|-------------|
| `temperature_c/f` | -99.99 to 999.99 | Application |
| `wind_speed_*` | 0.00 to 999.99 | Application |
| `wind_direction_degrees` | 0 to 360 | Application |
| `precipitation_*` | 0.00 to 9999.99 | Application |
| `*_percent` | 0 to 100 | Application |
| `weather_code` | 0 to 99 | Application |
| `park_id` | EXISTS in parks table | Database (FK) |
| `observation_time` | UTC, on the hour | Application |
| `forecast_time` | > issued_at | Application |

### Entity-Level Validation

| Rule | Enforcement |
|------|-------------|
| Unique (park_id, observation_time) | Database (UNIQUE KEY) |
| Unique (park_id, issued_at, forecast_time) | Database (UNIQUE KEY) |
| Parks must have coordinates | Application (skip if NULL) |
| Idempotent inserts | Database (ON DUPLICATE KEY UPDATE) |

---

## Migration Strategy

**Migration File**: `018_weather_schema.sql`

**Migration Steps**:
1. Create `weather_observations` table with indexes
2. Create `weather_forecasts` table with indexes
3. Add foreign key constraints to `parks` table
4. No data migration (new tables start empty)

**Rollback Strategy**:
```sql
DROP TABLE IF EXISTS weather_forecasts;
DROP TABLE IF EXISTS weather_observations;
```

**Testing Migration**:
1. Run migration on local dev database (mirrored from production)
2. Verify table schemas match data model
3. Verify foreign key constraints work (insert valid/invalid park_id)
4. Verify unique constraints work (insert duplicate observations)
5. Run collection script to populate tables
6. Verify indexes are used (EXPLAIN queries)

---

## Next Steps

✅ **Data Model Complete**

**Proceed to**:
1. Create `contracts/openmeteo-api.yaml` (API contract validation)
2. Create `quickstart.md` (developer setup instructions)
3. **Mandatory**: Run Zen thinkdeep or analyze review on this data model
4. Implement recommendations before Phase 2
