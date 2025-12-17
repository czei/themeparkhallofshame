# Feature Specification: Weather Data Collection

**Feature ID**: 002-weather-collection
**Date**: 2025-12-17
**Status**: Planning

## Overview

Add comprehensive weather data collection for all tracked theme parks to enable future correlation analysis between weather conditions and ride operational status. Weather data will be collected as a parallel process to existing ride data collection, with the two datasets joined during analysis phase.

## Business Requirements

### Primary Goal
Capture hourly weather observations and 6-hourly forecasts for ~150 theme parks to support machine learning correlation analysis between weather conditions (particularly thunderstorms) and ride downtime patterns.

### Success Criteria
1. Hourly weather observations collected for all parks with latitude/longitude coordinates
2. 7-day forecasts updated every 6 hours
3. Collection completes within 5-minute window (150 parks @ 1 req/sec)
4. 2-year retention of hourly observations, 90-day retention of forecasts
5. Zero impact on existing ride data collection processes

### Out of Scope
- Historical weather backfill (no historical ride data to correlate against)
- Commercial lightning APIs (start with WMO thunderstorm codes as proxy)
- Real-time weather display on website
- Severe weather alert integration
- Ride weather sensitivity tagging

## Functional Requirements

### FR1: Weather API Integration
- **Source**: Open-Meteo API (https://api.open-meteo.com/v1/forecast)
- **No API key required**: Zero authentication overhead
- **Rate limiting**: 1 request per second (respectful usage)
- **Global coverage**: Handles international Disney/Universal parks
- **Concurrent collection**: ThreadPoolExecutor with TokenBucket rate limiter

### FR2: Current Weather Collection
Collect **hourly** (on the hour) for each park:
- Temperature (actual and feels-like) in °C and °F
- Wind speed and gusts (km/h and mph)
- Wind direction (degrees)
- Precipitation amount (mm)
- Precipitation probability (%)
- Rain and snowfall amounts (mm)
- Cloud cover percentage (%)
- Visibility (meters)
- Weather code (WMO standard - codes 95/96/99 for thunderstorm detection)
- Humidity (%)
- Barometric pressure (hPa)

**Schedule**: Every hour at `:00` (00:00, 01:00, 02:00, etc. UTC)

### FR3: Forecast Collection
Collect **every 6 hours** (00:00, 06:00, 12:00, 18:00 UTC) with 7-day hourly forecasts:
- Same variables as current weather observations
- Track `issued_at` (when forecast was generated) vs. `forecast_time` (what time is being forecast)
- Store 7 days × 24 hours = 168 forecast hours per collection run

**Rationale for 7 days**: Open-Meteo provides up to 16 days, but accuracy degrades significantly beyond 5 days. 7 days balances forecast utility with storage efficiency.

### FR4: Data Retention Policy
- **Hourly observations**: 2 years
  - **Why**: Park visitors need hourly granularity to correlate weather with specific visit times
  - **Cleanup**: Delete observations older than 2 years (730 days)

- **Forecasts**: 90 days
  - **Why**: Sufficient for forecast accuracy analysis at different lead times
  - **Cleanup**: Delete forecasts issued more than 90 days ago

- **Daily aggregates**: Indefinitely
  - **Why**: Long-term trend analysis (not implemented in this feature)

### FR5: Park Coordinate Handling
- Use **existing** `parks` table (latitude, longitude, timezone columns already exist)
- Only collect weather for parks where `latitude IS NOT NULL AND longitude IS NOT NULL`
- Skip parks without coordinates (log warning, continue with other parks)

### FR6: Error Handling & Resilience
- Single park API failure MUST NOT block collection for other parks
- Retry failed requests with exponential backoff (tenacity @retry decorator)
- Log all errors with structured JSON for CloudWatch Logs Insights
- Continue collection even if some parks fail (partial data better than no data)

### FR7: Monitoring & Observability
- Log collection start/end times and duration
- Log successful vs. failed park collection counts
- **Alert metric**: Any park with data older than 3 hours = CRITICAL
- CloudWatch metric: `WeatherCollectionFailures` by park_id
- Monitor API response times (detect Open-Meteo degradation)

## Non-Functional Requirements

### NFR1: Performance
- Collection cycle MUST complete within 5 minutes (150 parks @ 1 req/sec = 2.5 min + margin)
- Concurrent collection with max 10 workers (ThreadPoolExecutor)
- Token bucket rate limiter ensures global 1 req/sec compliance
- Database inserts via batching (avoid 1 row at a time)

### NFR2: Storage Efficiency
- Hourly observations: ~150 parks × 24 hours × 730 days = 2.6M rows
- Forecasts: ~150 parks × 168 hours × 4 collections/day × 90 days = 9M rows
- Use `TIMESTAMP` fields for UTC storage (handles DST automatically)
- Indexes on (park_id, observation_time) for fast queries

### NFR3: Integration with Existing System
- **Zero impact** on existing ride data collection (parallel processes)
- Use existing timezone utilities (`utils/timezone.py`)
- Follow existing repository pattern (SQLAlchemy Core, text() queries)
- Use existing Config class for environment variable management
- Wrap in `cron_wrapper.py` for failure alerting

### NFR4: Testability
- Unit tests with mocked API responses (avoid real API calls in tests)
- Integration tests with real database (test transaction rollback)
- Use `freezegun` for deterministic time-based testing
- Test timezone handling (UTC storage, Pacific Time queries)

## Data Model

### Table: weather_observations
Stores hourly current weather observations.

```sql
CREATE TABLE weather_observations (
    observation_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    observation_time TIMESTAMP NOT NULL,  -- UTC, when observation was recorded
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- UTC, when we fetched it

    -- Temperature
    temperature_c DECIMAL(5,2),
    temperature_f DECIMAL(5,2),
    apparent_temperature_c DECIMAL(5,2),
    apparent_temperature_f DECIMAL(5,2),

    -- Wind
    wind_speed_kmh DECIMAL(5,2),
    wind_speed_mph DECIMAL(5,2),
    wind_gusts_kmh DECIMAL(5,2),
    wind_gusts_mph DECIMAL(5,2),
    wind_direction_degrees SMALLINT,

    -- Precipitation
    precipitation_mm DECIMAL(6,2),
    precipitation_probability TINYINT,  -- 0-100%
    rain_mm DECIMAL(6,2),
    snowfall_mm DECIMAL(6,2),

    -- Atmospheric
    cloud_cover_percent TINYINT,
    visibility_meters INT,
    humidity_percent TINYINT,
    pressure_hpa DECIMAL(6,2),

    -- Weather code (WMO)
    weather_code SMALLINT,  -- 95/96/99 = thunderstorm

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_time (park_id, observation_time),
    INDEX idx_weather_code (weather_code),
    UNIQUE KEY unique_observation (park_id, observation_time)
) ENGINE=InnoDB;
```

### Table: weather_forecasts
Stores hourly forecasts for next 7 days.

```sql
CREATE TABLE weather_forecasts (
    forecast_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    issued_at TIMESTAMP NOT NULL,  -- UTC, when forecast was generated
    forecast_time TIMESTAMP NOT NULL,  -- UTC, what time this forecast is for

    -- Same fields as weather_observations (temperature, wind, precipitation, etc.)
    temperature_c DECIMAL(5,2),
    temperature_f DECIMAL(5,2),
    apparent_temperature_c DECIMAL(5,2),
    apparent_temperature_f DECIMAL(5,2),
    wind_speed_kmh DECIMAL(5,2),
    wind_speed_mph DECIMAL(5,2),
    wind_gusts_kmh DECIMAL(5,2),
    wind_gusts_mph DECIMAL(5,2),
    wind_direction_degrees SMALLINT,
    precipitation_mm DECIMAL(6,2),
    precipitation_probability TINYINT,
    rain_mm DECIMAL(6,2),
    snowfall_mm DECIMAL(6,2),
    cloud_cover_percent TINYINT,
    visibility_meters INT,
    humidity_percent TINYINT,
    pressure_hpa DECIMAL(6,2),
    weather_code SMALLINT,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_issued (park_id, issued_at),
    INDEX idx_park_forecast_time (park_id, forecast_time),
    INDEX idx_weather_code (weather_code),
    UNIQUE KEY unique_forecast (park_id, issued_at, forecast_time)
) ENGINE=InnoDB;
```

## API Contracts

### Open-Meteo API Request
```
GET https://api.open-meteo.com/v1/forecast
?latitude={lat}
&longitude={lon}
&hourly=temperature_2m,apparent_temperature,precipitation,rain,snowfall,
        weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m,
        wind_direction_10m,relative_humidity_2m,surface_pressure,visibility
&temperature_unit=fahrenheit
&wind_speed_unit=mph
&precipitation_unit=inch
&timezone=UTC
&forecast_days=7
```

**Response Structure**:
```json
{
  "hourly": {
    "time": ["2025-12-17T00:00", "2025-12-17T01:00", ...],
    "temperature_2m": [72.5, 71.8, ...],
    "apparent_temperature": [70.2, 69.5, ...],
    "precipitation": [0.0, 0.05, ...],
    "weather_code": [0, 0, 95, ...]
  }
}
```

## Implementation Phases

### Phase 1: Database Schema (Migration 018)
- Create `weather_observations` table
- Create `weather_forecasts` table
- Add indexes for query performance
- **Deliverable**: `018_weather_schema.sql` migration

### Phase 2: Weather API Client
- Singleton pattern with global instance
- Tenacity @retry for exponential backoff
- Parse Open-Meteo JSON responses
- Convert units (Celsius to Fahrenheit, km/h to mph)
- **Deliverable**: `src/api/openmeteo_client.py`

### Phase 3: Repository Layer
- `WeatherObservationRepository` for inserts/queries
- `WeatherForecastRepository` for inserts/queries
- Idempotent upserts with ON DUPLICATE KEY UPDATE
- **Deliverable**: `src/database/repositories/weather_repository.py`

### Phase 4: Collection Script
- Hourly current weather collector
- 6-hourly forecast collector
- Concurrent collection with ThreadPoolExecutor
- TokenBucket rate limiter (1 req/sec global)
- **Deliverable**: `src/scripts/collect_weather.py`

### Phase 5: Scheduled Jobs
- Cron: `0 * * * *` (hourly current weather)
- Cron: `0 */6 * * *` (every 6 hours for forecasts)
- Wrap with `cron_wrapper.py` for alerting
- **Deliverable**: Update crontab, deployment docs

### Phase 6: Cleanup Job
- Delete observations older than 2 years
- Delete forecasts issued > 90 days ago
- Run daily at low-traffic time (4am UTC)
- **Deliverable**: `src/scripts/cleanup_weather.py`

### Phase 7: Monitoring
- CloudWatch metric: `WeatherCollectionFailures`
- Alert: Any park missing data for 3+ hours
- Dashboard: API response times, collection duration
- **Deliverable**: CloudWatch alarm configuration

## Testing Strategy

### Unit Tests
- Mock Open-Meteo API responses
- Test TokenBucket rate limiter
- Test unit conversions (C→F, km/h→mph)
- Test error handling (API timeout, invalid JSON)
- **Coverage target**: >80%

### Integration Tests
- Real database with test transaction rollback
- Test idempotent inserts (duplicate key handling)
- Test timezone conversions (UTC storage)
- Verify foreign key constraints
- **Coverage target**: All repository methods

### Contract Tests
- Validate Open-Meteo API response schema
- Test API parameter formatting
- Verify WMO weather code ranges (0-99)

## Risks & Mitigations

### Risk 1: Open-Meteo API Reliability
- **Impact**: Missing weather data gaps correlation dataset
- **Likelihood**: Low (99%+ uptime historically)
- **Mitigation**: Retry with exponential backoff, graceful degradation, monitoring alerts

### Risk 2: Rate Limiting Enforcement
- **Impact**: API blocks our requests if we exceed 1 req/sec
- **Likelihood**: Medium (if TokenBucket implementation has bugs)
- **Mitigation**: Unit tests for rate limiter, monitor API response codes for 429 errors

### Risk 3: Storage Growth
- **Impact**: Database size grows faster than expected
- **Likelihood**: Low (2.6M + 9M rows well within MySQL capacity)
- **Mitigation**: Cleanup job runs daily, monitor disk usage, archive to S3 if needed

### Risk 4: Timezone Handling Errors
- **Impact**: Weather observations misaligned with ride data timestamps
- **Likelihood**: Medium (timezone bugs are common)
- **Mitigation**: Use TIMESTAMP for UTC storage, existing timezone utilities, comprehensive tests with freezegun

## Dependencies

### External Dependencies
- Open-Meteo API (https://api.open-meteo.com)
- No API key required (zero credentials to manage)

### Internal Dependencies
- Existing `parks` table (latitude, longitude, timezone columns)
- Existing timezone utilities (`utils/timezone.py`)
- Existing Config class (`utils/config.py`)
- Existing repository pattern (SQLAlchemy Core)
- Existing cron_wrapper.py for job alerting

## Success Metrics

### Immediate (Week 1)
- Collection script runs successfully for all parks with coordinates
- Zero errors in CloudWatch logs
- 100% data completeness (all parks updated hourly)

### Short-term (Month 1)
- 730 hourly observations per park (24 hours × 30 days)
- Forecast accuracy baseline established (compare forecast vs. actual)
- Storage usage within projections (<500MB growth)

### Long-term (Month 6)
- 2-year dataset complete for correlation analysis
- Machine learning model correlates thunderstorm codes with ride closures
- Cost-benefit analysis for commercial lightning API upgrade
