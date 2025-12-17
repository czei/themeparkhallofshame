# Weather Data Collection System - Requirements Document

## Project Context

This document specifies requirements for adding weather data collection to the Theme Park Hall of Shame project. The goal is to capture comprehensive weather data for all tracked theme parks to enable future correlation analysis between weather conditions and ride operational status.

### Existing System Overview
- **Backend:** Java
- **Database:** MariaDB
- **Data Source:** Queue-Times.com API
- **Current Scope:** ~150+ theme parks, 5,000-7,500 rides
- **Collection Interval:** 10-minute snapshots for ride status
- **Ride Data Retention:** 24-hour rolling window for raw data, then daily aggregation
- **Weather Data Retention:** 2 years hourly (to support user-specific time correlation), daily aggregates indefinitely

### Integration Principle
Weather data collection should run as a parallel process to existing ride data collection. The two datasets will be joined during analysis phase, not during collection.

---

## 1. Weather API Selection

### Primary: Open-Meteo API
- **Base URL:** `https://api.open-meteo.com/v1/forecast`
- **Cost:** Free, no API key required
- **Rate Limits:** No hard limits; requests respectful usage (1 second between requests)
- **Documentation:** https://open-meteo.com/en/docs

**Note:** Open-Meteo also has a Historical API at `https://archive-api.open-meteo.com/v1/archive` but we are not using it. Without historical ride data to correlate against, historical weather data has no value.

### Why Open-Meteo (Alternatives Evaluated)

| Service | Free Tier | Problem at Our Scale |
|---------|-----------|----------------------|
| **Open-Meteo** | **Unlimited, no key** | **None - this is the choice** |
| Visual Crossing | 1,000 records/day | 150 parks × 24 hours = 3,600 calls/day. Exceeds limit. |
| OpenWeatherMap | 1,000 calls/day | Same math, same problem |
| Tomorrow.io | 1,000 calls/month | Completely inadequate |
| WeatherAPI.com | 1M calls/month | Would work, but requires API key management |
| Weatherstack | 1,000 calls/month | Completely inadequate |
| NWS API | Free, unlimited | US only, no international parks, reliability issues |

**Open-Meteo advantages:**
1. Truly free for commercial use at any volume
2. No API key required (zero management overhead)
3. Global coverage (handles international Disney/Universal parks)
4. Includes WMO weather codes for thunderstorm detection
5. Hourly resolution matches our analysis needs
6. **Provides forecasts up to 16 days out** from the same endpoint (we use 7 days; accuracy degrades beyond 3-5 days)

### Update Frequency

**Current Weather:** Every hour, on the hour
**Forecasts:** Every 6 hours

**Rationale:**
- Open-Meteo's underlying weather models update hourly. Polling more frequently returns the same data.
- Weather conditions that cause closures (thunderstorms, high wind) develop over 15-60 minute timescales, not minutes.
- Ride status is collected every 10 minutes, but correlation analysis will aggregate to hourly buckets anyway.
- Collecting more frequently than hourly just creates redundant data points with no analytical value.
- 150 parks at 1 request/second = 2.5 minutes to complete a collection cycle. Hourly gives plenty of margin.

**Collection Schedule:**
- `:00` - Begin hourly weather collection for all parks
- `:03` - Hourly collection complete (with margin)
- Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC) - Forecast collection

### Lightning Data Limitation

**The Problem:** Lightning is the primary driver of ride closures in Florida and other thunderstorm-prone regions. Theme parks typically have lightning policies requiring closure of outdoor attractions when strikes are detected within 5-10 miles.

**Available Lightning APIs (all commercial):**
- Xweather/Vaisala: Industry standard, but standard tier only provides 5 minutes of history
- DTN Lightning: Up to 7 days history, commercial pricing
- Earth Networks: Commercial pricing
- Blitzortung.org: Free community project, but programmatic API access restricted to participants who operate detection stations

**Our Workaround:** Use WMO weather codes as a thunderstorm proxy:
- Code 95: Thunderstorm (slight or moderate)
- Code 96: Thunderstorm with slight hail
- Code 99: Thunderstorm with heavy hail

These codes indicate "thunderstorm conditions present," which is what triggers park lightning policies. While we won't have strike counts or precise proximity data, the ML model can learn the correlation between thunderstorm weather codes and ride closures.

**Schema Impact:** The `weather_code` field in our observations table captures this. The daily aggregation includes `thunderstorm_hours` count.

**Future Enhancement:** If correlation analysis shows thunderstorm codes are highly predictive, we could evaluate adding a commercial lightning API. But start with the free proxy first—it may be sufficient.

---

## 2. Database Schema

### 2.1 Park Geography Table
Store latitude/longitude for each park. This may already exist; if not, create it.

```sql
CREATE TABLE IF NOT EXISTS park_locations (
    park_id INT PRIMARY KEY,
    park_name VARCHAR(255) NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    timezone VARCHAR(50) NOT NULL,
    country_code CHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (park_id) REFERENCES parks(park_id)  -- Verify actual column name in existing schema
);
```

### 2.2 Weather Observations Table (Raw Hourly Data)
Store hourly weather snapshots. This is the primary collection table.

**CRITICAL: Use TIMESTAMP, not DATETIME, for observation_time to prevent data corruption during DST transitions.**

```sql
CREATE TABLE weather_observations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    observation_time TIMESTAMP NOT NULL,  -- TIMESTAMP ensures UTC storage, prevents DST duplicates

    -- Temperature
    temperature_c DECIMAL(5,2),           -- Celsius
    apparent_temperature_c DECIMAL(5,2),  -- Feels-like

    -- Wind (critical for ride closures)
    wind_speed_kmh DECIMAL(6,2),
    wind_gusts_kmh DECIMAL(6,2),
    wind_direction_degrees SMALLINT,

    -- Precipitation
    precipitation_mm DECIMAL(6,2),
    precipitation_probability TINYINT,    -- 0-100
    rain_mm DECIMAL(6,2),
    snowfall_cm DECIMAL(6,2),

    -- Visibility and conditions
    cloud_cover_percent TINYINT,          -- 0-100
    visibility_m INT,
    weather_code SMALLINT,                -- WMO weather code

    -- Atmospheric
    humidity_percent TINYINT,
    pressure_hpa DECIMAL(6,1),

    -- Metadata
    is_forecast BOOLEAN DEFAULT FALSE,    -- TRUE if this was a forecast, FALSE if historical
    data_source VARCHAR(50) DEFAULT 'open-meteo',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_park_time (park_id, observation_time),
    INDEX idx_observation_time (observation_time),
    UNIQUE KEY unique_park_observation (park_id, observation_time)
);
```

**Why TIMESTAMP instead of DATETIME:**
- TIMESTAMP stores UTC and automatically handles timezone conversions
- DATETIME is timezone-naive and will create duplicate rows during DST "fall back" when the same hour occurs twice
- The unique constraint `(park_id, observation_time)` would reject the second observation of 1:00 AM on DST transition day
- TIMESTAMP prevents this silent data corruption

### 2.3 Weather Forecasts Table
Store forecasts separately to enable forecast accuracy analysis later.

```sql
CREATE TABLE weather_forecasts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    forecast_time TIMESTAMP NOT NULL,      -- When this forecast is FOR (UTC)
    issued_at TIMESTAMP NOT NULL,          -- When the forecast was MADE (UTC)
    hours_ahead SMALLINT NOT NULL,         -- forecast_time - issued_at in hours

    -- Same weather variables as observations
    temperature_c DECIMAL(5,2),
    apparent_temperature_c DECIMAL(5,2),
    wind_speed_kmh DECIMAL(6,2),
    wind_gusts_kmh DECIMAL(6,2),
    wind_direction_degrees SMALLINT,
    precipitation_probability TINYINT,
    precipitation_mm DECIMAL(6,2),
    weather_code SMALLINT,

    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_park_forecast (park_id, forecast_time),
    INDEX idx_issued (issued_at),
    INDEX idx_hours_ahead (hours_ahead)
);
```

### 2.4 Weather Daily Aggregates Table
For long-term storage and fast queries across years.

```sql
CREATE TABLE weather_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    observation_date DATE NOT NULL,
    
    -- Temperature aggregates
    temp_min_c DECIMAL(5,2),
    temp_max_c DECIMAL(5,2),
    temp_mean_c DECIMAL(5,2),
    
    -- Wind aggregates (most important for analysis)
    wind_speed_max_kmh DECIMAL(6,2),
    wind_gust_max_kmh DECIMAL(6,2),
    wind_speed_mean_kmh DECIMAL(6,2),
    hours_wind_over_30kmh TINYINT,        -- Count of hours with wind > 30 km/h
    hours_wind_over_50kmh TINYINT,        -- Count of hours with wind > 50 km/h
    
    -- Precipitation aggregates
    precipitation_total_mm DECIMAL(7,2),
    precipitation_hours TINYINT,           -- Hours with measurable precipitation
    max_hourly_precipitation_mm DECIMAL(6,2),
    
    -- Condition summaries
    cloud_cover_mean_percent TINYINT,
    dominant_weather_code SMALLINT,        -- Most frequent weather code
    thunderstorm_hours TINYINT,            -- Hours with weather_code in (95, 96, 99)
    
    -- Operating hours context
    park_operating_hours TINYINT,          -- If known
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY unique_park_date (park_id, observation_date),
    INDEX idx_observation_date (observation_date)
);
```

### 2.5 WMO Weather Codes Reference Table

```sql
CREATE TABLE weather_codes (
    code SMALLINT PRIMARY KEY,
    description VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,         -- 'clear', 'cloudy', 'rain', 'snow', 'thunderstorm', etc.
    severity TINYINT NOT NULL              -- 0=none, 1=light, 2=moderate, 3=heavy
);

-- Populate with WMO codes
INSERT INTO weather_codes (code, description, category, severity) VALUES
(0, 'Clear sky', 'clear', 0),
(1, 'Mainly clear', 'clear', 0),
(2, 'Partly cloudy', 'cloudy', 0),
(3, 'Overcast', 'cloudy', 0),
(45, 'Fog', 'fog', 1),
(48, 'Depositing rime fog', 'fog', 2),
(51, 'Light drizzle', 'rain', 1),
(53, 'Moderate drizzle', 'rain', 2),
(55, 'Dense drizzle', 'rain', 3),
(61, 'Slight rain', 'rain', 1),
(63, 'Moderate rain', 'rain', 2),
(65, 'Heavy rain', 'rain', 3),
(71, 'Slight snow', 'snow', 1),
(73, 'Moderate snow', 'snow', 2),
(75, 'Heavy snow', 'snow', 3),
(80, 'Slight rain showers', 'rain', 1),
(81, 'Moderate rain showers', 'rain', 2),
(82, 'Violent rain showers', 'rain', 3),
(85, 'Slight snow showers', 'snow', 1),
(86, 'Heavy snow showers', 'snow', 3),
(95, 'Thunderstorm', 'thunderstorm', 2),
(96, 'Thunderstorm with slight hail', 'thunderstorm', 3),
(99, 'Thunderstorm with heavy hail', 'thunderstorm', 3);
```

---

## 3. Data Collection Components

### 3.1 Park Location Initializer

**Purpose:** Populate park_locations table with coordinates for all tracked parks.

**Data Source Options:**
1. Queue-Times API may include coordinates
2. Manual lookup for major parks
3. Geocoding API (Google, OpenStreetMap) as fallback

**Implementation Notes:**
- Run once initially, then on-demand when new parks are added
- Store timezone for each park (critical for aligning weather data with operating hours)
- Validate coordinates are reasonable (within expected country boundaries)

**Priority Parks (ensure these have accurate coordinates):**
- All Disney parks (WDW, DLR, international)
- All Universal parks
- All Six Flags parks
- All Cedar Fair parks
- All SeaWorld parks

### 3.2 Current Weather Collector

**Purpose:** Collect current weather data hourly for all parks.

**Frequency:** Every hour, on the hour (e.g., 00:00, 01:00, 02:00 UTC)

**Why Hourly:**
- Open-Meteo's models update hourly; more frequent polling returns stale data
- Matches the granularity needed for ride closure correlation
- Respectful to free API while ensuring no gaps

**API Call Pattern:**
```
GET https://api.open-meteo.com/v1/forecast
    ?latitude={lat}
    &longitude={lon}
    &current=temperature_2m,apparent_temperature,precipitation,rain,weather_code,
              cloud_cover,wind_speed_10m,wind_direction_10m,wind_gusts_10m,
              relative_humidity_2m,surface_pressure,visibility
    &timezone=auto
```

**Batch Optimization:**
Open-Meteo does NOT support multiple locations in one call. However, you can:
1. **Parallelize requests** (respect rate limits)
   - Serial execution: 150 parks × 1 second = 2.5 minutes
   - Parallel execution (10 threads): 150 parks ÷ 10 threads × 1 second = 15 seconds
   - Recommended: Start with 5-10 parallel threads, monitor API response times
   - Use thread pool with rate limiter to prevent overwhelming API
2. Group parks by region and stagger collection
3. Use connection pooling to reduce overhead

**Error Handling (CRITICAL - Prevents Permanent Data Gaps):**

**1. Retry Strategy with Exponential Backoff:**
```java
int maxRetries = 3;
long baseDelayMs = 1000;  // 1 second

for (int attempt = 0; attempt <= maxRetries; attempt++) {
    try {
        WeatherObservation data = apiClient.getCurrentWeather(lat, lon);
        repository.save(data);
        break;  // Success
    } catch (ApiException e) {
        if (attempt == maxRetries) {
            logger.error("Failed after {} retries for park {}: {}", maxRetries, parkId, e.getMessage());
            // Record failure in monitoring table
            break;
        }
        long delay = baseDelayMs * (long) Math.pow(2, attempt);  // 1s, 2s, 4s
        Thread.sleep(delay);
    }
}
```

**2. Circuit Breaker Pattern:**
- If API fails for 10+ consecutive parks, pause collection for 5 minutes
- Prevents hammering a degraded API
- Resume normal collection after cool-down period

**3. Idempotent Upserts:**
```sql
INSERT INTO weather_observations (park_id, observation_time, temperature_c, ...)
VALUES (?, ?, ?, ...)
ON DUPLICATE KEY UPDATE
    temperature_c = VALUES(temperature_c),
    wind_speed_kmh = VALUES(wind_speed_kmh),
    -- Update all fields
    collected_at = CURRENT_TIMESTAMP;
```
This allows safe re-runs without creating duplicates or failing on constraint violations.

**4. Job Recovery on Restart:**
- On startup, check if previous hour's collection completed
- If incomplete, backfill missing parks before starting current hour
- Prevents permanent gaps from service restarts

**5. Failure Logging:**
- Log each failure with: park_id, timestamp, error type, HTTP status code
- Store in separate `weather_collection_failures` table for monitoring
- Alert if any park has >6 consecutive hours of failures

### 3.3 Forecast Collector

**Purpose:** Capture forecasts to enable future forecast accuracy analysis and ride closure predictions.

**Frequency:** Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)

**Why Every 6 Hours:**
- Weather forecast models run on 6-hour cycles; more frequent polling returns same forecasts
- Reduces storage overhead while capturing forecast evolution
- Sufficient for ML training on forecast accuracy

**API Call Pattern:**
```
GET https://api.open-meteo.com/v1/forecast
    ?latitude={lat}
    &longitude={lon}
    &hourly=temperature_2m,apparent_temperature,precipitation_probability,
            precipitation,weather_code,cloud_cover,wind_speed_10m,
            wind_direction_10m,wind_gusts_10m
    &forecast_days=7
    &timezone=auto
```

**Why 7 Days (not 16):**
Open-Meteo supports up to 16-day forecasts, but forecast accuracy degrades significantly after 3-5 days. Storing 16 days of increasingly unreliable predictions wastes space without analytical value. 7 days captures the useful forecast window.

**Storage Strategy:**
- Store each forecast hour as a separate row
- Calculate hours_ahead = forecast_time - issued_at
- This enables later analysis of "how accurate was the 24-hour forecast vs 48-hour forecast"

### 3.4 Daily Aggregation Job

**Purpose:** Create daily summaries for fast queries and long-term trend analysis. Daily aggregates enable quick charting across years without scanning millions of hourly rows.

**Note:** This runs in addition to hourly retention, not instead of it. Hourly data is kept for 2 years; daily aggregates are kept indefinitely.

**Frequency:** Run daily at 00:30 UTC (after all timezones have completed previous day)

**Aggregation Logic:**
```sql
INSERT INTO weather_daily (park_id, observation_date, temp_min_c, temp_max_c, ...)
SELECT 
    park_id,
    DATE(observation_time) as observation_date,
    MIN(temperature_c) as temp_min_c,
    MAX(temperature_c) as temp_max_c,
    AVG(temperature_c) as temp_mean_c,
    MAX(wind_speed_kmh) as wind_speed_max_kmh,
    MAX(wind_gusts_kmh) as wind_gust_max_kmh,
    AVG(wind_speed_kmh) as wind_speed_mean_kmh,
    SUM(CASE WHEN wind_speed_kmh > 30 THEN 1 ELSE 0 END) as hours_wind_over_30kmh,
    SUM(CASE WHEN wind_speed_kmh > 50 THEN 1 ELSE 0 END) as hours_wind_over_50kmh,
    SUM(precipitation_mm) as precipitation_total_mm,
    SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) as precipitation_hours,
    MAX(precipitation_mm) as max_hourly_precipitation_mm,
    AVG(cloud_cover_percent) as cloud_cover_mean_percent,
    SUM(CASE WHEN weather_code IN (95, 96, 99) THEN 1 ELSE 0 END) as thunderstorm_hours
FROM weather_observations
WHERE DATE(observation_time) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
GROUP BY park_id, DATE(observation_time);
```

### 3.5 Data Retention Manager

**Purpose:** Purge old hourly data after retention period expires.

**Retention Policy:**
- weather_observations: Keep 2 years of hourly data (park visitors need hourly granularity to correlate with specific visit times)
- weather_forecasts: Keep 90 days (to analyze forecast accuracy over time)
- weather_daily: Keep indefinitely

**Storage Estimates:**
- **weather_observations:** 150 parks × 2 years × 365 days × 24 hours = ~2.6 million rows
  - At ~200 bytes per row = ~520 MB
- **weather_forecasts:** 150 parks × 90 days retention × 4 collections/day × 7 days ahead × 24 hours/day = ~15.1 million rows
  - At ~120 bytes per row = ~1.8 GB
- **weather_daily:** 150 parks × indefinite retention (~10 years estimated) = ~550,000 rows
  - At ~150 bytes per row = ~82 MB
- **Total steady-state storage:** ~2.4 GB (well within reasonable limits)
- No historical backfill planned

**Purge Schedule:** Run daily

```sql
DELETE FROM weather_observations 
WHERE observation_time < DATE_SUB(CURRENT_DATE, INTERVAL 2 YEAR);

DELETE FROM weather_forecasts 
WHERE issued_at < DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY);
```

---

## 4. Implementation Phases

### Phase 1: Foundation (Week 1)
1. Create database tables
2. Populate park_locations with coordinates for top 50 parks
3. Implement basic Open-Meteo API client in Java
4. Test with single park

### Phase 2: Live Collection (Week 2)
1. Implement CurrentWeatherCollector as scheduled job
2. Implement ForecastCollector as scheduled job
3. Add logging and error handling
4. Deploy and monitor for stability

### Phase 3: Full Coverage & Maintenance (Week 3+)
1. Complete park_locations for all ~150 parks
2. Implement DailyAggregationJob
3. Implement DataRetentionManager
4. Add monitoring/alerting for collection failures
5. Document any API quirks discovered

**Note:** There is no historical backfill phase. We do not have historical ride/wait time data to correlate against, so historical weather data would be useless. The correlation dataset begins the day weather collection goes live.

---

## 5. Java Implementation Notes

### Recommended Libraries
- **HTTP Client:** Java 11+ HttpClient (built-in) or OkHttp
- **JSON Parsing:** Jackson or Gson
- **Scheduling:** Quartz Scheduler or Spring @Scheduled if using Spring
- **Database:** HikariCP for connection pooling, plain JDBC or JPA

### Package Structure Suggestion
```
com.themeparkhallofshame.weather/
├── api/
│   └── OpenMeteoClient.java          # API client
├── model/
│   ├── WeatherObservation.java       # Domain object
│   ├── WeatherForecast.java
│   └── ParkLocation.java
├── repository/
│   ├── WeatherObservationRepository.java
│   └── ParkLocationRepository.java
├── service/
│   ├── CurrentWeatherCollector.java
│   ├── ForecastCollector.java
│   └── DailyAggregationService.java
└── job/
    ├── HourlyWeatherJob.java         # Scheduled trigger
    ├── ForecastJob.java
    └── AggregationJob.java
```

### API Client Example Structure

```java
public class OpenMeteoClient {
    private static final String FORECAST_URL = "https://api.open-meteo.com/v1/forecast";
    
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    public WeatherObservation getCurrentWeather(double latitude, double longitude) {
        // Build URL with query parameters
        // Make GET request
        // Parse JSON response
        // Map to WeatherObservation object
    }
    
    public List<WeatherForecast> getHourlyForecast(double latitude, double longitude, int days) {
        // Similar pattern, returns list of hourly forecasts
    }
}
```

### Rate Limiting

```java
public class RateLimitedOpenMeteoClient extends OpenMeteoClient {
    private final RateLimiter rateLimiter = RateLimiter.create(1.0); // 1 request per second
    
    @Override
    public WeatherObservation getCurrentWeather(double latitude, double longitude) {
        rateLimiter.acquire();
        return super.getCurrentWeather(latitude, longitude);
    }
}
```

---

## 6. Monitoring and Alerting

### Metrics to Track
- Successful vs failed API calls per hour
- Collection job duration
- **Data completeness metric:** `(parks_with_data / total_parks) * 100` for each collection run
  - Target: ≥99% (at most 1-2 parks missing per run)
  - Calculate hourly and track as timeseries
  - Alert if completeness drops below 95% for 2 consecutive hours
- Database table sizes

### Alert Conditions
- API error rate > 10% in 1 hour
- Any park missing data for > 6 hours
- Collection job fails to start

### Logging
- Log each collection run with: parks processed, success count, failure count, duration
- Log individual park failures with park_id and error details
- Log API response times for performance monitoring

---

## 7. Future Considerations (Not In Scope Yet)

These are noted for awareness but should NOT be implemented in this phase:

1. **Commercial Lightning Data:** If thunderstorm weather codes prove highly predictive, evaluate Xweather, DTN, or Earth Networks for precise strike data. Cost-benefit analysis needed after initial correlation results.

2. **Blitzortung.org Integration:** Free community lightning network. Would require either becoming a participant (running a detection station) or scraping their map interface. Legal/ToS review needed.

3. **Severe Weather Alerts:** NWS API for US parks, similar services for international. Could provide advance warning of incoming weather.

4. **Forecast Ensemble Data:** Open-Meteo offers ensemble forecasts for uncertainty quantification—useful for probabilistic predictions.

5. **Ride Weather Sensitivity Tags:** Database field to mark rides as wind-sensitive, lightning-sensitive, indoor, etc. Enables segmented analysis.

6. **Real-time Weather Display:** Showing current weather on the Hall of Shame website alongside ride status.

---

## 8. Acceptance Criteria

### Phase 1 Complete When:
- [ ] All database tables created and indexed
- [ ] At least 50 parks have verified coordinates in park_locations
- [ ] OpenMeteoClient successfully retrieves and parses weather data
- [ ] Single park test shows data flowing into weather_observations

### Phase 2 Complete When:
- [ ] Hourly collection running automatically for all parks with coordinates
- [ ] Forecast collection running every 6 hours
- [ ] Error handling prevents single park failures from blocking others
- [ ] Logging shows collection status and any failures

### Phase 3 Complete When:
- [ ] All ~150 parks have coordinates
- [ ] Daily aggregation running automatically
- [ ] Data retention purging old records on schedule
- [ ] System stable for 2+ weeks with minimal intervention

---

## 9. Test Strategy

### 9.1 Unit Tests

**OpenMeteoClient Tests:**
- `testParseCurrentWeatherResponse()` - Verify JSON parsing with mock API response
- `testHandleMalformedResponse()` - Verify error handling for invalid JSON
- `testHandleMissingFields()` - Verify graceful handling of incomplete data
- `testRateLimiterRespected()` - Verify 1 request/second limit enforced
- `testRetryLogicWithExponentialBackoff()` - Verify retry attempts and delays

**Data Validation Tests:**
- `testTemperatureRangeValidation()` - Reject temperatures outside -50°C to +60°C
- `testWindSpeedValidation()` - Reject negative wind speeds
- `testPrecipitationValidation()` - Reject negative precipitation
- `testCoordinateValidation()` - Verify lat/lon within valid ranges

**Daily Aggregation Logic Tests:**
- `testDailyAggregationCalculations()` - Verify MIN/MAX/AVG calculations with known data
- `testThunderstormHoursCounting()` - Verify weather_code filtering for thunderstorms
- `testWindThresholdCounting()` - Verify hours_wind_over_30kmh logic
- `testHandleIncompleteDay()` - Verify behavior with <24 hours of data

### 9.2 Integration Tests

**Database Integration Tests:**
- `testInsertWeatherObservation()` - Verify row insertion and retrieval
- `testUniqueConstraintEnforcement()` - Verify duplicate (park_id, observation_time) rejected
- `testIdempotentUpsert()` - Verify ON DUPLICATE KEY UPDATE works
- `testForecastStorageAndRetrieval()` - Verify forecast data persistence
- `testDSTTransitionHandling()` - **CRITICAL:** Insert observations during DST "fall back" hour
  - Create observations for 1:00 AM, 1:30 AM (first occurrence)
  - Advance clock by 1 hour (DST ends)
  - Create observations for 1:00 AM, 1:30 AM (second occurrence)
  - Verify all 4 observations stored without duplicates or rejections
  - This validates TIMESTAMP correctly handles timezone transitions

**API Integration Tests (with test endpoints or mocks):**
- `testLiveAPICall()` - Make actual Open-Meteo call for single park (smoke test)
- `testCircuitBreakerTriggering()` - Simulate 10 consecutive failures, verify circuit opens
- `testJobRecoveryBackfill()` - Simulate incomplete collection, verify backfill logic

**End-to-End Collection Tests:**
- `testHourlyCollectionCycle()` - Run full collection for 5 test parks, verify data inserted
- `testForecastCollectionCycle()` - Run forecast collection, verify 7 days × 24 hours stored
- `testDataRetentionPurge()` - Insert old data, run purge job, verify deleted

### 9.3 ETL Validation Tests

**Data Quality Checks (run against production data weekly):**
- `validateNoGapsInHourlyData()` - Check for missing hours in past 7 days
- `validateForecastHoursAhead()` - Verify hours_ahead calculated correctly
- `validateDailyAggregateCompleteness()` - Verify daily aggregates exist for all parks
- `validateWeatherCodeDistribution()` - Check for unexpected weather code values
- `validateCoordinateAccuracy()` - Verify park coordinates match known locations

**Correlation Sanity Checks:**
- `validateTemperatureReasonable()` - Check for outliers (e.g., 50°C in Alaska)
- `validateWindGustsGreaterThanSpeed()` - Verify wind_gusts_kmh ≥ wind_speed_kmh
- `validateThunderstormCodeAlignment()` - Check weather_code consistency with precipitation

### 9.4 Test Data Requirements

**Fixed Test Parks (use for all tests):**
1. **Magic Kingdom** (Orlando, FL) - Hot, humid, frequent thunderstorms
2. **Disneyland** (Anaheim, CA) - Mediterranean climate, rarely rains
3. **Cedar Point** (Sandusky, OH) - Four seasons, wind off Lake Erie
4. **Six Flags Magic Mountain** (Valencia, CA) - Windy location, ride closures common
5. **Dollywood** (Pigeon Forge, TN) - Mountain weather, rapid changes

**Mock Weather Scenarios:**
- Clear day: weather_code=0, no precipitation
- Thunderstorm: weather_code=95, high precipitation
- High wind: wind_gusts_kmh=60, simulates ride closures
- DST transition: Test data spanning "fall back" hour

### 9.5 Performance Tests

- `testCollectionDuration()` - Verify 150 parks complete within 3 minutes (serial) or 30 seconds (parallel)
- `testDatabaseWriteThroughput()` - Verify can insert 150 observations/second
- `testQueryPerformance()` - Verify daily aggregate queries return in <100ms

### 9.6 Acceptance Testing

Before declaring each phase complete, run:

**Phase 1 Tests:**
- [ ] All database tables exist and match schema
- [ ] 50+ parks have valid coordinates in park_locations
- [ ] OpenMeteoClient unit tests pass (100% coverage)
- [ ] Single park live API test returns valid data

**Phase 2 Tests:**
- [ ] Hourly collection runs automatically (verify 3 consecutive hours)
- [ ] Forecast collection runs every 6 hours (verify 2 cycles)
- [ ] Retry logic test passes (inject failures, verify retries)
- [ ] Circuit breaker test passes (inject API outage, verify pause)

**Phase 3 Tests:**
- [ ] All 150 parks have coordinates
- [ ] Daily aggregation test passes (verify calculations match manual)
- [ ] Data retention purge test passes (verify old data deleted)
- [ ] DST transition test passes (**CRITICAL**)
- [ ] System stable for 2+ weeks with ≥99% data completeness

---

## 10. Questions for Implementer

1. What is the exact schema of the existing `parks` table? Need to confirm foreign key relationship.

2. Does Queue-Times API provide park coordinates? If so, we can auto-populate park_locations.

3. What scheduling infrastructure exists? (Quartz, cron, Spring Scheduler, etc.)

4. What is the existing logging framework? (Log4j, SLF4J, etc.)

5. Is there an existing HTTP client in the codebase, or should this introduce one?

6. What timezone does the existing ride data use? (UTC, local park time, etc.)
