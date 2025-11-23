# Theme Park Downtime Tracker - Project Specification

## Project Overview

A comprehensive web application that monitors and displays ride downtimes, wait times, and operational trends across North American theme parks. The system collects real-time data from Queue-Times.com API and presents analytics showing park performance, ride reliability, and trend analysis.

**Website:** http://themeparkwaits.com (Shopify integration)

---

## 1. System Architecture

### 1.1 Components

```
┌─────────────────────┐
│  Queue-Times.com    │
│       API           │
└──────────┬──────────┘
           │ Every 10 min
           ▼
┌─────────────────────┐
│   Java Data         │
│   Collector         │
│   (Linux Server)    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   MariaDB           │
│   Database          │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Web Frontend      │
│   (Shopify Site)    │
└─────────────────────┘
```

### 1.2 Technology Stack

- **Backend:** Java (version 11 or higher recommended)
- **Database:** MariaDB (on Linux server)
- **Frontend:** HTML/CSS/JavaScript (Shopify-compatible)
- **API Source:** Queue-Times.com API (free tier)
- **Hosting:** Linux server (self-hosted or VPS)

---

## 2. Data Collection Requirements

### 2.1 Queue-Times API Integration

**API Documentation:** https://queue-times.com/pages/api

#### Available Endpoints:

1. **Get All Parks:**
   - URL: `https://queue-times.com/parks.json`
   - Returns: List of all parks grouped by operator
   - Fields: id, name, country, continent, latitude, longitude, timezone

2. **Get Park Queue Times:**
   - URL: `https://queue-times.com/parks/{park_id}/queue_times.json`
   - Returns: Current status for all rides in a park
   - Fields: ride id, name, is_open, wait_time, last_updated (UTC)

#### Data Collection Rules:

1. **API Updates:** Data is updated by Queue-Times every 5 minutes
2. **Collection Frequency:** Collect data every 10 minutes (6 times/hour)
3. **Geographic Filter:** Only collect data for North American parks (continent = "North America")
4. **Attribution Required:** Display "Powered by Queue-Times.com" with link to https://queue-times.com

### 2.2 Ride Status Logic

**Critical:** The API has a quirk where `is_open` may be false while `wait_time > 0`. Use this logic:

```
computed_is_open = (wait_time > 0) OR (is_open == true AND wait_time == 0)

If wait_time > 0: Ride is DEFINITELY open
If is_open == true AND wait_time == 0: Ride is open but no wait
Otherwise: Ride is closed
```

### 2.3 Park Operating Hours Detection

Parks do not publish operating hours in advance. Detect dynamically:

- **Park Opens:** First API collection where any ride has `computed_is_open = true` or `wait_time > 0`
- **Park Closes:** Last API collection where any ride was operational
- **Track per day:** Each calendar day has one operating session (open time → close time)

---

## 3. Database Design

### 3.1 Data Retention Strategy

**24-Hour Rolling Window for Raw Data:**
- Keep raw 10-minute snapshot data for 24 hours only
- After 24 hours: Calculate daily averages and DELETE raw data
- Store permanent daily/weekly/monthly/yearly summaries

**Storage Growth:**
- Year 1: ~450 MB
- Year 5: ~2 GB
- Year 10: ~4 GB

### 3.2 Core Tables

#### Reference Tables

**park_groups**
- id (INT, PK) - Queue-Times group ID
- name (VARCHAR 255) - e.g., "Cedar Fair", "Disney"
- created_at, updated_at (TIMESTAMP)

**parks**
- id (INT, PK) - Queue-Times park ID
- park_group_id (INT, FK)
- name (VARCHAR 255)
- country (VARCHAR 100)
- continent (VARCHAR 100)
- timezone (VARCHAR 100)
- latitude, longitude (DECIMAL)
- is_north_america (BOOLEAN, COMPUTED) - WHERE continent = 'North America'

**lands**
- id (INT, PK) - Queue-Times land ID
- park_id (INT, FK)
- name (VARCHAR 255)

**rides**
- id (INT, PK) - Queue-Times ride ID
- park_id (INT, FK)
- land_id (INT, FK, nullable)
- name (VARCHAR 255)
- is_active (BOOLEAN) - Track if ride still exists

#### Real-Time Data (24-hour retention)

**ride_status_snapshots**
- id (BIGINT, PK, AUTO_INCREMENT)
- ride_id (INT, FK)
- api_is_open (BOOLEAN) - Raw from API
- api_wait_time (INT) - Raw from API
- computed_is_open (BOOLEAN, COMPUTED) - Using logic above
- api_last_updated (TIMESTAMP) - From API, UTC
- recorded_at (TIMESTAMP) - When we collected it, UTC
- **RETENTION: Delete records older than 24 hours**

**ride_status_changes**
- id (BIGINT, PK, AUTO_INCREMENT)
- ride_id (INT, FK)
- previous_status (BOOLEAN, nullable)
- new_status (BOOLEAN)
- change_time (TIMESTAMP)
- wait_time_at_change (INT)
- duration_minutes (INT) - Time spent in previous status
- **RETENTION: Delete records older than 24 hours**

**park_activity_snapshots**
- id (BIGINT, PK, AUTO_INCREMENT)
- park_id (INT, FK)
- snapshot_time (TIMESTAMP)
- total_rides_tracked (INT)
- rides_open (INT)
- rides_closed (INT)
- rides_with_wait_times (INT) - Count where wait_time > 0
- avg_wait_time (DECIMAL 5,1)
- max_wait_time (INT)
- park_appears_open (BOOLEAN, COMPUTED) - WHERE rides_open > 0 OR rides_with_wait_times > 0
- **RETENTION: Delete records older than 24 hours**

**park_operating_sessions**
- id (BIGINT, PK, AUTO_INCREMENT)
- park_id (INT, FK)
- operating_date (DATE)
- session_start (TIMESTAMP) - First detected activity
- session_end (TIMESTAMP, nullable) - Last detected activity
- total_operating_minutes (INT, COMPUTED)

#### Historical Summary Tables (Permanent)

**daily_ride_stats**
- id (BIGINT, PK, AUTO_INCREMENT)
- ride_id (INT, FK)
- stat_date (DATE)
- park_operating_minutes (INT)
- total_uptime_minutes (INT)
- total_downtime_minutes (INT)
- uptime_percentage (DECIMAL 5,2) - Only during park operating hours
- avg_wait_time (DECIMAL 5,1)
- max_wait_time (INT)
- min_wait_time (INT)
- total_status_changes (INT)
- longest_downtime_minutes (INT)
- **UNIQUE KEY:** (ride_id, stat_date)

**daily_park_stats**
- id (BIGINT, PK, AUTO_INCREMENT)
- park_id (INT, FK)
- stat_date (DATE)
- park_operating_minutes (INT)
- total_rides_tracked (INT)
- operational_rides (INT) - Opened at least once
- never_opened_rides (INT)
- avg_park_uptime (DECIMAL 5,2)
- total_downtime_hours (DECIMAL 6,2)
- worst_performing_ride_id (INT, FK, nullable)
- best_performing_ride_id (INT, FK, nullable)
- **UNIQUE KEY:** (park_id, stat_date)

**weekly_ride_stats, monthly_ride_stats, yearly_ride_stats**
- Similar structure to daily_ride_stats
- Calculated from daily aggregates
- Include trend analysis fields

**weekly_park_stats, monthly_park_stats, yearly_park_stats**
- Similar structure to daily_park_stats
- Calculated from daily aggregates

### 3.3 Critical Indexes

```sql
-- For current status queries
CREATE INDEX idx_ride_computed_status ON ride_status_snapshots (ride_id, computed_is_open, recorded_at);
CREATE INDEX idx_park_open ON park_activity_snapshots (park_id, park_appears_open, snapshot_time);

-- For cleanup jobs
CREATE INDEX idx_recorded_date ON ride_status_snapshots (DATE(recorded_at));

-- For uptime calculations
CREATE INDEX idx_ride_date_uptime ON daily_ride_stats (ride_id, stat_date, uptime_percentage);
CREATE INDEX idx_park_downtime ON daily_park_stats (total_downtime_hours, stat_date);
```

---

## 4. Java Data Collector Implementation

### 4.1 Application Structure

```
src/
├── main/
│   ├── java/
│   │   ├── com.themeparkwaits/
│   │   │   ├── collector/
│   │   │   │   ├── QueueTimesApiClient.java
│   │   │   │   ├── DataCollectionService.java
│   │   │   │   └── ParkDiscoveryService.java
│   │   │   ├── processor/
│   │   │   │   ├── StatusChangeDetector.java
│   │   │   │   ├── OperatingHoursDetector.java
│   │   │   │   └── DailyAggregationService.java
│   │   │   ├── model/
│   │   │   │   ├── Park.java
│   │   │   │   ├── Ride.java
│   │   │   │   └── RideStatus.java
│   │   │   ├── repository/
│   │   │   │   ├── ParkRepository.java
│   │   │   │   ├── RideRepository.java
│   │   │   │   └── StatusRepository.java
│   │   │   ├── scheduler/
│   │   │   │   └── ScheduledTasks.java
│   │   │   └── Application.java
│   │   └── resources/
│   │       └── application.properties
└── test/
```

### 4.2 Key Components

#### QueueTimesApiClient
- HTTP client for Queue-Times API
- Methods:
  - `getAllParks()` - Get park list
  - `getParkQueueTimes(int parkId)` - Get ride statuses
- Handle rate limiting and errors gracefully
- Retry logic with exponential backoff

#### DataCollectionService
- Main collection orchestrator
- Run every 10 minutes via scheduler
- Steps:
  1. For each North American park
  2. Call API to get current ride statuses
  3. Insert into ride_status_snapshots
  4. Update park_activity_snapshots
  5. Trigger status change detection

#### StatusChangeDetector
- Compare new status with previous status
- If status changed: Insert into ride_status_changes
- Calculate duration in previous status

#### OperatingHoursDetector
- Run after each collection
- Detect park open/close times
- Update park_operating_sessions table

#### DailyAggregationService
- Run daily at 12:10 AM
- Calculate daily statistics from 24-hour raw data
- Steps:
  1. Calculate daily_ride_stats from raw snapshots
  2. Calculate daily_park_stats
  3. Calculate weekly/monthly/yearly if applicable
  4. DELETE raw data older than 24 hours

### 4.3 Scheduled Jobs

```java
@Scheduled(fixedRate = 600000) // Every 10 minutes
public void collectData() {
    // Run data collection
}

@Scheduled(cron = "0 10 0 * * *") // 12:10 AM daily
public void aggregateAndCleanup() {
    // 1. Calculate daily statistics
    // 2. Delete old raw data
}

@Scheduled(cron = "0 0 1 * * MON") // 1 AM every Monday
public void calculateWeeklyStats() {
    // Calculate weekly aggregates
}

@Scheduled(cron = "0 0 2 1 * *") // 2 AM first day of month
public void calculateMonthlyStats() {
    // Calculate monthly aggregates
}
```

### 4.4 Configuration (application.properties)

```properties
# Database Configuration
spring.datasource.url=jdbc:mariadb://localhost:3306/themeparkwaits
spring.datasource.username=collector
spring.datasource.password=${DB_PASSWORD}
spring.datasource.driver-class-name=org.mariadb.jdbc.Driver

# JPA Configuration
spring.jpa.hibernate.ddl-auto=validate
spring.jpa.show-sql=false

# Queue-Times API
queuetimes.api.base.url=https://queue-times.com
queuetimes.api.retry.attempts=3
queuetimes.api.retry.delay.ms=5000

# Collection Settings
collection.frequency.minutes=10
collection.retention.hours=24

# Logging
logging.level.com.themeparkwaits=INFO
logging.file.name=logs/collector.log
```

---

## 5. Frontend Requirements

### 5.1 Display Requirements

#### Page 1: Theme Parks - Highest Downtime

**Filters:**
- Time Period: Today | 7 Days | 30 Days

**Table Columns:**
1. Rank
2. Park Name
3. Location (City, State/Province)
4. Downtime Hours
5. Closed Rides (current count)
6. Open Rides (current count)
7. Trend (↗ +X% worsening | ↘ -X% improving | → 0% stable)
8. Status Badge (CRITICAL | MAINTENANCE | OPERATIONAL)

**Sorting:** By downtime hours (highest first)

#### Page 2: Ride Downtime - Worst Performers

**Filters:**
- Time Period: Today | 7 Days | 30 Days

**Table Columns:**
1. Rank
2. Ride Name
3. Park Name
4. Downtime Hours
5. Uptime Percentage
6. Incidents (status change count)
7. Trend (↗ +X% | ↘ -X% | → 0%)

**Sorting:** By downtime hours (highest first)

#### Page 3: Wait Times - Longest Waits

**Filters:**
- Time Period: Live | 7 Day Average | Peak Times

**Table Columns:**
1. Rank
2. Ride Name
3. Park Name
4. Current Wait (minutes)
5. Average Wait 7D (minutes)
6. Status Badge (OPERATIONAL | DOWN | MAINTENANCE)

**Sorting:** By current wait time (highest first)

### 5.2 Charting Requirements

**Required Chart Types:**

1. **Hourly Charts** (last 24 hours)
   - Ride uptime percentage by hour
   - Park rides open vs closed by hour
   - Average wait times by hour

2. **Daily Charts** (last 7, 30, 90 days)
   - Daily uptime percentage trends
   - Daily average wait times

3. **Weekly Charts** (last 12 weeks)
   - Weekly uptime averages
   - Weekly downtime totals

4. **Monthly Charts** (last 12 months)
   - Monthly uptime trends
   - Month-over-month comparisons

5. **Yearly Charts** (multi-year)
   - Annual performance comparison
   - Long-term trends

**Chart Library Suggestions:**
- Chart.js (recommended for Shopify compatibility)
- Recharts (if using React)
- D3.js (for custom visualizations)

### 5.3 Shopify Integration Options

#### Option A: Iframe Embed (Recommended)
**Pros:**
- Independent hosting of Java/DB backend
- Real-time data updates without Shopify limitations
- Full control over technology stack
- Easy to update without Shopify theme changes

**Cons:**
- Requires separate hosting
- Potential iframe sizing/responsive issues

**Implementation:**
```html
<iframe 
  src="https://data.themeparkwaits.com/dashboard" 
  width="100%" 
  height="800px"
  frameborder="0"
  scrolling="auto">
</iframe>
```

#### Option B: REST API + Liquid Templates
**Pros:**
- Native Shopify integration
- No iframe concerns

**Cons:**
- Liquid template limitations
- More complex data fetching
- Potential performance issues

**Implementation:**
- Create REST API endpoints from Java backend
- Use JavaScript fetch() to load data
- Render with JavaScript in Shopify theme

**Recommended:** Option A (Iframe) for initial implementation

### 5.4 Required Attribution

Display prominently on every page:
```html
<div style="text-align: center; padding: 10px;">
  Data powered by <a href="https://queue-times.com" target="_blank">Queue-Times.com</a>
</div>
```

---

## 6. Data Flow Summary

### 6.1 Real-Time Collection (Every 10 Minutes)

```
1. API Call → Queue-Times API
2. Parse Response → Extract ride statuses
3. Store Raw Data → ride_status_snapshots
4. Detect Changes → ride_status_changes (if status changed)
5. Update Park Activity → park_activity_snapshots
6. Detect Operating Hours → park_operating_sessions
```

### 6.2 Daily Processing (12:10 AM)

```
1. Query 24-hour raw data
2. Calculate daily statistics:
   - Uptime percentage (only during park operating hours)
   - Average wait times
   - Status change counts
   - Min/max wait times
3. Insert into daily_ride_stats and daily_park_stats
4. Calculate weekly stats (if Monday)
5. Calculate monthly stats (if 1st of month)
6. DELETE raw data older than 24 hours
```

### 6.3 Frontend Data Access

```
1. User visits themeparkwaits.com
2. JavaScript requests data from API endpoints
3. Backend queries pre-calculated summary tables
4. Response time: <100ms
5. Render tables and charts
```

---

## 7. API Endpoints for Frontend

### 7.1 Park Rankings

**GET /api/parks/downtime**
- Query params: `period` (today|7days|30days)
- Response: JSON array of parks with downtime metrics
- Sorted by total_downtime_hours DESC

**GET /api/parks/{parkId}/details**
- Response: Park details with current status

### 7.2 Ride Performance

**GET /api/rides/downtime**
- Query params: `period` (today|7days|30days)
- Response: JSON array of rides with downtime metrics
- Sorted by total_downtime_hours DESC

**GET /api/rides/waittimes**
- Query params: `period` (live|7days|peak)
- Response: JSON array of rides with wait time data
- Sorted by current_wait_time DESC

### 7.3 Chart Data

**GET /api/rides/{rideId}/history**
- Query params: `granularity` (hourly|daily|weekly|monthly), `period` (24h|7d|30d|90d|1y)
- Response: Time-series data for charts

**GET /api/parks/{parkId}/history**
- Query params: Same as above
- Response: Time-series data for park-wide metrics

---

## 8. Performance Requirements

### 8.1 Collection Performance
- API call latency: <2 seconds per park
- Total collection time: <5 minutes for all parks
- Database insert time: <1 second per batch

### 8.2 Query Performance
- Current status queries: <50ms
- Historical data queries: <100ms
- Chart data generation: <200ms
- Daily aggregation: <5 minutes

### 8.3 Availability
- Data collection uptime: 99%+ (allow for brief API outages)
- Website availability: 99.9%+
- Maximum data freshness: 10 minutes

---

## 9. Error Handling

### 9.1 API Errors
- **Retry logic:** 3 attempts with exponential backoff
- **Timeout:** 10 seconds per request
- **Log failures:** Record in api_collection_log table
- **Graceful degradation:** Continue with other parks if one fails

### 9.2 Database Errors
- **Connection pooling:** Maintain connection pool
- **Transaction management:** Rollback on errors
- **Deadlock handling:** Retry with delay

### 9.3 Data Quality
- **Validation:** Reject invalid API responses
- **Sanity checks:** Flag unrealistic values (e.g., wait_time > 300)
- **Missing data:** Handle nulls gracefully

---

## 10. Monitoring & Logging

### 10.1 Application Logging
- Collection success/failure rates
- API response times
- Processing times
- Error details with stack traces

### 10.2 Database Monitoring
- Table sizes
- Query performance
- Connection pool status
- Daily cleanup success

### 10.3 Alerts
- Failed collections (>3 consecutive failures)
- Database connection issues
- Disk space warnings (>80% full)
- Daily aggregation failures

---

## 11. Deployment Checklist

### 11.1 Server Setup
- [ ] Linux server provisioned (VPS or dedicated)
- [ ] MariaDB installed and configured
- [ ] Java 11+ installed
- [ ] Application user created with appropriate permissions
- [ ] Firewall configured (allow only necessary ports)

### 11.2 Database Setup
- [ ] Database created: `themeparkwaits`
- [ ] User created with appropriate grants
- [ ] Schema created (run DDL scripts)
- [ ] Indexes created
- [ ] Backup strategy configured

### 11.3 Application Deployment
- [ ] JAR built from source
- [ ] Configuration file created (application.properties)
- [ ] Systemd service configured for auto-start
- [ ] Logging directory created
- [ ] Log rotation configured

### 11.4 Initial Data Load
- [ ] Run park discovery (populate parks table)
- [ ] Run initial collection
- [ ] Verify data in database
- [ ] Test API endpoints

### 11.5 Shopify Integration
- [ ] Frontend hosted (same server or CDN)
- [ ] Iframe or API integration configured
- [ ] Attribution text added
- [ ] Test on themeparkwaits.com

---

## 12. Future Enhancements (Phase 2)

1. **Weather Integration:** Correlate downtime with weather conditions
2. **Predictive Analytics:** ML models to predict downtime
3. **Notifications:** Alert users to ride status changes
4. **Mobile App:** Native iOS/Android apps
5. **User Accounts:** Save favorite parks/rides
6. **Social Features:** Share wait times, reviews
7. **Historical Comparisons:** Year-over-year trends
8. **Park Efficiency Scores:** Composite reliability ratings

---

## 13. Success Metrics

### 13.1 Data Collection
- **Coverage:** 80+ North American parks tracked
- **Data freshness:** 95%+ collections succeed within 10-minute window
- **Uptime:** 99%+ collection service availability

### 13.2 Website Performance
- **Page load:** <2 seconds
- **API response:** <100ms average
- **Uptime:** 99.9%+ frontend availability

### 13.3 User Engagement
- **Visitors:** Track unique visitors per month
- **Page views:** Track most popular features
- **Return visitors:** Track user retention

---

## 14. Contact & Attribution

**Project Owner:** themeparkwaits.com  
**Data Source:** Queue-Times.com (https://queue-times.com)  
**API Documentation:** https://queue-times.com/pages/api  
**Support Patreon:** https://www.patreon.com/queue_times (optional but encouraged)

---

## Appendix A: Estimated Infrastructure Costs

### Year 1 Costs:
- VPS Server (4GB RAM, 80GB SSD): $10-20/month = $120-240/year
- Domain/SSL: $15/year
- **Total: ~$135-255/year**

### Year 5 Costs:
- Same server (database ~2GB)
- **Total: ~$135-255/year**

### Notes:
- Free Queue-Times API (with attribution)
- Free Shopify integration (assuming existing Shopify plan)
- Very cost-effective at scale