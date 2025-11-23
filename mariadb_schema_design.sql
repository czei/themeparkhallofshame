-- MariaDB Schema for Theme Park Uptime Tracking
-- Optimized for dynamic operating hours detection and ride status inference

-- ============================================================================
-- REFERENCE TABLES (Static/Semi-Static Data)
-- ============================================================================

-- Park Groups (e.g., Cedar Fair, Disney, Universal)
CREATE TABLE park_groups (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Parks
CREATE TABLE parks (
    id INT PRIMARY KEY,  -- Use Queue-Times park ID directly
    park_group_id INT,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    continent VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    timezone VARCHAR(100) NOT NULL,
    is_north_america BOOLEAN GENERATED ALWAYS AS (continent = 'North America') STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_group_id) REFERENCES park_groups(id),
    INDEX idx_north_america (is_north_america),
    INDEX idx_country (country)
);

-- Lands (themed areas within parks)
CREATE TABLE lands (
    id INT PRIMARY KEY,  -- Use Queue-Times land ID directly
    park_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    INDEX idx_park (park_id)
);

-- Rides
CREATE TABLE rides (
    id INT PRIMARY KEY,  -- Use Queue-Times ride ID directly
    park_id INT NOT NULL,
    land_id INT,
    name VARCHAR(255) NOT NULL,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,  -- Track if ride still exists
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    FOREIGN KEY (land_id) REFERENCES lands(id) ON DELETE SET NULL,
    INDEX idx_park (park_id),
    INDEX idx_active (is_active),
    INDEX idx_park_active (park_id, is_active)
);

-- ============================================================================
-- OPERATIONAL DATA TABLES
-- ============================================================================

-- Park Operating Sessions (Dynamically Detected)
-- Each record represents one operating period (open to close)
CREATE TABLE park_operating_sessions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    operating_date DATE NOT NULL,
    session_start TIMESTAMP NOT NULL,    -- First ride activity detected (UTC)
    session_end TIMESTAMP,               -- Last ride activity detected (UTC) - NULL if still open
    total_operating_minutes INT GENERATED ALWAYS AS (
        CASE 
            WHEN session_end IS NOT NULL 
            THEN TIMESTAMPDIFF(MINUTE, session_start, session_end)
            ELSE NULL 
        END
    ) STORED,
    rides_active_at_start INT DEFAULT 0,  -- Number of rides open when session started
    max_concurrent_rides INT DEFAULT 0,   -- Peak number of rides open simultaneously
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    INDEX idx_park_date (park_id, operating_date),
    INDEX idx_session_times (session_start, session_end),
    INDEX idx_operating_date (operating_date)
);

-- Ride Status Snapshots (Every API call - ~5 minute intervals)
-- Raw data from Queue-Times API with computed actual status
CREATE TABLE ride_status_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    api_is_open BOOLEAN NOT NULL,         -- Raw is_open from API
    api_wait_time INT,                    -- Raw wait_time from API (NULL if closed)
    computed_is_open BOOLEAN GENERATED ALWAYS AS (
        CASE 
            WHEN api_wait_time > 0 THEN TRUE  -- If wait_time > 0, ride is definitely open
            WHEN api_is_open = TRUE THEN TRUE -- If API says open and wait_time = 0, still open
            ELSE FALSE                         -- Otherwise closed
        END
    ) STORED,
    api_last_updated TIMESTAMP NOT NULL,  -- From API (UTC)
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we collected it (UTC)
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    INDEX idx_ride_recorded (ride_id, recorded_at),
    INDEX idx_ride_computed_status (ride_id, computed_is_open, recorded_at),
    INDEX idx_recorded_date (DATE(recorded_at)),
    INDEX idx_computed_status_time (computed_is_open, recorded_at)
);

-- Ride Status Changes (State Transitions Only)
-- Optimized table for uptime calculations - only stores when computed status changes
CREATE TABLE ride_status_changes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    previous_status BOOLEAN,              -- NULL for first record
    new_status BOOLEAN NOT NULL,          -- TRUE = open, FALSE = closed
    change_time TIMESTAMP NOT NULL,       -- When the change occurred (UTC)
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we detected it
    wait_time_at_change INT,              -- Wait time when status changed
    duration_minutes INT,                 -- Minutes in previous status (NULL for first record)
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    INDEX idx_ride_time (ride_id, change_time),
    INDEX idx_change_date (DATE(change_time)),
    INDEX idx_status_change (new_status, change_time)
);

-- Park Activity Snapshots (Aggregated per collection)
-- Summary of park-wide activity at each API collection time
CREATE TABLE park_activity_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rides_tracked INT NOT NULL,
    rides_open INT NOT NULL,
    rides_closed INT NOT NULL,
    rides_with_wait_times INT NOT NULL,    -- Rides with wait_time > 0
    avg_wait_time DECIMAL(5,1),           -- Average wait time of open rides
    max_wait_time INT,                    -- Highest wait time recorded
    park_appears_open BOOLEAN GENERATED ALWAYS AS (
        rides_open > 0 OR rides_with_wait_times > 0
    ) STORED,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    INDEX idx_park_time (park_id, snapshot_time),
    INDEX idx_park_open (park_id, park_appears_open, snapshot_time),
    INDEX idx_snapshot_date (DATE(snapshot_time))
);

-- ============================================================================
-- TIME-SERIES STATISTICS TABLES (For Charting)
-- ============================================================================

-- Hourly Ride Statistics
-- Granular hourly data for detailed charting
CREATE TABLE hourly_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    stat_hour TIMESTAMP NOT NULL,        -- Hour bucket (e.g., '2024-01-15 14:00:00')
    local_hour TIMESTAMP NOT NULL,       -- Hour in park's local timezone
    uptime_minutes INT DEFAULT 0,        -- Minutes ride was open this hour
    downtime_minutes INT DEFAULT 0,      -- Minutes ride was closed this hour
    uptime_percentage DECIMAL(5,2),      -- Uptime % for this hour
    avg_wait_time DECIMAL(5,1),          -- Average wait time when open
    min_wait_time INT,                   -- Minimum wait time this hour
    max_wait_time INT,                   -- Maximum wait time this hour
    median_wait_time DECIMAL(5,1),       -- Median wait time this hour
    total_status_changes INT DEFAULT 0,  -- Number of open/close transitions
    samples_collected INT DEFAULT 0,     -- Number of API samples this hour
    was_park_open BOOLEAN DEFAULT FALSE, -- Was park operating this hour
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_hour (ride_id, stat_hour),
    INDEX idx_hour (stat_hour),
    INDEX idx_local_hour (local_hour),
    INDEX idx_ride_hour (ride_id, stat_hour),
    INDEX idx_uptime_hour (uptime_percentage, stat_hour),
    INDEX idx_wait_time_hour (avg_wait_time, stat_hour)
);

-- Hourly Park Statistics
-- Park-wide hourly metrics for operational dashboards
CREATE TABLE hourly_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    stat_hour TIMESTAMP NOT NULL,        -- Hour bucket (e.g., '2024-01-15 14:00:00')
    local_hour TIMESTAMP NOT NULL,       -- Hour in park's local timezone
    total_rides_tracked INT DEFAULT 0,   -- Total rides being monitored
    rides_open INT DEFAULT 0,            -- Number of rides open
    rides_closed INT DEFAULT 0,          -- Number of rides closed
    rides_percentage_open DECIMAL(5,2),  -- Percentage of rides open
    avg_wait_time_all_rides DECIMAL(5,1), -- Average wait across all open rides
    avg_wait_time_weighted DECIMAL(5,1), -- Wait time weighted by ride popularity
    min_wait_time_park DECIMAL(5,1),     -- Shortest wait in park
    max_wait_time_park DECIMAL(5,1),     -- Longest wait in park
    total_guest_minutes_saved INT,       -- Est. guest time saved vs max waits
    park_operational BOOLEAN DEFAULT FALSE, -- Was park operating this hour
    weather_impact_score DECIMAL(3,2),   -- Future: weather impact on operations
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    UNIQUE KEY unique_park_hour (park_id, stat_hour),
    INDEX idx_hour (stat_hour),
    INDEX idx_local_hour (local_hour),
    INDEX idx_park_hour (park_id, stat_hour),
    INDEX idx_operational (park_operational, stat_hour),
    INDEX idx_rides_open (rides_percentage_open, stat_hour)
);

-- ============================================================================
-- CALCULATED STATISTICS TABLES (For Performance)
-- ============================================================================

-- Daily Ride Statistics
-- Pre-calculated daily metrics for fast queries
CREATE TABLE daily_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    stat_date DATE NOT NULL,
    park_operating_minutes INT,           -- Total minutes park was detected as open
    total_downtime_minutes INT,           -- Minutes ride was closed during park operation
    total_uptime_minutes INT,             -- Minutes ride was open during park operation
    uptime_percentage DECIMAL(5,2),      -- Uptime % during park operating hours only
    total_status_changes INT,             -- Number of open/close transitions
    avg_wait_time DECIMAL(5,1),          -- Average wait time when open
    max_wait_time INT,                   -- Maximum wait time recorded
    min_wait_time INT,                   -- Minimum wait time when open (excluding 0)
    first_open_time TIMESTAMP,          -- First time ride opened that day
    last_close_time TIMESTAMP,          -- Last time ride closed that day
    longest_downtime_minutes INT,       -- Longest continuous downtime period
    total_operating_periods INT,        -- Number of times ride opened during day
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_date (ride_id, stat_date),
    INDEX idx_date (stat_date),
    INDEX idx_uptime (uptime_percentage),
    INDEX idx_ride_date_uptime (ride_id, stat_date, uptime_percentage),
    INDEX idx_downtime_rank (total_downtime_minutes, stat_date)
);

-- Daily Park Statistics
-- Aggregated park-level metrics
CREATE TABLE daily_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    stat_date DATE NOT NULL,
    park_operating_minutes INT,           -- Minutes park was detected as open
    total_rides_tracked INT,              -- Total rides monitored that day
    operational_rides INT,                -- Rides that opened at least once
    never_opened_rides INT,               -- Rides that never opened
    avg_park_uptime DECIMAL(5,2),        -- Average uptime across all rides
    median_park_uptime DECIMAL(5,2),     -- Median uptime across all rides
    total_downtime_hours DECIMAL(6,2),   -- Total downtime hours across all rides
    worst_performing_ride_id INT,        -- Ride with lowest uptime
    worst_uptime_percentage DECIMAL(5,2), -- Uptime of worst performing ride
    best_performing_ride_id INT,         -- Ride with highest uptime
    best_uptime_percentage DECIMAL(5,2), -- Uptime of best performing ride
    park_first_activity TIMESTAMP,      -- First detected ride activity
    park_last_activity TIMESTAMP,       -- Last detected ride activity
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    FOREIGN KEY (worst_performing_ride_id) REFERENCES rides(id) ON DELETE SET NULL,
    FOREIGN KEY (best_performing_ride_id) REFERENCES rides(id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_date (park_id, stat_date),
    INDEX idx_date (stat_date),
    INDEX idx_downtime (total_downtime_hours),
    INDEX idx_avg_uptime (avg_park_uptime)
);

-- Weekly Ride Statistics
-- Aggregated weekly data for trend analysis
CREATE TABLE weekly_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    week_start_date DATE NOT NULL,       -- Monday of the week
    week_end_date DATE NOT NULL,         -- Sunday of the week
    year_week VARCHAR(7) NOT NULL,       -- Format: '2024-03' (ISO week)
    total_operating_hours DECIMAL(6,2),  -- Hours park was open this week
    total_uptime_hours DECIMAL(6,2),     -- Hours ride was operational
    total_downtime_hours DECIMAL(6,2),   -- Hours ride was down during park hours
    uptime_percentage DECIMAL(5,2),      -- Weekly uptime percentage
    avg_wait_time DECIMAL(5,1),          -- Average wait time for the week
    median_wait_time DECIMAL(5,1),       -- Median wait time for the week
    peak_wait_time INT,                  -- Highest wait time this week
    peak_wait_day ENUM('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'),
    total_status_changes INT,            -- Total open/close transitions
    longest_downtime_hours DECIMAL(4,2), -- Longest continuous downtime
    busiest_day ENUM('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_week (ride_id, week_start_date),
    INDEX idx_week (week_start_date),
    INDEX idx_year_week (year_week),
    INDEX idx_uptime_week (uptime_percentage, week_start_date),
    INDEX idx_wait_time_week (avg_wait_time, week_start_date)
);

-- Monthly Ride Statistics
-- Month-over-month performance tracking
CREATE TABLE monthly_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    year_month VARCHAR(7) NOT NULL,      -- Format: '2024-03'
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    total_operating_hours DECIMAL(7,2),
    total_uptime_hours DECIMAL(7,2),
    total_downtime_hours DECIMAL(7,2),
    uptime_percentage DECIMAL(5,2),
    avg_wait_time DECIMAL(5,1),
    median_wait_time DECIMAL(5,1),
    peak_wait_time INT,
    total_status_changes INT,
    maintenance_days INT,                -- Days with extended downtime
    weather_impact_days INT,             -- Days likely affected by weather
    seasonal_performance_score DECIMAL(4,2), -- Performance relative to season avg
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_month (ride_id, year_month),
    INDEX idx_year_month (year_month),
    INDEX idx_month_start (month_start_date),
    INDEX idx_uptime_month (uptime_percentage, month_start_date),
    INDEX idx_seasonal_score (seasonal_performance_score, year_month)
);

-- Quarterly Ride Statistics
-- Quarterly business reporting and trend analysis
CREATE TABLE quarterly_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    year_quarter VARCHAR(7) NOT NULL,    -- Format: '2024-Q1'
    quarter_start_date DATE NOT NULL,
    quarter_end_date DATE NOT NULL,
    total_operating_hours DECIMAL(8,2),
    total_uptime_hours DECIMAL(8,2),
    total_downtime_hours DECIMAL(8,2),
    uptime_percentage DECIMAL(5,2),
    avg_wait_time DECIMAL(5,1),
    median_wait_time DECIMAL(5,1),
    peak_wait_time INT,
    total_status_changes INT,
    major_maintenance_events INT,
    guest_satisfaction_score DECIMAL(3,2), -- If available from external data
    reliability_trend ENUM('improving','stable','declining','unknown'),
    cost_impact_estimate DECIMAL(10,2), -- Estimated cost of downtime
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_quarter (ride_id, year_quarter),
    INDEX idx_year_quarter (year_quarter),
    INDEX idx_quarter_start (quarter_start_date),
    INDEX idx_uptime_quarter (uptime_percentage, quarter_start_date),
    INDEX idx_reliability_trend (reliability_trend, year_quarter)
);

-- Yearly Ride Statistics
-- Annual performance summaries and long-term trends
CREATE TABLE yearly_ride_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id INT NOT NULL,
    year INT NOT NULL,
    total_operating_hours DECIMAL(9,2),
    total_uptime_hours DECIMAL(9,2),
    total_downtime_hours DECIMAL(9,2),
    uptime_percentage DECIMAL(5,2),
    avg_wait_time DECIMAL(5,1),
    median_wait_time DECIMAL(5,1),
    peak_wait_time INT,
    total_status_changes INT,
    best_month VARCHAR(7),               -- Month with highest uptime
    worst_month VARCHAR(7),              -- Month with lowest uptime
    total_maintenance_hours DECIMAL(7,2),
    guest_throughput_estimate BIGINT,   -- Estimated guests served
    ride_age_years DECIMAL(4,1),        -- Age of ride (if known)
    major_refurbishments INT,            -- Number of major updates
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (ride_id) REFERENCES rides(id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride_year (ride_id, year),
    INDEX idx_year (year),
    INDEX idx_uptime_year (uptime_percentage, year),
    INDEX idx_age_performance (ride_age_years, uptime_percentage)
);

-- Weekly Park Statistics
-- Park-wide weekly operational metrics
CREATE TABLE weekly_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    year_week VARCHAR(7) NOT NULL,
    total_operating_hours DECIMAL(6,2),
    avg_rides_open_percentage DECIMAL(5,2),
    avg_park_wait_time DECIMAL(5,1),
    peak_park_wait_time DECIMAL(5,1),
    total_park_downtime_hours DECIMAL(7,2),
    worst_performing_ride_id INT,
    best_performing_ride_id INT,
    guest_satisfaction_week DECIMAL(3,2),
    weather_impact_score DECIMAL(3,2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    FOREIGN KEY (worst_performing_ride_id) REFERENCES rides(id) ON DELETE SET NULL,
    FOREIGN KEY (best_performing_ride_id) REFERENCES rides(id) ON DELETE SET NULL,
    UNIQUE KEY unique_park_week (park_id, week_start_date),
    INDEX idx_week (week_start_date),
    INDEX idx_year_week (year_week),
    INDEX idx_rides_open (avg_rides_open_percentage, week_start_date)
);

-- Monthly Park Statistics  
-- Monthly park performance for business reporting
CREATE TABLE monthly_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    total_operating_hours DECIMAL(7,2),
    avg_rides_open_percentage DECIMAL(5,2),
    avg_park_wait_time DECIMAL(5,1),
    median_park_wait_time DECIMAL(5,1),
    peak_park_wait_time DECIMAL(5,1),
    total_park_downtime_hours DECIMAL(8,2),
    operational_efficiency_score DECIMAL(5,2),
    guest_throughput_estimate BIGINT,
    revenue_impact_estimate DECIMAL(12,2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    UNIQUE KEY unique_park_month (park_id, year_month),
    INDEX idx_year_month (year_month),
    INDEX idx_month_start (month_start_date),
    INDEX idx_efficiency (operational_efficiency_score, month_start_date)
);

-- Quarterly Park Statistics
-- Quarterly business metrics and strategic planning data
CREATE TABLE quarterly_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    year_quarter VARCHAR(7) NOT NULL,
    quarter_start_date DATE NOT NULL,
    quarter_end_date DATE NOT NULL,
    total_operating_hours DECIMAL(8,2),
    avg_rides_open_percentage DECIMAL(5,2),
    avg_park_wait_time DECIMAL(5,1),
    peak_park_wait_time DECIMAL(5,1),
    total_park_downtime_hours DECIMAL(9,2),
    operational_efficiency_score DECIMAL(5,2),
    guest_satisfaction_score DECIMAL(3,2),
    competitive_ranking INT,             -- Ranking vs other parks
    maintenance_cost_estimate DECIMAL(12,2),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    UNIQUE KEY unique_park_quarter (park_id, year_quarter),
    INDEX idx_year_quarter (year_quarter),
    INDEX idx_quarter_start (quarter_start_date),
    INDEX idx_ranking (competitive_ranking, year_quarter)
);

-- Yearly Park Statistics
-- Annual park performance and long-term strategic metrics
CREATE TABLE yearly_park_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    year INT NOT NULL,
    total_operating_days INT,
    total_operating_hours DECIMAL(9,2),
    avg_rides_open_percentage DECIMAL(5,2),
    avg_park_wait_time DECIMAL(5,1),
    peak_park_wait_time DECIMAL(5,1),
    total_park_downtime_hours DECIMAL(10,2),
    operational_efficiency_score DECIMAL(5,2),
    guest_satisfaction_score DECIMAL(3,2),
    total_guest_visits_estimate BIGINT,
    annual_revenue_impact DECIMAL(15,2),
    major_capital_investments DECIMAL(15,2),
    new_rides_added INT,
    rides_retired INT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    UNIQUE KEY unique_park_year (park_id, year),
    INDEX idx_year (year),
    INDEX idx_efficiency_year (operational_efficiency_score, year),
    INDEX idx_guest_visits (total_guest_visits_estimate, year)
);

-- ============================================================================
-- DATA COLLECTION TRACKING
-- ============================================================================

-- API Collection Log
-- Track API calls and data quality
CREATE TABLE api_collection_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    park_id INT NOT NULL,
    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rides_in_response INT,               -- Number of rides in API response
    rides_with_wait_times INT,           -- Rides reporting wait times > 0
    rides_marked_open INT,               -- Rides with is_open = true
    rides_computed_open INT,             -- Rides determined to be actually open
    new_rides_discovered INT,            -- New rides found this collection
    api_response_time_ms INT,            -- API response time
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    
    FOREIGN KEY (park_id) REFERENCES parks(id) ON DELETE CASCADE,
    INDEX idx_park_time (park_id, collection_time),
    INDEX idx_success_time (success, collection_time)
);

-- ============================================================================
-- STORED PROCEDURES FOR TIME-SERIES CALCULATIONS
-- ============================================================================

DELIMITER //

-- Calculate hourly statistics for rides
CREATE PROCEDURE calculate_hourly_ride_stats(p_ride_id INT, p_stat_hour TIMESTAMP)
BEGIN
    DECLARE v_park_id INT;
    DECLARE v_timezone VARCHAR(100);
    DECLARE v_local_hour TIMESTAMP;
    DECLARE v_uptime_minutes INT DEFAULT 0;
    DECLARE v_downtime_minutes INT DEFAULT 0;
    DECLARE v_was_park_open BOOLEAN DEFAULT FALSE;
    
    -- Get park info for timezone conversion
    SELECT r.park_id, p.timezone INTO v_park_id, v_timezone
    FROM rides r 
    JOIN parks p ON r.park_id = p.id 
    WHERE r.id = p_ride_id;
    
    -- Convert to local time (simplified - would need proper timezone handling)
    SET v_local_hour = p_stat_hour;
    
    -- Check if park was operating this hour
    SELECT COUNT(*) > 0 INTO v_was_park_open
    FROM park_activity_snapshots
    WHERE park_id = v_park_id
        AND snapshot_time >= p_stat_hour 
        AND snapshot_time < DATE_ADD(p_stat_hour, INTERVAL 1 HOUR)
        AND park_appears_open = TRUE;
    
    -- Calculate uptime/downtime minutes during this hour
    SELECT 
        COALESCE(SUM(CASE WHEN computed_is_open = TRUE THEN 5 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN computed_is_open = FALSE THEN 5 ELSE 0 END), 0)
    INTO v_uptime_minutes, v_downtime_minutes
    FROM ride_status_snapshots
    WHERE ride_id = p_ride_id
        AND recorded_at >= p_stat_hour 
        AND recorded_at < DATE_ADD(p_stat_hour, INTERVAL 1 HOUR);
    
    -- Insert/update hourly stats
    INSERT INTO hourly_ride_stats (
        ride_id, stat_hour, local_hour, uptime_minutes, downtime_minutes,
        uptime_percentage, avg_wait_time, min_wait_time, max_wait_time,
        total_status_changes, samples_collected, was_park_open
    )
    SELECT 
        p_ride_id,
        p_stat_hour,
        v_local_hour,
        v_uptime_minutes,
        v_downtime_minutes,
        CASE 
            WHEN (v_uptime_minutes + v_downtime_minutes) > 0 
            THEN (v_uptime_minutes / (v_uptime_minutes + v_downtime_minutes)) * 100
            ELSE 0 
        END,
        AVG(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        MIN(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        MAX(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        (SELECT COUNT(*) FROM ride_status_changes 
         WHERE ride_id = p_ride_id 
         AND change_time >= p_stat_hour 
         AND change_time < DATE_ADD(p_stat_hour, INTERVAL 1 HOUR)),
        COUNT(*),
        v_was_park_open
    FROM ride_status_snapshots
    WHERE ride_id = p_ride_id
        AND recorded_at >= p_stat_hour 
        AND recorded_at < DATE_ADD(p_stat_hour, INTERVAL 1 HOUR)
    ON DUPLICATE KEY UPDATE
        uptime_minutes = VALUES(uptime_minutes),
        downtime_minutes = VALUES(downtime_minutes),
        uptime_percentage = VALUES(uptime_percentage),
        avg_wait_time = VALUES(avg_wait_time),
        min_wait_time = VALUES(min_wait_time),
        max_wait_time = VALUES(max_wait_time),
        total_status_changes = VALUES(total_status_changes),
        samples_collected = VALUES(samples_collected),
        was_park_open = VALUES(was_park_open);
END //

-- Calculate hourly statistics for parks
CREATE PROCEDURE calculate_hourly_park_stats(p_park_id INT, p_stat_hour TIMESTAMP)
BEGIN
    DECLARE v_timezone VARCHAR(100);
    DECLARE v_local_hour TIMESTAMP;
    
    -- Get timezone for local time conversion
    SELECT timezone INTO v_timezone FROM parks WHERE id = p_park_id;
    SET v_local_hour = p_stat_hour; -- Simplified
    
    -- Insert/update hourly park stats
    INSERT INTO hourly_park_stats (
        park_id, stat_hour, local_hour, total_rides_tracked,
        rides_open, rides_closed, rides_percentage_open,
        avg_wait_time_all_rides, min_wait_time_park, max_wait_time_park,
        park_operational
    )
    SELECT 
        p_park_id,
        p_stat_hour,
        v_local_hour,
        COUNT(*) as total_rides,
        SUM(CASE WHEN computed_is_open = TRUE THEN 1 ELSE 0 END) as rides_open,
        SUM(CASE WHEN computed_is_open = FALSE THEN 1 ELSE 0 END) as rides_closed,
        (SUM(CASE WHEN computed_is_open = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100 as pct_open,
        AVG(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        MIN(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        MAX(CASE WHEN computed_is_open = TRUE AND api_wait_time > 0 THEN api_wait_time END),
        (SUM(CASE WHEN computed_is_open = TRUE THEN 1 ELSE 0 END) > 0) as park_open
    FROM ride_status_snapshots rss
    JOIN rides r ON rss.ride_id = r.id
    WHERE r.park_id = p_park_id
        AND rss.recorded_at >= p_stat_hour 
        AND rss.recorded_at < DATE_ADD(p_stat_hour, INTERVAL 1 HOUR)
        AND r.is_active = TRUE
    ON DUPLICATE KEY UPDATE
        total_rides_tracked = VALUES(total_rides_tracked),
        rides_open = VALUES(rides_open),
        rides_closed = VALUES(rides_closed),
        rides_percentage_open = VALUES(rides_percentage_open),
        avg_wait_time_all_rides = VALUES(avg_wait_time_all_rides),
        min_wait_time_park = VALUES(min_wait_time_park),
        max_wait_time_park = VALUES(max_wait_time_park),
        park_operational = VALUES(park_operational);
END //

-- Calculate weekly statistics by aggregating daily data
CREATE PROCEDURE calculate_weekly_ride_stats(p_ride_id INT, p_week_start DATE)
BEGIN
    DECLARE v_week_end DATE;
    DECLARE v_year_week VARCHAR(7);
    
    SET v_week_end = DATE_ADD(p_week_start, INTERVAL 6 DAY);
    SET v_year_week = CONCAT(YEAR(p_week_start), '-', LPAD(WEEK(p_week_start, 1), 2, '0'));
    
    INSERT INTO weekly_ride_stats (
        ride_id, week_start_date, week_end_date, year_week,
        total_operating_hours, total_uptime_hours, total_downtime_hours,
        uptime_percentage, avg_wait_time, median_wait_time, peak_wait_time,
        total_status_changes, longest_downtime_hours
    )
    SELECT 
        p_ride_id,
        p_week_start,
        v_week_end,
        v_year_week,
        SUM(park_operating_minutes) / 60.0,
        SUM(total_uptime_minutes) / 60.0,
        SUM(total_downtime_minutes) / 60.0,
        (SUM(total_uptime_minutes) / SUM(park_operating_minutes)) * 100,
        AVG(avg_wait_time),
        -- Median calculation would need more complex query
        AVG(avg_wait_time), -- Simplified as average for now
        MAX(max_wait_time),
        SUM(total_status_changes),
        MAX(longest_downtime_minutes) / 60.0
    FROM daily_ride_stats
    WHERE ride_id = p_ride_id
        AND stat_date BETWEEN p_week_start AND v_week_end
    ON DUPLICATE KEY UPDATE
        total_operating_hours = VALUES(total_operating_hours),
        total_uptime_hours = VALUES(total_uptime_hours),
        total_downtime_hours = VALUES(total_downtime_hours),
        uptime_percentage = VALUES(uptime_percentage),
        avg_wait_time = VALUES(avg_wait_time),
        median_wait_time = VALUES(median_wait_time),
        peak_wait_time = VALUES(peak_wait_time),
        total_status_changes = VALUES(total_status_changes),
        longest_downtime_hours = VALUES(longest_downtime_hours);
END //

-- Calculate monthly statistics by aggregating daily data
CREATE PROCEDURE calculate_monthly_ride_stats(p_ride_id INT, p_year_month VARCHAR(7))
BEGIN
    DECLARE v_month_start DATE;
    DECLARE v_month_end DATE;
    
    SET v_month_start = STR_TO_DATE(CONCAT(p_year_month, '-01'), '%Y-%m-%d');
    SET v_month_end = LAST_DAY(v_month_start);
    
    INSERT INTO monthly_ride_stats (
        ride_id, year_month, month_start_date, month_end_date,
        total_operating_hours, total_uptime_hours, total_downtime_hours,
        uptime_percentage, avg_wait_time, median_wait_time, peak_wait_time,
        total_status_changes
    )
    SELECT 
        p_ride_id,
        p_year_month,
        v_month_start,
        v_month_end,
        SUM(park_operating_minutes) / 60.0,
        SUM(total_uptime_minutes) / 60.0,
        SUM(total_downtime_minutes) / 60.0,
        (SUM(total_uptime_minutes) / SUM(park_operating_minutes)) * 100,
        AVG(avg_wait_time),
        AVG(avg_wait_time), -- Simplified median
        MAX(max_wait_time),
        SUM(total_status_changes)
    FROM daily_ride_stats
    WHERE ride_id = p_ride_id
        AND stat_date BETWEEN v_month_start AND v_month_end
    ON DUPLICATE KEY UPDATE
        total_operating_hours = VALUES(total_operating_hours),
        total_uptime_hours = VALUES(total_uptime_hours),
        total_downtime_hours = VALUES(total_downtime_hours),
        uptime_percentage = VALUES(uptime_percentage),
        avg_wait_time = VALUES(avg_wait_time),
        median_wait_time = VALUES(median_wait_time),
        peak_wait_time = VALUES(peak_wait_time),
        total_status_changes = VALUES(total_status_changes);
END //

-- Batch calculate all time periods for a specific ride
CREATE PROCEDURE calculate_all_ride_stats(p_ride_id INT, p_date DATE)
BEGIN
    DECLARE v_hour_start TIMESTAMP;
    DECLARE v_week_start DATE;
    DECLARE v_year_month VARCHAR(7);
    
    -- Calculate hourly stats for the day
    SET v_hour_start = TIMESTAMP(p_date);
    WHILE v_hour_start < DATE_ADD(TIMESTAMP(p_date), INTERVAL 1 DAY) DO
        CALL calculate_hourly_ride_stats(p_ride_id, v_hour_start);
        SET v_hour_start = DATE_ADD(v_hour_start, INTERVAL 1 HOUR);
    END WHILE;
    
    -- Calculate weekly stats (if it's end of week)
    IF DAYOFWEEK(p_date) = 1 THEN -- Sunday
        SET v_week_start = DATE_SUB(p_date, INTERVAL 6 DAY);
        CALL calculate_weekly_ride_stats(p_ride_id, v_week_start);
    END IF;
    
    -- Calculate monthly stats (if it's end of month)
    IF p_date = LAST_DAY(p_date) THEN
        SET v_year_month = DATE_FORMAT(p_date, '%Y-%m');
        CALL calculate_monthly_ride_stats(p_ride_id, v_year_month);
    END IF;
END //

DELIMITER ;

DELIMITER //

-- Detect and update park operating sessions
CREATE PROCEDURE detect_park_operating_sessions(p_park_id INT, p_date DATE)
BEGIN
    DECLARE v_session_start TIMESTAMP;
    DECLARE v_session_end TIMESTAMP;
    DECLARE v_current_session_id BIGINT;
    
    -- Find first activity of the day (park opening)
    SELECT MIN(snapshot_time) INTO v_session_start
    FROM park_activity_snapshots
    WHERE park_id = p_park_id 
        AND DATE(snapshot_time) = p_date
        AND park_appears_open = TRUE;
    
    -- Find last activity of the day (park closing)
    SELECT MAX(snapshot_time) INTO v_session_end
    FROM park_activity_snapshots
    WHERE park_id = p_park_id 
        AND DATE(snapshot_time) = p_date
        AND park_appears_open = TRUE;
    
    -- Only proceed if we found activity
    IF v_session_start IS NOT NULL THEN
        -- Check if session already exists
        SELECT id INTO v_current_session_id
        FROM park_operating_sessions
        WHERE park_id = p_park_id 
            AND operating_date = p_date
        LIMIT 1;
        
        IF v_current_session_id IS NOT NULL THEN
            -- Update existing session
            UPDATE park_operating_sessions
            SET session_start = v_session_start,
                session_end = v_session_end,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = v_current_session_id;
        ELSE
            -- Create new session
            INSERT INTO park_operating_sessions (
                park_id, operating_date, session_start, session_end
            ) VALUES (
                p_park_id, p_date, v_session_start, v_session_end
            );
        END IF;
    END IF;
END //

-- Process ride status changes with improved logic
CREATE PROCEDURE process_ride_status_changes(p_park_id INT)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_ride_id INT;
    DECLARE v_current_status BOOLEAN;
    DECLARE v_last_known_status BOOLEAN;
    DECLARE v_change_time TIMESTAMP;
    DECLARE v_wait_time INT;
    DECLARE v_last_change_time TIMESTAMP;
    
    DECLARE ride_cursor CURSOR FOR
        SELECT DISTINCT ride_id FROM ride_status_snapshots
        WHERE ride_id IN (SELECT id FROM rides WHERE park_id = p_park_id)
        AND recorded_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
        ORDER BY ride_id;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN ride_cursor;
    
    read_loop: LOOP
        FETCH ride_cursor INTO v_ride_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- Get the most recent computed status
        SELECT computed_is_open, api_last_updated, api_wait_time
        INTO v_current_status, v_change_time, v_wait_time
        FROM ride_status_snapshots
        WHERE ride_id = v_ride_id
        ORDER BY recorded_at DESC
        LIMIT 1;
        
        -- Get the last known status from status_changes
        SELECT new_status, change_time INTO v_last_known_status, v_last_change_time
        FROM ride_status_changes
        WHERE ride_id = v_ride_id
        ORDER BY change_time DESC
        LIMIT 1;
        
        -- If status changed, record it
        IF v_last_known_status IS NULL OR v_current_status != v_last_known_status THEN
            INSERT INTO ride_status_changes (
                ride_id, previous_status, new_status, 
                change_time, wait_time_at_change,
                duration_minutes
            ) VALUES (
                v_ride_id, v_last_known_status, v_current_status, 
                v_change_time, v_wait_time,
                CASE 
                    WHEN v_last_change_time IS NOT NULL 
                    THEN TIMESTAMPDIFF(MINUTE, v_last_change_time, v_change_time)
                    ELSE NULL 
                END
            ) ON DUPLICATE KEY UPDATE
                wait_time_at_change = v_wait_time;
        END IF;
        
    END LOOP;
    
    CLOSE ride_cursor;
END //

-- Calculate uptime percentage for a ride during detected operating hours
CREATE FUNCTION calculate_ride_uptime_dynamic(
    p_ride_id INT,
    p_start_date DATE,
    p_end_date DATE
) RETURNS DECIMAL(5,2)
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE v_total_operating_minutes INT DEFAULT 0;
    DECLARE v_total_uptime_minutes INT DEFAULT 0;
    DECLARE v_uptime_percentage DECIMAL(5,2) DEFAULT 0.00;
    
    -- Get total park operating minutes for the period (from detected sessions)
    SELECT COALESCE(SUM(pos.total_operating_minutes), 0)
    INTO v_total_operating_minutes
    FROM park_operating_sessions pos
    JOIN rides r ON pos.park_id = r.park_id
    WHERE r.id = p_ride_id
        AND pos.operating_date BETWEEN p_start_date AND p_end_date
        AND pos.total_operating_minutes IS NOT NULL;
    
    -- Get total uptime minutes for this ride during operating periods
    SELECT COALESCE(SUM(drs.total_uptime_minutes), 0)
    INTO v_total_uptime_minutes
    FROM daily_ride_stats drs
    WHERE drs.ride_id = p_ride_id
        AND drs.stat_date BETWEEN p_start_date AND p_end_date;
    
    -- Calculate percentage
    IF v_total_operating_minutes > 0 THEN
        SET v_uptime_percentage = (v_total_uptime_minutes / v_total_operating_minutes) * 100;
    END IF;
    
    RETURN LEAST(v_uptime_percentage, 100.00);
END //

DELIMITER ;

-- ============================================================================
-- VIEWS FOR TIME-SERIES CHARTING
-- ============================================================================

-- Hourly ride performance for detailed charts
CREATE VIEW hourly_ride_performance AS
SELECT 
    hrs.ride_id,
    r.name as ride_name,
    p.name as park_name,
    hrs.stat_hour,
    hrs.local_hour,
    hrs.uptime_percentage,
    hrs.avg_wait_time,
    hrs.min_wait_time,
    hrs.max_wait_time,
    hrs.was_park_open,
    HOUR(hrs.local_hour) as hour_of_day,
    DAYNAME(hrs.local_hour) as day_of_week
FROM hourly_ride_stats hrs
JOIN rides r ON hrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE;

-- Hourly park operational dashboard
CREATE VIEW hourly_park_performance AS
SELECT 
    hps.park_id,
    p.name as park_name,
    p.country,
    hps.stat_hour,
    hps.local_hour,
    hps.total_rides_tracked,
    hps.rides_open,
    hps.rides_closed,
    hps.rides_percentage_open,
    hps.avg_wait_time_all_rides,
    hps.min_wait_time_park,
    hps.max_wait_time_park,
    hps.park_operational,
    HOUR(hps.local_hour) as hour_of_day,
    DAYNAME(hps.local_hour) as day_of_week
FROM hourly_park_stats hps
JOIN parks p ON hps.park_id = p.id
WHERE p.is_north_america = TRUE;

-- Weekly trending data for charts
CREATE VIEW weekly_trending_rides AS
SELECT 
    wrs.ride_id,
    r.name as ride_name,
    p.name as park_name,
    wrs.week_start_date,
    wrs.week_end_date,
    wrs.year_week,
    wrs.uptime_percentage,
    wrs.avg_wait_time,
    wrs.peak_wait_time,
    wrs.total_status_changes,
    LAG(wrs.uptime_percentage) OVER (PARTITION BY wrs.ride_id ORDER BY wrs.week_start_date) as prev_week_uptime,
    CASE 
        WHEN LAG(wrs.uptime_percentage) OVER (PARTITION BY wrs.ride_id ORDER BY wrs.week_start_date) IS NULL THEN 'new'
        WHEN wrs.uptime_percentage > LAG(wrs.uptime_percentage) OVER (PARTITION BY wrs.ride_id ORDER BY wrs.week_start_date) THEN 'improving'
        WHEN wrs.uptime_percentage < LAG(wrs.uptime_percentage) OVER (PARTITION BY wrs.ride_id ORDER BY wrs.week_start_date) THEN 'worsening'
        ELSE 'stable'
    END as trend_direction
FROM weekly_ride_stats wrs
JOIN rides r ON wrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE;

-- Monthly performance comparison view
CREATE VIEW monthly_performance_comparison AS
SELECT 
    mrs.ride_id,
    r.name as ride_name,
    p.name as park_name,
    mrs.year_month,
    mrs.uptime_percentage,
    mrs.avg_wait_time,
    mrs.total_downtime_hours,
    AVG(mrs.uptime_percentage) OVER (PARTITION BY mrs.ride_id ORDER BY mrs.month_start_date ROWS 2 PRECEDING) as rolling_3month_avg,
    RANK() OVER (PARTITION BY p.id ORDER BY mrs.uptime_percentage DESC) as park_ranking_this_month,
    LAG(mrs.uptime_percentage, 12) OVER (PARTITION BY mrs.ride_id ORDER BY mrs.month_start_date) as same_month_last_year
FROM monthly_ride_stats mrs
JOIN rides r ON mrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE;

-- Time-series data export view for charting APIs
CREATE VIEW ride_timeseries_export AS
SELECT 
    'hourly' as granularity,
    hrs.ride_id,
    hrs.stat_hour as time_period,
    hrs.uptime_percentage,
    hrs.avg_wait_time,
    hrs.total_status_changes,
    hrs.was_park_open as operational_context
FROM hourly_ride_stats hrs
JOIN rides r ON hrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE

UNION ALL

SELECT 
    'daily' as granularity,
    drs.ride_id,
    TIMESTAMP(drs.stat_date) as time_period,
    drs.uptime_percentage,
    drs.avg_wait_time,
    drs.total_status_changes,
    TRUE as operational_context
FROM daily_ride_stats drs
JOIN rides r ON drs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE

UNION ALL

SELECT 
    'weekly' as granularity,
    wrs.ride_id,
    TIMESTAMP(wrs.week_start_date) as time_period,
    wrs.uptime_percentage,
    wrs.avg_wait_time,
    wrs.total_status_changes,
    TRUE as operational_context
FROM weekly_ride_stats wrs
JOIN rides r ON wrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE

UNION ALL

SELECT 
    'monthly' as granularity,
    mrs.ride_id,
    TIMESTAMP(mrs.month_start_date) as time_period,
    mrs.uptime_percentage,
    mrs.avg_wait_time,
    mrs.total_status_changes,
    TRUE as operational_context
FROM monthly_ride_stats mrs
JOIN rides r ON mrs.ride_id = r.id
JOIN parks p ON r.park_id = p.id
WHERE p.is_north_america = TRUE;

-- Current ride status with computed logic
CREATE VIEW current_ride_status AS
SELECT 
    r.id as ride_id,
    r.name as ride_name,
    p.id as park_id,
    p.name as park_name,
    p.country,
    rss.api_is_open,
    rss.api_wait_time,
    rss.computed_is_open as is_actually_open,
    rss.api_last_updated,
    rss.recorded_at,
    CASE 
        WHEN rss.computed_is_open = TRUE AND rss.api_wait_time > 0 
        THEN rss.api_wait_time
        ELSE 0
    END as effective_wait_time
FROM rides r
JOIN parks p ON r.park_id = p.id
LEFT JOIN ride_status_snapshots rss ON r.id = rss.ride_id
LEFT JOIN ride_status_snapshots rss2 ON r.id = rss2.ride_id 
    AND rss.recorded_at < rss2.recorded_at
WHERE p.is_north_america = TRUE
    AND r.is_active = TRUE
    AND rss2.id IS NULL;

-- Park performance with dynamic operating hours
CREATE VIEW current_park_performance AS
SELECT 
    p.id as park_id,
    p.name as park_name,
    p.country,
    dps.total_rides_tracked as total_rides,
    dps.operational_rides as open_rides,
    dps.never_opened_rides as closed_rides,
    dps.avg_park_uptime,
    dps.total_downtime_hours,
    pos.total_operating_minutes / 60.0 as operating_hours,
    CASE 
        WHEN dps.avg_park_uptime >= 95 THEN 'OPERATIONAL'
        WHEN dps.avg_park_uptime >= 85 THEN 'MAINTENANCE'
        ELSE 'CRITICAL'
    END as status,
    CASE
        WHEN rps7.trend_direction = 'improving' THEN CONCAT('↘ -', rps7.trend_percentage, '%')
        WHEN rps7.trend_direction = 'worsening' THEN CONCAT('↗ +', rps7.trend_percentage, '%')
        ELSE '→ 0%'
    END as trend_7day
FROM parks p
LEFT JOIN daily_park_stats dps ON p.id = dps.park_id 
    AND dps.stat_date = CURDATE()
LEFT JOIN park_operating_sessions pos ON p.id = pos.park_id 
    AND pos.operating_date = CURDATE()
LEFT JOIN rolling_period_stats rps7 ON p.id = rps7.entity_id 
    AND rps7.entity_type = 'park' 
    AND rps7.period_days = 7 
    AND rps7.end_date = CURDATE()
WHERE p.is_north_america = TRUE
ORDER BY dps.total_downtime_hours DESC;

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Additional composite indexes for common query patterns
CREATE INDEX idx_snapshots_computed_date ON ride_status_snapshots (ride_id, computed_is_open, DATE(recorded_at));
CREATE INDEX idx_changes_duration ON ride_status_changes (ride_id, new_status, duration_minutes);
CREATE INDEX idx_activity_park_open ON park_activity_snapshots (park_id, park_appears_open, snapshot_time);
CREATE INDEX idx_daily_stats_uptime_date ON daily_ride_stats (uptime_percentage DESC, stat_date);
CREATE INDEX idx_park_stats_downtime_date ON daily_park_stats (total_downtime_hours DESC, stat_date);