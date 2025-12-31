-- Theme Park Downtime Tracker - Data Quality Issues
-- Migration: 010_data_quality_issues.sql
-- Purpose: Track stale/suspicious data from external APIs for reporting
-- Date: 2025-12-01
-- Updated: 2025-12-16 - Made index creation idempotent

-- ============================================
-- DATA QUALITY ISSUES TABLE
-- ============================================
-- Stores detected data quality issues from ThemeParks.wiki and Queue-Times APIs
-- Useful for reporting upstream to API maintainers

CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- What data source had the issue
    data_source ENUM('themeparks_wiki', 'queue_times') NOT NULL,

    -- Issue type
    issue_type ENUM('STALE_DATA', 'MISSING_DATA', 'INVALID_STATUS', 'INCONSISTENT_DATA') NOT NULL,

    -- When we detected this issue
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Park/Ride context
    park_id INT NULL,
    ride_id INT NULL,

    -- External IDs for reporting to upstream
    themeparks_wiki_id VARCHAR(36) NULL COMMENT 'UUID from ThemeParks.wiki API',
    queue_times_id INT NULL,

    -- Issue details
    entity_name VARCHAR(255) NULL COMMENT 'Name of ride/park from API',
    last_updated_api DATETIME NULL COMMENT 'lastUpdated timestamp from API',
    data_age_minutes INT NULL COMMENT 'How stale the data was in minutes',
    reported_status VARCHAR(50) NULL COMMENT 'Status the API returned',

    -- Additional context
    details TEXT NULL COMMENT 'JSON with additional context',

    -- Tracking
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at DATETIME NULL,

    -- Foreign keys (optional - data might not be in our DB)
    CONSTRAINT fk_dqi_park FOREIGN KEY (park_id)
        REFERENCES parks(park_id) ON DELETE SET NULL,
    CONSTRAINT fk_dqi_ride FOREIGN KEY (ride_id)
        REFERENCES rides(ride_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for common queries (idempotent)
DROP INDEX IF EXISTS idx_dqi_detected ON data_quality_issues;
CREATE INDEX idx_dqi_detected ON data_quality_issues (detected_at);

DROP INDEX IF EXISTS idx_dqi_source ON data_quality_issues;
CREATE INDEX idx_dqi_source ON data_quality_issues (data_source, issue_type);

-- Indexes on foreign key columns - can't be dropped if FK uses them, so check before creating
SET @idx_exists := (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'data_quality_issues'
      AND index_name = 'idx_dqi_park'
);
SET @ddl := IF(
    @idx_exists = 0,
    'CREATE INDEX idx_dqi_park ON data_quality_issues (park_id, detected_at);',
    'SELECT "idx_dqi_park already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'data_quality_issues'
      AND index_name = 'idx_dqi_ride'
);
SET @ddl := IF(
    @idx_exists = 0,
    'CREATE INDEX idx_dqi_ride ON data_quality_issues (ride_id, detected_at);',
    'SELECT "idx_dqi_ride already exists" AS message;'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

DROP INDEX IF EXISTS idx_dqi_unresolved ON data_quality_issues;
CREATE INDEX idx_dqi_unresolved ON data_quality_issues (is_resolved, detected_at);

DROP INDEX IF EXISTS idx_dqi_wiki_id ON data_quality_issues;
CREATE INDEX idx_dqi_wiki_id ON data_quality_issues (themeparks_wiki_id);

-- ============================================
-- EXAMPLE QUERIES
-- ============================================

-- Get recent unresolved issues:
-- SELECT * FROM data_quality_issues
-- WHERE is_resolved = FALSE
-- ORDER BY detected_at DESC LIMIT 50;

-- Get stale data issues for ThemeParks.wiki:
-- SELECT dqi.*, r.name as ride_name, p.name as park_name
-- FROM data_quality_issues dqi
-- LEFT JOIN rides r ON dqi.ride_id = r.ride_id
-- LEFT JOIN parks p ON dqi.park_id = p.park_id
-- WHERE dqi.data_source = 'themeparks_wiki'
--   AND dqi.issue_type = 'STALE_DATA'
--   AND dqi.detected_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
-- ORDER BY dqi.data_age_minutes DESC;

-- Summary by entity for reporting:
-- SELECT
--     themeparks_wiki_id,
--     entity_name,
--     COUNT(*) as issue_count,
--     MAX(data_age_minutes) as max_staleness_minutes,
--     MIN(detected_at) as first_detected,
--     MAX(detected_at) as last_detected
-- FROM data_quality_issues
-- WHERE data_source = 'themeparks_wiki'
--   AND issue_type = 'STALE_DATA'
--   AND detected_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
-- GROUP BY themeparks_wiki_id, entity_name
-- ORDER BY issue_count DESC;
