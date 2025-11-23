-- Theme Park Downtime Tracker - Initial Schema Migration
-- Migration: 001_initial_schema.sql
-- Purpose: Create core entities (parks, rides, ride_classifications)
-- Date: 2025-11-23

-- Core Entity: Parks
CREATE TABLE IF NOT EXISTS parks (
    park_id INT PRIMARY KEY AUTO_INCREMENT,
    queue_times_id INT UNIQUE NOT NULL COMMENT 'External ID from Queue-Times.com API',
    name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state_province VARCHAR(100) DEFAULT NULL COMMENT 'State/Province abbreviation (e.g., FL, CA, QC)',
    country VARCHAR(2) NOT NULL COMMENT 'ISO 3166-1 alpha-2 country code (US, CA)',
    latitude DECIMAL(10, 8) DEFAULT NULL,
    longitude DECIMAL(11, 8) DEFAULT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'America/New_York' COMMENT 'IANA timezone (e.g., America/Los_Angeles)',
    operator VARCHAR(100) DEFAULT NULL COMMENT 'Park operator (Disney, Universal, Six Flags, etc.)',
    is_disney BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Denormalized flag for Disney & Universal filter',
    is_universal BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Denormalized flag for Disney & Universal filter',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Whether park is currently tracked',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_operator (operator),
    INDEX idx_disney_universal (is_disney, is_universal),
    INDEX idx_country (country),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Core Entity: Rides
CREATE TABLE IF NOT EXISTS rides (
    ride_id INT PRIMARY KEY AUTO_INCREMENT,
    queue_times_id INT UNIQUE NOT NULL COMMENT 'External ID from Queue-Times.com API',
    park_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    land_area VARCHAR(100) DEFAULT NULL COMMENT 'Themed land/area within park (e.g., Fantasyland)',
    tier TINYINT DEFAULT NULL COMMENT 'Ride tier classification: 1 (major, 3x weight), 2 (standard, 2x weight), 3 (minor, 1x weight)',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Whether ride is currently tracked',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_id (park_id),
    INDEX idx_active (is_active),
    INDEX idx_park_active (park_id, is_active),
    INDEX idx_tier (tier),
    CHECK (tier IN (1, 2, 3) OR tier IS NULL)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Ride Classification System
CREATE TABLE IF NOT EXISTS ride_classifications (
    classification_id INT PRIMARY KEY AUTO_INCREMENT,
    ride_id INT NOT NULL,
    tier TINYINT NOT NULL COMMENT 'Tier: 1 (3x weight), 2 (2x weight), 3 (1x weight)',
    tier_weight TINYINT NOT NULL COMMENT 'Multiplier for weighted downtime: 3, 2, or 1',
    classification_method ENUM('manual_override', 'cached_ai', 'pattern_match', 'ai_agent') NOT NULL COMMENT 'Method used: manual (Priority 1), cached_ai (Priority 2), pattern (Priority 3), ai_agent (Priority 4)',
    confidence_score DECIMAL(3, 2) DEFAULT NULL COMMENT 'Confidence: 0.00 to 1.00 (1.00 for manual overrides, NULL accepted)',
    reasoning_text TEXT DEFAULT NULL COMMENT 'Explanation for classification (e.g., "310 ft giga coaster, world-renowned")',
    override_reason VARCHAR(500) DEFAULT NULL COMMENT 'Manual override justification if classification_method = manual_override',
    research_sources JSON DEFAULT NULL COMMENT 'Array of URLs used by AI agent for classification (e.g., ["https://rcdb.com/11130.htm"])',
    cache_key VARCHAR(50) DEFAULT NULL COMMENT 'Cache key format: {park_id}:{ride_id} for exact_matches.json lookup',
    schema_version VARCHAR(10) DEFAULT '1.0' COMMENT 'Classification schema version for cache invalidation',
    classified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (ride_id) REFERENCES rides(ride_id) ON DELETE CASCADE,
    UNIQUE KEY unique_ride (ride_id),
    INDEX idx_tier (tier),
    INDEX idx_method (classification_method),
    INDEX idx_confidence (confidence_score),
    INDEX idx_cache_key (cache_key),
    CHECK (tier IN (1, 2, 3)),
    CHECK (tier_weight IN (1, 2, 3)),
    CHECK (confidence_score IS NULL OR (confidence_score >= 0.00 AND confidence_score <= 1.00))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
