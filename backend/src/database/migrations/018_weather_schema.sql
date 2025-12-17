-- Migration 018: Weather Data Collection Schema
-- ================================================
-- Creates weather_observations and weather_forecasts tables for Open-Meteo API data collection
-- Feature: 002-weather-collection
-- Date: 2025-12-17

-- Table: weather_observations
-- Purpose: Store hourly weather observations for each park
-- Retention: 2 years (730 days)
CREATE TABLE IF NOT EXISTS weather_observations (
    observation_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    observation_time TIMESTAMP NOT NULL COMMENT 'UTC datetime when weather was observed',
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'UTC datetime when we fetched this data',

    -- Temperature
    temperature_c DECIMAL(5,2) NULL COMMENT 'Temperature in Celsius (-99.99 to 999.99)',
    temperature_f DECIMAL(5,2) NULL COMMENT 'Temperature in Fahrenheit (-99.99 to 999.99)',
    apparent_temperature_c DECIMAL(5,2) NULL COMMENT 'Feels like temperature in Celsius',
    apparent_temperature_f DECIMAL(5,2) NULL COMMENT 'Feels like temperature in Fahrenheit',

    -- Wind
    wind_speed_kmh DECIMAL(5,2) NULL COMMENT 'Wind speed in km/h (0.00 to 999.99)',
    wind_speed_mph DECIMAL(5,2) NULL COMMENT 'Wind speed in mph (0.00 to 999.99)',
    wind_gusts_kmh DECIMAL(5,2) NULL COMMENT 'Wind gust speed in km/h',
    wind_gusts_mph DECIMAL(5,2) NULL COMMENT 'Wind gust speed in mph',
    wind_direction_degrees SMALLINT NULL COMMENT 'Wind direction (0-360 degrees, 0=North)',

    -- Precipitation
    precipitation_mm DECIMAL(6,2) NULL COMMENT 'Total precipitation in mm (0.00 to 9999.99)',
    precipitation_probability TINYINT NULL COMMENT 'Probability of precipitation (0-100%)',
    rain_mm DECIMAL(6,2) NULL COMMENT 'Rainfall amount in mm',
    snowfall_mm DECIMAL(6,2) NULL COMMENT 'Snowfall amount in mm (water equivalent)',

    -- Atmospheric
    cloud_cover_percent TINYINT NULL COMMENT 'Cloud coverage (0-100%)',
    visibility_meters INT NULL COMMENT 'Visibility in meters (0 to 999999)',
    humidity_percent TINYINT NULL COMMENT 'Relative humidity (0-100%)',
    pressure_hpa DECIMAL(6,2) NULL COMMENT 'Barometric pressure in hPa (0.00 to 9999.99)',

    -- Weather code (WMO standard)
    weather_code SMALLINT NULL COMMENT 'WMO weather code (0-99, 95/96/99 = thunderstorm)',

    -- Foreign key and indexes
    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_time (park_id, observation_time),
    INDEX idx_weather_code (weather_code),
    INDEX idx_observation_time (observation_time),
    UNIQUE KEY unique_observation (park_id, observation_time)
) ENGINE=InnoDB COMMENT='Hourly weather observations for parks (2-year retention)';

-- Table: weather_forecasts
-- Purpose: Store hourly forecasts for next 7 days (issued every 6 hours)
-- Retention: 90 days from issued_at
CREATE TABLE IF NOT EXISTS weather_forecasts (
    forecast_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    park_id INT NOT NULL,
    issued_at TIMESTAMP NOT NULL COMMENT 'UTC datetime when forecast was generated',
    forecast_time TIMESTAMP NOT NULL COMMENT 'UTC datetime that this forecast predicts',

    -- Temperature
    temperature_c DECIMAL(5,2) NULL COMMENT 'Forecasted temperature in Celsius',
    temperature_f DECIMAL(5,2) NULL COMMENT 'Forecasted temperature in Fahrenheit',
    apparent_temperature_c DECIMAL(5,2) NULL COMMENT 'Forecasted feels like temperature in Celsius',
    apparent_temperature_f DECIMAL(5,2) NULL COMMENT 'Forecasted feels like temperature in Fahrenheit',

    -- Wind
    wind_speed_kmh DECIMAL(5,2) NULL COMMENT 'Forecasted wind speed in km/h',
    wind_speed_mph DECIMAL(5,2) NULL COMMENT 'Forecasted wind speed in mph',
    wind_gusts_kmh DECIMAL(5,2) NULL COMMENT 'Forecasted wind gust speed in km/h',
    wind_gusts_mph DECIMAL(5,2) NULL COMMENT 'Forecasted wind gust speed in mph',
    wind_direction_degrees SMALLINT NULL COMMENT 'Forecasted wind direction (0-360 degrees)',

    -- Precipitation
    precipitation_mm DECIMAL(6,2) NULL COMMENT 'Forecasted precipitation in mm',
    precipitation_probability TINYINT NULL COMMENT 'Forecasted probability of precipitation (0-100%)',
    rain_mm DECIMAL(6,2) NULL COMMENT 'Forecasted rainfall amount in mm',
    snowfall_mm DECIMAL(6,2) NULL COMMENT 'Forecasted snowfall amount in mm',

    -- Atmospheric
    cloud_cover_percent TINYINT NULL COMMENT 'Forecasted cloud coverage (0-100%)',
    visibility_meters INT NULL COMMENT 'Forecasted visibility in meters',
    humidity_percent TINYINT NULL COMMENT 'Forecasted relative humidity (0-100%)',
    pressure_hpa DECIMAL(6,2) NULL COMMENT 'Forecasted barometric pressure in hPa',

    -- Weather code (WMO standard)
    weather_code SMALLINT NULL COMMENT 'Forecasted WMO weather code (0-99)',

    -- Foreign key and indexes
    FOREIGN KEY (park_id) REFERENCES parks(park_id) ON DELETE CASCADE,
    INDEX idx_park_issued (park_id, issued_at),
    INDEX idx_park_forecast_time (park_id, forecast_time),
    INDEX idx_weather_code (weather_code),
    INDEX idx_issued_at (issued_at),
    UNIQUE KEY unique_forecast (park_id, issued_at, forecast_time)
) ENGINE=InnoDB COMMENT='Hourly weather forecasts for parks (90-day retention)';

-- Verification queries (run after migration)
-- SELECT COUNT(*) FROM weather_observations;
-- SELECT COUNT(*) FROM weather_forecasts;
-- SHOW CREATE TABLE weather_observations;
-- SHOW CREATE TABLE weather_forecasts;
