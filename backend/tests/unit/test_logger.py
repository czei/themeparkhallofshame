"""
Theme Park Downtime Tracker - Logger Unit Tests

Tests structured JSON logging functionality:
- Logger setup and configuration
- Collection logging (start, complete, error)
- Aggregation logging (start, complete, error)
- API request logging
- Database error logging

Priority: P1 - Quick win for coverage increase
"""

import pytest
import logging
import json
from io import StringIO
from utils.logger import (
    setup_logger,
    logger,
    log_collection_start,
    log_collection_complete,
    log_collection_error,
    log_aggregation_start,
    log_aggregation_complete,
    log_aggregation_error,
    log_api_request,
    log_database_error
)


class TestSetupLogger:
    """Test logger setup and configuration."""

    def test_setup_logger_returns_logger_instance(self):
        """setup_logger() should return a logging.Logger instance."""
        test_logger = setup_logger("test_logger")

        assert isinstance(test_logger, logging.Logger)
        assert test_logger.name == "test_logger"

    def test_setup_logger_prevents_duplicate_handlers(self):
        """setup_logger() should not add duplicate handlers."""
        test_logger = setup_logger("test_duplicate")
        handler_count_1 = len(test_logger.handlers)

        # Call setup_logger again
        test_logger = setup_logger("test_duplicate")
        handler_count_2 = len(test_logger.handlers)

        assert handler_count_1 == handler_count_2

    def test_global_logger_exists(self):
        """Global logger instance should be initialized."""
        assert logger is not None
        assert isinstance(logger, logging.Logger)
        assert logger.name == "themepark_tracker"


class TestCollectionLogging:
    """Test collection logging functions."""

    def test_log_collection_start(self, caplog):
        """log_collection_start() should log collection start event."""
        with caplog.at_level(logging.INFO):
            log_collection_start(park_count=85)

        # Check log message was created
        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Data collection started" in record.message
        assert record.levelname == "INFO"

    def test_log_collection_complete(self, caplog):
        """log_collection_complete() should log collection completion."""
        with caplog.at_level(logging.INFO):
            log_collection_complete(
                duration_seconds=142.5,
                parks_processed=85,
                rides_updated=1247
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Data collection completed" in record.message
        assert record.levelname == "INFO"

    def test_log_collection_error(self, caplog):
        """log_collection_error() should log collection error with exception."""
        error = ValueError("API timeout")

        with caplog.at_level(logging.ERROR):
            log_collection_error(error, park_id=101)

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Data collection failed" in record.message
        assert record.levelname == "ERROR"
        assert record.exc_info is not None

    def test_log_collection_error_without_park_id(self, caplog):
        """log_collection_error() should work without park_id."""
        error = ConnectionError("Database connection lost")

        with caplog.at_level(logging.ERROR):
            log_collection_error(error)

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Data collection failed" in record.message
        assert record.levelname == "ERROR"


class TestAggregationLogging:
    """Test aggregation logging functions."""

    def test_log_aggregation_start(self, caplog):
        """log_aggregation_start() should log aggregation start event."""
        with caplog.at_level(logging.INFO):
            log_aggregation_start(
                aggregation_type="daily",
                aggregation_date="2024-01-15"
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Aggregation started" in record.message
        assert record.levelname == "INFO"

    def test_log_aggregation_complete(self, caplog):
        """log_aggregation_complete() should log aggregation completion."""
        with caplog.at_level(logging.INFO):
            log_aggregation_complete(
                aggregation_type="daily",
                parks_processed=85,
                rides_processed=1247
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Aggregation completed" in record.message
        assert record.levelname == "INFO"

    def test_log_aggregation_error(self, caplog):
        """log_aggregation_error() should log aggregation error with exception."""
        error = RuntimeError("SQL query failed")

        with caplog.at_level(logging.ERROR):
            log_aggregation_error(error, aggregation_type="weekly")

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Aggregation failed" in record.message
        assert record.levelname == "ERROR"
        assert record.exc_info is not None


class TestAPILogging:
    """Test API request logging functions."""

    def test_log_api_request(self, caplog):
        """log_api_request() should log API request metrics."""
        with caplog.at_level(logging.INFO):
            log_api_request(
                method="GET",
                path="/api/parks/101",
                status_code=200,
                duration_ms=45.3
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "API request" in record.message
        assert record.levelname == "INFO"

    def test_log_api_request_error_status(self, caplog):
        """log_api_request() should log error status codes."""
        with caplog.at_level(logging.INFO):
            log_api_request(
                method="POST",
                path="/api/rides",
                status_code=500,
                duration_ms=125.7
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "API request" in record.message
        assert record.levelname == "INFO"


class TestDatabaseLogging:
    """Test database error logging functions."""

    def test_log_database_error(self, caplog):
        """log_database_error() should log database error with exception."""
        error = Exception("Connection timeout")

        with caplog.at_level(logging.ERROR):
            log_database_error(error, query_context="INSERT INTO parks")

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Database error" in record.message
        assert record.levelname == "ERROR"
        assert record.exc_info is not None

    def test_log_database_error_without_query_context(self, caplog):
        """log_database_error() should work without query_context."""
        error = Exception("Pool exhausted")

        with caplog.at_level(logging.ERROR):
            log_database_error(error)

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Database error" in record.message
        assert record.levelname == "ERROR"


class TestLoggerIntegration:
    """Test logger integration scenarios."""

    def test_logger_logs_at_info_level(self, caplog):
        """Logger should log INFO level messages."""
        with caplog.at_level(logging.INFO):
            logger.info("Test info message")

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Test info message" in record.message
        assert record.levelname == "INFO"

    def test_logger_logs_at_error_level(self, caplog):
        """Logger should log ERROR level messages."""
        with caplog.at_level(logging.ERROR):
            logger.error("Test error message")

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Test error message" in record.message
        assert record.levelname == "ERROR"

    def test_logger_logs_with_extra_context(self, caplog):
        """Logger should support extra context fields."""
        with caplog.at_level(logging.INFO):
            logger.info("Test with context", extra={
                "park_id": 101,
                "ride_count": 45
            })

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Test with context" in record.message
        assert record.levelname == "INFO"

    def test_logger_captures_exception_info(self, caplog):
        """Logger should capture exception stack traces."""
        with caplog.at_level(logging.ERROR):
            try:
                raise ValueError("Test exception")
            except ValueError as e:
                logger.error("Exception occurred", exc_info=True)

        assert len(caplog.records) >= 1
        record = caplog.records[-1]

        assert "Exception occurred" in record.message
        assert record.levelname == "ERROR"
        assert record.exc_info is not None


class TestEdgeCases:
    """Test edge cases for logging functions."""

    def test_log_collection_error_with_nested_exception(self, caplog):
        """log_collection_error() should handle nested exceptions."""
        try:
            try:
                raise ValueError("Inner error")
            except ValueError:
                raise RuntimeError("Outer error") from ValueError("Inner error")
        except RuntimeError as e:
            with caplog.at_level(logging.ERROR):
                log_collection_error(e, park_id=101)

        assert len(caplog.records) >= 1
        record = caplog.records[-1]
        assert record.levelname == "ERROR"

    def test_log_api_request_with_long_path(self, caplog):
        """log_api_request() should handle very long URL paths."""
        long_path = "/api/parks/101/rides/1234/stats/daily/2024-01-15/details"

        with caplog.at_level(logging.INFO):
            log_api_request(
                method="GET",
                path=long_path,
                status_code=200,
                duration_ms=89.2
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]
        assert "API request" in record.message

    def test_log_aggregation_complete_zero_values(self, caplog):
        """log_aggregation_complete() should handle zero parks/rides."""
        with caplog.at_level(logging.INFO):
            log_aggregation_complete(
                aggregation_type="daily",
                parks_processed=0,
                rides_processed=0
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]
        assert "Aggregation completed" in record.message

    def test_log_collection_complete_high_values(self, caplog):
        """log_collection_complete() should handle high volume metrics."""
        with caplog.at_level(logging.INFO):
            log_collection_complete(
                duration_seconds=3600.5,  # 1 hour
                parks_processed=500,
                rides_updated=25000
            )

        assert len(caplog.records) >= 1
        record = caplog.records[-1]
        assert "Data collection completed" in record.message
