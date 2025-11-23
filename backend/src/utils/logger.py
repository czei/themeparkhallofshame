"""
Theme Park Downtime Tracker - Structured Logging
Provides JSON-formatted logging for CloudWatch Logs Insights queries.
"""

import logging
import sys
from pythonjsonlogger import jsonlogger

from .config import LOG_LEVEL, config


def setup_logger(name: str = __name__) -> logging.Logger:
    """
    Configure structured JSON logger for CloudWatch integration.

    Args:
        name: Logger name (typically __name__ from calling module)

    Returns:
        Configured logger instance

    Example:
        >>> logger = setup_logger(__name__)
        >>> logger.info("Collection completed", extra={
        ...     "park_count": 85,
        ...     "duration_seconds": 142,
        ...     "rides_updated": 1247
        ... })
    """
    logger = logging.getLogger(name)

    # Prevent duplicate handlers
    if logger.hasHandlers():
        return logger

    # Set log level
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # JSON formatter for structured logs
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Don't propagate to root logger
    logger.propagate = False

    return logger


# Global logger instance
logger = setup_logger('themepark_tracker')


def log_collection_start(park_count: int):
    """Log the start of data collection cycle."""
    logger.info("Data collection started", extra={
        "event_type": "collection_start",
        "park_count": park_count,
        "environment": config.environment
    })


def log_collection_complete(duration_seconds: float, parks_processed: int, rides_updated: int):
    """Log successful collection completion."""
    logger.info("Data collection completed", extra={
        "event_type": "collection_complete",
        "duration_seconds": duration_seconds,
        "parks_processed": parks_processed,
        "rides_updated": rides_updated
    })


def log_collection_error(error: Exception, park_id: int = None):
    """Log collection error with context."""
    logger.error("Data collection failed", extra={
        "event_type": "collection_error",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "park_id": park_id
    }, exc_info=True)


def log_aggregation_start(aggregation_type: str, aggregation_date: str):
    """Log the start of aggregation job."""
    logger.info("Aggregation started", extra={
        "event_type": "aggregation_start",
        "aggregation_type": aggregation_type,
        "aggregation_date": aggregation_date
    })


def log_aggregation_complete(aggregation_type: str, parks_processed: int, rides_processed: int):
    """Log successful aggregation completion."""
    logger.info("Aggregation completed", extra={
        "event_type": "aggregation_complete",
        "aggregation_type": aggregation_type,
        "parks_processed": parks_processed,
        "rides_processed": rides_processed
    })


def log_aggregation_error(error: Exception, aggregation_type: str):
    """Log aggregation error with context."""
    logger.error("Aggregation failed", extra={
        "event_type": "aggregation_error",
        "aggregation_type": aggregation_type,
        "error_type": type(error).__name__,
        "error_message": str(error)
    }, exc_info=True)


def log_api_request(method: str, path: str, status_code: int, duration_ms: float):
    """Log API request metrics."""
    logger.info("API request", extra={
        "event_type": "api_request",
        "method": method,
        "path": path,
        "status_code": status_code,
        "duration_ms": duration_ms
    })


def log_database_error(error: Exception, query_context: str = None):
    """Log database error with context."""
    logger.error("Database error", extra={
        "event_type": "database_error",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "query_context": query_context
    }, exc_info=True)
