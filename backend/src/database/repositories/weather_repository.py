"""
Weather Repository
==================

Repositories for weather_observations and weather_forecasts tables.

Features:
- Idempotent inserts (ON DUPLICATE KEY UPDATE)
- Batch insert support (executemany)
- Query methods for latest observations
- Structured logging

Design Pattern:
- Repository pattern (SQLAlchemy Core, no ORM)
- Follows existing project patterns
"""

import logging
from typing import Dict, List, Optional, Any

# Configure structured logging
logger = logging.getLogger(__name__)


class WeatherObservationRepository:
    """Repository for weather_observations table.

    Handles insert/query operations for hourly weather observations.
    All inserts are idempotent via ON DUPLICATE KEY UPDATE.

    Usage:
        ```python
        from database.connection import get_db_connection
        repo = WeatherObservationRepository(get_db_connection())

        observation = {
            'park_id': 1,
            'observation_time': datetime(...),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            # ... other fields
        }

        repo.insert_observation(observation)
        ```
    """

    # Allowlist of valid field names (prevents SQL injection)
    ALLOWED_FIELDS = {
        'park_id', 'observation_time', 'collected_at',
        'temperature_c', 'temperature_f',
        'apparent_temperature_c', 'apparent_temperature_f',
        'wind_speed_kmh', 'wind_speed_mph',
        'wind_gusts_kmh', 'wind_gusts_mph',
        'wind_direction_degrees',
        'precipitation_mm', 'precipitation_probability',
        'rain_mm', 'snowfall_mm',
        'cloud_cover_percent', 'visibility_meters',
        'humidity_percent', 'pressure_hpa',
        'weather_code'
    }

    def __init__(self, db: Any):
        """Initialize repository with database connection.

        Args:
            db: MySQL database connection
        """
        self.db = db

    def insert_observation(self, observation: Dict) -> None:
        """Insert or update a weather observation.

        Uses ON DUPLICATE KEY UPDATE for idempotent inserts.
        Unique key: (park_id, observation_time)

        Args:
            observation: Dictionary with observation data

        Raises:
            ValueError: If no valid fields found in observation data
        """
        # Validate and filter fields against allowlist (prevents SQL injection)
        fields = [key for key in observation.keys() if key in self.ALLOWED_FIELDS]
        if not fields:
            raise ValueError("No valid fields found in observation data")

        # Backtick field names to prevent SQL keyword conflicts
        safe_fields = [f'`{field}`' for field in fields]
        placeholders = [f'%({field})s' for field in fields]
        update_fields = [f'`{field}`=VALUES(`{field}`)' for field in fields
                        if field not in ('park_id', 'observation_time')]

        sql = f"""
            INSERT INTO weather_observations
            ({', '.join(safe_fields)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE
            {', '.join(update_fields)}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, observation)

            logger.debug(
                "Inserted weather observation",
                extra={
                    'park_id': observation.get('park_id'),
                    'observation_time': observation.get('observation_time')
                }
            )

        except Exception as e:
            logger.error(
                "Failed to insert observation",
                extra={
                    'park_id': observation.get('park_id'),
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise

    def batch_insert_observations(self, observations: List[Dict]) -> None:
        """Insert or update multiple observations in batch.

        Uses executemany() for efficiency.
        Up to 10x faster than individual inserts.

        Args:
            observations: List of observation dictionaries

        Raises:
            ValueError: If no valid fields found in observation data
        """
        if not observations:
            logger.debug("No observations to insert")
            return

        # Validate and filter fields against allowlist (prevents SQL injection)
        fields = [key for key in observations[0].keys() if key in self.ALLOWED_FIELDS]
        if not fields:
            raise ValueError("No valid fields found in observation data")

        # Backtick field names to prevent SQL keyword conflicts
        safe_fields = [f'`{field}`' for field in fields]
        placeholders = [f'%({field})s' for field in fields]
        update_fields = [f'`{field}`=VALUES(`{field}`)' for field in fields
                        if field not in ('park_id', 'observation_time')]

        sql = f"""
            INSERT INTO weather_observations
            ({', '.join(safe_fields)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE
            {', '.join(update_fields)}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.executemany(sql, observations)

            logger.info(
                "Batch inserted weather observations",
                extra={
                    'count': len(observations),
                    'first_park_id': observations[0].get('park_id'),
                    'last_park_id': observations[-1].get('park_id')
                }
            )

        except Exception as e:
            logger.error(
                "Failed to batch insert observations",
                extra={
                    'count': len(observations),
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise

    def get_latest_observation(self, park_id: int) -> Optional[Dict]:
        """Get most recent weather observation for a park.

        Args:
            park_id: Park ID to query

        Returns:
            Latest observation dict, or None if no observations exist
        """
        sql = """
            SELECT *
            FROM weather_observations
            WHERE park_id = %(park_id)s
            ORDER BY observation_time DESC
            LIMIT 1
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, {'park_id': park_id})
                result = cursor.fetchone()

            return result

        except Exception as e:
            logger.error(
                "Failed to get latest observation",
                extra={
                    'park_id': park_id,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise


class WeatherForecastRepository:
    """Repository for weather_forecasts table.

    Handles insert/query operations for weather forecasts.
    All inserts are idempotent via ON DUPLICATE KEY UPDATE.

    Usage:
        ```python
        from database.connection import get_db_connection
        repo = WeatherForecastRepository(get_db_connection())

        forecast = {
            'park_id': 1,
            'issued_at': datetime(...),
            'forecast_time': datetime(...),
            'temperature_c': 24.0,
            'precipitation_probability': 30,
            # ... other fields
        }

        repo.insert_forecast(forecast)
        ```
    """

    # Allowlist of valid field names (prevents SQL injection)
    ALLOWED_FIELDS = {
        'park_id', 'issued_at', 'forecast_time',
        'temperature_c', 'temperature_f',
        'apparent_temperature_c', 'apparent_temperature_f',
        'wind_speed_kmh', 'wind_speed_mph',
        'wind_gusts_kmh', 'wind_gusts_mph',
        'wind_direction_degrees',
        'precipitation_mm', 'precipitation_probability',
        'rain_mm', 'snowfall_mm',
        'cloud_cover_percent', 'visibility_meters',
        'humidity_percent', 'pressure_hpa',
        'weather_code'
    }

    def __init__(self, db: Any):
        """Initialize repository with database connection.

        Args:
            db: MySQL database connection
        """
        self.db = db

    def insert_forecast(self, forecast: Dict) -> None:
        """Insert or update a weather forecast.

        Uses ON DUPLICATE KEY UPDATE for idempotent inserts.
        Unique key: (park_id, issued_at, forecast_time)

        Args:
            forecast: Dictionary with forecast data

        Raises:
            ValueError: If no valid fields found in forecast data
        """
        # Validate and filter fields against allowlist (prevents SQL injection)
        fields = [key for key in forecast.keys() if key in self.ALLOWED_FIELDS]
        if not fields:
            raise ValueError("No valid fields found in forecast data")

        # Backtick field names to prevent SQL keyword conflicts
        safe_fields = [f'`{field}`' for field in fields]
        placeholders = [f'%({field})s' for field in fields]
        update_fields = [f'`{field}`=VALUES(`{field}`)' for field in fields
                        if field not in ('park_id', 'issued_at', 'forecast_time')]

        sql = f"""
            INSERT INTO weather_forecasts
            ({', '.join(safe_fields)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE
            {', '.join(update_fields)}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.execute(sql, forecast)

            logger.debug(
                "Inserted weather forecast",
                extra={
                    'park_id': forecast.get('park_id'),
                    'issued_at': forecast.get('issued_at'),
                    'forecast_time': forecast.get('forecast_time')
                }
            )

        except Exception as e:
            logger.error(
                "Failed to insert forecast",
                extra={
                    'park_id': forecast.get('park_id'),
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise

    def batch_insert_forecasts(self, forecasts: List[Dict]) -> None:
        """Insert or update multiple forecasts in batch.

        Uses executemany() for efficiency.
        Up to 10x faster than individual inserts.

        Args:
            forecasts: List of forecast dictionaries

        Raises:
            ValueError: If no valid fields found in forecast data
        """
        if not forecasts:
            logger.debug("No forecasts to insert")
            return

        # Validate and filter fields against allowlist (prevents SQL injection)
        fields = [key for key in forecasts[0].keys() if key in self.ALLOWED_FIELDS]
        if not fields:
            raise ValueError("No valid fields found in forecast data")

        # Backtick field names to prevent SQL keyword conflicts
        safe_fields = [f'`{field}`' for field in fields]
        placeholders = [f'%({field})s' for field in fields]
        update_fields = [f'`{field}`=VALUES(`{field}`)' for field in fields
                        if field not in ('park_id', 'issued_at', 'forecast_time')]

        sql = f"""
            INSERT INTO weather_forecasts
            ({', '.join(safe_fields)})
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE
            {', '.join(update_fields)}
        """

        try:
            with self.db.cursor() as cursor:
                cursor.executemany(sql, forecasts)

            logger.info(
                "Batch inserted weather forecasts",
                extra={
                    'count': len(forecasts),
                    'first_park_id': forecasts[0].get('park_id'),
                    'issued_at': forecasts[0].get('issued_at')
                }
            )

        except Exception as e:
            logger.error(
                "Failed to batch insert forecasts",
                extra={
                    'count': len(forecasts),
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise
