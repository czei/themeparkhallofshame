"""
Weather Repository (ORM Version)
=================================

Repositories for weather_observations and weather_forecasts tables using SQLAlchemy ORM.

Features:
- Idempotent inserts using session.merge()
- Batch insert support
- Query methods for latest observations
- Structured logging

Migration Note: Uses SQLAlchemy ORM instead of raw SQL.
For ON DUPLICATE KEY UPDATE behavior, uses session.merge() which performs SELECT + INSERT/UPDATE.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func

try:
    from ...models.orm_weather import WeatherObservation, WeatherForecast
    from ...models.orm_park import Park
except ImportError:
    from models.orm_weather import WeatherObservation, WeatherForecast
    from models.orm_park import Park

# Configure structured logging
logger = logging.getLogger(__name__)


class WeatherObservationRepository:
    """
    Repository for weather_observations table using SQLAlchemy ORM.

    Handles insert/query operations for hourly weather observations.
    All inserts are idempotent via session.merge().
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def insert_observation(self, observation: Dict) -> None:
        """
        Insert or update a weather observation.

        Uses session.merge() for idempotent inserts.
        Unique key: (park_id, observation_time)

        Args:
            observation: Dictionary with observation data

        Raises:
            ValueError: If required fields are missing
        """
        if 'park_id' not in observation or 'observation_time' not in observation:
            raise ValueError("Required fields: park_id, observation_time")

        try:
            # Check if observation already exists
            existing = (
                self.session.query(WeatherObservation)
                .filter(
                    and_(
                        WeatherObservation.park_id == observation['park_id'],
                        WeatherObservation.observation_time == observation['observation_time']
                    )
                )
                .first()
            )

            if existing:
                # Update existing observation
                for key, value in observation.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
            else:
                # Create new observation
                obs = WeatherObservation(**observation)
                self.session.add(obs)

            # Note: flush() is removed from individual inserts for better batch performance
            # Batch operations will flush once at the end

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
        """
        Insert or update multiple observations in batch.

        Uses bulk_insert_mappings for efficiency when all observations are new,
        otherwise falls back to individual merge operations.

        Args:
            observations: List of observation dictionaries
        """
        if not observations:
            logger.debug("No observations to insert")
            return

        try:
            # For simplicity, use individual insert_observation calls
            # (ORM bulk operations are complex with ON DUPLICATE KEY UPDATE behavior)
            for observation in observations:
                self.insert_observation(observation)

            # Flush once at the end of the batch for better performance
            self.session.flush()

            logger.info(f"Batch inserted {len(observations)} observations")

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
        """
        Get the most recent weather observation for a park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with observation data or None if not found
        """
        observation = (
            self.session.query(WeatherObservation)
            .filter(WeatherObservation.park_id == park_id)
            .order_by(WeatherObservation.observation_time.desc())
            .first()
        )

        if observation is None:
            return None

        return {
            'park_id': observation.park_id,
            'observation_time': observation.observation_time,
            'collected_at': observation.collected_at,
            'temperature_c': float(observation.temperature_c) if observation.temperature_c else None,
            'temperature_f': float(observation.temperature_f) if observation.temperature_f else None,
            'apparent_temperature_c': float(observation.apparent_temperature_c) if observation.apparent_temperature_c else None,
            'apparent_temperature_f': float(observation.apparent_temperature_f) if observation.apparent_temperature_f else None,
            'wind_speed_kmh': float(observation.wind_speed_kmh) if observation.wind_speed_kmh else None,
            'wind_speed_mph': float(observation.wind_speed_mph) if observation.wind_speed_mph else None,
            'wind_gusts_kmh': float(observation.wind_gusts_kmh) if observation.wind_gusts_kmh else None,
            'wind_gusts_mph': float(observation.wind_gusts_mph) if observation.wind_gusts_mph else None,
            'wind_direction_degrees': observation.wind_direction_degrees,
            'precipitation_mm': float(observation.precipitation_mm) if observation.precipitation_mm else None,
            'precipitation_probability': observation.precipitation_probability,
            'rain_mm': float(observation.rain_mm) if observation.rain_mm else None,
            'snowfall_mm': float(observation.snowfall_mm) if observation.snowfall_mm else None,
            'cloud_cover_percent': observation.cloud_cover_percent,
            'visibility_meters': observation.visibility_meters,
            'humidity_percent': observation.humidity_percent,
            'pressure_hpa': float(observation.pressure_hpa) if observation.pressure_hpa else None,
            'weather_code': observation.weather_code
        }


class WeatherForecastRepository:
    """
    Repository for weather_forecasts table using SQLAlchemy ORM.

    Handles insert/query operations for weather forecasts.
    All inserts are idempotent via session.merge().
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def insert_forecast(self, forecast: Dict) -> None:
        """
        Insert or update a weather forecast.

        Uses session.merge() for idempotent inserts.
        Unique key: (park_id, forecast_time, forecast_hour)

        Args:
            forecast: Dictionary with forecast data

        Raises:
            ValueError: If required fields are missing
        """
        if 'park_id' not in forecast or 'forecast_time' not in forecast or 'forecast_hour' not in forecast:
            raise ValueError("Required fields: park_id, forecast_time, forecast_hour")

        try:
            # Check if forecast already exists
            existing = (
                self.session.query(WeatherForecast)
                .filter(
                    and_(
                        WeatherForecast.park_id == forecast['park_id'],
                        WeatherForecast.forecast_time == forecast['forecast_time'],
                        WeatherForecast.forecast_hour == forecast['forecast_hour']
                    )
                )
                .first()
            )

            if existing:
                # Update existing forecast
                for key, value in forecast.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
            else:
                # Create new forecast
                fc = WeatherForecast(**forecast)
                self.session.add(fc)

            # Note: flush() is removed from individual inserts for better batch performance
            # Batch operations will flush once at the end

            logger.debug(
                "Inserted weather forecast",
                extra={
                    'park_id': forecast.get('park_id'),
                    'forecast_time': forecast.get('forecast_time'),
                    'forecast_hour': forecast.get('forecast_hour')
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
        """
        Insert or update multiple forecasts in batch.

        Args:
            forecasts: List of forecast dictionaries
        """
        if not forecasts:
            logger.debug("No forecasts to insert")
            return

        try:
            # For simplicity, use individual insert_forecast calls
            for forecast in forecasts:
                self.insert_forecast(forecast)

            # Flush once at the end of the batch for better performance
            self.session.flush()

            logger.info(f"Batch inserted {len(forecasts)} forecasts")

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
