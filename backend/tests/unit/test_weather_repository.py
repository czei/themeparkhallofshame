"""
Unit Tests: Weather Repository
================================

Tests weather repositories with mocked SQLAlchemy sessions.

Test Strategy:
- Mock SQLAlchemy Session (no real DB)
- Test ORM operations (session.query, session.add, session.flush)
- Test error handling

Coverage:
- T028: WeatherObservationRepository insert
- T029: WeatherForecastRepository insert
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from src.database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)
from src.models.orm_weather import WeatherObservation, WeatherForecast


@pytest.fixture
def mock_session() -> MagicMock:
    """Mock SQLAlchemy Session used by the repository."""
    session = MagicMock(name="Session")
    session.query = MagicMock(name="query")
    session.add = MagicMock(name="add")
    session.flush = MagicMock(name="flush")
    return session


@pytest.fixture
def observation_repo(mock_session: MagicMock) -> WeatherObservationRepository:
    """WeatherObservationRepository under test."""
    return WeatherObservationRepository(session=mock_session)


@pytest.fixture
def forecast_repo(mock_session: MagicMock) -> WeatherForecastRepository:
    """WeatherForecastRepository under test."""
    return WeatherForecastRepository(session=mock_session)


def _configure_query_chain(
    mock_session: MagicMock,
    *,
    first_return: Optional[Any] = None,
) -> MagicMock:
    """
    Configure session.query(...).filter(...).order_by(...).first() chain.

    Returns the query mock so tests can assert intermediate calls if desired.
    """
    query = MagicMock(name="query_obj")
    filtered = MagicMock(name="filtered")
    ordered = MagicMock(name="ordered")

    mock_session.query.return_value = query
    query.filter.return_value = filtered
    filtered.order_by.return_value = ordered

    # Support both patterns:
    # - query.filter(...).first()
    # - query.filter(...).order_by(...).first()
    filtered.first.return_value = first_return
    ordered.first.return_value = first_return

    return query


class TestWeatherObservationRepository:
    """Unit tests for WeatherObservationRepository."""

    def test_initialization(self, mock_session: MagicMock):
        """Repository should initialize with session attribute."""
        repo = WeatherObservationRepository(mock_session)
        assert repo.session is mock_session

    def test_insert_observation_basic(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """insert_observation() should add new observation via ORM."""
        _configure_query_chain(mock_session, first_return=None)

        observation = {
            'park_id': 42,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': Decimal("24.0"),
            'temperature_f': Decimal("75.2"),
            'wind_speed_kmh': Decimal("8.37"),
            'wind_speed_mph': Decimal("5.2"),
            'weather_code': 0,
        }

        observation_repo.insert_observation(observation)

        # Verify session.query was called to check for existing
        mock_session.query.assert_called_once()
        # Verify session.add was called for new observation
        mock_session.add.assert_called_once()

    def test_insert_observation_params(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """insert_observation() should create ORM object with correct attributes."""
        _configure_query_chain(mock_session, first_return=None)

        observation = {
            'park_id': 42,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': Decimal("24.0"),
            'temperature_f': Decimal("75.2"),
        }

        observation_repo.insert_observation(observation)

        # Get the ORM object passed to session.add
        added_obj = mock_session.add.call_args[0][0]
        assert isinstance(added_obj, WeatherObservation)
        assert added_obj.park_id == 42
        assert added_obj.observation_time == datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        assert added_obj.temperature_c == Decimal("24.0")
        assert added_obj.temperature_f == Decimal("75.2")

    def test_insert_observation_updates_existing(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """insert_observation() should update existing observation when found."""
        existing = SimpleNamespace(
            park_id=42,
            observation_time=datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            temperature_f=Decimal("70.0"),
        )
        _configure_query_chain(mock_session, first_return=existing)

        observation = {
            'park_id': 42,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_f': Decimal("75.2"),
        }

        observation_repo.insert_observation(observation)

        # Should NOT call add for existing observation
        mock_session.add.assert_not_called()
        # Should update existing object
        assert existing.temperature_f == Decimal("75.2")

    def test_insert_observation_requires_park_id(self, observation_repo: WeatherObservationRepository):
        """insert_observation() should raise ValueError if park_id is missing."""
        with pytest.raises(ValueError, match="Required fields"):
            observation_repo.insert_observation({
                'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            })

    def test_insert_observation_requires_observation_time(self, observation_repo: WeatherObservationRepository):
        """insert_observation() should raise ValueError if observation_time is missing."""
        with pytest.raises(ValueError, match="Required fields"):
            observation_repo.insert_observation({
                'park_id': 42,
            })

    def test_batch_insert_observations_iterates(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """batch_insert_observations() should insert each observation and flush."""
        _configure_query_chain(mock_session, first_return=None)

        observations = [
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_c': Decimal("24.0"),
                'temperature_f': Decimal("75.2"),
            },
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
                'temperature_c': Decimal("23.5"),
                'temperature_f': Decimal("74.3"),
            },
        ]

        observation_repo.batch_insert_observations(observations)

        # Verify query was called twice (once per observation)
        assert mock_session.query.call_count == 2
        # Verify add was called twice
        assert mock_session.add.call_count == 2
        # Verify flush was called once at the end
        mock_session.flush.assert_called_once()

    def test_batch_insert_observations_empty_list(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """batch_insert_observations() should handle empty list gracefully."""
        observation_repo.batch_insert_observations([])

        # Should not call any session methods for empty list
        mock_session.query.assert_not_called()
        mock_session.add.assert_not_called()
        mock_session.flush.assert_not_called()

    def test_get_latest_observation_query(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """get_latest_observation() should query for most recent observation."""
        # Must include ALL fields that get_latest_observation() accesses
        obs_obj = SimpleNamespace(
            park_id=42,
            observation_time=datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            collected_at=datetime(2025, 12, 17, 0, 5, 0, tzinfo=timezone.utc),
            temperature_c=Decimal("24.0"),
            temperature_f=Decimal("75.2"),
            apparent_temperature_c=None,
            apparent_temperature_f=None,
            wind_speed_kmh=None,
            wind_speed_mph=None,
            wind_gusts_kmh=None,
            wind_gusts_mph=None,
            wind_direction_degrees=None,
            precipitation_mm=None,
            precipitation_probability=None,
            rain_mm=None,
            snowfall_mm=None,
            cloud_cover_percent=None,
            visibility_meters=None,
            humidity_percent=None,
            pressure_hpa=None,
            weather_code=0,
        )
        _configure_query_chain(mock_session, first_return=obs_obj)

        result = observation_repo.get_latest_observation(park_id=42)

        # Verify session.query was called
        mock_session.query.assert_called_once()
        # Verify result is a dict with correct values
        assert result['park_id'] == 42
        assert result['temperature_f'] == 75.2  # Converted to float
        assert result['temperature_c'] == 24.0  # Converted to float

    def test_get_latest_observation_no_data(self, observation_repo: WeatherObservationRepository, mock_session: MagicMock):
        """get_latest_observation() should return None when no data exists."""
        _configure_query_chain(mock_session, first_return=None)

        result = observation_repo.get_latest_observation(park_id=42)

        assert result is None


class TestWeatherForecastRepository:
    """Unit tests for WeatherForecastRepository."""

    def test_initialization(self, mock_session: MagicMock):
        """Repository should initialize with session attribute."""
        repo = WeatherForecastRepository(mock_session)
        assert repo.session is mock_session

    def test_insert_forecast_basic(self, forecast_repo: WeatherForecastRepository, mock_session: MagicMock):
        """insert_forecast() should add new forecast via ORM."""
        _configure_query_chain(mock_session, first_return=None)

        forecast = {
            'park_id': 42,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': Decimal("24.0"),
            'temperature_f': Decimal("75.2"),
            'precipitation_probability': 30,
        }

        forecast_repo.insert_forecast(forecast)

        # Verify session.query was called to check for existing
        mock_session.query.assert_called_once()
        # Verify session.add was called for new forecast
        mock_session.add.assert_called_once()

    def test_insert_forecast_params(self, forecast_repo: WeatherForecastRepository, mock_session: MagicMock):
        """insert_forecast() should create ORM object with correct attributes."""
        _configure_query_chain(mock_session, first_return=None)

        forecast = {
            'park_id': 42,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': Decimal("24.0"),
            'precipitation_probability': 30,
        }

        forecast_repo.insert_forecast(forecast)

        # Get the ORM object passed to session.add
        added_obj = mock_session.add.call_args[0][0]
        assert isinstance(added_obj, WeatherForecast)
        assert added_obj.park_id == 42
        assert added_obj.forecast_time == datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)
        assert added_obj.precipitation_probability == 30

    def test_insert_forecast_updates_existing(self, forecast_repo: WeatherForecastRepository, mock_session: MagicMock):
        """insert_forecast() should update existing forecast when found."""
        existing = SimpleNamespace(
            park_id=42,
            forecast_time=datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            precipitation_probability=20,
        )
        _configure_query_chain(mock_session, first_return=existing)

        forecast = {
            'park_id': 42,
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'precipitation_probability': 50,
        }

        forecast_repo.insert_forecast(forecast)

        # Should NOT call add for existing forecast
        mock_session.add.assert_not_called()
        # Should update existing object
        assert existing.precipitation_probability == 50

    def test_batch_insert_forecasts_iterates(self, forecast_repo: WeatherForecastRepository, mock_session: MagicMock):
        """batch_insert_forecasts() should insert each forecast and flush."""
        _configure_query_chain(mock_session, first_return=None)

        forecasts = [
            {
                'park_id': 1,
                'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_c': Decimal("24.0"),
            },
            {
                'park_id': 1,
                'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'forecast_time': datetime(2025, 12, 18, 1, 0, 0, tzinfo=timezone.utc),
                'temperature_c': Decimal("23.5"),
            },
        ]

        forecast_repo.batch_insert_forecasts(forecasts)

        # Verify query was called twice (once per forecast)
        assert mock_session.query.call_count == 2
        # Verify add was called twice
        assert mock_session.add.call_count == 2
        # Verify flush was called once at the end
        mock_session.flush.assert_called_once()

    def test_batch_insert_forecasts_empty_list(self, forecast_repo: WeatherForecastRepository, mock_session: MagicMock):
        """batch_insert_forecasts() should handle empty list gracefully."""
        forecast_repo.batch_insert_forecasts([])

        # Should not call any session methods for empty list
        mock_session.query.assert_not_called()
        mock_session.add.assert_not_called()
        mock_session.flush.assert_not_called()
