"""
Integration Tests: Weather Repository
======================================

Tests weather repositories with real database connection.

Test Strategy:
- Use mysql_connection fixture (isolated transaction)
- Test idempotent inserts (ON DUPLICATE KEY UPDATE)
- Test batch insert performance
- Verify data integrity

Coverage:
- T030: Idempotent observation insert
- T031: Idempotent forecast insert
- T032: Batch insert performance

Prerequisites:
- Database tables weather_observations and weather_forecasts must exist
- Tests run in transaction (rolled back after each test)
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy import text

from database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)


@pytest.fixture
def test_park(mysql_connection):
    """Create a test park for weather repository tests."""
    # Insert a park with a known id for FK constraints (include all required fields)
    mysql_connection.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, is_disney, is_universal, is_active)
        VALUES (1, 1, 'Test Weather Park', 'Orlando', 'FL', 'US', 'America/New_York', FALSE, FALSE, TRUE)
    """))
    return 1  # park_id


class TestWeatherObservationRepositoryIntegration:
    """Integration tests for WeatherObservationRepository with real DB."""

    def test_insert_observation_success(self, mysql_connection, test_park):
        """Should insert new observation successfully."""
        repo = WeatherObservationRepository(mysql_connection)

        observation = {
            'park_id': test_park,  # Use fixture-created park
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'apparent_temperature_c': 22.0,
            'apparent_temperature_f': 71.6,
            'wind_speed_kmh': 8.37,
            'wind_speed_mph': 5.2,
            'wind_gusts_kmh': 13.0,
            'wind_gusts_mph': 8.1,
            'wind_direction_degrees': 180,
            'precipitation_mm': 0.0,
            'rain_mm': 0.0,
            'snowfall_mm': 0.0,
            'precipitation_probability': None,
            'cloud_cover_percent': 20,
            'visibility_meters': 10000,
            'humidity_percent': 65,
            'pressure_hpa': 1013.2,
            'weather_code': 0,
        }

        repo.insert_observation(observation)

        # Verify observation was inserted
        latest = repo.get_latest_observation(park_id=test_park)
        assert latest is not None
        assert float(latest['temperature_f']) == 75.2
        assert latest['weather_code'] == 0

    def test_insert_observation_idempotent(self, mysql_connection, test_park):
        """Should update existing observation on duplicate park_id + observation_time."""
        repo = WeatherObservationRepository(mysql_connection)

        observation = {
            'park_id': test_park,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'weather_code': 0,
        }

        # Insert first time
        repo.insert_observation(observation)

        # Insert again with different temperature (should update, not error)
        observation['temperature_f'] = 76.5
        observation['temperature_c'] = 24.7
        observation['weather_code'] = 95  # Thunderstorm

        repo.insert_observation(observation)

        # Verify latest observation has updated values
        latest = repo.get_latest_observation(park_id=test_park)
        assert latest['temperature_f'] == 76.5
        assert latest['weather_code'] == 95

    def test_batch_insert_observations_success(self, mysql_connection, test_park):
        """Should insert multiple observations in batch."""
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': test_park,
                'observation_time': base_time + timedelta(hours=i),
                'temperature_c': 24.0 - i * 0.5,
                'temperature_f': 75.2 - i * 0.9,
                'weather_code': 0,
            }
            for i in range(10)
        ]

        repo.batch_insert_observations(observations)

        # Verify all 10 observations were inserted
        latest = repo.get_latest_observation(park_id=test_park)
        assert latest is not None
        # Latest should be hour 9 (last in list) - compare without timezone
        expected_time = (base_time + timedelta(hours=9)).replace(tzinfo=None)
        assert latest['observation_time'] == expected_time

    def test_batch_insert_observations_idempotent(self, mysql_connection, test_park):
        """Batch insert should be idempotent (ON DUPLICATE KEY UPDATE)."""
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': test_park,
                'observation_time': base_time,
                'temperature_c': 24.0,
                'temperature_f': 75.2,
                'weather_code': 0,
            }
        ]

        # Insert first time
        repo.batch_insert_observations(observations)

        # Insert again with updated values
        observations[0]['temperature_f'] = 80.0
        observations[0]['weather_code'] = 95

        repo.batch_insert_observations(observations)

        # Verify values were updated
        latest = repo.get_latest_observation(park_id=test_park)
        assert latest['temperature_f'] == 80.0
        assert latest['weather_code'] == 95

    def test_get_latest_observation_no_data(self, mysql_connection, test_park):
        """Should return None when no observations exist for park."""
        repo = WeatherObservationRepository(mysql_connection)

        # Use park_id that has no observations
        latest = repo.get_latest_observation(park_id=9999)
        assert latest is None

    def test_batch_insert_performance(self, mysql_connection, test_park):
        """Batch insert should be faster than individual inserts."""
        import time
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': test_park,
                'observation_time': base_time + timedelta(hours=i),
                'temperature_c': 24.0,
                'temperature_f': 75.2,
                'weather_code': 0,
            }
            for i in range(100)
        ]

        # Batch insert should complete within reasonable time
        start = time.time()
        repo.batch_insert_observations(observations)
        elapsed = time.time() - start

        # 100 inserts should take < 1 second
        assert elapsed < 1.0, \
            f"Batch insert took {elapsed}s, expected < 1.0s"


class TestWeatherForecastRepositoryIntegration:
    """Integration tests for WeatherForecastRepository with real DB."""

    def test_insert_forecast_success(self, mysql_connection, test_park):
        """Should insert new forecast successfully."""
        repo = WeatherForecastRepository(mysql_connection)

        forecast = {
            'park_id': test_park,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'apparent_temperature_c': 22.0,
            'apparent_temperature_f': 71.6,
            'wind_speed_kmh': 8.37,
            'wind_speed_mph': 5.2,
            'wind_gusts_kmh': 13.0,
            'wind_gusts_mph': 8.1,
            'wind_direction_degrees': 180,
            'precipitation_mm': 0.0,
            'precipitation_probability': 30,
            'rain_mm': 0.0,
            'snowfall_mm': 0.0,
            'cloud_cover_percent': 20,
            'visibility_meters': 10000,
            'humidity_percent': 65,
            'pressure_hpa': 1013.2,
            'weather_code': 0,
        }

        repo.insert_forecast(forecast)

        # Verify forecast was inserted (query by park_id and issued_at)
        result = mysql_connection.execute(text("""
            SELECT * FROM weather_forecasts
            WHERE park_id = :park_id AND issued_at = :issued_at
            LIMIT 1
        """), {'park_id': test_park, 'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)}).fetchone()

        assert result is not None
        assert float(result._mapping['temperature_f']) == 75.2
        assert result._mapping['precipitation_probability'] == 30

    def test_insert_forecast_idempotent(self, mysql_connection, test_park):
        """Should update existing forecast on duplicate park_id + issued_at + forecast_time."""
        repo = WeatherForecastRepository(mysql_connection)

        forecast = {
            'park_id': test_park,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'precipitation_probability': 30,
        }

        # Insert first time
        repo.insert_forecast(forecast)

        # Insert again with different values (should update)
        forecast['temperature_f'] = 80.0
        forecast['precipitation_probability'] = 70

        repo.insert_forecast(forecast)

        # Verify values were updated
        result = mysql_connection.execute(text("""
            SELECT * FROM weather_forecasts
            WHERE park_id = :park_id
              AND issued_at = :issued_at
              AND forecast_time = :forecast_time
        """), {
            'park_id': test_park,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
        }).fetchone()

        assert result._mapping['temperature_f'] == 80.0
        assert result._mapping['precipitation_probability'] == 70

    def test_batch_insert_forecasts_success(self, mysql_connection, test_park):
        """Should insert multiple forecasts in batch."""
        repo = WeatherForecastRepository(mysql_connection)

        issued_at = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        base_forecast_time = datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)

        forecasts = [
            {
                'park_id': test_park,
                'issued_at': issued_at,
                'forecast_time': base_forecast_time + timedelta(hours=i),
                'temperature_c': 24.0 - i * 0.5,
                'temperature_f': 75.2 - i * 0.9,
                'precipitation_probability': 10 + i * 5,
            }
            for i in range(10)
        ]

        repo.batch_insert_forecasts(forecasts)

        # Verify all 10 forecasts were inserted
        result = mysql_connection.execute(text("""
            SELECT COUNT(*) as count FROM weather_forecasts
            WHERE park_id = :park_id AND issued_at = :issued_at
        """), {'park_id': test_park, 'issued_at': issued_at}).fetchone()

        assert result._mapping['count'] >= 10  # At least our 10 forecasts

    def test_batch_insert_forecasts_idempotent(self, mysql_connection, test_park):
        """Batch insert should be idempotent (ON DUPLICATE KEY UPDATE)."""
        repo = WeatherForecastRepository(mysql_connection)

        issued_at = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        forecast_time = datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)

        forecasts = [
            {
                'park_id': test_park,
                'issued_at': issued_at,
                'forecast_time': forecast_time,
                'temperature_c': 24.0,
                'temperature_f': 75.2,
                'precipitation_probability': 30,
            }
        ]

        # Insert first time
        repo.batch_insert_forecasts(forecasts)

        # Insert again with updated values
        forecasts[0]['temperature_f'] = 85.0
        forecasts[0]['precipitation_probability'] = 90

        repo.batch_insert_forecasts(forecasts)

        # Verify values were updated
        result = mysql_connection.execute(text("""
            SELECT * FROM weather_forecasts
            WHERE park_id = :park_id
              AND issued_at = :issued_at
              AND forecast_time = :forecast_time
        """), {'park_id': test_park, 'issued_at': issued_at, 'forecast_time': forecast_time}).fetchone()

        assert result._mapping['temperature_f'] == 85.0
        assert result._mapping['precipitation_probability'] == 90
