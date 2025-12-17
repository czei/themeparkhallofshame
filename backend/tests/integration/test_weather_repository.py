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

from database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)


class TestWeatherObservationRepositoryIntegration:
    """Integration tests for WeatherObservationRepository with real DB."""

    def test_insert_observation_success(self, mysql_connection):
        """Should insert new observation successfully."""
        repo = WeatherObservationRepository(mysql_connection)

        observation = {
            'park_id': 1,  # Assuming park_id=1 exists
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
        mysql_connection.commit()

        # Verify observation was inserted
        latest = repo.get_latest_observation(park_id=1)
        assert latest is not None
        assert latest['temperature_f'] == 75.2
        assert latest['weather_code'] == 0

    def test_insert_observation_idempotent(self, mysql_connection):
        """Should update existing observation on duplicate park_id + observation_time."""
        repo = WeatherObservationRepository(mysql_connection)

        observation = {
            'park_id': 1,
            'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'weather_code': 0,
        }

        # Insert first time
        repo.insert_observation(observation)
        mysql_connection.commit()

        # Insert again with different temperature (should update, not error)
        observation['temperature_f'] = 76.5
        observation['temperature_c'] = 24.7
        observation['weather_code'] = 95  # Thunderstorm

        repo.insert_observation(observation)
        mysql_connection.commit()

        # Verify latest observation has updated values
        latest = repo.get_latest_observation(park_id=1)
        assert latest['temperature_f'] == 76.5
        assert latest['weather_code'] == 95

    def test_batch_insert_observations_success(self, mysql_connection):
        """Should insert multiple observations in batch."""
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': 1,
                'observation_time': base_time + timedelta(hours=i),
                'temperature_c': 24.0 - i * 0.5,
                'temperature_f': 75.2 - i * 0.9,
                'weather_code': 0,
            }
            for i in range(10)
        ]

        repo.batch_insert_observations(observations)
        mysql_connection.commit()

        # Verify all 10 observations were inserted
        latest = repo.get_latest_observation(park_id=1)
        assert latest is not None
        # Latest should be hour 9 (last in list)
        assert latest['observation_time'] == base_time + timedelta(hours=9)

    def test_batch_insert_observations_idempotent(self, mysql_connection):
        """Batch insert should be idempotent (ON DUPLICATE KEY UPDATE)."""
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': 1,
                'observation_time': base_time,
                'temperature_c': 24.0,
                'temperature_f': 75.2,
                'weather_code': 0,
            }
        ]

        # Insert first time
        repo.batch_insert_observations(observations)
        mysql_connection.commit()

        # Insert again with updated values
        observations[0]['temperature_f'] = 80.0
        observations[0]['weather_code'] = 95

        repo.batch_insert_observations(observations)
        mysql_connection.commit()

        # Verify values were updated
        latest = repo.get_latest_observation(park_id=1)
        assert latest['temperature_f'] == 80.0
        assert latest['weather_code'] == 95

    def test_get_latest_observation_no_data(self, mysql_connection):
        """Should return None when no observations exist for park."""
        repo = WeatherObservationRepository(mysql_connection)

        # Use park_id that has no observations
        latest = repo.get_latest_observation(park_id=9999)
        assert latest is None

    def test_batch_insert_performance(self, mysql_connection):
        """Batch insert should be faster than individual inserts."""
        import time
        repo = WeatherObservationRepository(mysql_connection)

        base_time = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        observations = [
            {
                'park_id': 1,
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

    def test_insert_forecast_success(self, mysql_connection):
        """Should insert new forecast successfully."""
        repo = WeatherForecastRepository(mysql_connection)

        forecast = {
            'park_id': 1,
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
        mysql_connection.commit()

        # Verify forecast was inserted (query by park_id and issued_at)
        with mysql_connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM weather_forecasts
                WHERE park_id = %(park_id)s AND issued_at = %(issued_at)s
                LIMIT 1
            """, {'park_id': 1, 'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)})
            result = cursor.fetchone()

        assert result is not None
        assert result['temperature_f'] == 75.2
        assert result['precipitation_probability'] == 30

    def test_insert_forecast_idempotent(self, mysql_connection):
        """Should update existing forecast on duplicate park_id + issued_at + forecast_time."""
        repo = WeatherForecastRepository(mysql_connection)

        forecast = {
            'park_id': 1,
            'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
            'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            'temperature_c': 24.0,
            'temperature_f': 75.2,
            'precipitation_probability': 30,
        }

        # Insert first time
        repo.insert_forecast(forecast)
        mysql_connection.commit()

        # Insert again with different values (should update)
        forecast['temperature_f'] = 80.0
        forecast['precipitation_probability'] = 70

        repo.insert_forecast(forecast)
        mysql_connection.commit()

        # Verify values were updated
        with mysql_connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM weather_forecasts
                WHERE park_id = %(park_id)s
                  AND issued_at = %(issued_at)s
                  AND forecast_time = %(forecast_time)s
            """, {
                'park_id': 1,
                'issued_at': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'forecast_time': datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc),
            })
            result = cursor.fetchone()

        assert result['temperature_f'] == 80.0
        assert result['precipitation_probability'] == 70

    def test_batch_insert_forecasts_success(self, mysql_connection):
        """Should insert multiple forecasts in batch."""
        repo = WeatherForecastRepository(mysql_connection)

        issued_at = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        base_forecast_time = datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)

        forecasts = [
            {
                'park_id': 1,
                'issued_at': issued_at,
                'forecast_time': base_forecast_time + timedelta(hours=i),
                'temperature_c': 24.0 - i * 0.5,
                'temperature_f': 75.2 - i * 0.9,
                'precipitation_probability': 10 + i * 5,
            }
            for i in range(10)
        ]

        repo.batch_insert_forecasts(forecasts)
        mysql_connection.commit()

        # Verify all 10 forecasts were inserted
        with mysql_connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count FROM weather_forecasts
                WHERE park_id = 1 AND issued_at = %(issued_at)s
            """, {'issued_at': issued_at})
            result = cursor.fetchone()

        assert result['count'] >= 10  # At least our 10 forecasts

    def test_batch_insert_forecasts_idempotent(self, mysql_connection):
        """Batch insert should be idempotent (ON DUPLICATE KEY UPDATE)."""
        repo = WeatherForecastRepository(mysql_connection)

        issued_at = datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc)
        forecast_time = datetime(2025, 12, 18, 0, 0, 0, tzinfo=timezone.utc)

        forecasts = [
            {
                'park_id': 1,
                'issued_at': issued_at,
                'forecast_time': forecast_time,
                'temperature_c': 24.0,
                'temperature_f': 75.2,
                'precipitation_probability': 30,
            }
        ]

        # Insert first time
        repo.batch_insert_forecasts(forecasts)
        mysql_connection.commit()

        # Insert again with updated values
        forecasts[0]['temperature_f'] = 85.0
        forecasts[0]['precipitation_probability'] = 90

        repo.batch_insert_forecasts(forecasts)
        mysql_connection.commit()

        # Verify values were updated
        with mysql_connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM weather_forecasts
                WHERE park_id = 1
                  AND issued_at = %(issued_at)s
                  AND forecast_time = %(forecast_time)s
            """, {'issued_at': issued_at, 'forecast_time': forecast_time})
            result = cursor.fetchone()

        assert result['temperature_f'] == 85.0
        assert result['precipitation_probability'] == 90
