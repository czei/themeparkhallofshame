"""
Integration Tests: Weather Collection
======================================

Tests WeatherCollector with real database and API mocks.

Test Strategy:
- Use mysql_session fixture (isolated transaction)
- Mock API client to avoid real API calls
- Test concurrent collection with ThreadPoolExecutor
- Test rate limiting behavior
- Test graceful failure handling

Coverage:
- T043: Concurrent collection with 10 workers
- T044: Rate limiting (1 req/sec)
- T045: Graceful park failure handling

Prerequisites:
- Database tables weather_observations must exist
- Parks table must have test data
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from scripts.collect_weather import WeatherCollector


class TestWeatherCollectionIntegration:
    """Integration tests for WeatherCollector with real DB."""

    @pytest.fixture
    def mock_api_client(self):
        """Mock OpenMeteo API client for integration tests."""
        client = MagicMock()
        client.fetch_weather.return_value = {
            'hourly': {
                'time': ['2025-12-17T00:00', '2025-12-17T01:00'],
                'temperature_2m': [75.2, 74.8],
                'apparent_temperature': [72.1, 71.5],
                'precipitation': [0.0, 0.1],
                'precipitation_probability': [0, 30],
                'rain': [0.0, 0.1],
                'snowfall': [0.0, 0.0],
                'weather_code': [0, 61],
                'cloud_cover': [20, 60],
                'wind_speed_10m': [5.2, 7.8],
                'wind_gusts_10m': [8.1, 11.2],
                'wind_direction_10m': [180, 225],
                'relative_humidity_2m': [65, 70],
                'surface_pressure': [1013.2, 1012.8],
                'visibility': [10000, 8000],
            }
        }
        def parse_observations_side_effect(weather_data, park_id):
            """Return observations that match the park being processed to satisfy FK constraints."""
            obs_time_0 = datetime(2025, 12, 17, 0, 0, 0)  # naive UTC to satisfy MySQL DATETIME
            obs_time_1 = datetime(2025, 12, 17, 1, 0, 0)
            return [
                {
                    'park_id': park_id,
                    'observation_time': obs_time_0,
                    'temperature_f': 75.2,
                    'temperature_c': 24.0,
                    'weather_code': 0,
                },
                {
                    'park_id': park_id,
                    'observation_time': obs_time_1,
                    'temperature_f': 74.8,
                    'temperature_c': 23.8,
                    'weather_code': 61,
                },
            ]

        client.parse_observations.side_effect = parse_observations_side_effect
        return client

    def test_concurrent_collection_with_multiple_parks(self, mysql_session, mock_api_client):
        """Should collect weather for multiple parks concurrently."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            # Query test parks using SQLAlchemy
            from sqlalchemy import text
            result = mysql_session.execute(text("SELECT park_id, latitude, longitude FROM parks LIMIT 3"))
            parks = [dict(row._mapping) for row in result]

            if len(parks) < 3:
                pytest.skip("Need at least 3 parks in database for this test")

            # Run collection
            start_time = time.time()
            results = collector.run(mode='current', test_mode=True)
            elapsed = time.time() - start_time

            # Should complete quickly (concurrent execution)
            # 3 parks with 1 req/sec rate limit should take ~3 seconds
            # With 10 workers, they run in parallel, so should be ~1-2 seconds
            assert elapsed < 5.0, \
                f"Concurrent collection took {elapsed}s, expected < 5s"

            # Should have results for all parks
            assert len(results) >= 3

            # Should have mostly successful results
            successful = [r for r in results if r['success']]
            assert len(successful) >= 2

    def test_rate_limiting_enforced(self, mysql_session, mock_api_client):
        """Should enforce 1 req/sec rate limit."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            # Create 5 test parks
            parks = [
                {'park_id': i, 'latitude': 28.0 + i, 'longitude': -81.0}
                for i in range(1, 6)
            ]

            # Collect sequentially (easier to measure rate limiting)
            start_time = time.time()
            for park in parks:
                collector._collect_for_park(park)
            elapsed = time.time() - start_time

            # 5 requests at 1 req/sec should take ~4-5 seconds
            assert elapsed >= 4.0, \
                f"Rate limiting not enforced: 5 requests took {elapsed}s, expected >= 4s"

    def test_graceful_park_failure_handling(self, mysql_session, mock_api_client):
        """Should handle individual park failures gracefully."""
        # Make API fail for specific park
        def side_effect_fetch(latitude, longitude, **kwargs):
            if latitude == 99.0:  # Invalid coordinate
                raise Exception("Invalid coordinates")
            return mock_api_client.fetch_weather.return_value

        mock_api_client.fetch_weather.side_effect = side_effect_fetch

        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            parks = [
                {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116},  # Valid
                {'park_id': 2, 'latitude': 99.0, 'longitude': -81.0},          # Invalid
                {'park_id': 3, 'latitude': 33.8121, 'longitude': -117.9190},   # Valid
            ]

            # Ensure parks exist in DB to satisfy FK constraints on weather_observations
            from sqlalchemy import text
            park_ids = [p['park_id'] for p in parks]
            mysql_session.execute(text("DELETE FROM parks WHERE park_id IN :ids"), {'ids': tuple(park_ids)})
            for p in parks:
                mysql_session.execute(text("""
                    INSERT INTO parks (
                        park_id, queue_times_id, name, city, state_province, country,
                        latitude, longitude, timezone, operator, is_disney, is_universal, is_active
                    )
                    VALUES (
                        :park_id, :queue_times_id, :name, 'TestCity', 'TS', 'US',
                        :latitude, :longitude, 'America/New_York', 'TestOperator', 0, 0, 1
                    )
                    ON DUPLICATE KEY UPDATE name = VALUES(name)
                """), {
                    'park_id': p['park_id'],
                    'queue_times_id': 900000 + p['park_id'],
                    'name': f"Test Park {p['park_id']}",
                    'latitude': p['latitude'],
                    'longitude': p['longitude']
                })

            # Mock _get_parks to return our test parks
            with patch.object(collector, '_get_parks', return_value=parks):
                results = collector.run(mode='current', test_mode=True)

            # Should have 3 results
            assert len(results) == 3

            # Should have 2 successes and 1 failure
            successful = [r for r in results if r['success']]
            failed = [r for r in results if not r['success']]

            assert len(successful) == 2
            assert len(failed) == 1
            assert failed[0]['park_id'] == 2

    def test_batch_insert_observations(self, mysql_session, mock_api_client):
        """Should batch insert observations to database."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            # Get a real park from the database
            from sqlalchemy import text
            result = mysql_session.execute(text("SELECT park_id, latitude, longitude FROM parks LIMIT 1"))
            park_row = result.first()

            if not park_row:
                pytest.skip("Need at least 1 park in database for this test")

            park = dict(park_row._mapping)

            result = collector._collect_for_park(park)

            assert result['success'] is True

            # Verify observations were inserted using SQLAlchemy
            count_result = mysql_session.execute(text("""
                SELECT COUNT(*) as count
                FROM weather_observations
                WHERE park_id = :park_id
                  AND observation_time >= '2025-12-17 00:00:00'
            """), {'park_id': park['park_id']})
            row = count_result.first()

            # Should have inserted at least 1 observation (test mode)
            assert row[0] >= 1

    def test_failure_threshold_aborts_on_systemic_failure(self, mysql_session, mock_api_client):
        """Should abort when >50% of parks fail."""
        # Make API fail for most parks
        mock_api_client.fetch_weather.side_effect = Exception("API down")

        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            parks = [
                {'park_id': i, 'latitude': 28.0 + i, 'longitude': -81.0}
                for i in range(1, 11)  # 10 parks
            ]

            with patch.object(collector, '_get_parks', return_value=parks):
                # Should raise RuntimeError due to failure threshold
                with pytest.raises(RuntimeError, match="Collection failed for"):
                    collector.run(mode='current', test_mode=True)

    def test_concurrent_execution_uses_thread_pool(self, mysql_session, mock_api_client):
        """Should use ThreadPoolExecutor for concurrent collection."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            # Verify collector has ThreadPoolExecutor
            assert hasattr(collector, 'max_workers')
            assert collector.max_workers == 10

    def test_test_mode_limits_parks(self, mysql_session, mock_api_client):
        """test_mode should limit collection to 5 parks."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_session)

            # Create 10 test parks
            parks = [
                {'park_id': i, 'latitude': 28.0 + i, 'longitude': -81.0}
                for i in range(1, 11)
            ]

            # Ensure parks exist in DB for FK constraints
            from sqlalchemy import text
            park_ids = [p['park_id'] for p in parks]
            mysql_session.execute(text("DELETE FROM parks WHERE park_id IN :ids"), {'ids': tuple(park_ids)})
            for p in parks:
                mysql_session.execute(text("""
                    INSERT INTO parks (
                        park_id, queue_times_id, name, city, state_province, country,
                        latitude, longitude, timezone, operator, is_disney, is_universal, is_active
                    )
                    VALUES (
                        :park_id, :queue_times_id, :name, 'TestCity', 'TS', 'US',
                        :latitude, :longitude, 'America/New_York', 'TestOperator', 0, 0, 1
                    )
                    ON DUPLICATE KEY UPDATE name = VALUES(name)
                """), {
                    'park_id': p['park_id'],
                    'queue_times_id': 910000 + p['park_id'],
                    'name': f"Test Park {p['park_id']}",
                    'latitude': p['latitude'],
                    'longitude': p['longitude']
                })

            with patch.object(collector, '_get_parks', return_value=parks):
                results = collector.run(mode='current', test_mode=True)

            # Should only process 5 parks in test mode
            assert len(results) == 5
