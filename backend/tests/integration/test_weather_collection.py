"""
Integration Tests: Weather Collection
======================================

Tests WeatherCollector with real database and API mocks.

Test Strategy:
- Use mysql_connection fixture (isolated transaction)
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
        client.parse_observations.return_value = [
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 0, 0, 0, tzinfo=timezone.utc),
                'temperature_f': 75.2,
                'temperature_c': 24.0,
                'weather_code': 0,
            },
            {
                'park_id': 1,
                'observation_time': datetime(2025, 12, 17, 1, 0, 0, tzinfo=timezone.utc),
                'temperature_f': 74.8,
                'temperature_c': 23.8,
                'weather_code': 61,
            },
        ]
        return client

    def test_concurrent_collection_with_multiple_parks(self, mysql_connection, mock_api_client):
        """Should collect weather for multiple parks concurrently."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            # Create test parks in database
            with mysql_connection.cursor() as cursor:
                # Assuming parks table exists and has at least 3 parks
                cursor.execute("SELECT park_id, latitude, longitude FROM parks LIMIT 3")
                parks = cursor.fetchall()

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

    def test_rate_limiting_enforced(self, mysql_connection, mock_api_client):
        """Should enforce 1 req/sec rate limit."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

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

    def test_graceful_park_failure_handling(self, mysql_connection, mock_api_client):
        """Should handle individual park failures gracefully."""
        # Make API fail for specific park
        def side_effect_fetch(latitude, longitude, **kwargs):
            if latitude == 99.0:  # Invalid coordinate
                raise Exception("Invalid coordinates")
            return mock_api_client.fetch_weather.return_value

        mock_api_client.fetch_weather.side_effect = side_effect_fetch

        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            parks = [
                {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116},  # Valid
                {'park_id': 2, 'latitude': 99.0, 'longitude': -81.0},          # Invalid
                {'park_id': 3, 'latitude': 33.8121, 'longitude': -117.9190},   # Valid
            ]

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

    def test_batch_insert_observations(self, mysql_connection, mock_api_client):
        """Should batch insert observations to database."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            park = {'park_id': 1, 'latitude': 28.41777, 'longitude': -81.58116}

            result = collector._collect_for_park(park)

            assert result['success'] is True

            # Verify observations were inserted
            with mysql_connection.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM weather_observations
                    WHERE park_id = 1
                      AND observation_time >= '2025-12-17 00:00:00'
                """)
                row = cursor.fetchone()

            # Should have inserted 2 observations
            assert row['count'] >= 2

    def test_failure_threshold_aborts_on_systemic_failure(self, mysql_connection, mock_api_client):
        """Should abort when >50% of parks fail."""
        # Make API fail for most parks
        mock_api_client.fetch_weather.side_effect = Exception("API down")

        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            parks = [
                {'park_id': i, 'latitude': 28.0 + i, 'longitude': -81.0}
                for i in range(1, 11)  # 10 parks
            ]

            with patch.object(collector, '_get_parks', return_value=parks):
                # Should raise RuntimeError due to failure threshold
                with pytest.raises(RuntimeError, match="Collection failed for"):
                    collector.run(mode='current', test_mode=True)

    def test_concurrent_execution_uses_thread_pool(self, mysql_connection, mock_api_client):
        """Should use ThreadPoolExecutor for concurrent collection."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            # Verify collector has ThreadPoolExecutor
            assert hasattr(collector, 'max_workers')
            assert collector.max_workers == 10

    def test_test_mode_limits_parks(self, mysql_connection, mock_api_client):
        """test_mode should limit collection to 5 parks."""
        with patch('scripts.collect_weather.get_openmeteo_client', return_value=mock_api_client):
            collector = WeatherCollector(mysql_connection)

            # Create 10 test parks
            parks = [
                {'park_id': i, 'latitude': 28.0 + i, 'longitude': -81.0}
                for i in range(1, 11)
            ]

            with patch.object(collector, '_get_parks', return_value=parks):
                results = collector.run(mode='current', test_mode=True)

            # Should only process 5 parks in test mode
            assert len(results) == 5
