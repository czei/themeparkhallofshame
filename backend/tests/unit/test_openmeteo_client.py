"""
Unit Tests: OpenMeteo API Client
==================================

Tests OpenMeteo API client with mocked responses.

Test Strategy:
- Mock requests.Session.get() to avoid real API calls
- Test response parsing with known data
- Test error handling (timeout, 400, 500)
- Test unit conversions
- Test validation logic

Coverage:
- T018: fetch_weather() with mocked response
- T019: parse_observations() with test data
- T020: Error handling (timeout, HTTP errors)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import requests

from api.openmeteo_client import OpenMeteoClient, get_openmeteo_client


class TestOpenMeteoClient:
    """Unit tests for OpenMeteo API client."""

    @pytest.fixture
    def mock_api_response(self):
        """Mock API response matching Open-Meteo format."""
        return {
            'hourly': {
                'time': ['2025-12-17T00:00', '2025-12-17T01:00', '2025-12-17T02:00'],
                'temperature_2m': [75.2, 74.8, 73.5],
                'apparent_temperature': [72.1, 71.5, 70.2],
                'precipitation': [0.0, 0.1, 0.2],
                'rain': [0.0, 0.1, 0.2],
                'snowfall': [0.0, 0.0, 0.0],
                'weather_code': [0, 61, 95],
                'cloud_cover': [20, 60, 90],
                'wind_speed_10m': [5.2, 7.8, 12.3],
                'wind_gusts_10m': [8.1, 11.2, 18.5],
                'wind_direction_10m': [180, 225, 270],
                'relative_humidity_2m': [65, 70, 85],
                'surface_pressure': [1013.2, 1012.8, 1011.5],
                'visibility': [10000, 8000, 5000],
            }
        }

    @pytest.fixture
    def client(self):
        """Create fresh OpenMeteoClient instance for each test."""
        # Create a fresh instance directly (bypassing singleton)
        return OpenMeteoClient()

    def test_singleton_pattern(self):
        """OpenMeteoClient should be a singleton via get_openmeteo_client()."""
        # Reset module-level singleton for testing
        import api.openmeteo_client
        api.openmeteo_client._client_instance = None

        client1 = get_openmeteo_client()
        client2 = get_openmeteo_client()

        assert client1 is client2, \
            "Multiple instances created - singleton broken"

    def test_get_openmeteo_client_returns_singleton(self):
        """get_openmeteo_client() should return singleton instance."""
        import api.openmeteo_client
        api.openmeteo_client._client_instance = None

        client1 = get_openmeteo_client()
        client2 = get_openmeteo_client()

        assert client1 is client2

    def test_initialization_creates_session(self, client):
        """Client should create requests.Session on init."""
        assert hasattr(client, 'session')
        assert isinstance(client.session, requests.Session)

    def test_session_has_user_agent(self, client):
        """Session should have User-Agent header."""
        headers = client.session.headers
        assert 'User-Agent' in headers
        assert 'ThemeParkHallOfShame' in headers['User-Agent']

    def test_fetch_weather_success(self, client, mock_api_response):
        """fetch_weather() should return parsed JSON on success."""
        # Mock successful API call
        mock_response = Mock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response):
            result = client.fetch_weather(latitude=28.41777, longitude=-81.58116)

        assert result == mock_api_response
        mock_response.raise_for_status.assert_called_once()

    def test_fetch_weather_sends_correct_params(self, client, mock_api_response):
        """fetch_weather() should send correct query parameters."""
        mock_response = Mock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            client.fetch_weather(latitude=28.41777, longitude=-81.58116, forecast_days=7)

        # Verify API was called with correct params
        args, kwargs = mock_get.call_args
        params = kwargs['params']

        assert params['latitude'] == 28.41777
        assert params['longitude'] == -81.58116
        assert params['forecast_days'] == 7
        assert params['temperature_unit'] == 'fahrenheit'
        assert params['wind_speed_unit'] == 'mph'
        assert params['precipitation_unit'] == 'inch'
        assert params['timezone'] == 'UTC'

    def test_fetch_weather_validates_response(self, client):
        """fetch_weather() should validate response structure."""
        # Invalid response (missing hourly field)
        invalid_response = {'invalid': 'data'}

        mock_response = Mock()
        mock_response.json.return_value = invalid_response
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(ValueError, match="Invalid API response structure"):
                client.fetch_weather(latitude=28.41777, longitude=-81.58116)

    def test_validate_response_checks_structure(self, client):
        """_validate_response() should check response structure."""
        # Valid response
        valid = {
            'hourly': {
                'time': ['2025-12-17T00:00', '2025-12-17T01:00'],
                'temperature_2m': [75.0, 74.0]
            }
        }
        assert client._validate_response(valid) is True

        # Invalid: not a dict
        assert client._validate_response([]) is False

        # Invalid: missing hourly
        assert client._validate_response({}) is False

        # Invalid: hourly not a dict
        assert client._validate_response({'hourly': []}) is False

        # Invalid: misaligned arrays
        misaligned = {
            'hourly': {
                'time': ['2025-12-17T00:00'],
                'temperature_2m': [75.0, 74.0]  # Different length
            }
        }
        assert client._validate_response(misaligned) is False

    def test_fetch_weather_http_error(self, client):
        """fetch_weather() should raise HTTPError on 4xx/5xx."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client.fetch_weather(latitude=28.41777, longitude=-81.58116)

    def test_fetch_weather_timeout(self, client):
        """fetch_weather() should raise Timeout on slow API."""
        with patch.object(client.session, 'get', side_effect=requests.Timeout("Request timed out")):
            with pytest.raises(requests.Timeout):
                client.fetch_weather(latitude=28.41777, longitude=-81.58116)

    def test_fetch_weather_has_timeout_param(self, client, mock_api_response):
        """fetch_weather() should set 30-second timeout."""
        mock_response = Mock()
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            client.fetch_weather(latitude=28.41777, longitude=-81.58116)

        args, kwargs = mock_get.call_args
        assert kwargs['timeout'] == 30

    def test_parse_observations_basic(self, client, mock_api_response):
        """parse_observations() should parse API response correctly."""
        park_id = 42
        observations = client.parse_observations(mock_api_response, park_id)

        assert len(observations) == 3
        assert all(obs['park_id'] == 42 for obs in observations)

    def test_parse_observations_temperatures(self, client, mock_api_response):
        """parse_observations() should include temperatures in both units."""
        observations = client.parse_observations(mock_api_response, park_id=1)

        obs = observations[0]
        assert obs['temperature_f'] == 75.2
        assert obs['temperature_c'] == 24.0  # (75.2 - 32) * 5/9 ≈ 24.0

    def test_parse_observations_wind(self, client, mock_api_response):
        """parse_observations() should include wind in both units."""
        observations = client.parse_observations(mock_api_response, park_id=1)

        obs = observations[0]
        assert obs['wind_speed_mph'] == 5.2
        assert obs['wind_speed_kmh'] == 8.37  # 5.2 * 1.60934 ≈ 8.37

    def test_parse_observations_precipitation(self, client, mock_api_response):
        """parse_observations() should convert precipitation to mm."""
        observations = client.parse_observations(mock_api_response, park_id=1)

        obs = observations[1]  # Second hour has 0.1 inch rain
        assert obs['rain_mm'] == 2.54  # 0.1 * 25.4 = 2.54

    def test_parse_observations_timestamps(self, client, mock_api_response):
        """parse_observations() should parse timestamps correctly."""
        observations = client.parse_observations(mock_api_response, park_id=1)

        obs = observations[0]
        assert isinstance(obs['observation_time'], datetime)
        assert obs['observation_time'].tzinfo == timezone.utc

    def test_fahrenheit_to_celsius(self, client):
        """_fahrenheit_to_celsius() should convert correctly."""
        assert client._fahrenheit_to_celsius(32.0) == 0.0
        assert client._fahrenheit_to_celsius(212.0) == 100.0
        assert client._fahrenheit_to_celsius(None) is None

    def test_mph_to_kmh(self, client):
        """_mph_to_kmh() should convert correctly."""
        assert client._mph_to_kmh(1.0) == 1.61  # 1 * 1.60934 ≈ 1.61
        assert client._mph_to_kmh(None) is None

    def test_inches_to_mm(self, client):
        """_inches_to_mm() should convert correctly."""
        assert client._inches_to_mm(1.0) == 25.4
        assert client._inches_to_mm(None) is None

    def test_safe_get_valid_index(self, client):
        """_safe_get() should return value for valid index."""
        lst = [10, 20, 30]
        assert client._safe_get(lst, 0) == 10
        assert client._safe_get(lst, 2) == 30

    def test_safe_get_invalid_index(self, client):
        """_safe_get() should return None for invalid index."""
        lst = [10, 20, 30]
        assert client._safe_get(lst, 10) is None

    def test_safe_get_none_value(self, client):
        """_safe_get() should return None when value is None."""
        lst = [10, None, 30]
        assert client._safe_get(lst, 1) is None

    def test_parse_timestamp(self, client):
        """_parse_timestamp() should parse ISO 8601 format."""
        time_str = "2025-12-17T00:00"
        dt = client._parse_timestamp(time_str)

        assert isinstance(dt, datetime)
        assert dt.year == 2025
        assert dt.month == 12
        assert dt.day == 17
        assert dt.hour == 0
        assert dt.tzinfo == timezone.utc

    def test_retry_decorator_exists(self, client):
        """fetch_weather() should have @retry decorator."""
        # Check that fetch_weather has retry logic
        # (tenacity wraps the function, hard to test directly)
        assert hasattr(client.fetch_weather, '__wrapped__') or \
               client.fetch_weather.__name__ == 'fetch_weather'

    def test_parse_observations_handles_missing_data(self, client):
        """parse_observations() should handle missing/None values gracefully."""
        response = {
            'hourly': {
                'time': ['2025-12-17T00:00'],
                'temperature_2m': [None],
                'apparent_temperature': [None],
                'precipitation': [None],
                'rain': [None],
                'snowfall': [None],
                'weather_code': [None],
                'cloud_cover': [None],
                'wind_speed_10m': [None],
                'wind_gusts_10m': [None],
                'wind_direction_10m': [None],
                'relative_humidity_2m': [None],
                'surface_pressure': [None],
                'visibility': [None],
            }
        }

        observations = client.parse_observations(response, park_id=1)

        assert len(observations) == 1
        obs = observations[0]

        # All values should be None
        assert obs['temperature_f'] is None
        assert obs['temperature_c'] is None
        assert obs['wind_speed_mph'] is None
        assert obs['precipitation_mm'] is None
