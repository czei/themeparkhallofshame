"""
Contract Test: Open-Meteo API Response Schema
==============================================

Validates that Open-Meteo API returns expected structure.

Purpose:
- Catch API contract changes early
- Document expected API response format
- Validate schema before parsing

Test Strategy:
- Make real API call to Open-Meteo
- Validate response structure matches contract
- Check required fields exist
- Verify data types are correct
"""

import pytest
import requests
from typing import Dict, Any


class TestOpenMeteoContract:
    """Contract tests for Open-Meteo API response schema."""

    # Test coordinates (Disney World, Orlando)
    TEST_LAT = 28.41777
    TEST_LON = -81.58116

    @pytest.fixture(scope="class")
    def api_response(self) -> Dict[str, Any]:
        """Fetch real API response for contract validation.

        Returns:
            JSON response from Open-Meteo API
        """
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': self.TEST_LAT,
            'longitude': self.TEST_LON,
            'hourly': 'temperature_2m,apparent_temperature,precipitation,rain,snowfall,weather_code,cloud_cover,wind_speed_10m,wind_gusts_10m,wind_direction_10m,relative_humidity_2m,surface_pressure,visibility',
            'temperature_unit': 'fahrenheit',
            'wind_speed_unit': 'mph',
            'precipitation_unit': 'inch',
            'timezone': 'UTC',
            'forecast_days': 7,
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def test_response_is_dict(self, api_response):
        """API response should be a dictionary."""
        assert isinstance(api_response, dict), \
            f"Expected dict, got {type(api_response)}"

    def test_hourly_field_exists(self, api_response):
        """Response must have 'hourly' field."""
        assert 'hourly' in api_response, \
            "Response missing 'hourly' field"

    def test_hourly_is_dict(self, api_response):
        """hourly field must be a dictionary."""
        assert isinstance(api_response['hourly'], dict), \
            f"Expected hourly to be dict, got {type(api_response['hourly'])}"

    def test_time_array_exists(self, api_response):
        """hourly.time must exist and be a list."""
        hourly = api_response['hourly']
        assert 'time' in hourly, "hourly missing 'time' field"
        assert isinstance(hourly['time'], list), \
            f"Expected time to be list, got {type(hourly['time'])}"

    def test_temperature_array_exists(self, api_response):
        """hourly.temperature_2m must exist and be a list."""
        hourly = api_response['hourly']
        assert 'temperature_2m' in hourly, "hourly missing 'temperature_2m' field"
        assert isinstance(hourly['temperature_2m'], list), \
            f"Expected temperature_2m to be list, got {type(hourly['temperature_2m'])}"

    def test_time_temp_arrays_aligned(self, api_response):
        """time and temperature_2m arrays must have same length."""
        hourly = api_response['hourly']
        time_count = len(hourly['time'])
        temp_count = len(hourly['temperature_2m'])

        assert time_count == temp_count, \
            f"Misaligned arrays: time={time_count}, temp={temp_count}"

    def test_all_required_variables_present(self, api_response):
        """All requested hourly variables must be present."""
        hourly = api_response['hourly']

        required_vars = [
            'time',
            'temperature_2m',
            'apparent_temperature',
            'precipitation',
            'rain',
            'snowfall',
            'weather_code',
            'cloud_cover',
            'wind_speed_10m',
            'wind_gusts_10m',
            'wind_direction_10m',
            'relative_humidity_2m',
            'surface_pressure',
            'visibility',
        ]

        missing = [var for var in required_vars if var not in hourly]
        assert not missing, f"Missing required variables: {missing}"

    def test_all_arrays_same_length(self, api_response):
        """All hourly arrays must have same length as time array."""
        hourly = api_response['hourly']
        time_count = len(hourly['time'])

        for key, values in hourly.items():
            if key == 'time':
                continue

            assert isinstance(values, list), \
                f"{key} is not a list: {type(values)}"

            assert len(values) == time_count, \
                f"{key} has {len(values)} values, expected {time_count}"

    def test_time_format_is_iso8601(self, api_response):
        """time values should be ISO 8601 format."""
        hourly = api_response['hourly']
        times = hourly['time']

        # Check first time value format
        if times:
            time_str = times[0]
            assert isinstance(time_str, str), \
                f"Expected time to be string, got {type(time_str)}"

            # ISO 8601 format: "2025-12-17T00:00"
            assert 'T' in time_str, \
                f"Time not in ISO 8601 format: {time_str}"

    def test_temperature_values_are_numeric(self, api_response):
        """temperature_2m values should be numeric or None."""
        hourly = api_response['hourly']
        temps = hourly['temperature_2m']

        for i, temp in enumerate(temps):
            if temp is not None:
                assert isinstance(temp, (int, float)), \
                    f"temperature_2m[{i}] is not numeric: {temp} ({type(temp)})"

    def test_weather_code_values_valid(self, api_response):
        """weather_code should be integers in WMO range (0-99)."""
        hourly = api_response['hourly']
        codes = hourly['weather_code']

        for i, code in enumerate(codes):
            if code is not None:
                assert isinstance(code, int), \
                    f"weather_code[{i}] is not int: {code} ({type(code)})"

                assert 0 <= code <= 99, \
                    f"weather_code[{i}] out of WMO range: {code}"

    def test_forecast_days_returned(self, api_response):
        """Should return approximately 7 days * 24 hours = 168 hours."""
        hourly = api_response['hourly']
        time_count = len(hourly['time'])

        # Allow some variance (API might return 167-169 hours)
        assert 160 <= time_count <= 176, \
            f"Expected ~168 hours for 7 days, got {time_count}"
