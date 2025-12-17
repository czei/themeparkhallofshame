"""
Open-Meteo Weather API Client
===============================

Singleton client for fetching weather data from Open-Meteo API.

Features:
- Tenacity retry with exponential backoff
- API response validation (from Zen expert review)
- Unit conversions (C→F, km/h→mph)
- Structured JSON logging
- Thread-safe singleton pattern

API Documentation: https://open-meteo.com/en/docs
"""

import logging
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# Configure structured logging
logger = logging.getLogger(__name__)


class OpenMeteoClient:
    """Singleton client for Open-Meteo Weather API.

    Usage:
        ```python
        client = get_openmeteo_client()
        weather_data = client.fetch_weather(latitude=28.41777, longitude=-81.58116)
        ```
    """

    _instance: Optional['OpenMeteoClient'] = None
    _lock = __import__('threading').Lock()

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Weather variables to fetch (matches data model)
    HOURLY_VARIABLES = [
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        "cloud_cover",
        "wind_speed_10m",
        "wind_gusts_10m",
        "wind_direction_10m",
        "relative_humidity_2m",
        "surface_pressure",
        "visibility",
    ]

    def __new__(cls):
        """Singleton pattern: only one instance exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize API client (called once due to singleton)."""
        if not hasattr(self, 'initialized'):
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'ThemeParkHallOfShame/1.0 (weather-collection)'
            })
            self.initialized = True
            logger.info("OpenMeteoClient initialized")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_weather(
        self,
        latitude: float,
        longitude: float,
        forecast_days: int = 7
    ) -> Dict:
        """Fetch weather data for coordinates.

        Args:
            latitude: Latitude in decimal degrees (-90 to 90)
            longitude: Longitude in decimal degrees (-180 to 180)
            forecast_days: Number of forecast days (1-16, default 7)

        Returns:
            Parsed weather data with observations

        Raises:
            requests.HTTPError: API returned error status
            requests.Timeout: API request timed out
            ValueError: Invalid API response structure
        """
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'hourly': ','.join(self.HOURLY_VARIABLES),
            'temperature_unit': 'fahrenheit',
            'wind_speed_unit': 'mph',
            'precipitation_unit': 'inch',
            'timezone': 'UTC',
            'forecast_days': forecast_days,
        }

        logger.info(
            "Fetching weather",
            extra={
                'latitude': latitude,
                'longitude': longitude,
                'forecast_days': forecast_days
            }
        )

        try:
            response = self.session.get(
                self.BASE_URL,
                params=params,
                timeout=30  # 30-second timeout for slow API responses
            )
            response.raise_for_status()

            data = response.json()

            # Validate response structure (from Zen expert review)
            if not self._validate_response(data):
                raise ValueError(f"Invalid API response structure: {data}")

            logger.info(
                "Weather fetch successful",
                extra={
                    'latitude': latitude,
                    'longitude': longitude,
                    'hours_returned': len(data.get('hourly', {}).get('time', []))
                }
            )

            return data

        except requests.HTTPError as e:
            logger.error(
                "API HTTP error",
                extra={
                    'status_code': e.response.status_code if e.response else None,
                    'latitude': latitude,
                    'longitude': longitude,
                    'error': str(e)
                }
            )
            raise

        except requests.Timeout as e:
            logger.error(
                "API timeout",
                extra={
                    'latitude': latitude,
                    'longitude': longitude,
                    'error': str(e)
                }
            )
            raise

        except Exception as e:
            logger.error(
                "API request failed",
                extra={
                    'latitude': latitude,
                    'longitude': longitude,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
            )
            raise

    def _validate_response(self, response_data: Dict) -> bool:
        """Validate API response structure.

        From Zen expert review: Check structure before parsing to catch
        API contract changes early and prevent data corruption.

        Args:
            response_data: JSON response from API

        Returns:
            True if structure is valid, False otherwise
        """
        if not isinstance(response_data, dict):
            logger.error("Response is not a dictionary", extra={'type': type(response_data)})
            return False

        if 'hourly' not in response_data:
            logger.error("Response missing 'hourly' field")
            return False

        hourly_data = response_data['hourly']
        if not isinstance(hourly_data, dict):
            logger.error("hourly field is not a dictionary")
            return False

        times = hourly_data.get('time', [])
        temps = hourly_data.get('temperature_2m', [])

        if not isinstance(times, list) or not isinstance(temps, list):
            logger.error(
                "Invalid hourly data types",
                extra={'time_type': type(times), 'temp_type': type(temps)}
            )
            return False

        if len(times) == 0:
            logger.error("No time data in response")
            return False

        if len(times) != len(temps):
            logger.error(
                "Misaligned time/temperature data",
                extra={'time_count': len(times), 'temp_count': len(temps)}
            )
            return False

        return True

    def parse_observations(
        self,
        response_data: Dict,
        park_id: int
    ) -> List[Dict]:
        """Parse API response into observation records.

        Args:
            response_data: JSON response from fetch_weather()
            park_id: Park ID for these observations

        Returns:
            List of observation dictionaries ready for database insert
        """
        hourly_data = response_data.get('hourly', {})

        times = hourly_data.get('time', [])
        temps_f = hourly_data.get('temperature_2m', [])
        apparent_temps_f = hourly_data.get('apparent_temperature', [])
        precipitation_in = hourly_data.get('precipitation', [])
        rain_in = hourly_data.get('rain', [])
        snow_in = hourly_data.get('snowfall', [])
        weather_codes = hourly_data.get('weather_code', [])
        cloud_cover = hourly_data.get('cloud_cover', [])
        wind_speed_mph = hourly_data.get('wind_speed_10m', [])
        wind_gusts_mph = hourly_data.get('wind_gusts_10m', [])
        wind_direction = hourly_data.get('wind_direction_10m', [])
        humidity = hourly_data.get('relative_humidity_2m', [])
        pressure = hourly_data.get('surface_pressure', [])
        visibility = hourly_data.get('visibility', [])

        observations = []

        for i, time_str in enumerate(times):
            observation = {
                'park_id': park_id,
                'observation_time': self._parse_timestamp(time_str),

                # Temperature (API returns Fahrenheit, calculate Celsius)
                'temperature_f': self._safe_get(temps_f, i),
                'temperature_c': self._fahrenheit_to_celsius(self._safe_get(temps_f, i)),
                'apparent_temperature_f': self._safe_get(apparent_temps_f, i),
                'apparent_temperature_c': self._fahrenheit_to_celsius(
                    self._safe_get(apparent_temps_f, i)
                ),

                # Wind (API returns mph, calculate km/h)
                'wind_speed_mph': self._safe_get(wind_speed_mph, i),
                'wind_speed_kmh': self._mph_to_kmh(self._safe_get(wind_speed_mph, i)),
                'wind_gusts_mph': self._safe_get(wind_gusts_mph, i),
                'wind_gusts_kmh': self._mph_to_kmh(self._safe_get(wind_gusts_mph, i)),
                'wind_direction_degrees': self._safe_get(wind_direction, i),

                # Precipitation (API returns inches, convert to mm)
                'precipitation_mm': self._inches_to_mm(self._safe_get(precipitation_in, i)),
                'rain_mm': self._inches_to_mm(self._safe_get(rain_in, i)),
                'snowfall_mm': self._inches_to_mm(self._safe_get(snow_in, i)),
                'precipitation_probability': None,  # Not in current obs, only forecasts

                # Atmospheric
                'cloud_cover_percent': self._safe_get(cloud_cover, i),
                'visibility_meters': self._safe_get(visibility, i),
                'humidity_percent': self._safe_get(humidity, i),
                'pressure_hpa': self._safe_get(pressure, i),

                # Weather code
                'weather_code': self._safe_get(weather_codes, i),
            }

            observations.append(observation)

        return observations

    @staticmethod
    def _safe_get(lst: List, index: int) -> Optional[float]:
        """Safely get list element, return None if out of range or None."""
        try:
            val = lst[index]
            return val if val is not None else None
        except (IndexError, TypeError):
            return None

    @staticmethod
    def _parse_timestamp(time_str: str) -> datetime:
        """Parse ISO 8601 timestamp to UTC datetime."""
        # Open-Meteo returns format: "2025-12-17T00:00"
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    @staticmethod
    def _fahrenheit_to_celsius(fahrenheit: Optional[float]) -> Optional[float]:
        """Convert Fahrenheit to Celsius."""
        if fahrenheit is None:
            return None
        return round((fahrenheit - 32) * 5/9, 2)

    @staticmethod
    def _mph_to_kmh(mph: Optional[float]) -> Optional[float]:
        """Convert miles per hour to kilometers per hour."""
        if mph is None:
            return None
        return round(mph * 1.60934, 2)

    @staticmethod
    def _inches_to_mm(inches: Optional[float]) -> Optional[float]:
        """Convert inches to millimeters."""
        if inches is None:
            return None
        return round(inches * 25.4, 2)


# Global instance and getter function
_client_instance: Optional[OpenMeteoClient] = None


def get_openmeteo_client() -> OpenMeteoClient:
    """Get global OpenMeteo API client instance (singleton).

    Returns:
        OpenMeteoClient singleton instance
    """
    global _client_instance
    if _client_instance is None:
        _client_instance = OpenMeteoClient()
    return _client_instance
