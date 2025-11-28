"""
Theme Park Downtime Tracker - Geocoding Client
Reverse geocoding using OpenStreetMap Nominatim (free, no API key required).
"""

import requests
import time
from typing import Optional, Dict

from utils.logger import logger


class GeocodingClient:
    """Reverse geocoding using OpenStreetMap Nominatim (free, no API key)."""

    BASE_URL = "https://nominatim.openstreetmap.org/reverse"

    def __init__(self, user_agent: str = "ThemeParkTracker/1.0"):
        """
        Initialize geocoding client.

        Args:
            user_agent: User-Agent header (required by Nominatim ToS)
        """
        self.user_agent = user_agent
        self.last_request_time = 0.0
        self.min_request_interval = 1.0  # Nominatim requires max 1 req/sec

    def reverse_geocode(self, lat: float, lng: float) -> Optional[Dict[str, str]]:
        """
        Convert lat/lng coordinates to city, state, country.

        Args:
            lat: Latitude
            lng: Longitude

        Returns:
            Dict with 'city', 'state', 'country' keys or None on failure
        """
        # Rate limiting (Nominatim requires max 1 request/second)
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)

        try:
            response = requests.get(
                self.BASE_URL,
                params={
                    'lat': lat,
                    'lon': lng,
                    'format': 'json',
                    'addressdetails': 1
                },
                headers={'User-Agent': self.user_agent},
                timeout=10
            )
            self.last_request_time = time.time()

            if response.status_code != 200:
                logger.warning(f"Geocoding API returned {response.status_code} for ({lat}, {lng})")
                return None

            data = response.json()
            address = data.get('address', {})

            # Different countries use different address fields
            city = (
                address.get('city') or
                address.get('town') or
                address.get('village') or
                address.get('municipality') or
                address.get('county')  # Fallback for rural areas
            )

            state = (
                address.get('state') or
                address.get('province') or
                address.get('region')
            )

            country = address.get('country_code', '').upper()

            return {
                'city': city,
                'state': state,
                'country': country
            }

        except requests.exceptions.Timeout:
            logger.warning(f"Geocoding timeout for ({lat}, {lng})")
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Geocoding request failed for ({lat}, {lng}): {e}")
            return None
        except Exception as e:
            logger.warning(f"Geocoding failed for ({lat}, {lng}): {e}")
            return None
