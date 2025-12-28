"""
Theme Park Downtime Tracker - ThemeParks.wiki API Client
Fetches ride status data with retry logic using tenacity.

API Documentation: https://api.themeparks.wiki/docs/v1/
"""

import requests
from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from utils.config import MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_MULTIPLIER
from utils.logger import logger


# ThemeParks.wiki API base URL
THEMEPARKS_WIKI_API_BASE_URL = "https://api.themeparks.wiki/v1"


class RideStatus(Enum):
    """Ride status values from ThemeParks.wiki API."""
    OPERATING = "OPERATING"
    DOWN = "DOWN"
    CLOSED = "CLOSED"
    REFURBISHMENT = "REFURBISHMENT"


class EntityType(Enum):
    """Entity types from ThemeParks.wiki API."""
    ATTRACTION = "ATTRACTION"
    SHOW = "SHOW"
    RESTAURANT = "RESTAURANT"
    DESTINATION = "DESTINATION"
    PARK = "PARK"


@dataclass
class LiveRideData:
    """Parsed live data for a single ride."""
    entity_id: str
    name: str
    entity_type: str
    status: str
    wait_time: Optional[int]
    operating_hours: Optional[List[Dict]]
    last_updated: Optional[str]


class ThemeParksWikiClient:
    """
    Client for ThemeParks.wiki API with automatic retry logic.

    Implements exponential backoff for transient failures (network, timeouts).
    """

    def __init__(self, base_url: str = THEMEPARKS_WIKI_API_BASE_URL):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ThemeParkHallOfShame/1.0 (Data Collection Bot)',
            'Accept': 'application/json'
        })

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_destinations(self) -> List[Dict]:
        """
        Fetch list of all destinations (resorts) from ThemeParks.wiki API.

        Returns:
            List of destination dictionaries with parks nested inside

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/destinations"
        logger.debug(f"Fetching destinations from {url}")

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()
        destinations = data.get("destinations", [])
        logger.info(f"Fetched {len(destinations)} destinations from ThemeParks.wiki")
        return destinations

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_entity_children(self, entity_id: str) -> List[Dict]:
        """
        Fetch children (attractions, shows, restaurants) for an entity.

        Args:
            entity_id: ThemeParks.wiki entity UUID (park or destination)

        Returns:
            List of child entity dictionaries

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/entity/{entity_id}/children"
        logger.debug(f"Fetching children for entity {entity_id}")

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()
        children = data.get("children", [])
        logger.debug(f"Fetched {len(children)} children for entity {entity_id}")
        return children

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_entity(self, entity_id: str) -> Dict:
        """
        Fetch entity document for any entity (park, ride, restaurant, etc).

        Args:
            entity_id: ThemeParks.wiki entity UUID (or slug)

        Returns:
            Dictionary with entity metadata

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/entity/{entity_id}"
        logger.debug(f"Fetching entity document for {entity_id}")

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        return response.json()

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_entity_live(self, entity_id: str) -> Dict:
        """
        Fetch live data (wait times, status, operating hours) for an entity.

        This is the primary endpoint for collecting ride data.

        Args:
            entity_id: ThemeParks.wiki entity UUID (park)

        Returns:
            Dictionary with liveData array containing all attractions

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/entity/{entity_id}/live"
        logger.debug(f"Fetching live data for entity {entity_id}")

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()
        return data

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_entity_schedule(self, entity_id: str) -> Dict:
        """
        Fetch operating schedule for a park.

        Note: This returns park-level schedules, not per-attraction.
        Use get_entity_live() for per-attraction operating hours.

        Args:
            entity_id: ThemeParks.wiki entity UUID (park)

        Returns:
            Dictionary with schedule array

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/entity/{entity_id}/schedule"
        logger.debug(f"Fetching schedule for entity {entity_id}")

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()
        return data

    def get_park_live_data(self, park_entity_id: str) -> List[LiveRideData]:
        """
        Get parsed live data for all attractions in a park.

        Args:
            park_entity_id: ThemeParks.wiki park UUID

        Returns:
            List of LiveRideData objects for attractions only
        """
        data = self.get_entity_live(park_entity_id)
        live_data = data.get("liveData", [])

        results = []
        for item in live_data:
            entity_type = item.get("entityType", "")

            # Only process attractions
            if entity_type != "ATTRACTION":
                continue

            # Extract wait time from queue data
            wait_time = None
            queue = item.get("queue", {})
            if queue and "STANDBY" in queue:
                wait_time = queue["STANDBY"].get("waitTime")

            ride = LiveRideData(
                entity_id=item.get("id", ""),
                name=item.get("name", ""),
                entity_type=entity_type,
                status=item.get("status", ""),
                wait_time=wait_time,
                operating_hours=item.get("operatingHours"),
                last_updated=item.get("lastUpdated")
            )
            results.append(ride)

        logger.debug(f"Parsed {len(results)} attractions from live data")
        return results

    def get_all_parks(self) -> List[Dict]:
        """
        Fetch all parks across all destinations.

        Returns:
            List of park dictionaries with id, name, and destination info
        """
        destinations = self.get_destinations()
        parks = []

        for dest in destinations:
            dest_name = dest.get("name", "")
            dest_slug = dest.get("slug", "")

            for park in dest.get("parks", []):
                park["destination_name"] = dest_name
                park["destination_slug"] = dest_slug
                parks.append(park)

        logger.info(f"Found {len(parks)} total parks across all destinations")
        return parks

    def close(self):
        """Close the HTTP session."""
        self.session.close()


# Singleton instance
_client: Optional[ThemeParksWikiClient] = None


def get_themeparks_wiki_client() -> ThemeParksWikiClient:
    """
    Get or create singleton ThemeParks.wiki API client.

    Returns:
        ThemeParksWikiClient instance
    """
    global _client
    if _client is None:
        _client = ThemeParksWikiClient()
    return _client
