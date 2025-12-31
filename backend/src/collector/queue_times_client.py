"""
Theme Park Downtime Tracker - Queue-Times.com API Client
Fetches ride status data with retry logic using tenacity.
"""

import requests
from typing import Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from utils.config import QUEUE_TIMES_API_BASE_URL, MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_MULTIPLIER
from utils.logger import logger


class QueueTimesClient:
    """
    Client for Queue-Times.com API with automatic retry logic.

    Implements exponential backoff for transient failures (network, timeouts).
    """

    def __init__(self, base_url: str = QUEUE_TIMES_API_BASE_URL):
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
    def get_parks(self) -> List[Dict]:
        """
        Fetch list of all parks from Queue-Times.com API.

        Returns:
            List of park dictionaries with metadata

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/parks.json"
        logger.debug(f"Fetching parks list from {url}")

        response = self.session.get(url, timeout=10)
        response.raise_for_status()

        parks = response.json()
        logger.info(f"Fetched {len(parks)} parks from Queue-Times.com")
        return parks

    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=RETRY_BACKOFF_MULTIPLIER, min=4, max=60),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
    )
    def get_park_wait_times(self, park_id: int) -> Dict:
        """
        Fetch current wait times for all rides at a specific park.

        Args:
            park_id: Queue-Times.com park ID

        Returns:
            Dictionary with park info and ride wait times

        Raises:
            requests.HTTPError: If API returns error status
            requests.Timeout: If request times out (after retries)
        """
        url = f"{self.base_url}/parks/{park_id}/queue_times.json"
        logger.debug(f"Fetching wait times for park {park_id}")

        response = self.session.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        return data

    def close(self):
        """Close the HTTP session."""
        self.session.close()


# Singleton instance
_client: Optional[QueueTimesClient] = None


def get_queue_times_client() -> QueueTimesClient:
    """
    Get or create singleton Queue-Times API client.

    Returns:
        QueueTimesClient instance
    """
    global _client
    if _client is None:
        _client = QueueTimesClient()
    return _client
