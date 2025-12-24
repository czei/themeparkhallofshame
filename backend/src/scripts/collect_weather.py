"""
Weather Collection Script
==========================

Collects weather data for all parks using Open-Meteo API.

Features:
- Concurrent collection with ThreadPoolExecutor (10 workers)
- Rate limiting (1 request/second)
- Failure threshold (>50% fail = abort)
- Structured JSON logging
- CLI flags for current/forecast/test modes

Usage:
    # Collect current weather for all parks
    python collect_weather.py --current

    # Collect 7-day forecasts
    python collect_weather.py --forecast

    # Test mode (5 parks only)
    python collect_weather.py --current --test

Environment:
    PYTHONPATH should include backend/src

Example:
    PYTHONPATH=backend/src python backend/src/scripts/collect_weather.py --test
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from datetime import datetime, timezone

from database.connection import get_db_session
from database.repositories.weather_repository import (
    WeatherObservationRepository,
    WeatherForecastRepository
)
from api.openmeteo_client import get_openmeteo_client
from utils.rate_limiter import TokenBucket

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class WeatherCollector:
    """Weather data collector with concurrent execution.

    Collects weather observations and forecasts for all parks using
    Open-Meteo API with rate limiting and failure threshold.

    Usage:
        ```python
        with get_db_session() as session:
            collector = WeatherCollector(session)
            results = collector.run(mode='current')
        ```
    """

    def __init__(self, session):
        """Initialize weather collector.

        Args:
            session: SQLAlchemy ORM session
        """
        self.session = session
        self.api_client = get_openmeteo_client()
        self.obs_repo = WeatherObservationRepository(session)
        self.fcst_repo = WeatherForecastRepository(session)
        self.rate_limiter = TokenBucket(rate=1.0)  # 1 request per second
        self.max_workers = 10

    def run(self, mode: str = 'current', test_mode: bool = False) -> List[Dict]:
        """Run weather collection for all parks.

        Args:
            mode: Collection mode ('current' or 'forecast')
            test_mode: If True, limit to 5 parks for testing

        Returns:
            List of result dictionaries with success/failure status

        Raises:
            RuntimeError: If >50% of parks fail collection
        """
        start_time = datetime.now(timezone.utc)

        logger.info(
            "Starting weather collection",
            extra={
                'mode': mode,
                'test_mode': test_mode,
                'start_time': start_time.isoformat()
            }
        )

        # Get parks to collect
        parks = self._get_parks()

        # Limit parks in test mode
        if test_mode:
            parks = parks[:5]
            logger.info(f"Test mode: limiting to {len(parks)} parks")

        # Collect weather concurrently
        results = self._collect_concurrent(parks, mode)

        # Check failure threshold
        self._check_failure_threshold(results)

        # Log summary
        end_time = datetime.now(timezone.utc)
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        logger.info(
            "Weather collection complete",
            extra={
                'mode': mode,
                'total_parks': len(parks),
                'successful': len(successful),
                'failed': len(failed),
                'duration_seconds': (end_time - start_time).total_seconds(),
                'end_time': end_time.isoformat()
            }
        )

        return results

    def _get_parks(self) -> List[Dict]:
        """Get list of parks from database.

        Returns:
            List of park dictionaries with park_id, latitude, longitude
        """
        from sqlalchemy import select
        from src.models import Park

        stmt = (
            select(Park.park_id, Park.latitude, Park.longitude, Park.name)
            .where(Park.latitude.isnot(None))
            .where(Park.longitude.isnot(None))
            .order_by(Park.park_id)
        )

        result = self.session.execute(stmt)
        parks = [dict(row._mapping) for row in result]

        logger.info(f"Found {len(parks)} parks to collect")
        return parks

    def _collect_concurrent(self, parks: List[Dict], mode: str) -> List[Dict]:
        """Collect weather for parks concurrently.

        Args:
            parks: List of park dictionaries
            mode: Collection mode ('current' or 'forecast')

        Returns:
            List of result dictionaries
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all collection tasks
            future_to_park = {
                executor.submit(self._collect_for_park, park, mode): park
                for park in parks
            }

            # Collect results as they complete
            for future in as_completed(future_to_park):
                result = future.result()
                results.append(result)

        return results

    def _collect_for_park(self, park: Dict, mode: str = 'current') -> Dict:
        """Collect weather for a single park.

        Args:
            park: Park dictionary with park_id, latitude, longitude
            mode: Collection mode ('current' or 'forecast')

        Returns:
            Result dictionary with success status
        """
        park_id = park['park_id']
        latitude = park['latitude']
        longitude = park['longitude']

        try:
            # Rate limit API requests
            self.rate_limiter.acquire()

            # Fetch weather from API
            logger.debug(
                f"Fetching weather for park {park_id}",
                extra={'park_id': park_id, 'mode': mode}
            )

            weather_data = self.api_client.fetch_weather(
                latitude=latitude,
                longitude=longitude,
                forecast_days=7
            )

            # Parse observations
            observations = self.api_client.parse_observations(
                weather_data,
                park_id=park_id
            )

            # Insert into database
            if mode == 'current':
                # For current mode, only insert first observation (most recent)
                self.obs_repo.batch_insert_observations(observations[:1])
            else:
                # For forecast mode, insert all observations
                self.obs_repo.batch_insert_observations(observations)

            # Note: No explicit commit needed - connection context manager handles it

            logger.info(
                f"Successfully collected weather for park {park_id}",
                extra={
                    'park_id': park_id,
                    'observations_count': len(observations),
                    'mode': mode
                }
            )

            return {
                'success': True,
                'park_id': park_id,
                'observations_count': len(observations)
            }

        except Exception as e:
            logger.error(
                f"Failed to collect weather for park {park_id}",
                extra={
                    'park_id': park_id,
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'mode': mode
                }
            )

            return {
                'success': False,
                'park_id': park_id,
                'error': str(e)
            }

    def _check_failure_threshold(self, results: List[Dict]) -> None:
        """Check if failure rate exceeds threshold.

        Args:
            results: List of collection results

        Raises:
            RuntimeError: If >50% of parks failed collection
        """
        if not results:
            return

        failed_count = sum(1 for r in results if not r['success'])
        total_count = len(results)
        failure_rate = (failed_count / total_count) * 100

        if failure_rate >= 50.0:
            raise RuntimeError(
                f"Collection failed for {failure_rate}% of parks "
                f"({failed_count}/{total_count}). "
                "This indicates a systemic issue (API down, network failure, etc.)"
            )


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='Collect weather data for theme parks'
    )
    parser.add_argument(
        '--current',
        action='store_true',
        help='Collect current weather observations (default when no flags specified)'
    )
    parser.add_argument(
        '--forecast',
        action='store_true',
        help='Collect 7-day weather forecasts'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: collect for 5 parks only'
    )

    args = parser.parse_args()

    # Default to --current if no mode specified (for cron job compatibility)
    if not args.current and not args.forecast:
        args.current = True

    # Connect to database
    try:
        with get_db_session() as session:
            collector = WeatherCollector(session)

            # Collect current weather
            if args.current:
                logger.info("=" * 60)
                logger.info("COLLECTING CURRENT WEATHER")
                logger.info("=" * 60)
                results = collector.run(mode='current', test_mode=args.test)

                successful = sum(1 for r in results if r['success'])
                logger.info(f"Current weather: {successful}/{len(results)} parks successful")

            # Collect forecasts
            if args.forecast:
                logger.info("=" * 60)
                logger.info("COLLECTING WEATHER FORECASTS")
                logger.info("=" * 60)
                results = collector.run(mode='forecast', test_mode=args.test)

                successful = sum(1 for r in results if r['success'])
                logger.info(f"Forecasts: {successful}/{len(results)} parks successful")

            logger.info("=" * 60)
            logger.info("COLLECTION COMPLETE")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(
            "Collection failed",
            extra={'error': str(e), 'error_type': type(e).__name__}
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
