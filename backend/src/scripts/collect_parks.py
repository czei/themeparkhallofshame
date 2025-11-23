#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Park & Ride Collection Script
Fetches all parks and rides from Queue-Times.com API and populates the database.

This is a ONE-TIME setup script that should be run initially to populate the parks
and rides tables. After initial setup, only run this when new parks/rides are added.

Usage:
    python -m scripts.collect_parks [--force]

Options:
    --force     Clear existing data and re-fetch everything
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.config import FILTER_COUNTRY
from utils.logger import logger
from collector.queue_times_client import QueueTimesClient
from classifier.pattern_matcher import PatternMatcher
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.connection import get_db_connection


class ParkCollector:
    """
    Collects parks and rides from Queue-Times.com API and stores in database.
    """

    def __init__(self, force: bool = False):
        """
        Initialize the park collector.

        Args:
            force: If True, clear existing data before collecting
        """
        self.force = force
        self.api_client = QueueTimesClient()
        self.classifier = PatternMatcher()

        self.stats = {
            'parks_processed': 0,
            'parks_inserted': 0,
            'parks_updated': 0,
            'parks_skipped': 0,
            'rides_processed': 0,
            'rides_inserted': 0,
            'rides_updated': 0,
            'rides_classified': 0,
            'errors': 0
        }

    def run(self):
        """
        Main execution method.
        """
        logger.info("=" * 60)
        logger.info("PARK & RIDE COLLECTION - Starting")
        logger.info("=" * 60)

        if FILTER_COUNTRY:
            logger.info(f"Geographic Filter: {FILTER_COUNTRY} only")
        else:
            logger.info("Geographic Filter: All countries")

        if self.force:
            logger.warning("FORCE MODE: Existing data will be cleared")
            self._clear_existing_data()

        try:
            # Step 1: Fetch all parks from Queue-Times API
            logger.info("Step 1: Fetching parks from Queue-Times.com...")
            parks_data = self._fetch_parks()
            logger.info(f"Found {len(parks_data)} parks from API")

            # Step 2: Process each park with database connection
            logger.info("Step 2: Processing parks and rides...")
            with get_db_connection() as conn:
                park_repo = ParkRepository(conn)
                ride_repo = RideRepository(conn)

                for park_data in parks_data:
                    self._process_park(park_data, park_repo, ride_repo)

            # Step 3: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("PARK & RIDE COLLECTION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during collection: {e}", exc_info=True)
            sys.exit(1)

    def _clear_existing_data(self):
        """Clear existing parks and rides data."""
        try:
            with get_db_connection() as conn:
                from sqlalchemy import text

                # Delete in order due to foreign keys
                logger.info("Clearing existing data...")
                conn.execute(text("DELETE FROM ride_classifications"))
                conn.execute(text("DELETE FROM rides"))
                conn.execute(text("DELETE FROM parks"))
                logger.info("Existing data cleared")

        except Exception as e:
            logger.error(f"Failed to clear existing data: {e}")
            raise

    def _fetch_parks(self) -> List[Dict]:
        """
        Fetch all parks from Queue-Times API.

        Returns:
            List of park dictionaries
        """
        try:
            parks = self.api_client.fetch_all_parks()

            # Filter by country if configured
            if FILTER_COUNTRY:
                parks = [p for p in parks if p.get('country', '').upper() == FILTER_COUNTRY.upper()]
                logger.info(f"Filtered to {len(parks)} parks in {FILTER_COUNTRY}")

            return parks

        except Exception as e:
            logger.error(f"Failed to fetch parks: {e}")
            raise

    def _process_park(self, park_data: Dict, park_repo: ParkRepository, ride_repo: RideRepository):
        """
        Process a single park: store park data and fetch/classify rides.

        Args:
            park_data: Park data from Queue-Times API
            park_repo: Park repository instance with database connection
            ride_repo: Ride repository instance with database connection
        """
        queue_times_id = park_data.get('id')
        park_name = park_data.get('name', 'Unknown')

        try:
            logger.info(f"Processing park: {park_name} (ID: {queue_times_id})")
            self.stats['parks_processed'] += 1

            # Step 1: Insert or update park
            park_id = self._upsert_park(park_data, park_repo)
            if park_id is None:
                self.stats['parks_skipped'] += 1
                return

            # Step 2: Fetch rides for this park
            rides_data = self._fetch_park_rides(queue_times_id)
            logger.info(f"  Found {len(rides_data)} rides")

            # Step 3: Process each ride
            for ride_data in rides_data:
                self._process_ride(ride_data, park_id, park_name, ride_repo)

        except Exception as e:
            logger.error(f"Error processing park {park_name}: {e}")
            self.stats['errors'] += 1

    def _upsert_park(self, park_data: Dict, park_repo: ParkRepository) -> Optional[int]:
        """
        Insert or update park in database.

        Args:
            park_data: Park data from Queue-Times API
            park_repo: Park repository instance with database connection

        Returns:
            park_id if successful, None otherwise
        """
        try:
            queue_times_id = park_data.get('id')

            # Check if park already exists
            existing_park = park_repo.get_by_queue_times_id(queue_times_id)

            # Determine operator and flags
            park_name = park_data.get('name', '')
            operator = self._detect_operator(park_name)
            is_disney = 'disney' in park_name.lower()
            is_universal = 'universal' in park_name.lower()

            park_record = {
                'queue_times_id': queue_times_id,
                'name': park_data.get('name'),
                'city': park_data.get('city'),
                'state_province': park_data.get('state'),
                'country': park_data.get('country'),
                'latitude': park_data.get('latitude'),
                'longitude': park_data.get('longitude'),
                'timezone': park_data.get('timezone', 'UTC'),
                'operator': operator,
                'is_disney': is_disney,
                'is_universal': is_universal,
                'is_active': True
            }

            if existing_park:
                # Update existing park
                park_record['park_id'] = existing_park['park_id']
                park_record['updated_at'] = datetime.now()
                park_repo.update(park_record)
                self.stats['parks_updated'] += 1
                return existing_park['park_id']
            else:
                # Insert new park
                park_record['created_at'] = datetime.now()
                park_record['updated_at'] = datetime.now()
                park_id = park_repo.insert(park_record)
                self.stats['parks_inserted'] += 1
                logger.info(f"  ✓ Inserted park: {park_record['name']}")
                return park_id

        except Exception as e:
            logger.error(f"Failed to upsert park: {e}")
            return None

    def _detect_operator(self, park_name: str) -> str:
        """
        Detect park operator from park name.

        Args:
            park_name: Name of the park

        Returns:
            Operator name or 'Unknown'
        """
        name_lower = park_name.lower()

        if 'disney' in name_lower:
            return 'Disney'
        elif 'universal' in name_lower:
            return 'Universal'
        elif 'cedar point' in name_lower or "king's island" in name_lower or 'carowinds' in name_lower:
            return 'Cedar Fair'
        elif 'six flags' in name_lower:
            return 'Six Flags'
        elif 'seaworld' in name_lower or 'busch gardens' in name_lower:
            return 'SeaWorld'
        else:
            return 'Independent'

    def _fetch_park_rides(self, queue_times_park_id: int) -> List[Dict]:
        """
        Fetch all rides for a specific park.

        Args:
            queue_times_park_id: Queue-Times park ID

        Returns:
            List of ride dictionaries
        """
        try:
            return self.api_client.fetch_park_rides(queue_times_park_id)
        except Exception as e:
            logger.error(f"Failed to fetch rides for park {queue_times_park_id}: {e}")
            return []

    def _process_ride(self, ride_data: Dict, park_id: int, park_name: str, ride_repo: RideRepository):
        """
        Process a single ride: classify and store.

        Args:
            ride_data: Ride data from Queue-Times API
            park_id: Database park ID
            park_name: Park name for classification context
            ride_repo: Ride repository instance with database connection
        """
        try:
            self.stats['rides_processed'] += 1

            queue_times_id = ride_data.get('id')
            ride_name = ride_data.get('name', 'Unknown')

            # Check if ride already exists
            existing_ride = ride_repo.get_by_queue_times_id(queue_times_id)

            # Classify the ride
            classification = self.classifier.classify(ride_name, park_name)

            ride_record = {
                'queue_times_id': queue_times_id,
                'park_id': park_id,
                'name': ride_name,
                'land_area': ride_data.get('land'),
                'tier': classification.tier,
                'is_active': ride_data.get('is_open', True)
            }

            if existing_ride:
                # Update existing ride
                ride_record['ride_id'] = existing_ride['ride_id']
                ride_record['updated_at'] = datetime.now()
                ride_repo.update(ride_record)
                self.stats['rides_updated'] += 1
            else:
                # Insert new ride
                ride_record['created_at'] = datetime.now()
                ride_record['updated_at'] = datetime.now()
                ride_id = ride_repo.insert(ride_record)
                self.stats['rides_inserted'] += 1

                # Store classification
                self._store_classification(ride_id, classification)

                tier_symbol = "🌟" * classification.tier
                logger.info(f"    ✓ {ride_name} → Tier {classification.tier} {tier_symbol} "
                           f"(confidence: {classification.confidence:.2f})")

        except Exception as e:
            logger.error(f"Error processing ride {ride_data.get('name')}: {e}")
            self.stats['errors'] += 1

    def _store_classification(self, ride_id: int, classification):
        """
        Store ride classification in database.

        Args:
            ride_id: Database ride ID
            classification: Classification result from PatternMatcher
        """
        try:
            from database.repositories.classification_repository import RideClassificationRepository

            classification_repo = RideClassificationRepository()

            # Tier weights: Tier 1 = 3.0, Tier 2 = 2.0, Tier 3 = 1.0, Tier 4 = 0.5
            tier_weights = {1: 3.0, 2: 2.0, 3: 1.0, 4: 0.5}

            classification_record = {
                'ride_id': ride_id,
                'tier': classification.tier,
                'tier_weight': tier_weights.get(classification.tier, 1.0),
                'confidence': classification.confidence,
                'classification_method': classification.method,
                'classified_at': datetime.now()
            }

            classification_repo.insert(classification_record)
            self.stats['rides_classified'] += 1

        except Exception as e:
            logger.error(f"Failed to store classification for ride {ride_id}: {e}")

    def _print_summary(self):
        """Print collection summary statistics."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("COLLECTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Parks:")
        logger.info(f"  - Processed: {self.stats['parks_processed']}")
        logger.info(f"  - Inserted:  {self.stats['parks_inserted']}")
        logger.info(f"  - Updated:   {self.stats['parks_updated']}")
        logger.info(f"  - Skipped:   {self.stats['parks_skipped']}")
        logger.info("")
        logger.info(f"Rides:")
        logger.info(f"  - Processed:  {self.stats['rides_processed']}")
        logger.info(f"  - Inserted:   {self.stats['rides_inserted']}")
        logger.info(f"  - Updated:    {self.stats['rides_updated']}")
        logger.info(f"  - Classified: {self.stats['rides_classified']}")
        logger.info("")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Collect parks and rides from Queue-Times.com API'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Clear existing data and re-fetch everything'
    )

    args = parser.parse_args()

    collector = ParkCollector(force=args.force)
    collector.run()


if __name__ == '__main__':
    main()
