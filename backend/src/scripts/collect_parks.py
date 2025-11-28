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
from collector.geocoding_client import GeocodingClient
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
        self.geocoder = GeocodingClient()

        self.stats = {
            'parks_processed': 0,
            'parks_inserted': 0,
            'parks_updated': 0,
            'parks_skipped': 0,
            'rides_processed': 0,
            'rides_inserted': 0,
            'rides_updated': 0,
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
            groups = self.api_client.get_parks()

            # Flatten: API returns company groups with nested parks
            # Preserve company name for Disney/Universal classification
            all_parks = []
            for group in groups:
                company_name = group.get('name', '')
                if 'parks' in group:
                    for park in group['parks']:
                        park['_company'] = company_name  # Preserve for classification
                        all_parks.append(park)
                else:
                    # In case some are individual parks
                    all_parks.append(group)

            logger.info(f"Total parks found: {len(all_parks)}")

            # Filter by country if configured
            if FILTER_COUNTRY:
                # Support both "US" and "United States"
                country_filter = FILTER_COUNTRY.upper()
                filtered_parks = []
                for p in all_parks:
                    country = p.get('country', '').upper()
                    if country == country_filter or \
                       (country_filter == 'US' and country == 'UNITED STATES') or \
                       (country_filter == 'UNITED STATES' and country == 'US'):
                        filtered_parks.append(p)

                logger.info(f"Filtered to {len(filtered_parks)} parks in {FILTER_COUNTRY}")
                return filtered_parks

            return all_parks

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

            # Determine operator and flags based on company name from API
            park_name = park_data.get('name', '')
            company_name = park_data.get('_company', '').lower()
            operator = self._detect_operator(park_name)
            # Use company name for accurate Disney/Universal classification
            # "Walt Disney Attractions" includes EPCOT, Animal Kingdom, etc.
            # "Universal Parks & Resorts" includes Epic Universe, etc.
            is_disney = 'disney' in company_name
            is_universal = 'universal' in company_name

            # Convert country to ISO 2-letter code (API returns full names)
            country_name = park_data.get('country', '')
            country_code = self._convert_country_to_iso(country_name)

            # Get coordinates from API (may be strings or numbers)
            latitude = park_data.get('latitude')
            longitude = park_data.get('longitude')

            # Convert to float if provided as strings
            try:
                if latitude is not None:
                    latitude = float(latitude)
                if longitude is not None:
                    longitude = float(longitude)
            except (ValueError, TypeError):
                pass  # Keep as-is if conversion fails

            # Fix longitude sign for US parks (Queue-Times API sometimes has wrong sign)
            # US parks should always have negative longitude (west of prime meridian)
            if country_code == 'US' and longitude is not None and longitude > 0:
                logger.warning(f"  Fixing longitude sign for US park: {longitude} -> {-longitude}")
                longitude = -longitude

            park_record = {
                'queue_times_id': queue_times_id,
                'name': park_data.get('name'),
                'city': park_data.get('city') or 'Unknown',  # API doesn't provide city
                'state_province': park_data.get('state') or park_data.get('state_province') or '',
                'country': country_code,
                'latitude': latitude,
                'longitude': longitude,
                'timezone': park_data.get('timezone', 'UTC'),
                'operator': operator,
                'is_disney': is_disney,
                'is_universal': is_universal,
                'is_active': True
            }

            # Geocode if city is Unknown and we have coordinates
            if park_record['city'] == 'Unknown' and park_record['latitude'] and park_record['longitude']:
                location = self.geocoder.reverse_geocode(park_record['latitude'], park_record['longitude'])
                if location:
                    if location['city']:
                        park_record['city'] = location['city']
                    if location['state']:
                        park_record['state_province'] = location['state']
                    if location['country']:
                        park_record['country'] = location['country']
                    logger.info(f"  Geocoded: {location['city']}, {location['state']}, {location['country']}")

            if existing_park:
                # Update existing park
                park_id = existing_park.park_id
                park_repo.update(park_id, park_record)
                self.stats['parks_updated'] += 1
                return park_id
            else:
                # Insert new park
                park = park_repo.create(park_record)
                self.stats['parks_inserted'] += 1
                logger.info(f"  ✓ Inserted park: {park.name}")
                return park.park_id

        except Exception as e:
            logger.error(f"Failed to upsert park: {e}")
            return None

    def _convert_country_to_iso(self, country_name: str) -> str:
        """
        Convert country name to ISO 3166-1 alpha-2 code.

        Args:
            country_name: Full country name from API

        Returns:
            2-letter ISO country code
        """
        # Common country mappings from Queue-Times API
        country_mapping = {
            'United States': 'US',
            'United Kingdom': 'GB',
            'Canada': 'CA',
            'France': 'FR',
            'Germany': 'DE',
            'Spain': 'ES',
            'Italy': 'IT',
            'Netherlands': 'NL',
            'Belgium': 'BE',
            'Japan': 'JP',
            'China': 'CN',
            'South Korea': 'KR',
            'Australia': 'AU',
            'Mexico': 'MX',
            'Brazil': 'BR',
        }

        return country_mapping.get(country_name, 'US')  # Default to US if unknown

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
            result = self.api_client.get_park_wait_times(queue_times_park_id)

            # Extract rides from nested lands structure (Disney/Universal parks use this)
            all_rides = []
            if 'lands' in result:
                for land in result['lands']:
                    land_rides = land.get('rides', [])
                    # Add land name to each ride for context
                    for ride in land_rides:
                        ride['land'] = land.get('name', '')
                    all_rides.extend(land_rides)

            # Also check for flat rides array (some parks use this format)
            if 'rides' in result:
                all_rides.extend(result.get('rides', []))

            return all_rides
        except Exception as e:
            logger.error(f"Failed to fetch rides for park {queue_times_park_id}: {e}")
            return []

    def _process_ride(self, ride_data: Dict, park_id: int, park_name: str, ride_repo: RideRepository):
        """
        Process a single ride: store ride data (classification happens separately).

        Args:
            ride_data: Ride data from Queue-Times API
            park_id: Database park ID
            park_name: Park name (unused, kept for consistency)
            ride_repo: Ride repository instance with database connection
        """
        try:
            queue_times_id = ride_data.get('id')
            ride_name = ride_data.get('name', 'Unknown')

            # Skip Single Rider lines - they don't represent actual ride status
            # Single Rider queues open/close independently and create false downtime
            if 'single rider' in ride_name.lower():
                logger.debug(f"    Skipping Single Rider line: {ride_name}")
                return

            self.stats['rides_processed'] += 1

            # Check if ride already exists
            existing_ride = ride_repo.get_by_queue_times_id(queue_times_id)

            ride_record = {
                'queue_times_id': queue_times_id,
                'park_id': park_id,
                'name': ride_name,
                'land_area': ride_data.get('land'),
                'tier': None,  # Will be populated by separate classification process
                'is_active': ride_data.get('is_open', True)
            }

            if existing_ride:
                # Update existing ride
                ride_id = existing_ride.ride_id
                ride_repo.update(ride_id, ride_record)
                self.stats['rides_updated'] += 1
                logger.info(f"    ✓ Updated: {ride_name}")
            else:
                # Insert new ride
                ride = ride_repo.create(ride_record)
                self.stats['rides_inserted'] += 1
                logger.info(f"    ✓ New ride: {ride_name}")

        except Exception as e:
            logger.error(f"Error processing ride {ride_data.get('name')}: {e}")
            self.stats['errors'] += 1

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
