#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Wait Time Snapshot Collection Script
Collects current wait times for all active rides and stores snapshots in database.

This script should be run every 10 minutes via cron or similar scheduler.

Usage:
    python -m scripts.collect_snapshots

Cron example (every 10 minutes):
    */10 * * * * cd /path/to/backend && python -m scripts.collect_snapshots
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from collector.queue_times_client import QueueTimesClient
from collector.status_calculator import computed_is_open, validate_wait_time
from database.connection import get_db_connection
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.snapshot_repository import RideStatusSnapshotRepository, ParkActivitySnapshotRepository
from database.repositories.status_change_repository import RideStatusChangeRepository


class SnapshotCollector:
    """
    Collects real-time wait time snapshots from Queue-Times.com API.
    """

    def __init__(self):
        self.api_client = QueueTimesClient()

        self.stats = {
            'parks_processed': 0,
            'rides_processed': 0,
            'snapshots_created': 0,
            'status_changes': 0,
            'errors': 0
        }

        # Cache for previous ride statuses (to detect status changes)
        self.previous_statuses = {}

    def run(self):
        """Main execution method."""
        logger.info("=" * 60)
        logger.info(f"SNAPSHOT COLLECTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        try:
            # Step 1: Get all active parks with database connection
            with get_db_connection() as conn:
                park_repo = ParkRepository(conn)
                ride_repo = RideRepository(conn)
                snapshot_repo = RideStatusSnapshotRepository(conn)
                park_activity_repo = ParkActivitySnapshotRepository(conn)
                status_change_repo = RideStatusChangeRepository(conn)

                parks = park_repo.get_all_active()
                logger.info(f"Processing {len(parks)} active parks...")

                # Step 2: Process each park
                for park in parks:
                    self._process_park(park, park_activity_repo, ride_repo, snapshot_repo, status_change_repo)

            # Step 3: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("SNAPSHOT COLLECTION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during snapshot collection: {e}", exc_info=True)
            sys.exit(1)

    def _process_park(self, park: Dict, park_activity_repo: ParkActivitySnapshotRepository,
                      ride_repo: RideRepository, snapshot_repo: RideStatusSnapshotRepository,
                      status_change_repo: RideStatusChangeRepository):
        """
        Process a single park: fetch wait times and store snapshots.

        Args:
            park: Park record from database
            park_activity_repo: Park activity snapshot repository
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
        """
        park_id = park.park_id
        park_name = park.name
        queue_times_id = park.queue_times_id

        try:
            logger.info(f"Processing: {park_name}")
            self.stats['parks_processed'] += 1

            # Fetch current wait times from Queue-Times API
            api_response = self.api_client.get_park_wait_times(queue_times_id)

            # Extract rides from nested lands structure (Disney/Universal parks use this)
            rides_data = []
            if api_response:
                if 'lands' in api_response:
                    for land in api_response['lands']:
                        rides_data.extend(land.get('rides', []))
                # Also check for flat rides array (some parks use this format)
                if 'rides' in api_response:
                    rides_data.extend(api_response.get('rides', []))

            # Filter out Single Rider lines - they don't represent actual ride status
            rides_data = [r for r in rides_data if 'single rider' not in r.get('name', '').lower()]

            if not rides_data:
                logger.warning(f"  No ride data returned for {park_name}")
                return

            # Track park activity - calculate statistics
            total_rides = len(rides_data)
            rides_open = sum(1 for r in rides_data if r.get('wait_time', 0) > 0 or r.get('is_open'))
            rides_closed = total_rides - rides_open
            park_appears_open = rides_open > 0

            # Calculate wait time statistics for open rides
            open_wait_times = [r.get('wait_time', 0) for r in rides_data
                             if (r.get('wait_time', 0) > 0 or r.get('is_open')) and r.get('wait_time') is not None]
            avg_wait = sum(open_wait_times) / len(open_wait_times) if open_wait_times else None
            max_wait = max(open_wait_times) if open_wait_times else None

            self._store_park_activity(park_id, park_appears_open, total_rides, rides_open,
                                    rides_closed, avg_wait, max_wait, park_activity_repo)

            # Process each ride
            for ride_data in rides_data:
                self._process_ride(ride_data, park_id, ride_repo, snapshot_repo, status_change_repo)

            logger.info(f"  ✓ Processed {len(rides_data)} rides "
                       f"({rides_open} open, park appears {'open' if park_appears_open else 'closed'})")

        except Exception as e:
            logger.error(f"Error processing park {park_name}: {e}")
            self.stats['errors'] += 1

    def _store_park_activity(self, park_id: int, appears_open: bool, total_rides: int,
                             rides_open: int, rides_closed: int, avg_wait: Optional[float],
                             max_wait: Optional[int], park_activity_repo: ParkActivitySnapshotRepository):
        """
        Store park activity snapshot.

        Args:
            park_id: Database park ID
            appears_open: Whether park appears to be operating
            total_rides: Total number of rides tracked
            rides_open: Number of open rides
            rides_closed: Number of closed rides
            avg_wait: Average wait time across open rides
            max_wait: Maximum wait time across all rides
            park_activity_repo: Park activity snapshot repository
        """
        try:
            activity_record = {
                'park_id': park_id,
                'recorded_at': datetime.now(),
                'total_rides_tracked': total_rides,
                'rides_open': rides_open,
                'rides_closed': rides_closed,
                'avg_wait_time': avg_wait,
                'max_wait_time': max_wait,
                'park_appears_open': appears_open
            }

            park_activity_repo.insert(activity_record)

        except Exception as e:
            logger.error(f"Failed to store park activity: {e}")

    def _process_ride(self, ride_data: Dict, park_id: int, ride_repo: RideRepository,
                     snapshot_repo: RideStatusSnapshotRepository, status_change_repo: RideStatusChangeRepository):
        """
        Process a single ride: store snapshot and detect status changes.

        Args:
            ride_data: Ride data from Queue-Times API
            park_id: Database park ID
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
        """
        try:
            queue_times_id = ride_data.get('id')

            # Find ride in database
            ride = ride_repo.get_by_queue_times_id(queue_times_id)
            if not ride:
                logger.warning(f"  Ride ID {queue_times_id} not found in database (may need to run collect_parks)")
                return

            ride_id = ride.ride_id
            self.stats['rides_processed'] += 1

            # Extract wait time and status from API
            wait_time_raw = ride_data.get('wait_time')
            is_open_api = ride_data.get('is_open')

            # Validate and compute status
            wait_time = validate_wait_time(wait_time_raw)
            computed_status = computed_is_open(wait_time, is_open_api)

            # Store snapshot
            self._store_snapshot(ride_id, wait_time, is_open_api, computed_status, snapshot_repo)

            # Detect status change
            self._detect_status_change(ride_id, computed_status, snapshot_repo, status_change_repo)

        except Exception as e:
            logger.error(f"Error processing ride: {e}")
            self.stats['errors'] += 1

    def _store_snapshot(self, ride_id: int, wait_time: Optional[int],
                       is_open_api: Optional[bool], computed_status: bool,
                       snapshot_repo: RideStatusSnapshotRepository):
        """
        Store ride status snapshot.

        Args:
            ride_id: Database ride ID
            wait_time: Validated wait time (None if invalid)
            is_open_api: API-reported open status
            computed_status: Computed open/closed status
            snapshot_repo: Ride status snapshot repository
        """
        try:
            snapshot_record = {
                'ride_id': ride_id,
                'recorded_at': datetime.now(),
                'wait_time': wait_time,
                'is_open': is_open_api,
                'computed_is_open': computed_status,
                'last_updated_api': None  # Could be populated from API if available
            }

            snapshot_repo.insert(snapshot_record)
            self.stats['snapshots_created'] += 1

        except Exception as e:
            logger.error(f"Failed to store snapshot for ride {ride_id}: {e}")

    def _detect_status_change(self, ride_id: int, current_status: bool,
                             snapshot_repo: RideStatusSnapshotRepository,
                             status_change_repo: RideStatusChangeRepository):
        """
        Detect if ride status has changed since last snapshot.

        Args:
            ride_id: Database ride ID
            current_status: Current computed open/closed status
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
        """
        try:
            # Get previous status from cache or database
            if ride_id not in self.previous_statuses:
                last_snapshot = snapshot_repo.get_latest_by_ride(ride_id)
                if last_snapshot:
                    self.previous_statuses[ride_id] = {
                        'status': last_snapshot['computed_is_open'],
                        'timestamp': last_snapshot['recorded_at']
                    }
                else:
                    # First snapshot for this ride
                    self.previous_statuses[ride_id] = {
                        'status': current_status,
                        'timestamp': datetime.now()
                    }
                    return

            previous_status = self.previous_statuses[ride_id]['status']
            previous_timestamp = self.previous_statuses[ride_id]['timestamp']

            # Check if status changed
            if current_status != previous_status:
                # Status changed!
                now = datetime.now()
                downtime_duration = None

                # If changing from open to closed, calculate downtime duration
                if previous_status and not current_status:
                    downtime_duration = int((now - previous_timestamp).total_seconds() / 60)

                # Store status change
                change_record = {
                    'ride_id': ride_id,
                    'changed_at': now,
                    'previous_status': previous_status,
                    'new_status': current_status,
                    'duration_in_previous_status': downtime_duration or 0,
                    'wait_time_at_change': None  # Could be populated from current snapshot
                }

                status_change_repo.insert(change_record)
                self.stats['status_changes'] += 1

                status_text = "OPEN → CLOSED" if not current_status else "CLOSED → OPEN"
                logger.info(f"  ⚠ Status change detected for ride {ride_id}: {status_text}")

                # Update cache
                self.previous_statuses[ride_id] = {
                    'status': current_status,
                    'timestamp': now
                }

        except Exception as e:
            logger.error(f"Failed to detect status change for ride {ride_id}: {e}")

    def _print_summary(self):
        """Print collection summary statistics."""
        logger.info("")
        logger.info(f"Parks processed:     {self.stats['parks_processed']}")
        logger.info(f"Rides processed:     {self.stats['rides_processed']}")
        logger.info(f"Snapshots created:   {self.stats['snapshots_created']}")
        logger.info(f"Status changes:      {self.stats['status_changes']}")
        logger.info(f"Errors:              {self.stats['errors']}")


def main():
    """Main entry point."""
    collector = SnapshotCollector()
    collector.run()


if __name__ == '__main__':
    main()
