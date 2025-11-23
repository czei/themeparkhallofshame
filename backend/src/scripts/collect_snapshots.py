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
        self.park_repo = ParkRepository()
        self.ride_repo = RideRepository()
        self.snapshot_repo = RideStatusSnapshotRepository()
        self.park_activity_repo = ParkActivitySnapshotRepository()
        self.status_change_repo = RideStatusChangeRepository()

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
            # Step 1: Get all active parks
            parks = self.park_repo.get_all_active()
            logger.info(f"Processing {len(parks)} active parks...")

            # Step 2: Process each park
            for park in parks:
                self._process_park(park)

            # Step 3: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("SNAPSHOT COLLECTION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during snapshot collection: {e}", exc_info=True)
            sys.exit(1)

    def _process_park(self, park: Dict):
        """
        Process a single park: fetch wait times and store snapshots.

        Args:
            park: Park record from database
        """
        park_id = park['park_id']
        park_name = park['name']
        queue_times_id = park['queue_times_id']

        try:
            logger.info(f"Processing: {park_name}")
            self.stats['parks_processed'] += 1

            # Fetch current wait times from Queue-Times API
            rides_data = self.api_client.fetch_park_rides(queue_times_id)

            if not rides_data:
                logger.warning(f"  No ride data returned for {park_name}")
                return

            # Track park activity
            active_rides = sum(1 for r in rides_data if r.get('wait_time', 0) > 0 or r.get('is_open'))
            total_rides = len(rides_data)
            park_appears_open = active_rides > 0

            self._store_park_activity(park_id, park_appears_open, active_rides, total_rides)

            # Process each ride
            for ride_data in rides_data:
                self._process_ride(ride_data, park_id)

            logger.info(f"  ✓ Processed {len(rides_data)} rides "
                       f"({active_rides} active, park appears {'open' if park_appears_open else 'closed'})")

        except Exception as e:
            logger.error(f"Error processing park {park_name}: {e}")
            self.stats['errors'] += 1

    def _store_park_activity(self, park_id: int, appears_open: bool, active_count: int, total_count: int):
        """
        Store park activity snapshot.

        Args:
            park_id: Database park ID
            appears_open: Whether park appears to be operating
            active_count: Number of active rides
            total_count: Total number of rides tracked
        """
        try:
            activity_record = {
                'park_id': park_id,
                'recorded_at': datetime.now(),
                'park_appears_open': appears_open,
                'active_rides_count': active_count,
                'total_rides_count': total_count,
                'created_at': datetime.now()
            }

            self.park_activity_repo.insert(activity_record)

        except Exception as e:
            logger.error(f"Failed to store park activity: {e}")

    def _process_ride(self, ride_data: Dict, park_id: int):
        """
        Process a single ride: store snapshot and detect status changes.

        Args:
            ride_data: Ride data from Queue-Times API
            park_id: Database park ID
        """
        try:
            queue_times_id = ride_data.get('id')

            # Find ride in database
            ride = self.ride_repo.get_by_queue_times_id(queue_times_id)
            if not ride:
                logger.warning(f"  Ride ID {queue_times_id} not found in database (may need to run collect_parks)")
                return

            ride_id = ride['ride_id']
            self.stats['rides_processed'] += 1

            # Extract wait time and status from API
            wait_time_raw = ride_data.get('wait_time')
            is_open_api = ride_data.get('is_open')

            # Validate and compute status
            wait_time = validate_wait_time(wait_time_raw)
            computed_status = computed_is_open(wait_time, is_open_api)

            # Store snapshot
            self._store_snapshot(ride_id, wait_time, is_open_api, computed_status)

            # Detect status change
            self._detect_status_change(ride_id, computed_status)

        except Exception as e:
            logger.error(f"Error processing ride: {e}")
            self.stats['errors'] += 1

    def _store_snapshot(self, ride_id: int, wait_time: Optional[int],
                       is_open_api: Optional[bool], computed_status: bool):
        """
        Store ride status snapshot.

        Args:
            ride_id: Database ride ID
            wait_time: Validated wait time (None if invalid)
            is_open_api: API-reported open status
            computed_status: Computed open/closed status
        """
        try:
            snapshot_record = {
                'ride_id': ride_id,
                'recorded_at': datetime.now(),
                'wait_time': wait_time,
                'is_open': is_open_api,
                'computed_is_open': computed_status,
                'created_at': datetime.now()
            }

            self.snapshot_repo.insert(snapshot_record)
            self.stats['snapshots_created'] += 1

        except Exception as e:
            logger.error(f"Failed to store snapshot for ride {ride_id}: {e}")

    def _detect_status_change(self, ride_id: int, current_status: bool):
        """
        Detect if ride status has changed since last snapshot.

        Args:
            ride_id: Database ride ID
            current_status: Current computed open/closed status
        """
        try:
            # Get previous status from cache or database
            if ride_id not in self.previous_statuses:
                last_snapshot = self.snapshot_repo.get_latest_by_ride(ride_id)
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
                    'old_status': previous_status,
                    'new_status': current_status,
                    'downtime_duration_minutes': downtime_duration,
                    'created_at': now
                }

                self.status_change_repo.insert(change_record)
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
