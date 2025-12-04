#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Wait Time Snapshot Collection Script
Collects current wait times for all active rides and stores snapshots in database.

Supports two data providers:
- ThemeParks.wiki (preferred, has rich status: OPERATING/DOWN/CLOSED/REFURBISHMENT)
- Queue-Times.com (fallback, only has boolean is_open)

Parks with themeparks_wiki_id will use ThemeParks.wiki; others use Queue-Times.

This script should be run every 10 minutes via cron or similar scheduler.

Usage:
    python -m scripts.collect_snapshots

Cron example (every 10 minutes):
    */10 * * * * cd /path/to/backend && python -m scripts.collect_snapshots
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from collector.queue_times_client import QueueTimesClient
from collector.themeparks_wiki_client import get_themeparks_wiki_client
from collector.status_calculator import computed_is_open, validate_wait_time
from database.connection import get_db_connection
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.snapshot_repository import RideStatusSnapshotRepository, ParkActivitySnapshotRepository
from database.repositories.status_change_repository import RideStatusChangeRepository
from database.repositories.schedule_repository import ScheduleRepository
from database.repositories.data_quality_repository import DataQualityRepository


class SnapshotCollector:
    """
    Collects real-time wait time snapshots from ThemeParks.wiki or Queue-Times.com.

    Uses ThemeParks.wiki for parks with themeparks_wiki_id mapped.
    Falls back to Queue-Times.com for unmapped parks.
    """

    # Data older than this is considered stale and flagged for reporting
    # Buzz Lightyear case: data was 5+ months old!
    STALE_DATA_THRESHOLD_MINUTES = 60

    def __init__(self):
        self.queue_times_client = QueueTimesClient()
        self.themeparks_wiki_client = get_themeparks_wiki_client()

        self.stats = {
            'parks_processed': 0,
            'parks_themeparks_wiki': 0,
            'parks_queue_times': 0,
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
                schedule_repo = ScheduleRepository(conn)
                data_quality_repo = DataQualityRepository(conn)

                parks = park_repo.get_all_active()
                logger.info(f"Processing {len(parks)} active parks...")

                # Step 1.5: Refresh schedules for parks that need it (every 24 hours)
                self._refresh_schedules_if_needed(parks, schedule_repo)

                # Step 2: Process each park
                for park in parks:
                    self._process_park(park, park_activity_repo, ride_repo, snapshot_repo, status_change_repo, schedule_repo, data_quality_repo)

            # Step 3: Print summary
            self._print_summary()

            # Step 4: Pre-aggregate live rankings for instant API responses
            self._aggregate_live_rankings()

            logger.info("=" * 60)
            logger.info("SNAPSHOT COLLECTION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during snapshot collection: {e}", exc_info=True)
            sys.exit(1)

    def _refresh_schedules_if_needed(self, parks: List, schedule_repo: ScheduleRepository):
        """
        Refresh park schedules from ThemeParks.wiki API if stale (>24 hours).

        Only refreshes a few parks per collection run to avoid API overload.

        Args:
            parks: List of park objects
            schedule_repo: Schedule repository
        """
        MAX_REFRESHES_PER_RUN = 5  # Limit API calls per collection run

        refresh_count = 0
        for park in parks:
            if refresh_count >= MAX_REFRESHES_PER_RUN:
                break

            park_id = park.park_id
            themeparks_wiki_id = getattr(park, 'themeparks_wiki_id', None)

            # Only fetch schedules for parks with ThemeParks.wiki IDs
            if not themeparks_wiki_id:
                continue

            # Check if schedule needs refresh
            if schedule_repo.has_recent_schedule(park_id, max_age_hours=24):
                continue

            try:
                logger.info(f"Refreshing schedule for {park.name}...")
                entries = schedule_repo.fetch_and_store_schedule(park_id, themeparks_wiki_id)
                self.stats['schedules_refreshed'] = self.stats.get('schedules_refreshed', 0) + 1
                refresh_count += 1
            except Exception as e:
                logger.warning(f"Failed to refresh schedule for {park.name}: {e}")

    def _process_park(self, park: Dict, park_activity_repo: ParkActivitySnapshotRepository,
                      ride_repo: RideRepository, snapshot_repo: RideStatusSnapshotRepository,
                      status_change_repo: RideStatusChangeRepository, schedule_repo: ScheduleRepository,
                      data_quality_repo: DataQualityRepository):
        """
        Process a single park: fetch wait times and store snapshots.

        Uses ThemeParks.wiki if park has themeparks_wiki_id, otherwise Queue-Times.com.

        Args:
            park: Park record from database
            park_activity_repo: Park activity snapshot repository
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
            schedule_repo: Schedule repository for checking park hours
            data_quality_repo: Data quality issue tracking repository
        """
        park_id = park.park_id
        park_name = park.name
        queue_times_id = park.queue_times_id
        themeparks_wiki_id = getattr(park, 'themeparks_wiki_id', None)

        try:
            self.stats['parks_processed'] += 1

            # Route to appropriate provider
            if themeparks_wiki_id:
                logger.info(f"Processing: {park_name} [ThemeParks.wiki]")
                self.stats['parks_themeparks_wiki'] += 1
                self._process_park_themeparks_wiki(
                    park, themeparks_wiki_id, park_activity_repo,
                    ride_repo, snapshot_repo, status_change_repo, schedule_repo,
                    data_quality_repo
                )
            else:
                logger.info(f"Processing: {park_name} [Queue-Times]")
                self.stats['parks_queue_times'] += 1
                self._process_park_queue_times(
                    park, queue_times_id, park_activity_repo,
                    ride_repo, snapshot_repo, status_change_repo, schedule_repo
                )

        except Exception as e:
            logger.error(f"Error processing park {park_name}: {e}")
            self.stats['errors'] += 1

    def _process_park_themeparks_wiki(self, park: Dict, wiki_id: str,
                                       park_activity_repo: ParkActivitySnapshotRepository,
                                       ride_repo: RideRepository,
                                       snapshot_repo: RideStatusSnapshotRepository,
                                       status_change_repo: RideStatusChangeRepository,
                                       schedule_repo: ScheduleRepository,
                                       data_quality_repo: DataQualityRepository):
        """
        Process park using ThemeParks.wiki API.

        Args:
            park: Park record from database
            wiki_id: ThemeParks.wiki entity UUID
            park_activity_repo: Park activity snapshot repository
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
            schedule_repo: Schedule repository for checking park hours
            data_quality_repo: Data quality issue tracking repository
        """
        park_id = park.park_id
        park_name = park.name

        # Fetch live data from ThemeParks.wiki
        live_data = self.themeparks_wiki_client.get_park_live_data(wiki_id)

        if not live_data:
            logger.warning(f"  No ride data returned for {park_name}")
            return

        # Calculate park activity stats
        total_rides = len(live_data)
        rides_operating = sum(1 for r in live_data if r.status == 'OPERATING')
        rides_down = sum(1 for r in live_data if r.status == 'DOWN')
        rides_closed = sum(1 for r in live_data if r.status in ('CLOSED', 'REFURBISHMENT'))

        # Use schedule-based park open detection (SINGLE SOURCE OF TRUTH)
        # If no schedule, park is CLOSED - we don't trust API status alone
        park_appears_open = schedule_repo.is_park_open_now(park_id)
        if not park_appears_open and not schedule_repo.has_recent_schedule(park_id, max_age_hours=48):
            # No schedule data available - log warning but keep park as CLOSED
            # We can't trust API "OPERATING" status because some parks report
            # rides as OPERATING even when the park is closed for the season
            logger.warning(f"  No schedule data for {park_name} - treating as CLOSED")
            park_appears_open = False

        # Calculate wait time statistics
        open_wait_times = [r.wait_time for r in live_data
                          if r.status == 'OPERATING' and r.wait_time is not None]
        avg_wait = sum(open_wait_times) / len(open_wait_times) if open_wait_times else None
        max_wait = max(open_wait_times) if open_wait_times else None

        self._store_park_activity(park_id, park_appears_open, total_rides,
                                 rides_operating, rides_closed + rides_down,
                                 avg_wait, max_wait, park_activity_repo)

        # Process each ride
        for ride_data in live_data:
            self._process_ride_themeparks_wiki(
                ride_data, park_id, ride_repo, snapshot_repo, status_change_repo,
                data_quality_repo
            )

        logger.info(f"  ✓ Processed {len(live_data)} rides "
                   f"(OPERATING:{rides_operating}, DOWN:{rides_down}, CLOSED:{rides_closed})")

    def _process_ride_themeparks_wiki(self, ride_data, park_id: int,
                                       ride_repo: RideRepository,
                                       snapshot_repo: RideStatusSnapshotRepository,
                                       status_change_repo: RideStatusChangeRepository,
                                       data_quality_repo: DataQualityRepository):
        """
        Process a single ride from ThemeParks.wiki data.

        Args:
            ride_data: LiveRideData object from ThemeParks.wiki client
            park_id: Database park ID
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
            data_quality_repo: Data quality issue tracking repository
        """
        try:
            # Find ride in database by themeparks_wiki_id
            ride = ride_repo.get_by_themeparks_wiki_id(ride_data.entity_id)
            if not ride:
                # Try fuzzy match by name if not mapped yet
                logger.debug(f"  Ride not mapped: {ride_data.name} [{ride_data.entity_id[:8]}...]")
                return

            ride_id = ride.ride_id
            self.stats['rides_processed'] += 1

            # Map ThemeParks.wiki status to our boolean + rich status
            status = ride_data.status
            is_operating = (status == 'OPERATING')
            wait_time = ride_data.wait_time

            # Update last_operated_at for 7-day hybrid denominator calculation
            if is_operating:
                self._update_last_operated_at(ride_id, ride_repo)

            # Check for stale data (data quality issue detection)
            # This catches issues like Buzz Lightyear where lastUpdated was 5+ months old
            if ride_data.last_updated:
                try:
                    # Parse the lastUpdated timestamp
                    last_updated_str = ride_data.last_updated
                    if last_updated_str:
                        # Handle ISO format with Z suffix
                        last_updated_str = last_updated_str.replace('Z', '+00:00')
                        last_updated_dt = datetime.fromisoformat(last_updated_str)
                        # Make timezone-naive for comparison
                        if last_updated_dt.tzinfo:
                            last_updated_dt = last_updated_dt.replace(tzinfo=None)

                        now = datetime.now()
                        age_minutes = int((now - last_updated_dt).total_seconds() / 60)

                        if age_minutes > self.STALE_DATA_THRESHOLD_MINUTES:
                            # Log stale data issue for reporting to ThemeParks.wiki
                            data_quality_repo.log_stale_data(
                                data_source='themeparks_wiki',
                                park_id=park_id,
                                ride_id=ride_id,
                                themeparks_wiki_id=ride_data.entity_id,
                                queue_times_id=None,
                                entity_name=ride_data.name,
                                last_updated_api=last_updated_dt,
                                data_age_minutes=age_minutes,
                                reported_status=status,
                            )
                            self.stats['stale_data_issues'] = self.stats.get('stale_data_issues', 0) + 1

                except (ValueError, TypeError) as e:
                    logger.debug(f"  Could not parse lastUpdated for {ride_data.name}: {e}")

            # Store snapshot with rich status
            self._store_snapshot_with_status(
                ride_id, wait_time, is_operating, is_operating, status,
                ride_data.last_updated, snapshot_repo
            )

            # Detect status change using rich status
            self._detect_status_change_rich(
                ride_id, status, snapshot_repo, status_change_repo
            )

        except Exception as e:
            logger.error(f"Error processing ride {ride_data.name}: {e}")
            self.stats['errors'] += 1

    def _store_snapshot_with_status(self, ride_id: int, wait_time: Optional[int],
                                     is_open_api: Optional[bool], computed_status: bool,
                                     status_enum: Optional[str], last_updated_api: Optional[str],
                                     snapshot_repo: RideStatusSnapshotRepository):
        """
        Store ride status snapshot with rich status enum.

        Args:
            ride_id: Database ride ID
            wait_time: Wait time in minutes
            is_open_api: API-reported open status (for backwards compat)
            computed_status: Computed open/closed status (for backwards compat)
            status_enum: Rich status (OPERATING/DOWN/CLOSED/REFURBISHMENT)
            last_updated_api: API timestamp
            snapshot_repo: Ride status snapshot repository
        """
        try:
            snapshot_record = {
                'ride_id': ride_id,
                'recorded_at': datetime.now(),
                'wait_time': wait_time,
                'is_open': is_open_api,
                'computed_is_open': computed_status,
                'status': status_enum,
                'last_updated_api': last_updated_api
            }

            snapshot_repo.insert(snapshot_record)
            self.stats['snapshots_created'] += 1

        except Exception as e:
            logger.error(f"Failed to store snapshot for ride {ride_id}: {e}")

    def _detect_status_change_rich(self, ride_id: int, current_status: str,
                                    snapshot_repo: RideStatusSnapshotRepository,
                                    status_change_repo: RideStatusChangeRepository):
        """
        Detect if ride status has changed using rich status enum.

        Args:
            ride_id: Database ride ID
            current_status: Current status (OPERATING/DOWN/CLOSED/REFURBISHMENT)
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
        """
        try:
            # Convert to boolean for backwards compat
            is_operating = (current_status == 'OPERATING')

            # Get previous status from cache or database
            if ride_id not in self.previous_statuses:
                last_snapshot = snapshot_repo.get_latest_by_ride(ride_id)
                if last_snapshot:
                    prev_status = last_snapshot.get('status') or \
                                 ('OPERATING' if last_snapshot['computed_is_open'] else 'DOWN')
                    self.previous_statuses[ride_id] = {
                        'status': prev_status,
                        'status_bool': last_snapshot['computed_is_open'],
                        'timestamp': last_snapshot['recorded_at']
                    }
                else:
                    self.previous_statuses[ride_id] = {
                        'status': current_status,
                        'status_bool': is_operating,
                        'timestamp': datetime.now()
                    }
                    return

            prev = self.previous_statuses[ride_id]
            previous_status = prev['status']
            previous_status_bool = prev['status_bool']
            previous_timestamp = prev['timestamp']

            # Check if status changed (compare strings for richer change detection)
            if current_status != previous_status:
                now = datetime.now()
                duration = int((now - previous_timestamp).total_seconds() / 60)

                change_record = {
                    'ride_id': ride_id,
                    'changed_at': now,
                    'previous_status': previous_status_bool,
                    'new_status': is_operating,
                    'previous_status_enum': previous_status,
                    'new_status_enum': current_status,
                    'duration_in_previous_status': duration,
                    'wait_time_at_change': None
                }

                status_change_repo.insert(change_record)
                self.stats['status_changes'] += 1

                logger.info(f"  ⚠ Status change: {previous_status} → {current_status}")

                self.previous_statuses[ride_id] = {
                    'status': current_status,
                    'status_bool': is_operating,
                    'timestamp': now
                }

        except Exception as e:
            logger.error(f"Failed to detect status change for ride {ride_id}: {e}")

    def _process_park_queue_times(self, park: Dict, queue_times_id: int,
                                   park_activity_repo: ParkActivitySnapshotRepository,
                                   ride_repo: RideRepository,
                                   snapshot_repo: RideStatusSnapshotRepository,
                                   status_change_repo: RideStatusChangeRepository,
                                   schedule_repo: ScheduleRepository):
        """
        Process park using Queue-Times.com API (legacy provider).

        Args:
            park: Park record from database
            queue_times_id: Queue-Times.com park ID
            park_activity_repo: Park activity snapshot repository
            ride_repo: Ride repository
            snapshot_repo: Ride status snapshot repository
            status_change_repo: Ride status change repository
            schedule_repo: Schedule repository for checking park hours
        """
        park_id = park.park_id
        park_name = park.name

        try:
            # Fetch current wait times from Queue-Times API
            api_response = self.queue_times_client.get_park_wait_times(queue_times_id)

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

            # Filter out stale data - Queue-Times API caches old wait times for hours after parks close
            # If last_updated > 30 min ago, treat the ride as closed
            STALE_THRESHOLD_MINUTES = 30
            now = datetime.now(timezone.utc)
            for ride in rides_data:
                last_updated_str = ride.get('last_updated')
                if last_updated_str:
                    try:
                        # Parse ISO timestamp: "2025-11-28T05:56:23.000Z"
                        last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                        age_minutes = (now - last_updated).total_seconds() / 60
                        if age_minutes > STALE_THRESHOLD_MINUTES:
                            # Data is stale - treat ride as closed
                            ride['wait_time'] = 0
                            ride['is_open'] = False
                    except (ValueError, TypeError):
                        pass  # If parsing fails, use data as-is

            if not rides_data:
                logger.warning(f"  No ride data returned for {park_name}")
                return

            # Track park activity - calculate statistics
            total_rides = len(rides_data)
            # Count rides with actual wait times (indicates park is operating)
            rides_with_wait = sum(1 for r in rides_data if r.get('wait_time', 0) > 0)
            # Count rides reported as "open" by API (may include closed parks with is_open=True)
            rides_open = sum(1 for r in rides_data if r.get('wait_time', 0) > 0 or r.get('is_open'))
            rides_closed = total_rides - rides_open

            # Use schedule-based park open detection (SINGLE SOURCE OF TRUTH)
            # If no schedule, park is CLOSED - we don't trust API status alone
            park_appears_open = schedule_repo.is_park_open_now(park_id)
            if not park_appears_open and not schedule_repo.has_recent_schedule(park_id, max_age_hours=48):
                # No schedule data available - log warning but keep park as CLOSED
                logger.warning(f"  No schedule data for {park_name} - treating as CLOSED")
                park_appears_open = False

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

            # Skip non-ATTRACTION categories (shows, meet & greets, experiences)
            # Only track mechanical rides for downtime/wait time statistics
            if ride.category and ride.category != 'ATTRACTION':
                logger.debug(f"  Skipping {ride.name} (category: {ride.category})")
                return

            ride_id = ride.ride_id
            self.stats['rides_processed'] += 1

            # Extract wait time and status from API
            wait_time_raw = ride_data.get('wait_time')
            is_open_api = ride_data.get('is_open')
            last_updated_api = ride_data.get('last_updated')

            # Validate and compute status
            wait_time = validate_wait_time(wait_time_raw)
            computed_status = computed_is_open(wait_time, is_open_api)

            # Update last_operated_at for 7-day hybrid denominator calculation
            if computed_status:
                self._update_last_operated_at(ride_id, ride_repo)

            # Store snapshot
            self._store_snapshot(ride_id, wait_time, is_open_api, computed_status, last_updated_api, snapshot_repo)

            # Detect status change
            self._detect_status_change(ride_id, computed_status, snapshot_repo, status_change_repo)

        except Exception as e:
            logger.error(f"Error processing ride: {e}")
            self.stats['errors'] += 1

    def _store_snapshot(self, ride_id: int, wait_time: Optional[int],
                       is_open_api: Optional[bool], computed_status: bool,
                       last_updated_api: Optional[str],
                       snapshot_repo: RideStatusSnapshotRepository):
        """
        Store ride status snapshot.

        Args:
            ride_id: Database ride ID
            wait_time: Validated wait time (None if invalid)
            is_open_api: API-reported open status
            computed_status: Computed open/closed status
            last_updated_api: API timestamp when ride data was last updated (ISO format)
            snapshot_repo: Ride status snapshot repository
        """
        try:
            snapshot_record = {
                'ride_id': ride_id,
                'recorded_at': datetime.now(),
                'wait_time': wait_time,
                'is_open': is_open_api,
                'computed_is_open': computed_status,
                'status': None,  # Queue-Times doesn't provide rich status
                'last_updated_api': last_updated_api
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

    def _update_last_operated_at(self, ride_id: int, ride_repo: RideRepository):
        """
        Update last_operated_at timestamp when a ride is OPERATING.

        This is used by the 7-day hybrid denominator to filter out rides
        that haven't operated recently (seasonal closures, refurbishments).

        Args:
            ride_id: Database ride ID
            ride_repo: Ride repository for database updates
        """
        try:
            ride_repo.update(ride_id, {"last_operated_at": datetime.now()})
            self.stats['last_operated_updates'] = self.stats.get('last_operated_updates', 0) + 1
        except Exception as e:
            logger.debug(f"Failed to update last_operated_at for ride {ride_id}: {e}")

    def _print_summary(self):
        """Print collection summary statistics."""
        logger.info("")
        logger.info(f"Parks processed:     {self.stats['parks_processed']}")
        logger.info(f"Rides processed:     {self.stats['rides_processed']}")
        logger.info(f"Snapshots created:   {self.stats['snapshots_created']}")
        logger.info(f"Status changes:      {self.stats['status_changes']}")
        logger.info(f"Last operated updates: {self.stats.get('last_operated_updates', 0)}")
        logger.info(f"Schedules refreshed: {self.stats.get('schedules_refreshed', 0)}")
        logger.info(f"Stale data issues:   {self.stats.get('stale_data_issues', 0)}")
        logger.info(f"Errors:              {self.stats['errors']}")

    def _aggregate_live_rankings(self):
        """
        Pre-aggregate live rankings for instant API responses.

        Runs after snapshot collection to populate park_live_rankings
        and ride_live_rankings tables. Uses atomic table swap for
        zero-downtime updates.
        """
        try:
            from scripts.aggregate_live_rankings import LiveRankingsAggregator

            logger.info("")
            logger.info("Pre-aggregating live rankings...")
            aggregator = LiveRankingsAggregator()
            stats = aggregator.run()

            self.stats['parks_aggregated'] = stats.get('parks_aggregated', 0)
            self.stats['rides_aggregated'] = stats.get('rides_aggregated', 0)

        except Exception as e:
            logger.error(f"Failed to aggregate live rankings: {e}", exc_info=True)
            # Don't fail the whole collection if aggregation fails
            # The API will fall back to the old (possibly stale) cached data
            self.stats['aggregation_error'] = str(e)


def main():
    """Main entry point."""
    collector = SnapshotCollector()
    collector.run()


if __name__ == '__main__':
    main()
