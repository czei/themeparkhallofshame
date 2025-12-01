#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Schedule Refresh Script
Fetches operating schedules from ThemeParks.wiki API for all parks.

This script refreshes schedule data for all active parks that have a
ThemeParks.wiki ID. The API returns 30+ days of schedule data per call.

Usage:
    python -m scripts.refresh_schedules [--force] [--park-id PARK_ID]

Options:
    --force      Refresh all parks regardless of last fetch time
    --park-id    Only refresh a specific park by ID

Cron example (daily at 6 AM UTC = 2 AM Eastern, low-traffic time):
    0 6 * * * cd /opt/themeparkhallofshame/backend && /opt/themeparkhallofshame/venv/bin/python -m scripts.refresh_schedules

The script:
1. Gets all active parks with ThemeParks.wiki IDs
2. Fetches schedule data from the API for each park
3. Upserts schedule entries into park_schedules table
4. Cleans up old schedule entries (>7 days old)
"""

import sys
import argparse
import time
from pathlib import Path
from typing import Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from database.connection import get_db_connection
from database.repositories.schedule_repository import ScheduleRepository
from database.repositories.park_repository import ParkRepository


class ScheduleRefresher:
    """
    Refreshes park schedules from ThemeParks.wiki API.
    """

    # Delay between API calls to be a good citizen (seconds)
    API_DELAY_SECONDS = 1.0

    # Max age before schedule is considered stale (hours)
    STALE_THRESHOLD_HOURS = 24

    def __init__(self, force: bool = False, park_id: Optional[int] = None):
        """
        Initialize the refresher.

        Args:
            force: If True, refresh all parks regardless of last fetch time
            park_id: If provided, only refresh this specific park
        """
        self.force = force
        self.park_id = park_id
        self.stats = {
            'parks_checked': 0,
            'parks_refreshed': 0,
            'schedules_stored': 0,
            'parks_skipped': 0,
            'parks_failed': 0,
            'old_schedules_cleaned': 0,
        }

    def run(self):
        """Main execution method."""
        logger.info("=" * 60)
        logger.info("SCHEDULE REFRESH - Starting")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            with get_db_connection() as conn:
                schedule_repo = ScheduleRepository(conn)
                park_repo = ParkRepository(conn)

                # Get parks to process
                if self.park_id:
                    parks = self._get_single_park(park_repo)
                else:
                    parks = self._get_parks_to_refresh(park_repo, schedule_repo)

                # Process each park
                for park in parks:
                    self._process_park(park, schedule_repo)

                # Clean up old schedules
                self._cleanup_old_schedules(schedule_repo)

                # Commit changes
                conn.commit()

        except Exception as e:
            logger.error(f"Schedule refresh failed: {e}")
            raise

        elapsed = time.time() - start_time
        self._log_summary(elapsed)

    def _get_single_park(self, park_repo: ParkRepository) -> list:
        """Get a single park by ID."""
        park = park_repo.get_park_by_id(self.park_id)
        if not park:
            logger.error(f"Park ID {self.park_id} not found")
            return []
        if not park.themeparks_wiki_id:
            logger.error(f"Park {park.name} has no ThemeParks.wiki ID")
            return []
        return [park]

    def _get_parks_to_refresh(
        self,
        park_repo: ParkRepository,
        schedule_repo: ScheduleRepository
    ) -> list:
        """
        Get list of parks that need schedule refresh.

        If force=True, returns all parks with wiki IDs.
        Otherwise, returns only parks with stale/missing schedules.
        """
        if self.force:
            logger.info("Force mode: refreshing all parks with ThemeParks.wiki IDs")
            parks = park_repo.get_all_active()
            return [p for p in parks if getattr(p, 'themeparks_wiki_id', None)]
        else:
            # Get parks needing refresh (stale or missing schedules)
            parks_needing_refresh = schedule_repo.get_parks_needing_schedule_refresh(
                max_age_hours=self.STALE_THRESHOLD_HOURS
            )
            logger.info(f"Found {len(parks_needing_refresh)} parks needing schedule refresh")
            return parks_needing_refresh

    def _process_park(self, park, schedule_repo: ScheduleRepository):
        """
        Process a single park: fetch and store schedule.

        Args:
            park: Park record (can be dict or object)
            schedule_repo: Schedule repository instance
        """
        self.stats['parks_checked'] += 1

        # Handle both dict and object forms
        if isinstance(park, dict):
            park_id = park['park_id']
            park_name = park['name']
            wiki_id = park['themeparks_wiki_id']
        else:
            park_id = park.park_id
            park_name = park.name
            wiki_id = getattr(park, 'themeparks_wiki_id', None)

        if not wiki_id:
            logger.debug(f"Skipping {park_name}: no ThemeParks.wiki ID")
            self.stats['parks_skipped'] += 1
            return

        try:
            logger.info(f"Refreshing schedule for {park_name}...")
            entries_stored = schedule_repo.fetch_and_store_schedule(park_id, wiki_id)

            self.stats['parks_refreshed'] += 1
            self.stats['schedules_stored'] += entries_stored
            logger.info(f"  Stored {entries_stored} schedule entries")

            # Be nice to the API
            time.sleep(self.API_DELAY_SECONDS)

        except Exception as e:
            logger.warning(f"Failed to refresh schedule for {park_name}: {e}")
            self.stats['parks_failed'] += 1

    def _cleanup_old_schedules(self, schedule_repo: ScheduleRepository):
        """Remove old schedule entries."""
        try:
            deleted = schedule_repo.cleanup_old_schedules(days_to_keep=7)
            self.stats['old_schedules_cleaned'] = deleted
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old schedule entries")
        except Exception as e:
            logger.warning(f"Failed to cleanup old schedules: {e}")

    def _log_summary(self, elapsed: float):
        """Log execution summary."""
        logger.info("=" * 60)
        logger.info("SCHEDULE REFRESH - Complete")
        logger.info(f"  Time elapsed: {elapsed:.1f} seconds")
        logger.info(f"  Parks checked: {self.stats['parks_checked']}")
        logger.info(f"  Parks refreshed: {self.stats['parks_refreshed']}")
        logger.info(f"  Schedules stored: {self.stats['schedules_stored']}")
        logger.info(f"  Parks skipped (no wiki ID): {self.stats['parks_skipped']}")
        logger.info(f"  Parks failed: {self.stats['parks_failed']}")
        logger.info(f"  Old schedules cleaned: {self.stats['old_schedules_cleaned']}")
        logger.info("=" * 60)


def main():
    """Entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description='Refresh park schedules from ThemeParks.wiki API'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Refresh all parks regardless of last fetch time'
    )
    parser.add_argument(
        '--park-id',
        type=int,
        help='Only refresh a specific park by ID'
    )

    args = parser.parse_args()

    refresher = ScheduleRefresher(
        force=args.force,
        park_id=args.park_id
    )
    refresher.run()


if __name__ == '__main__':
    main()
