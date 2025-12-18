#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Daily Aggregation Script
Calculates daily statistics from raw snapshots and stores in aggregate tables.

This script should be run once per day, typically at midnight or early morning.

Usage:
    python -m scripts.aggregate_daily [--date YYYY-MM-DD]

Options:
    --date    Specific date to aggregate (default: yesterday in Pacific Time)

Cron example (daily at 5 AM UTC = 1 AM Pacific, after PT day ends):
    0 5 * * * cd /path/to/backend && python -m scripts.aggregate_daily
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.aggregation_repository import AggregationLogRepository
from database.connection import get_db_connection
from sqlalchemy import text


class DailyAggregator:
    """
    Aggregates daily statistics from raw snapshot data.
    """

    def __init__(self, target_date: Optional[date] = None):
        """
        Initialize the aggregator.

        Args:
            target_date: Date to aggregate (default: yesterday)
        """
        # Use Pacific Time for US parks - aggregates "yesterday" in Pacific timezone
        self.target_date = target_date or (get_today_pacific() - timedelta(days=1))

        # Calculate UTC range for the Pacific date
        # Pacific day 2025-12-17 = UTC 2025-12-17 08:00 to 2025-12-18 08:00 (PST)
        # This fixes the timezone bug where DATE(recorded_at) compared UTC to Pacific
        self.day_start_utc, self.day_end_utc = get_pacific_day_range_utc(self.target_date)

        self.stats = {
            'parks_processed': 0,
            'rides_processed': 0,
            'errors': 0
        }

    def run(self):
        """Main execution method."""
        logger.info("=" * 60)
        logger.info(f"DAILY AGGREGATION - {self.target_date}")
        logger.info("=" * 60)

        try:
            with get_db_connection() as conn:
                aggregation_repo = AggregationLogRepository(conn)
                park_repo = ParkRepository(conn)
                ride_repo = RideRepository(conn)

                # Check if aggregation already completed for this date
                if self._check_already_aggregated(aggregation_repo):
                    logger.warning(f"Daily aggregation already completed for {self.target_date}")
                    logger.info("Use --force to re-aggregate")
                    return

                # Start aggregation log
                log_id = self._start_aggregation_log(aggregation_repo)

                # Step 1: Aggregate ride statistics
                logger.info("Step 1: Aggregating ride statistics...")
                self._aggregate_rides(ride_repo)

                # Step 2: Aggregate park statistics
                logger.info("Step 2: Aggregating park statistics...")
                self._aggregate_parks(park_repo)

                # Step 3: Mark aggregation as complete
                self._complete_aggregation_log(log_id, aggregation_repo)

            # Step 4: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("DAILY AGGREGATION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during aggregation: {e}", exc_info=True)
            with get_db_connection() as conn:
                aggregation_repo = AggregationLogRepository(conn)
                self._fail_aggregation_log(log_id, str(e), aggregation_repo)
            sys.exit(1)

    def _check_already_aggregated(self, aggregation_repo: AggregationLogRepository) -> bool:
        """
        Check if aggregation already completed for target date.

        Args:
            aggregation_repo: Aggregation log repository

        Returns:
            True if already aggregated, False otherwise
        """
        try:
            log = aggregation_repo.get_by_date_and_type(self.target_date, 'daily')
            return log is not None and log.get('status') == 'success'
        except:
            return False

    def _start_aggregation_log(self, aggregation_repo: AggregationLogRepository) -> int:
        """
        Create aggregation log entry.

        Args:
            aggregation_repo: Aggregation log repository

        Returns:
            log_id
        """
        try:
            log_record = {
                'aggregation_date': self.target_date,
                'aggregation_type': 'daily',
                'started_at': datetime.now(),
                'status': 'running',
                'parks_processed': 0,
                'rides_processed': 0
            }

            return aggregation_repo.insert(log_record)

        except Exception as e:
            logger.error(f"Failed to create aggregation log: {e}")
            raise

    def _aggregate_rides(self, ride_repo: RideRepository):
        """
        Aggregate statistics for all rides.

        Args:
            ride_repo: Ride repository
        """
        try:
            with get_db_connection() as conn:
                # Get all active rides
                rides = ride_repo.get_all_active()

                for ride in rides:
                    try:
                        self._aggregate_ride(conn, ride)
                        self.stats['rides_processed'] += 1
                    except Exception as e:
                        logger.error(f"Error aggregating ride {ride.name}: {e}")
                        self.stats['errors'] += 1

                logger.info(f"  ✓ Aggregated {self.stats['rides_processed']} rides")

        except Exception as e:
            logger.error(f"Failed to aggregate rides: {e}")
            raise

    def _aggregate_ride(self, conn, ride):
        """
        Aggregate statistics for a single ride.

        Args:
            conn: Database connection
            ride: Ride model object

        Downtime Calculation Logic:
        ==========================
        The "Hall of Shame" tracks unexpected ride failures, NOT scheduled closures.
        We apply two filters to ensure accurate downtime tracking:

        1. PARK OPERATING FILTER: Only count time when park_appears_open = 1
           - Prevents closed parks (e.g., water parks in winter) from showing
             false downtime when the entire park is not operating.
           - Example: Volcano Bay closed in November → 0 downtime (not "all rides down")

        2. RIDE OPERATED FILTER: Only count downtime if the ride operated at least
           once during the day (had at least one computed_is_open = 1 snapshot).
           - Prevents scheduled maintenance from being counted as downtime.
           - If a ride never opens all day, it's intentionally closed, not "broken."
           - Example: Knoebels running Christmas event with 2 of 64 rides →
             only those 2 rides can have downtime; the other 62 are maintenance.

        Scenarios:
        - Ride never opens (maintenance)     → 0 downtime ✓
        - Ride runs all day, breaks at 3pm   → counts 3pm-close as downtime ✓
        - Ride opens late, runs rest of day  → 0 downtime (late start, not breakdown) ✓
        - Park closed all day                → 0 downtime ✓
        - Ride breaks down 3 times           → counts all 3 breakdown periods ✓

        3. RICH STATUS FILTER (ThemeParks.wiki parks only):
           With the `status` column populated, we can distinguish:
           - OPERATING: Ride is running → counts as uptime
           - DOWN: Unscheduled breakdown → counts as downtime
           - CLOSED: Scheduled closure → NOT downtime (excluded)
           - REFURBISHMENT: Extended maintenance → NOT downtime (excluded)

           For parks using Queue-Times (status is NULL), falls back to computed_is_open.
        """
        ride_id = ride.ride_id
        park_id = ride.park_id

        result = conn.execute(text("""
            INSERT INTO ride_daily_stats (
                ride_id,
                stat_date,
                uptime_minutes,
                downtime_minutes,
                uptime_percentage,
                operating_hours_minutes,
                avg_wait_time,
                min_wait_time,
                max_wait_time,
                peak_wait_time,
                status_changes,
                longest_downtime_minutes,
                created_at
            )
            SELECT
                :ride_id,
                :stat_date,

                -- UPTIME: Minutes the ride was open during park operating hours
                COALESCE(SUM(CASE WHEN pas.park_appears_open = 1 AND rss.computed_is_open THEN 10 ELSE 0 END), 0) as uptime_minutes,

                -- DOWNTIME: Only count if BOTH conditions are met:
                --   1. Park was operating (park_appears_open = 1)
                --   2. Ride operated at least once today (filters out scheduled maintenance)
                -- This prevents "never opened" rides from counting as downtime.
                --
                -- For ThemeParks.wiki parks (status is NOT NULL):
                --   Only status='DOWN' counts as downtime (not CLOSED/REFURBISHMENT)
                -- For Queue-Times parks (status IS NULL):
                --   Falls back to NOT computed_is_open
                CASE
                    WHEN SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END) > 0
                    THEN COALESCE(SUM(
                        CASE
                            WHEN pas.park_appears_open = 1 AND (
                                (rss.status IS NOT NULL AND rss.status = 'DOWN') OR
                                (rss.status IS NULL AND NOT rss.computed_is_open)
                            ) THEN 10
                            ELSE 0
                        END
                    ), 0)
                    ELSE 0  -- Ride never opened = scheduled maintenance, not downtime
                END as downtime_minutes,

                -- UPTIME PERCENTAGE: Based on park operating hours only
                -- Returns 0 if ride never operated (maintenance) or park was closed
                CASE
                    WHEN SUM(CASE WHEN pas.park_appears_open = 1 THEN 10 ELSE 0 END) > 0
                         AND SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END) > 0
                    THEN ROUND(100.0 * SUM(CASE WHEN pas.park_appears_open = 1 AND rss.computed_is_open THEN 10 ELSE 0 END)
                              / SUM(CASE WHEN pas.park_appears_open = 1 THEN 10 ELSE 0 END), 2)
                    ELSE 0
                END as uptime_percentage,

                -- OPERATING HOURS: Time when park was open (regardless of ride status)
                COALESCE(SUM(CASE WHEN pas.park_appears_open = 1 THEN 10 ELSE 0 END), 0) as operating_hours_minutes,

                -- Wait time statistics (only when ride was actually open)
                ROUND(AVG(CASE WHEN rss.wait_time IS NOT NULL AND rss.computed_is_open THEN rss.wait_time END), 2) as avg_wait_time,
                MIN(CASE WHEN rss.wait_time IS NOT NULL AND rss.computed_is_open THEN rss.wait_time END) as min_wait_time,
                MAX(CASE WHEN rss.wait_time IS NOT NULL AND rss.computed_is_open THEN rss.wait_time END) as max_wait_time,
                MAX(CASE WHEN rss.wait_time IS NOT NULL THEN rss.wait_time END) as peak_wait_time,

                -- Status changes from ride_status_changes table
                -- Uses UTC range for Pacific day instead of DATE() to fix timezone bug
                (SELECT COUNT(*) FROM ride_status_changes WHERE ride_id = :ride_id AND changed_at >= :day_start_utc AND changed_at < :day_end_utc) as status_changes,
                (SELECT MAX(duration_in_previous_status) FROM ride_status_changes WHERE ride_id = :ride_id AND changed_at >= :day_start_utc AND changed_at < :day_end_utc AND new_status = 1) as longest_downtime,
                NOW()
            FROM ride_status_snapshots rss
            JOIN rides r ON rss.ride_id = r.ride_id
            JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.ride_id = :ride_id
              AND rss.recorded_at >= :day_start_utc
              AND rss.recorded_at < :day_end_utc
            ON DUPLICATE KEY UPDATE
                uptime_minutes = VALUES(uptime_minutes),
                downtime_minutes = VALUES(downtime_minutes),
                uptime_percentage = VALUES(uptime_percentage),
                operating_hours_minutes = VALUES(operating_hours_minutes),
                avg_wait_time = VALUES(avg_wait_time),
                min_wait_time = VALUES(min_wait_time),
                max_wait_time = VALUES(max_wait_time),
                peak_wait_time = VALUES(peak_wait_time),
                status_changes = VALUES(status_changes),
                longest_downtime_minutes = VALUES(longest_downtime_minutes)
        """), {
            'ride_id': ride_id,
            'stat_date': self.target_date,
            'day_start_utc': self.day_start_utc,
            'day_end_utc': self.day_end_utc
        })

    def _aggregate_parks(self, park_repo: ParkRepository):
        """
        Aggregate statistics for all parks.

        Args:
            park_repo: Park repository
        """
        try:
            with get_db_connection() as conn:
                # Get all active parks
                parks = park_repo.get_all_active()

                for park in parks:
                    try:
                        self._aggregate_park(conn, park)
                        self.stats['parks_processed'] += 1
                    except Exception as e:
                        logger.error(f"Error aggregating park {park.name}: {e}")
                        self.stats['errors'] += 1

                logger.info(f"  ✓ Aggregated {self.stats['parks_processed']} parks")

        except Exception as e:
            logger.error(f"Failed to aggregate parks: {e}")
            raise

    def _aggregate_park(self, conn, park):
        """
        Aggregate statistics for a single park.

        Args:
            conn: Database connection
            park: Park model object
        """
        park_id = park.park_id

        # Calculate park-wide statistics by rolling up ride statistics
        # First check if park has any ride stats for this date
        check_result = conn.execute(text("""
            SELECT COUNT(*) as ride_count
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            WHERE r.park_id = :park_id AND rds.stat_date = :stat_date
        """), {'park_id': park_id, 'stat_date': self.target_date})

        row = check_result.fetchone()
        if row is None or row[0] == 0:
            # No ride data for this park on this date, skip aggregation
            return

        result = conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id,
                stat_date,
                total_rides_tracked,
                avg_uptime_percentage,
                total_downtime_hours,
                rides_with_downtime,
                avg_wait_time,
                peak_wait_time,
                operating_hours_minutes,
                created_at
            )
            SELECT
                :park_id,
                :stat_date,
                COUNT(*) as total_rides,
                COALESCE(ROUND(AVG(uptime_percentage), 2), 0) as avg_uptime,
                COALESCE(ROUND(SUM(downtime_minutes) / 60.0, 2), 0) as total_downtime_hours,
                COALESCE(SUM(CASE WHEN downtime_minutes > 0 THEN 1 ELSE 0 END), 0) as rides_with_downtime,
                COALESCE(ROUND(AVG(avg_wait_time), 2), 0) as avg_wait_time,
                COALESCE(MAX(peak_wait_time), 0) as peak_wait_time,
                COALESCE(AVG(operating_hours_minutes), 0) as operating_hours_minutes,
                NOW()
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            WHERE r.park_id = :park_id
              AND rds.stat_date = :stat_date
            ON DUPLICATE KEY UPDATE
                total_rides_tracked = VALUES(total_rides_tracked),
                avg_uptime_percentage = VALUES(avg_uptime_percentage),
                total_downtime_hours = VALUES(total_downtime_hours),
                rides_with_downtime = VALUES(rides_with_downtime),
                avg_wait_time = VALUES(avg_wait_time),
                peak_wait_time = VALUES(peak_wait_time),
                operating_hours_minutes = VALUES(operating_hours_minutes)
        """), {
            'park_id': park_id,
            'stat_date': self.target_date
        })

    def _complete_aggregation_log(self, log_id: int, aggregation_repo: AggregationLogRepository):
        """
        Mark aggregation as successfully completed.

        Args:
            log_id: Aggregation log ID
            aggregation_repo: Aggregation log repository
        """
        try:
            aggregation_repo.update({
                'log_id': log_id,
                'completed_at': datetime.now(),
                'status': 'success',
                'parks_processed': self.stats['parks_processed'],
                'rides_processed': self.stats['rides_processed']
            })

        except Exception as e:
            logger.error(f"Failed to complete aggregation log: {e}")

    def _fail_aggregation_log(self, log_id: int, error_message: str, aggregation_repo: AggregationLogRepository):
        """
        Mark aggregation as failed.

        Args:
            log_id: Aggregation log ID
            error_message: Error details
            aggregation_repo: Aggregation log repository
        """
        try:
            aggregation_repo.update({
                'log_id': log_id,
                'status': 'failed',
                'error_message': error_message
            })

        except Exception as e:
            logger.error(f"Failed to update aggregation log: {e}")

    def _print_summary(self):
        """Print aggregation summary statistics."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("AGGREGATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Date:             {self.target_date}")
        logger.info(f"Parks processed:  {self.stats['parks_processed']}")
        logger.info(f"Rides processed:  {self.stats['rides_processed']}")
        logger.info(f"Errors:           {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Aggregate daily statistics from raw snapshots'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date to aggregate (YYYY-MM-DD format, default: yesterday)'
    )

    args = parser.parse_args()

    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)

    aggregator = DailyAggregator(target_date=target_date)
    aggregator.run()


if __name__ == '__main__':
    main()
