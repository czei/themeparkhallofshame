#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Daily Aggregation Script
Calculates daily statistics from raw snapshots and stores in aggregate tables.

This script should be run once per day, typically at midnight or early morning.

Usage:
    python -m scripts.aggregate_daily [--date YYYY-MM-DD]

Options:
    --date    Specific date to aggregate (default: yesterday)

Cron example (daily at 1 AM):
    0 1 * * * cd /path/to/backend && python -m scripts.aggregate_daily
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
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
        self.target_date = target_date or (date.today() - timedelta(days=1))
        self.park_repo = ParkRepository()
        self.ride_repo = RideRepository()
        self.aggregation_repo = AggregationLogRepository()

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
            # Check if aggregation already completed for this date
            if self._check_already_aggregated():
                logger.warning(f"Daily aggregation already completed for {self.target_date}")
                logger.info("Use --force to re-aggregate")
                return

            # Start aggregation log
            log_id = self._start_aggregation_log()

            # Step 1: Aggregate ride statistics
            logger.info("Step 1: Aggregating ride statistics...")
            self._aggregate_rides()

            # Step 2: Aggregate park statistics
            logger.info("Step 2: Aggregating park statistics...")
            self._aggregate_parks()

            # Step 3: Mark aggregation as complete
            self._complete_aggregation_log(log_id)

            # Step 4: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("DAILY AGGREGATION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during aggregation: {e}", exc_info=True)
            self._fail_aggregation_log(log_id, str(e))
            sys.exit(1)

    def _check_already_aggregated(self) -> bool:
        """
        Check if aggregation already completed for target date.

        Returns:
            True if already aggregated, False otherwise
        """
        try:
            log = self.aggregation_repo.get_by_date_and_type(self.target_date, 'daily')
            return log is not None and log.get('status') == 'success'
        except:
            return False

    def _start_aggregation_log(self) -> int:
        """
        Create aggregation log entry.

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

            return self.aggregation_repo.insert(log_record)

        except Exception as e:
            logger.error(f"Failed to create aggregation log: {e}")
            raise

    def _aggregate_rides(self):
        """Aggregate statistics for all rides."""
        try:
            with get_db_connection() as conn:
                # Get all active rides
                rides = self.ride_repo.get_all_active()

                for ride in rides:
                    try:
                        self._aggregate_ride(conn, ride)
                        self.stats['rides_processed'] += 1
                    except Exception as e:
                        logger.error(f"Error aggregating ride {ride['name']}: {e}")
                        self.stats['errors'] += 1

                logger.info(f"  ✓ Aggregated {self.stats['rides_processed']} rides")

        except Exception as e:
            logger.error(f"Failed to aggregate rides: {e}")
            raise

    def _aggregate_ride(self, conn, ride: Dict):
        """
        Aggregate statistics for a single ride.

        Args:
            conn: Database connection
            ride: Ride record
        """
        ride_id = ride['ride_id']
        park_id = ride['park_id']

        # Calculate ride statistics for the day
        # This is a simplified version - full implementation would:
        # 1. Detect park operating hours from park_activity_snapshots
        # 2. Calculate uptime/downtime during operating hours only
        # 3. Calculate wait time statistics
        # 4. Store in ride_daily_stats table

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
                COALESCE(SUM(CASE WHEN computed_is_open THEN 10 ELSE 0 END), 0) as uptime_minutes,
                COALESCE(SUM(CASE WHEN NOT computed_is_open THEN 10 ELSE 0 END), 0) as downtime_minutes,
                CASE
                    WHEN SUM(10) > 0 THEN
                        ROUND(100.0 * SUM(CASE WHEN computed_is_open THEN 10 ELSE 0 END) / SUM(10), 2)
                    ELSE 0
                END as uptime_percentage,
                COALESCE(SUM(10), 0) as operating_hours_minutes,
                ROUND(AVG(CASE WHEN wait_time IS NOT NULL AND computed_is_open THEN wait_time END), 2) as avg_wait_time,
                MIN(CASE WHEN wait_time IS NOT NULL AND computed_is_open THEN wait_time END) as min_wait_time,
                MAX(CASE WHEN wait_time IS NOT NULL AND computed_is_open THEN wait_time END) as max_wait_time,
                MAX(CASE WHEN wait_time IS NOT NULL THEN wait_time END) as peak_wait_time,
                (SELECT COUNT(*) FROM ride_status_changes WHERE ride_id = :ride_id AND DATE(changed_at) = :stat_date) as status_changes,
                (SELECT MAX(downtime_duration_minutes) FROM ride_status_changes WHERE ride_id = :ride_id AND DATE(changed_at) = :stat_date) as longest_downtime,
                NOW()
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
              AND DATE(recorded_at) = :stat_date
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
            'stat_date': self.target_date
        })

    def _aggregate_parks(self):
        """Aggregate statistics for all parks."""
        try:
            with get_db_connection() as conn:
                # Get all active parks
                parks = self.park_repo.get_all_active()

                for park in parks:
                    try:
                        self._aggregate_park(conn, park)
                        self.stats['parks_processed'] += 1
                    except Exception as e:
                        logger.error(f"Error aggregating park {park['name']}: {e}")
                        self.stats['errors'] += 1

                logger.info(f"  ✓ Aggregated {self.stats['parks_processed']} parks")

        except Exception as e:
            logger.error(f"Failed to aggregate parks: {e}")
            raise

    def _aggregate_park(self, conn, park: Dict):
        """
        Aggregate statistics for a single park.

        Args:
            conn: Database connection
            park: Park record
        """
        park_id = park['park_id']

        # Calculate park-wide statistics by rolling up ride statistics
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
                ROUND(AVG(uptime_percentage), 2) as avg_uptime,
                ROUND(SUM(downtime_minutes) / 60.0, 2) as total_downtime_hours,
                SUM(CASE WHEN downtime_minutes > 0 THEN 1 ELSE 0 END) as rides_with_downtime,
                ROUND(AVG(avg_wait_time), 2) as avg_wait_time,
                MAX(peak_wait_time) as peak_wait_time,
                AVG(operating_hours_minutes) as operating_hours_minutes,
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

    def _complete_aggregation_log(self, log_id: int):
        """
        Mark aggregation as successfully completed.

        Args:
            log_id: Aggregation log ID
        """
        try:
            self.aggregation_repo.update({
                'log_id': log_id,
                'completed_at': datetime.now(),
                'status': 'success',
                'parks_processed': self.stats['parks_processed'],
                'rides_processed': self.stats['rides_processed']
            })

        except Exception as e:
            logger.error(f"Failed to complete aggregation log: {e}")

    def _fail_aggregation_log(self, log_id: int, error_message: str):
        """
        Mark aggregation as failed.

        Args:
            log_id: Aggregation log ID
            error_message: Error details
        """
        try:
            self.aggregation_repo.update({
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
