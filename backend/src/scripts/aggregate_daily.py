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

Converted to SQLAlchemy 2.0 ORM (Phase 5).
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
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.aggregation_repository import AggregationLogRepository
from database.connection import get_db_session
from sqlalchemy import select, func, case, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.models import (
    Ride, RideStatusSnapshot, ParkActivitySnapshot,
    RideDailyStats, ParkDailyStats, RideStatusChange
)


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

        # Initialize log_id to None so exception handler can check if it was set
        log_id = None

        try:
            with get_db_session() as session:
                aggregation_repo = AggregationLogRepository(session)
                park_repo = ParkRepository(session)
                ride_repo = RideRepository(session)

                # Check if aggregation already completed for this date
                if self._check_already_aggregated(aggregation_repo):
                    logger.warning(f"Daily aggregation already completed for {self.target_date}")
                    logger.info("Use --force to re-aggregate")
                    return

                # Start aggregation log
                log_id = self._start_aggregation_log(aggregation_repo)

                # Step 1: Aggregate ride statistics
                logger.info("Step 1: Aggregating ride statistics...")
                self._aggregate_rides(ride_repo, session)

                # Step 2: Aggregate park statistics
                logger.info("Step 2: Aggregating park statistics...")
                self._aggregate_parks(park_repo, session)

                # Step 3: Mark aggregation as complete
                self._complete_aggregation_log(log_id, aggregation_repo)

            # Step 4: Print summary
            self._print_summary()

            logger.info("=" * 60)
            logger.info("DAILY AGGREGATION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during aggregation: {e}", exc_info=True)
            # Only try to mark as failed if we successfully created a log entry
            if log_id is not None:
                try:
                    with get_db_session() as session:
                        aggregation_repo = AggregationLogRepository(session)
                        self._fail_aggregation_log(log_id, str(e), aggregation_repo)
                except Exception as log_error:
                    logger.error(f"Failed to update aggregation log: {log_error}")
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

    def _aggregate_rides(self, ride_repo: RideRepository, session):
        """
        Aggregate statistics for all rides.

        Args:
            ride_repo: Ride repository
            session: Database session
        """
        try:
            # Get all active rides
            rides = ride_repo.get_all_active()

            for ride in rides:
                try:
                    self._aggregate_ride(session, ride)
                    self.stats['rides_processed'] += 1
                except Exception as e:
                    logger.error(f"Error aggregating ride {ride.name}: {e}")
                    self.stats['errors'] += 1

            logger.info(f"  ✓ Aggregated {self.stats['rides_processed']} rides")

        except Exception as e:
            logger.error(f"Failed to aggregate rides: {e}")
            raise

    def _aggregate_ride(self, session, ride):
        """
        Aggregate statistics for a single ride.

        Args:
            session: Database session
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

        # Build the aggregation query
        rss = RideStatusSnapshot.__table__.alias('rss')
        r = Ride.__table__.alias('r')
        pas = ParkActivitySnapshot.__table__.alias('pas')

        # Calculate if ride operated (for downtime filter)
        ride_operated = func.sum(
            case(
                (rss.c.computed_is_open == True, 1),
                else_=0
            )
        ) > 0

        # UPTIME: Minutes the ride was open during park operating hours
        uptime_minutes = func.coalesce(
            func.sum(
                case(
                    (and_(pas.c.park_appears_open == True, rss.c.computed_is_open == True), SNAPSHOT_INTERVAL_MINUTES),
                    else_=0
                )
            ),
            0
        )

        # DOWNTIME: Only count if BOTH conditions are met:
        #   1. Park was operating (park_appears_open = 1)
        #   2. Ride operated at least once today (filters out scheduled maintenance)
        # This prevents "never opened" rides from counting as downtime.
        #
        # For ThemeParks.wiki parks (status is NOT NULL):
        #   Only status='DOWN' counts as downtime (not CLOSED/REFURBISHMENT)
        # For Queue-Times parks (status IS NULL):
        #   Falls back to NOT computed_is_open
        downtime_minutes = case(
            (
                ride_operated,
                func.coalesce(
                    func.sum(
                        case(
                            (
                                and_(
                                    pas.c.park_appears_open == True,
                                    (
                                        (and_(rss.c.status != None, rss.c.status == 'DOWN')) |
                                        (and_(rss.c.status == None, rss.c.computed_is_open == False))
                                    )
                                ),
                                SNAPSHOT_INTERVAL_MINUTES
                            ),
                            else_=0
                        )
                    ),
                    0
                )
            ),
            else_=0  # Ride never opened = scheduled maintenance, not downtime
        )

        # UPTIME PERCENTAGE: Based on park operating hours only
        # Returns 0 if ride never operated (maintenance) or park was closed
        operating_hours_sum = func.sum(
            case(
                (pas.c.park_appears_open == True, SNAPSHOT_INTERVAL_MINUTES),
                else_=0
            )
        )

        uptime_percentage = case(
            (
                and_(
                    operating_hours_sum > 0,
                    ride_operated
                ),
                func.round(
                    100.0 * func.sum(
                        case(
                            (and_(pas.c.park_appears_open == True, rss.c.computed_is_open == True), SNAPSHOT_INTERVAL_MINUTES),
                            else_=0
                        )
                    ) / operating_hours_sum,
                    2
                )
            ),
            else_=0
        )

        # OPERATING HOURS: Time when park was open (regardless of ride status)
        operating_hours_minutes = func.coalesce(
            func.sum(
                case(
                    (pas.c.park_appears_open == True, SNAPSHOT_INTERVAL_MINUTES),
                    else_=0
                )
            ),
            0
        )

        # Wait time statistics (only when ride was actually open)
        avg_wait_time = func.round(
            func.avg(
                case(
                    (and_(rss.c.wait_time != None, rss.c.computed_is_open == True), rss.c.wait_time),
                    else_=None
                )
            ),
            2
        )

        min_wait_time = func.min(
            case(
                (and_(rss.c.wait_time != None, rss.c.computed_is_open == True), rss.c.wait_time),
                else_=None
            )
        )

        max_wait_time = func.max(
            case(
                (and_(rss.c.wait_time != None, rss.c.computed_is_open == True), rss.c.wait_time),
                else_=None
            )
        )

        peak_wait_time = func.max(
            case(
                (rss.c.wait_time != None, rss.c.wait_time),
                else_=None
            )
        )

        # Status changes from ride_status_changes table
        # Uses UTC range for Pacific day instead of DATE() to fix timezone bug
        status_changes_subq = (
            select(func.count())
            .select_from(RideStatusChange.__table__)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.changed_at >= self.day_start_utc,
                    RideStatusChange.changed_at < self.day_end_utc
                )
            )
            .scalar_subquery()
        )

        longest_downtime_subq = (
            select(func.max(RideStatusChange.duration_in_previous_status))
            .select_from(RideStatusChange.__table__)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.changed_at >= self.day_start_utc,
                    RideStatusChange.changed_at < self.day_end_utc,
                    RideStatusChange.new_status == True
                )
            )
            .scalar_subquery()
        )

        # Main aggregation query
        agg_query = (
            select(
                uptime_minutes.label('uptime_minutes'),
                downtime_minutes.label('downtime_minutes'),
                uptime_percentage.label('uptime_percentage'),
                operating_hours_minutes.label('operating_hours_minutes'),
                avg_wait_time.label('avg_wait_time'),
                min_wait_time.label('min_wait_time'),
                max_wait_time.label('max_wait_time'),
                peak_wait_time.label('peak_wait_time'),
                status_changes_subq.label('status_changes'),
                longest_downtime_subq.label('longest_downtime')
            )
            .select_from(rss)
            .join(r, rss.c.ride_id == r.c.ride_id)
            .join(
                pas,
                and_(
                    r.c.park_id == pas.c.park_id,
                    pas.c.recorded_at == rss.c.recorded_at
                )
            )
            .where(
                and_(
                    rss.c.ride_id == ride_id,
                    rss.c.recorded_at >= self.day_start_utc,
                    rss.c.recorded_at < self.day_end_utc
                )
            )
        )

        result = session.execute(agg_query).first()

        if result is None:
            # No snapshots for this ride on this date
            return

        # Insert or update using MySQL INSERT ... ON DUPLICATE KEY UPDATE
        stmt = mysql_insert(RideDailyStats).values(
            ride_id=ride_id,
            stat_date=self.target_date,
            uptime_minutes=int(result.uptime_minutes or 0),
            downtime_minutes=int(result.downtime_minutes or 0),
            uptime_percentage=float(result.uptime_percentage or 0),
            operating_hours_minutes=int(result.operating_hours_minutes or 0),
            avg_wait_time=float(result.avg_wait_time) if result.avg_wait_time is not None else None,
            min_wait_time=result.min_wait_time,
            max_wait_time=result.max_wait_time,
            peak_wait_time=result.peak_wait_time,
            status_changes=int(result.status_changes or 0),
            longest_downtime_minutes=result.longest_downtime,
            created_at=datetime.now()
        )

        stmt = stmt.on_duplicate_key_update(
            uptime_minutes=stmt.inserted.uptime_minutes,
            downtime_minutes=stmt.inserted.downtime_minutes,
            uptime_percentage=stmt.inserted.uptime_percentage,
            operating_hours_minutes=stmt.inserted.operating_hours_minutes,
            avg_wait_time=stmt.inserted.avg_wait_time,
            min_wait_time=stmt.inserted.min_wait_time,
            max_wait_time=stmt.inserted.max_wait_time,
            peak_wait_time=stmt.inserted.peak_wait_time,
            status_changes=stmt.inserted.status_changes,
            longest_downtime_minutes=stmt.inserted.longest_downtime_minutes
        )

        session.execute(stmt)

    def _aggregate_parks(self, park_repo: ParkRepository, session):
        """
        Aggregate statistics for all parks.

        Args:
            park_repo: Park repository
            session: Database session
        """
        try:
            # Get all active parks
            parks = park_repo.get_all_active()

            for park in parks:
                try:
                    self._aggregate_park(session, park)
                    self.stats['parks_processed'] += 1
                except Exception as e:
                    logger.error(f"Error aggregating park {park.name}: {e}")
                    self.stats['errors'] += 1

            logger.info(f"  ✓ Aggregated {self.stats['parks_processed']} parks")

        except Exception as e:
            logger.error(f"Failed to aggregate parks: {e}")
            raise

    def _aggregate_park(self, session, park):
        """
        Aggregate statistics for a single park.

        Args:
            session: Database session
            park: Park model object
        """
        park_id = park.park_id

        # Calculate park-wide statistics by rolling up ride statistics
        # First check if park has any ride stats for this date
        rds = RideDailyStats.__table__.alias('rds')
        r = Ride.__table__.alias('r')

        check_query = (
            select(func.count())
            .select_from(rds)
            .join(r, rds.c.ride_id == r.c.ride_id)
            .where(
                and_(
                    r.c.park_id == park_id,
                    rds.c.stat_date == self.target_date
                )
            )
        )

        ride_count = session.execute(check_query).scalar()
        if ride_count is None or ride_count == 0:
            # No ride data for this park on this date, skip aggregation
            return

        # Aggregation query
        # Join with ride_classifications for tier weights (shame_score calculation)
        from src.models import RideClassification
        rc = RideClassification.__table__.alias('rc')

        # Calculate weighted downtime and effective park weight for shame_score
        # shame_score = (weighted_downtime / effective_park_weight) * 10
        # - weighted_downtime = SUM(downtime_hours * tier_weight)
        # - effective_park_weight = SUM(tier_weight) for rides that operated (had any uptime)
        weighted_downtime = func.sum(
            (rds.c.downtime_minutes / 60.0) * func.coalesce(rc.c.tier_weight, 2)
        )
        effective_park_weight = func.sum(
            case(
                # Only count rides that operated (uptime_percentage > 0 or had any snapshots)
                (rds.c.uptime_percentage > 0, func.coalesce(rc.c.tier_weight, 2)),
                else_=0
            )
        )
        shame_score_expr = case(
            (effective_park_weight > 0,
             func.round((weighted_downtime / effective_park_weight) * 10, 2)),
            else_=0
        ).label('shame_score')

        agg_query = (
            select(
                func.count().label('total_rides'),
                func.coalesce(func.round(func.avg(rds.c.uptime_percentage), 2), 0).label('avg_uptime'),
                func.coalesce(func.round(func.sum(rds.c.downtime_minutes) / 60.0, 2), 0).label('total_downtime_hours'),
                func.coalesce(
                    func.sum(
                        case(
                            (rds.c.downtime_minutes > 0, 1),
                            else_=0
                        )
                    ),
                    0
                ).label('rides_with_downtime'),
                func.coalesce(func.round(func.avg(rds.c.avg_wait_time), 2), 0).label('avg_wait_time'),
                func.coalesce(func.max(rds.c.peak_wait_time), 0).label('peak_wait_time'),
                func.coalesce(func.avg(rds.c.operating_hours_minutes), 0).label('operating_hours_minutes'),
                shame_score_expr
            )
            .select_from(rds)
            .join(r, rds.c.ride_id == r.c.ride_id)
            .outerjoin(rc, rds.c.ride_id == rc.c.ride_id)  # LEFT JOIN for tier weights
            .where(
                and_(
                    r.c.park_id == park_id,
                    rds.c.stat_date == self.target_date
                )
            )
        )

        result = session.execute(agg_query).first()

        if result is None:
            return

        # Insert or update using MySQL INSERT ... ON DUPLICATE KEY UPDATE
        stmt = mysql_insert(ParkDailyStats).values(
            park_id=park_id,
            stat_date=self.target_date,
            total_rides_tracked=int(result.total_rides),
            avg_uptime_percentage=float(result.avg_uptime),
            total_downtime_hours=float(result.total_downtime_hours),
            rides_with_downtime=int(result.rides_with_downtime),
            avg_wait_time=float(result.avg_wait_time),
            peak_wait_time=int(result.peak_wait_time),
            operating_hours_minutes=int(result.operating_hours_minutes),
            shame_score=float(result.shame_score or 0),
            created_at=datetime.now()
        )

        stmt = stmt.on_duplicate_key_update(
            total_rides_tracked=stmt.inserted.total_rides_tracked,
            avg_uptime_percentage=stmt.inserted.avg_uptime_percentage,
            total_downtime_hours=stmt.inserted.total_downtime_hours,
            rides_with_downtime=stmt.inserted.rides_with_downtime,
            avg_wait_time=stmt.inserted.avg_wait_time,
            peak_wait_time=stmt.inserted.peak_wait_time,
            operating_hours_minutes=stmt.inserted.operating_hours_minutes,
            shame_score=stmt.inserted.shame_score
        )

        session.execute(stmt)

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
