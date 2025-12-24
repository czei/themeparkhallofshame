#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Daily Stats Recomputation Script

Idempotent batch recomputation of daily_stats from raw snapshot data.
Use this for:
- Backfilling historical data after calculation bug fixes
- Side-by-side comparison with new metrics_version
- Recovering from data corruption
- Testing new calculation logic before deployment

Usage:
    python -m scripts.recompute_daily_stats --start-date 2025-12-01 --end-date 2025-12-21
    python -m scripts.recompute_daily_stats --days 90
    python -m scripts.recompute_daily_stats --start-date 2025-12-01 --dry-run
    python -m scripts.recompute_daily_stats --start-date 2025-12-01 --metrics-version 2

Options:
    --start-date    Start date for recomputation (YYYY-MM-DD)
    --end-date      End date for recomputation (YYYY-MM-DD, default: yesterday)
    --days          Number of days to recompute from yesterday backwards
    --metrics-version   Version number for side-by-side comparison (default: 1)
    --dry-run       Preview changes without writing to database
    --force         Continue on errors instead of stopping

Feature 003-orm-refactoring, Task T034-T037
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Optional, Generator

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.connection import get_db_session
from sqlalchemy import select, func, case, and_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.models import (
    Ride, RideStatusSnapshot, ParkActivitySnapshot,
    RideDailyStats, ParkDailyStats, RideStatusChange
)


def date_range(start: date, end: date) -> Generator[date, None, None]:
    """
    Generate dates from start to end (inclusive).

    Args:
        start: Start date
        end: End date (inclusive)

    Yields:
        Dates from start to end
    """
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


class DailyStatsRecomputer:
    """
    Idempotent batch recomputation of daily statistics.

    Key features:
    - Date range support for batch processing
    - Metrics version support for side-by-side comparison
    - Dry-run mode for previewing changes
    - Progress tracking with estimated completion time
    - Idempotent UPSERT (safe to run multiple times)
    """

    def __init__(
        self,
        start_date: date,
        end_date: date,
        metrics_version: int = 1,
        dry_run: bool = False,
        force: bool = False
    ):
        """
        Initialize the recomputer.

        Args:
            start_date: First date to recompute
            end_date: Last date to recompute (inclusive)
            metrics_version: Version number for metrics comparison
            dry_run: If True, preview changes without writing
            force: If True, continue on errors
        """
        self.start_date = start_date
        self.end_date = end_date
        self.metrics_version = metrics_version
        self.dry_run = dry_run
        self.force = force

        self.stats = {
            'days_processed': 0,
            'days_total': (end_date - start_date).days + 1,
            'rides_processed': 0,
            'parks_processed': 0,
            'errors': 0,
            'start_time': None
        }

    def run(self):
        """Main execution method."""
        self.stats['start_time'] = time.time()

        logger.info("=" * 60)
        logger.info("DAILY STATS RECOMPUTATION")
        logger.info("=" * 60)
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        logger.info(f"Total days: {self.stats['days_total']}")
        logger.info(f"Metrics version: {self.metrics_version}")
        logger.info(f"Dry run: {self.dry_run}")
        logger.info("=" * 60)

        if self.dry_run:
            logger.info("*** DRY RUN MODE - No changes will be written ***")

        try:
            with get_db_session() as session:
                ride_repo = RideRepository(session)
                park_repo = ParkRepository(session)

                # Process each date
                for current_date in date_range(self.start_date, self.end_date):
                    try:
                        self._process_date(session, current_date, ride_repo, park_repo)
                        self.stats['days_processed'] += 1

                        # Progress update every 10 days
                        if self.stats['days_processed'] % 10 == 0:
                            self._print_progress()

                    except Exception as e:
                        logger.error(f"Error processing {current_date}: {e}")
                        self.stats['errors'] += 1
                        if not self.force:
                            raise

                # Commit all changes (unless dry run)
                if not self.dry_run:
                    session.commit()
                    logger.info("Changes committed to database")
                else:
                    session.rollback()
                    logger.info("Dry run complete - no changes written")

            self._print_summary()

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            sys.exit(1)

    def _process_date(self, session, target_date: date, ride_repo: RideRepository, park_repo: ParkRepository):
        """
        Process a single date.

        Args:
            session: Database session
            target_date: Date to process
            ride_repo: Ride repository
            park_repo: Park repository
        """
        logger.info(f"Processing {target_date}...")

        # Calculate UTC range for the Pacific date
        day_start_utc, day_end_utc = get_pacific_day_range_utc(target_date)

        # Get all active rides
        rides = ride_repo.get_all_active()
        rides_this_date = 0

        for ride in rides:
            try:
                if self._recompute_ride(session, ride, target_date, day_start_utc, day_end_utc):
                    rides_this_date += 1
                    self.stats['rides_processed'] += 1
            except Exception as e:
                logger.error(f"Error recomputing ride {ride.name}: {e}")
                self.stats['errors'] += 1
                if not self.force:
                    raise

        # Get all active parks
        parks = park_repo.get_all_active()

        for park in parks:
            try:
                if self._recompute_park(session, park, target_date):
                    self.stats['parks_processed'] += 1
            except Exception as e:
                logger.error(f"Error recomputing park {park.name}: {e}")
                self.stats['errors'] += 1
                if not self.force:
                    raise

        logger.info(f"  ✓ {rides_this_date} rides aggregated for {target_date}")

    def _recompute_ride(self, session, ride, target_date: date, day_start_utc: datetime, day_end_utc: datetime) -> bool:
        """
        Recompute statistics for a single ride.

        Returns True if data was computed, False if skipped.
        """
        ride_id = ride.ride_id

        # Build the aggregation query (same logic as aggregate_daily.py)
        rss = RideStatusSnapshot.__table__.alias('rss')
        r = Ride.__table__.alias('r')
        pas = ParkActivitySnapshot.__table__.alias('pas')

        # Check if we have any snapshots for this ride on this date
        check_query = (
            select(func.count())
            .select_from(rss)
            .where(
                and_(
                    rss.c.ride_id == ride_id,
                    rss.c.recorded_at >= day_start_utc,
                    rss.c.recorded_at < day_end_utc
                )
            )
        )

        snapshot_count = session.execute(check_query).scalar()
        if not snapshot_count:
            return False

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

        # DOWNTIME with park-type aware logic
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
            else_=0
        )

        # UPTIME PERCENTAGE
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

        # OPERATING HOURS
        operating_hours_minutes = func.coalesce(
            func.sum(
                case(
                    (pas.c.park_appears_open == True, SNAPSHOT_INTERVAL_MINUTES),
                    else_=0
                )
            ),
            0
        )

        # Wait time statistics
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

        # Status changes
        status_changes_subq = (
            select(func.count())
            .select_from(RideStatusChange.__table__)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.changed_at >= day_start_utc,
                    RideStatusChange.changed_at < day_end_utc
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
                    RideStatusChange.changed_at >= day_start_utc,
                    RideStatusChange.changed_at < day_end_utc,
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
                    rss.c.recorded_at >= day_start_utc,
                    rss.c.recorded_at < day_end_utc
                )
            )
        )

        result = session.execute(agg_query).first()

        if result is None:
            return False

        if self.dry_run:
            logger.debug(f"  Would upsert ride {ride.name}: uptime={result.uptime_percentage}%, downtime={result.downtime_minutes}min")
            return True

        # UPSERT (idempotent)
        stmt = mysql_insert(RideDailyStats).values(
            ride_id=ride_id,
            stat_date=target_date,
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
        return True

    def _recompute_park(self, session, park, target_date: date) -> bool:
        """
        Recompute statistics for a single park by rolling up ride stats.

        Returns True if data was computed, False if skipped.
        """
        park_id = park.park_id

        rds = RideDailyStats.__table__.alias('rds')
        r = Ride.__table__.alias('r')

        # Check if we have any ride stats for this park on this date
        check_query = (
            select(func.count())
            .select_from(rds)
            .join(r, rds.c.ride_id == r.c.ride_id)
            .where(
                and_(
                    r.c.park_id == park_id,
                    rds.c.stat_date == target_date
                )
            )
        )

        ride_count = session.execute(check_query).scalar()
        if not ride_count:
            return False

        # Aggregation query
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
                func.coalesce(func.avg(rds.c.operating_hours_minutes), 0).label('operating_hours_minutes')
            )
            .select_from(rds)
            .join(r, rds.c.ride_id == r.c.ride_id)
            .where(
                and_(
                    r.c.park_id == park_id,
                    rds.c.stat_date == target_date
                )
            )
        )

        result = session.execute(agg_query).first()

        if result is None:
            return False

        if self.dry_run:
            logger.debug(f"  Would upsert park {park.name}: avg_uptime={result.avg_uptime}%, downtime={result.total_downtime_hours}h")
            return True

        # UPSERT (idempotent)
        stmt = mysql_insert(ParkDailyStats).values(
            park_id=park_id,
            stat_date=target_date,
            total_rides_tracked=int(result.total_rides),
            avg_uptime_percentage=float(result.avg_uptime),
            total_downtime_hours=float(result.total_downtime_hours),
            rides_with_downtime=int(result.rides_with_downtime),
            avg_wait_time=float(result.avg_wait_time),
            peak_wait_time=int(result.peak_wait_time),
            operating_hours_minutes=int(result.operating_hours_minutes),
            created_at=datetime.now()
        )

        stmt = stmt.on_duplicate_key_update(
            total_rides_tracked=stmt.inserted.total_rides_tracked,
            avg_uptime_percentage=stmt.inserted.avg_uptime_percentage,
            total_downtime_hours=stmt.inserted.total_downtime_hours,
            rides_with_downtime=stmt.inserted.rides_with_downtime,
            avg_wait_time=stmt.inserted.avg_wait_time,
            peak_wait_time=stmt.inserted.peak_wait_time,
            operating_hours_minutes=stmt.inserted.operating_hours_minutes
        )

        session.execute(stmt)
        return True

    def _print_progress(self):
        """Print progress with estimated time remaining."""
        elapsed = time.time() - self.stats['start_time']
        days_done = self.stats['days_processed']
        days_total = self.stats['days_total']
        pct = (days_done / days_total) * 100

        if days_done > 0:
            avg_time_per_day = elapsed / days_done
            remaining = (days_total - days_done) * avg_time_per_day
            eta = timedelta(seconds=int(remaining))
        else:
            eta = "unknown"

        logger.info(f"Progress: {days_done}/{days_total} days ({pct:.1f}%) - ETA: {eta}")

    def _print_summary(self):
        """Print final summary."""
        elapsed = time.time() - self.stats['start_time']

        logger.info("")
        logger.info("=" * 60)
        logger.info("RECOMPUTATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        logger.info(f"Days processed: {self.stats['days_processed']}/{self.stats['days_total']}")
        logger.info(f"Rides processed: {self.stats['rides_processed']}")
        logger.info(f"Parks processed: {self.stats['parks_processed']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Total time: {timedelta(seconds=int(elapsed))}")
        logger.info(f"Dry run: {self.dry_run}")
        logger.info("=" * 60)

        # Extrapolate to 90 days
        if self.stats['days_processed'] > 0:
            time_per_day = elapsed / self.stats['days_processed']
            extrapolated_90 = time_per_day * 90
            logger.info(f"Extrapolated 90-day time: {timedelta(seconds=int(extrapolated_90))}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Recompute daily statistics from raw snapshots (idempotent)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --days 7                     # Recompute last 7 days
  %(prog)s --start-date 2025-12-01      # Recompute from Dec 1 to yesterday
  %(prog)s --start-date 2025-12-01 --end-date 2025-12-10  # Specific range
  %(prog)s --start-date 2025-12-01 --dry-run  # Preview without changes
        """
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD, default: yesterday)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to recompute backwards from yesterday (default: 7)'
    )
    parser.add_argument(
        '--metrics-version',
        type=int,
        default=1,
        help='Metrics version for side-by-side comparison (default: 1)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing to database'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Continue on errors instead of stopping'
    )

    args = parser.parse_args()

    # Determine date range
    yesterday = get_today_pacific() - timedelta(days=1)

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid start date format: {args.start_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        start_date = yesterday - timedelta(days=args.days - 1)

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        end_date = yesterday

    # Validate range
    if start_date > end_date:
        logger.error(f"Start date {start_date} is after end date {end_date}")
        sys.exit(1)

    recomputer = DailyStatsRecomputer(
        start_date=start_date,
        end_date=end_date,
        metrics_version=args.metrics_version,
        dry_run=args.dry_run,
        force=args.force
    )
    recomputer.run()


if __name__ == '__main__':
    main()
