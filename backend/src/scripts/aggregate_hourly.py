#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Hourly Aggregation Script

DEPRECATED (2025-12-24 - ORM Refactoring Feature 003):
=======================================================
This script is no longer used in production. Hourly metrics are now computed
on-the-fly via ORM queries from ride_status_snapshots table.

The cron job has been removed from deployment/config/crontab.prod.
The hourly_stats table has been dropped via migration 003.

This script is retained only for:
1. Historical reference
2. Potential backfill scenarios during migration rollback

DO NOT re-enable the cron job unless reverting the ORM refactoring.
See specs/003-orm-refactoring/tasks.md for migration context.

---
ORIGINAL DOCSTRING (for reference):

Pre-computes hourly statistics from raw snapshots for fast chart queries.

This script should be run at :05 past each hour to aggregate the previous completed hour.

Usage:
    python -m scripts.aggregate_hourly [--hour YYYY-MM-DD-HH]

Options:
    --hour    Specific hour to aggregate (default: previous completed hour in UTC)

Cron example (every hour at :05):
    5 * * * * cd /path/to/backend && python -m scripts.aggregate_hourly

Performance:
    - Aggregates ~12 snapshots per park per hour (5-min collection)
    - Stores to park_hourly_stats and ride_hourly_stats
    - Target: <10 seconds for 80 parks × 4200 rides
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple
import pytz

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from utils.sql_helpers import RideStatusSQL, ParkStatusSQL
from utils.metrics import SNAPSHOT_INTERVAL_MINUTES
from database.repositories.park_repository import ParkRepository
from database.repositories.ride_repository import RideRepository
from database.repositories.aggregation_repository import AggregationLogRepository
from database.connection import get_db_session
from sqlalchemy import text, select, func, distinct, and_, or_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from src.models import (
    Park, Ride,
    RideStatusSnapshot, ParkActivitySnapshot,
    ParkHourlyStats,
    AggregationLog, AggregationType, AggregationStatus
)


def get_pacific_day_range_utc(utc_dt: datetime) -> Tuple[datetime, datetime]:
    """
    For a given UTC datetime, find the start and end of the corresponding
    Pacific calendar day, returned in UTC.

    Args:
        utc_dt: UTC datetime

    Returns:
        Tuple of (day_start_utc, day_end_utc)
    """
    pacific_tz = pytz.timezone('America/Los_Angeles')
    # Convert the target UTC time to Pacific to find out what "day" it is
    target_pacific = utc_dt.astimezone(pacific_tz)
    # Get the start of that day in Pacific time (midnight)
    day_start_pacific = target_pacific.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Get the start of the next day
    day_end_pacific = day_start_pacific + timedelta(days=1)
    # Convert the day boundaries back to UTC
    day_start_utc = day_start_pacific.astimezone(pytz.utc).replace(tzinfo=None)
    day_end_utc = day_end_pacific.astimezone(pytz.utc).replace(tzinfo=None)
    return day_start_utc, day_end_utc


class HourlyAggregator:
    """
    Aggregates hourly statistics from raw snapshot data.
    Replicates DailyAggregator pattern but for hourly granularity.
    """

    def __init__(self, target_hour: Optional[datetime] = None):
        """
        Initialize the aggregator.

        Args:
            target_hour: Hour to aggregate (default: previous completed hour in UTC)
                        Must be on hour boundary (minutes=0, seconds=0)
        """
        if target_hour is None:
            # Default to previous completed hour
            now = datetime.utcnow()
            target_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        else:
            # Ensure target hour is on hour boundary
            target_hour = target_hour.replace(minute=0, second=0, microsecond=0)

        self.target_hour = target_hour
        self.hour_end = target_hour + timedelta(hours=1)

        self.stats = {
            'parks_processed': 0,
            'rides_processed': 0,
            'errors': 0
        }

    def run(self):
        """Main execution method."""
        logger.info("=" * 60)
        logger.info(f"HOURLY AGGREGATION - {self.target_hour.strftime('%Y-%m-%d %H:00 UTC')}")
        logger.info("=" * 60)

        log_id = None  # Initialize before try block for exception handler
        try:
            with get_db_session() as session:
                aggregation_repo = AggregationLogRepository(session)
                park_repo = ParkRepository(session)
                ride_repo = RideRepository(session)

                # Check if aggregation already completed for this hour
                if self._check_already_aggregated(aggregation_repo):
                    logger.warning(f"Hourly aggregation already completed for {self.target_hour}")
                    logger.info("Skipping (idempotent)")
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
            logger.info("HOURLY AGGREGATION - Complete ✓")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Fatal error during aggregation: {e}", exc_info=True)
            if log_id is not None:
                try:
                    with get_db_session() as session:
                        aggregation_repo = AggregationLogRepository(session)
                        self._fail_aggregation_log(log_id, str(e), aggregation_repo)
                except:
                    pass  # Best effort logging
            sys.exit(1)

    def _check_already_aggregated(self, aggregation_repo: AggregationLogRepository) -> bool:
        """
        Check if aggregation already completed for target hour.

        Args:
            aggregation_repo: Aggregation log repository

        Returns:
            True if already aggregated, False otherwise
        """
        try:
            # Check if park_hourly_stats has data for this hour
            # (More reliable than aggregation_log for hourly granularity)
            with get_db_session() as session:
                stmt = (
                    select(func.count())
                    .select_from(ParkHourlyStats)
                    .where(ParkHourlyStats.hour_start_utc == self.target_hour)
                )
                count = session.execute(stmt).scalar()
                return count > 0
        except:
            return False

    def _start_aggregation_log(self, aggregation_repo: AggregationLogRepository) -> int:
        """
        Create aggregation log entry for this specific hour.

        NOTE: For hourly aggregations, we create a NEW log entry for each hour
        to enable tracking of individual hourly job success/failure. Each hour
        gets a distinct row identified by its timestamp range.

        Args:
            aggregation_repo: Aggregation log repository

        Returns:
            log_id (newly created)
        """
        try:
            with get_db_session() as session:
                # Insert a new log entry for each hourly run
                # Migration 015 removed unique constraint to allow multiple hourly entries per day
                # This enables proper hourly progress monitoring in health checks
                stmt = mysql_insert(AggregationLog).values(
                    aggregation_date=self.target_hour.date(),
                    aggregation_type=AggregationType.HOURLY,
                    aggregated_until_ts=self.hour_end,
                    started_at=datetime.now(),
                    status=AggregationStatus.RUNNING,
                    parks_processed=0,
                    rides_processed=0
                )
                result = session.execute(stmt)
                session.flush()  # Ensure we can get lastrowid

                log_id = result.lastrowid
                logger.info(f"Aggregation log entry created: {log_id} for hour {self.target_hour.hour}:00 UTC")
                return log_id

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
            with get_db_session() as session:
                # Calculate Pacific day range for "operated today" CTE
                day_start_utc, day_end_utc = get_pacific_day_range_utc(self.target_hour)

                # Pre-calculate which rides operated today (fixes N+1 query problem)
                # This runs ONCE instead of once per ride
                #
                # PARK-TYPE AWARE LOGIC (from CLAUDE.md Rule 2):
                # - Disney/Universal: DOWN status is valid signal (they distinguish DOWN vs CLOSED)
                #   Fixes DINOSAUR bug where ride was DOWN before showing OPERATING
                # - Other parks: Require OPERATING status only (they use CLOSED for everything)
                #   Fixes Kennywood bug where rides showing only CLOSED were incorrectly counted
                stmt = (
                    select(distinct(RideStatusSnapshot.ride_id))
                    .select_from(RideStatusSnapshot)
                    .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
                    .join(Park, Ride.park_id == Park.park_id)
                    .join(
                        ParkActivitySnapshot,
                        and_(
                            Ride.park_id == ParkActivitySnapshot.park_id,
                            ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                        )
                    )
                    .where(
                        and_(
                            RideStatusSnapshot.recorded_at >= day_start_utc,
                            RideStatusSnapshot.recorded_at < day_end_utc,
                            or_(
                                ParkActivitySnapshot.park_appears_open.is_(True),
                                ParkActivitySnapshot.rides_open > 0
                            ),
                            or_(
                                # Standard: ride showed OPERATING
                                RideStatusSnapshot.status == 'OPERATING',
                                RideStatusSnapshot.computed_is_open.is_(True),
                                # Disney/Universal: DOWN is valid signal that ride attempted to operate
                                and_(
                                    RideStatusSnapshot.status == 'DOWN',
                                    or_(Park.is_disney.is_(True), Park.is_universal.is_(True))
                                )
                            )
                        )
                    )
                )
                operated_today_result = session.execute(stmt)
                operated_today_ride_ids = {row[0] for row in operated_today_result}
                logger.info(f"  Pre-calculated {len(operated_today_ride_ids)} rides that operated today")

                # Get all active rides
                rides = ride_repo.get_all_active()

                for ride in rides:
                    try:
                        self._aggregate_ride(session, ride, operated_today_ride_ids)
                        self.stats['rides_processed'] += 1
                    except Exception as e:
                        logger.error(f"Error aggregating ride {ride.name}: {e}")
                        self.stats['errors'] += 1

                logger.info(f"  ✓ Aggregated {self.stats['rides_processed']} rides")

        except Exception as e:
            logger.error(f"Failed to aggregate rides: {e}")
            raise

    def _aggregate_ride(self, session, ride, operated_today_ride_ids: set):
        """
        Aggregate statistics for a single ride for the target hour.

        Args:
            session: Database session
            ride: Ride model object
            operated_today_ride_ids: Set of ride IDs that operated anywhere during the Pacific calendar day

        Business Logic (from CLAUDE.md Rule 2 - HOURLY):
        ==================================================
        A ride has "operated" if it operated at ANY point during the Pacific calendar day.
        This ensures multi-hour outages persist across all hours after the ride operated.

        Example: Ride operates at 10:00am, goes down at 10:30am
        → Counts as down in 10am, 11am, 12pm, etc. hours (operated "today")

        Only rides that operated count toward downtime calculations.
        This prevents scheduled maintenance rides from showing false downtime.
        """
        ride_id = ride.ride_id
        park_id = ride.park_id

        # Pre-check: Skip rides with no snapshots during this hour
        # (avoids NULL ride_operated errors for rides that didn't operate)
        check_stmt = (
            select(RideStatusSnapshot.ride_id)
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= self.target_hour,
                    RideStatusSnapshot.recorded_at < self.hour_end
                )
            )
            .limit(1)
        )
        if session.execute(check_stmt).first() is None:
            # No snapshots for this ride during this hour - skip
            return

        # Use centralized SQL helpers for consistent business logic (SINGLE SOURCE OF TRUTH)
        is_down_sql = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open_sql = ParkStatusSQL.park_appears_open_filter("pas", with_fallback=True)

        # Complex INSERT ... SELECT with SQL helper injections must remain as text()
        # (Too complex for ORM: f-string injection from SQL helpers, INSERT ... ON DUPLICATE KEY UPDATE)
        session.execute(text(f"""
            INSERT INTO ride_hourly_stats (
                ride_id,
                park_id,
                hour_start_utc,
                avg_wait_time_minutes,
                operating_snapshots,
                down_snapshots,
                downtime_hours,
                uptime_percentage,
                snapshot_count,
                ride_operated,
                created_at
            )
            SELECT
                :ride_id,
                :park_id,
                :hour_start,

                -- Average wait time when ride was operating
                ROUND(AVG(CASE WHEN rss.computed_is_open AND rss.wait_time IS NOT NULL
                          THEN rss.wait_time END), 2) as avg_wait_time_minutes,

                -- Count snapshots where ride was operating
                SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END) as operating_snapshots,

                -- Count snapshots where ride was down (Rule 3: park-type aware)
                -- Uses RideStatusSQL.is_down() for consistency with canonical business rules
                SUM(CASE
                    WHEN {park_open_sql} AND ({is_down_sql})
                    THEN 1
                    ELSE 0
                END) as down_snapshots,

                -- Downtime hours: down_snapshots × SNAPSHOT_INTERVAL_MINUTES / 60
                -- Uses RideStatusSQL.is_down() for consistency with canonical business rules
                ROUND(SUM(CASE
                    WHEN {park_open_sql} AND ({is_down_sql})
                    THEN {SNAPSHOT_INTERVAL_MINUTES} / 60.0
                    ELSE 0
                END), 2) as downtime_hours,

                -- Uptime percentage (0-100)
                CASE
                    WHEN COUNT(*) > 0
                    THEN ROUND(100.0 * SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END) / COUNT(*), 2)
                    ELSE 0
                END as uptime_percentage,

                -- Total snapshots in hour
                COUNT(*) as snapshot_count,

                -- NEW: Did ride operate anywhere today? (Rule 2 - HOURLY)
                -- Pre-calculated in _aggregate_rides() to avoid N+1 query problem
                :ride_operated as ride_operated,

                NOW()

            FROM ride_status_snapshots rss
            JOIN rides r ON rss.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.ride_id = :ride_id
              AND rss.recorded_at >= :hour_start
              AND rss.recorded_at < :hour_end
            ON DUPLICATE KEY UPDATE
                avg_wait_time_minutes = VALUES(avg_wait_time_minutes),
                operating_snapshots = VALUES(operating_snapshots),
                down_snapshots = VALUES(down_snapshots),
                downtime_hours = VALUES(downtime_hours),
                uptime_percentage = VALUES(uptime_percentage),
                snapshot_count = VALUES(snapshot_count),
                ride_operated = VALUES(ride_operated),
                updated_at = NOW()
        """), {
            'ride_id': ride_id,
            'park_id': park_id,
            'hour_start': self.target_hour,
            'hour_end': self.hour_end,
            'ride_operated': 1 if ride_id in operated_today_ride_ids else 0
        })

    def _aggregate_parks(self, park_repo: ParkRepository):
        """
        Aggregate statistics for all parks.

        Args:
            park_repo: Park repository
        """
        try:
            with get_db_session() as session:
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
        Aggregate statistics for a single park for the target hour.

        Args:
            session: Database session
            park: Park model object

        Key Differences from Daily:
        ===========================
        - Stores shame_score (already computed at collection time) instead of calculating uptime
        - Uses AVG(shame_score) across snapshots in hour
        - Uses MAX(effective_park_weight) for monotonic accuracy within day
        - Counts rides_operating and rides_down for dashboard display
        """
        park_id = park.park_id

        # Check if park has any snapshots for this hour
        check_stmt = (
            select(func.count())
            .select_from(ParkActivitySnapshot)
            .where(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= self.target_hour,
                    ParkActivitySnapshot.recorded_at < self.hour_end
                )
            )
        )
        count = session.execute(check_stmt).scalar()
        if count == 0:
            # No snapshot data for this park during this hour, skip
            return

        # Complex INSERT ... SELECT with nested correlated subqueries must remain as text()
        # (Too complex for ORM: nested subqueries for shame_score calculation)
        session.execute(text("""
            INSERT INTO park_hourly_stats (
                park_id,
                hour_start_utc,
                shame_score,
                avg_wait_time_minutes,
                rides_operating,
                rides_down,
                total_downtime_hours,
                weighted_downtime_hours,
                effective_park_weight,
                snapshot_count,
                park_was_open,
                created_at
            )
            SELECT
                :park_id,
                :hour_start,

                -- Shame score: (weighted_downtime / effective_park_weight) * 10
                -- CRITICAL FIX (2025-12-21): Calculate from rides that operated TODAY for consistency
                -- Previously used AVG(pas.shame_score) which used 7-day hybrid denominator,
                -- causing mismatch: high shame_score but zero total_downtime_hours early in the day
                -- NOW: Both shame_score and downtime use "operated TODAY" logic (ride_operated = 1)
                ROUND(
                    CASE
                        -- Denominator: sum of tier weights for rides that operated TODAY
                        WHEN COALESCE((
                            SELECT SUM(COALESCE(rc.tier_weight, 2))
                            FROM ride_hourly_stats rhs
                            JOIN rides r ON rhs.ride_id = r.ride_id
                            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                            WHERE rhs.park_id = :park_id
                              AND rhs.hour_start_utc = :hour_start
                              AND rhs.ride_operated = 1
                        ), 0) > 0
                        THEN
                            -- Numerator: weighted downtime for rides that operated TODAY
                            COALESCE((
                                SELECT SUM(rhs.downtime_hours * COALESCE(rc.tier_weight, 2))
                                FROM ride_hourly_stats rhs
                                JOIN rides r ON rhs.ride_id = r.ride_id
                                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                                WHERE rhs.park_id = :park_id
                                  AND rhs.hour_start_utc = :hour_start
                                  AND rhs.ride_operated = 1
                            ), 0)
                            /
                            COALESCE((
                                SELECT SUM(COALESCE(rc.tier_weight, 2))
                                FROM ride_hourly_stats rhs
                                JOIN rides r ON rhs.ride_id = r.ride_id
                                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                                WHERE rhs.park_id = :park_id
                                  AND rhs.hour_start_utc = :hour_start
                                  AND rhs.ride_operated = 1
                            ), 0)
                            * 10
                        ELSE 0
                    END,
                    1
                ) as shame_score,

                -- Average wait time across all operating rides
                ROUND(AVG(CASE WHEN (pas.park_appears_open = 1 OR pas.rides_open > 0) THEN pas.avg_wait_time END), 2) as avg_wait_time_minutes,

                -- Average number of rides operating during hour
                ROUND(AVG(CASE WHEN (pas.park_appears_open = 1 OR pas.rides_open > 0) THEN pas.rides_open END), 0) as rides_operating,

                -- Average number of rides down during hour
                ROUND(AVG(CASE WHEN (pas.park_appears_open = 1 OR pas.rides_open > 0) THEN pas.rides_closed END), 0) as rides_down,

                -- Total downtime hours (unweighted, from ride_hourly_stats)
                COALESCE((
                    SELECT SUM(downtime_hours)
                    FROM ride_hourly_stats rhs
                    WHERE rhs.park_id = :park_id
                      AND rhs.hour_start_utc = :hour_start
                      AND rhs.ride_operated = 1
                ), 0) as total_downtime_hours,

                -- Weighted downtime hours (tier-weighted, from ride_hourly_stats + ride_classifications)
                COALESCE((
                    SELECT SUM(rhs.downtime_hours * COALESCE(rc.tier_weight, 2))
                    FROM ride_hourly_stats rhs
                    JOIN rides r ON rhs.ride_id = r.ride_id
                    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                    WHERE rhs.park_id = :park_id
                      AND rhs.hour_start_utc = :hour_start
                      AND rhs.ride_operated = 1
                ), 0) as weighted_downtime_hours,

                -- Effective park weight: sum of tier weights for rides that operated TODAY
                -- CRITICAL FIX (2025-12-21): Changed from 7-day/3-day window to "operated TODAY"
                -- This ensures consistency with shame_score and total_downtime_hours calculations
                -- All three metrics now use the SAME set of rides (ride_operated = 1)
                COALESCE((
                    SELECT SUM(COALESCE(rc.tier_weight, 2))
                    FROM ride_hourly_stats rhs
                    JOIN rides r ON rhs.ride_id = r.ride_id
                    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                    WHERE rhs.park_id = :park_id
                      AND rhs.hour_start_utc = :hour_start
                      AND rhs.ride_operated = 1
                ), 0) as effective_park_weight,

                -- Number of snapshots aggregated (expect ~12 for 5-min collection)
                COUNT(*) as snapshot_count,

                -- Was park open at all during hour?
                -- Uses same fallback heuristic: TRUE if schedule says open OR rides operating
                MAX(CASE WHEN pas.park_appears_open = 1 OR pas.rides_open > 0 THEN 1 ELSE 0 END) as park_was_open,

                NOW()

            FROM park_activity_snapshots pas
            WHERE pas.park_id = :park_id
              AND pas.recorded_at >= :hour_start
              AND pas.recorded_at < :hour_end
            ON DUPLICATE KEY UPDATE
                shame_score = VALUES(shame_score),
                avg_wait_time_minutes = VALUES(avg_wait_time_minutes),
                rides_operating = VALUES(rides_operating),
                rides_down = VALUES(rides_down),
                total_downtime_hours = VALUES(total_downtime_hours),
                weighted_downtime_hours = VALUES(weighted_downtime_hours),
                effective_park_weight = VALUES(effective_park_weight),
                snapshot_count = VALUES(snapshot_count),
                park_was_open = VALUES(park_was_open),
                updated_at = NOW()
        """), {
            'park_id': park_id,
            'hour_start': self.target_hour,
            'hour_end': self.hour_end
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
        logger.info(f"Hour:             {self.target_hour.strftime('%Y-%m-%d %H:00 UTC')}")
        logger.info(f"Parks processed:  {self.stats['parks_processed']}")
        logger.info(f"Rides processed:  {self.stats['rides_processed']}")
        logger.info(f"Errors:           {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Aggregate hourly statistics from raw snapshots'
    )
    parser.add_argument(
        '--hour',
        type=str,
        help='Hour to aggregate (YYYY-MM-DD-HH format, default: previous completed hour UTC)'
    )

    args = parser.parse_args()

    target_hour = None
    if args.hour:
        try:
            target_hour = datetime.strptime(args.hour, '%Y-%m-%d-%H')
        except ValueError:
            logger.error(f"Invalid hour format: {args.hour}. Use YYYY-MM-DD-HH (e.g., 2025-12-05-13)")
            sys.exit(1)

    aggregator = HourlyAggregator(target_hour=target_hour)
    aggregator.run()


if __name__ == '__main__':
    main()
