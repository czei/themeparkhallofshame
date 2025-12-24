#!/usr/bin/env python3
"""
Live Rankings Pre-Aggregation Script
=====================================

Pre-computes park and ride rankings and stores them in summary tables.
This allows the API to serve instant responses instead of running
expensive CTE queries on every request.

Uses atomic table swap (staging + RENAME) for zero-downtime updates.

Run after collect_snapshots.py completes:
    python -m scripts.aggregate_live_rankings

Expected runtime: ~10 seconds for all parks and rides.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from sqlalchemy import select, func, and_, or_, case, text, literal, literal_column, Integer
from database.connection import get_db_session
from models import (
    Park, Ride, RideClassification,
    RideStatusSnapshot, ParkActivitySnapshot, RideStatusChange
)
from utils.logger import logger
from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.metrics import LIVE_WINDOW_HOURS


class LiveRankingsAggregator:
    """
    Aggregates live and today rankings into pre-computed tables.

    Uses atomic table swap for zero-downtime updates:
    1. Truncate staging table
    2. Insert aggregated data into staging
    3. RENAME staging <-> live (atomic swap)
    """

    def __init__(self):
        self.stats = {
            "parks_aggregated": 0,
            "rides_aggregated": 0,
            "park_time_seconds": 0,
            "ride_time_seconds": 0,
            "errors": [],
        }

    def run(self):
        """Main execution method."""
        logger.info("=" * 60)
        logger.info(f"LIVE RANKINGS AGGREGATION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            with get_db_session() as session:
                # Aggregate parks
                self._aggregate_park_rankings(session)

                # Aggregate rides
                self._aggregate_ride_rankings(session)

                # Commit all changes
                session.commit()

        except Exception as e:
            logger.error(f"Aggregation failed: {e}", exc_info=True)
            self.stats["errors"].append(str(e))
            raise

        total_time = time.time() - start_time

        logger.info("=" * 60)
        logger.info("AGGREGATION COMPLETE")
        logger.info(f"  Parks: {self.stats['parks_aggregated']} ({self.stats['park_time_seconds']:.1f}s)")
        logger.info(f"  Rides: {self.stats['rides_aggregated']} ({self.stats['ride_time_seconds']:.1f}s)")
        logger.info(f"  Total time: {total_time:.1f}s")
        logger.info("=" * 60)

        return self.stats

    def _aggregate_park_rankings(self, session):
        """
        Aggregate park rankings into park_live_rankings table.
        Uses atomic table swap for zero-downtime.
        """
        logger.info("Aggregating park rankings...")
        start = time.time()

        # Get Pacific day bounds
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)
        calculated_at = datetime.utcnow()

        # Step 1: Truncate staging table (DDL - keep as text)
        session.execute(text("TRUNCATE TABLE park_live_rankings_staging"))

        # Step 2: Build CTEs

        # CTE: latest_snapshot - Find latest snapshot per ride in time window
        latest_snapshot = (
            select(
                RideStatusSnapshot.ride_id,
                func.max(RideStatusSnapshot.recorded_at).label('latest_recorded_at')
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(RideStatusSnapshot.recorded_at >= func.date_sub(
                func.now(),
                literal_column(f"INTERVAL {LIVE_WINDOW_HOURS} HOUR")
            ))
            .group_by(RideStatusSnapshot.ride_id)
        ).cte('latest_snapshot')

        # CTE: rides_currently_down - Rides DOWN in latest snapshot while park open
        # Park-type aware: Disney/Universal only count DOWN, others count CLOSED too
        is_down_latest = case(
            (
                or_(Park.is_disney == True, Park.is_universal == True),
                RideStatusSnapshot.status == 'DOWN'
            ),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(
                    RideStatusSnapshot.status == None,
                    RideStatusSnapshot.computed_is_open == False
                )
            )
        )

        rides_currently_down = (
            select(
                Ride.ride_id.distinct().label('ride_id'),
                Ride.park_id
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                latest_snapshot,
                and_(
                    RideStatusSnapshot.ride_id == latest_snapshot.c.ride_id,
                    RideStatusSnapshot.recorded_at == latest_snapshot.c.latest_recorded_at
                )
            )
            .join(
                ParkActivitySnapshot,
                and_(
                    Ride.park_id == ParkActivitySnapshot.park_id,
                    RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
                )
            )
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(is_down_latest)
            .where(ParkActivitySnapshot.park_appears_open == True)
            .where(Ride.last_operated_at >= func.utc_timestamp() - literal_column("INTERVAL 7 DAY"))
        ).cte('rides_currently_down')

        # CTE: park_weights - Total tier weight per park for 7-day active rides
        park_weights = (
            select(
                Park.park_id,
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight'),
                func.count(Ride.ride_id.distinct()).label('total_rides')
            )
            .select_from(Park)
            .join(Ride, Park.park_id == Ride.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Park.is_active == True)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.last_operated_at >= func.utc_timestamp() - literal_column("INTERVAL 7 DAY"))
            .group_by(Park.park_id)
        ).cte('park_weights')

        # CTE: latest_park_shame_score - Read shame_score from latest park snapshot
        latest_park_shame_score_subquery = (
            select(
                ParkActivitySnapshot.park_id,
                func.max(ParkActivitySnapshot.recorded_at).label('latest_at')
            )
            .where(ParkActivitySnapshot.recorded_at >= start_utc)
            .where(ParkActivitySnapshot.recorded_at < end_utc)
            .group_by(ParkActivitySnapshot.park_id)
        ).cte('latest_pas_times')

        latest_park_shame_score = (
            select(
                ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.shame_score
            )
            .select_from(ParkActivitySnapshot)
            .join(
                latest_park_shame_score_subquery,
                and_(
                    ParkActivitySnapshot.park_id == latest_park_shame_score_subquery.c.park_id,
                    ParkActivitySnapshot.recorded_at == latest_park_shame_score_subquery.c.latest_at
                )
            )
        ).cte('latest_park_shame_score')

        # Build park_is_open subquery (schedule-based + fallback to heuristic)
        park_is_open_sq = literal(1).label('park_is_open')  # Simplified for now

        # Park-type aware is_down for main query
        is_down = case(
            (
                or_(Park.is_disney == True, Park.is_universal == True),
                RideStatusSnapshot.status == 'DOWN'
            ),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(
                    RideStatusSnapshot.status == None,
                    RideStatusSnapshot.computed_is_open == False
                )
            )
        )

        # Main SELECT query
        park_rankings_select = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                (Park.city + literal(', ') + Park.state_province).label('location'),
                Park.timezone,
                Park.is_disney,
                Park.is_universal,

                # Rides currently down
                func.count(rides_currently_down.c.ride_id.distinct()).label('rides_down'),

                # Total rides
                park_weights.c.total_rides,

                # Shame score from snapshot
                func.coalesce(latest_park_shame_score.c.shame_score, 0.0).label('shame_score'),

                # Park is open
                park_is_open_sq,

                # Total downtime hours today
                func.round(
                    func.sum(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open == True,
                                    is_down
                                ),
                                5
                            ),
                            else_=0
                        )
                    ) / 60.0,
                    2
                ).label('total_downtime_hours'),

                # Weighted downtime hours today
                func.round(
                    func.sum(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open == True,
                                    is_down
                                ),
                                5 * func.coalesce(RideClassification.tier_weight, 2)
                            ),
                            else_=0
                        )
                    ) / 60.0,
                    2
                ).label('weighted_downtime_hours'),

                park_weights.c.total_park_weight,
                literal(calculated_at).label('calculated_at')
            )
            .select_from(Park)
            .join(Ride, and_(
                Park.park_id == Ride.park_id,
                Ride.is_active == True,
                Ride.category == 'ATTRACTION'
            ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .join(park_weights, Park.park_id == park_weights.c.park_id)
            .outerjoin(rides_currently_down, Ride.ride_id == rides_currently_down.c.ride_id)
            .outerjoin(latest_park_shame_score, Park.park_id == latest_park_shame_score.c.park_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Park.is_active == True)
            .group_by(
                Park.park_id, Park.name, Park.city, Park.state_province,
                Park.timezone, Park.queue_times_id, Park.is_disney, Park.is_universal,
                park_weights.c.total_park_weight, park_weights.c.total_rides,
                latest_park_shame_score.c.shame_score
            )
        )

        # Execute INSERT via compiled SQL (staging table has no ORM model)
        insert_sql = text(f"""
            INSERT INTO park_live_rankings_staging (
                park_id, queue_times_id, park_name, location, timezone,
                is_disney, is_universal,
                rides_down, total_rides, shame_score, park_is_open,
                total_downtime_hours, weighted_downtime_hours, total_park_weight,
                calculated_at
            )
            {park_rankings_select.compile(compile_kwargs={"literal_binds": True})}
        """)

        session.execute(insert_sql)

        # Get count before swap
        result = session.execute(text("SELECT COUNT(*) FROM park_live_rankings_staging"))
        count = result.scalar()

        # Step 3: Atomic table swap (DDL - keep as text)
        session.execute(text("""
            RENAME TABLE
                park_live_rankings TO park_live_rankings_old,
                park_live_rankings_staging TO park_live_rankings
        """))

        # Step 4: Rename old table to become new staging table
        session.execute(text("""
            RENAME TABLE park_live_rankings_old TO park_live_rankings_staging
        """))

        self.stats["parks_aggregated"] = count
        self.stats["park_time_seconds"] = time.time() - start
        logger.info(f"  Park rankings: {count} parks in {self.stats['park_time_seconds']:.1f}s")

    def _aggregate_ride_rankings(self, session):
        """
        Aggregate ride rankings into ride_live_rankings table.
        Uses atomic table swap for zero-downtime.
        """
        logger.info("Aggregating ride rankings...")
        start = time.time()

        # Get Pacific day bounds
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)
        calculated_at = datetime.utcnow()

        # Step 1: Truncate staging table (DDL - keep as text)
        session.execute(text("TRUNCATE TABLE ride_live_rankings_staging"))

        # Step 2: Build CTEs

        # CTE: latest_snapshot
        latest_snapshot = (
            select(
                RideStatusSnapshot.ride_id,
                func.max(RideStatusSnapshot.recorded_at).label('latest_recorded_at')
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(RideStatusSnapshot.recorded_at >= func.date_sub(
                func.now(),
                literal_column(f"INTERVAL {LIVE_WINDOW_HOURS} HOUR")
            ))
            .group_by(RideStatusSnapshot.ride_id)
        ).cte('latest_snapshot')

        # CTE: ride_current_status - Join to latest snapshot for current status
        # Minute-level timestamp matching using DATE_FORMAT
        ts_match_current = (
            func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
            func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
        )

        ride_current_status = (
            select(
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.status,
                RideStatusSnapshot.status.label('current_status'),
                RideStatusSnapshot.wait_time.label('current_wait_time'),
                RideStatusSnapshot.computed_is_open,
                ParkActivitySnapshot.park_appears_open
            )
            .select_from(RideStatusSnapshot)
            .join(
                latest_snapshot,
                and_(
                    RideStatusSnapshot.ride_id == latest_snapshot.c.ride_id,
                    RideStatusSnapshot.recorded_at == latest_snapshot.c.latest_recorded_at
                )
            )
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .outerjoin(
                ParkActivitySnapshot,
                and_(
                    ParkActivitySnapshot.park_id == Ride.park_id,
                    ts_match_current
                )
            )
        ).cte('ride_current_status')

        # CTE: last_status_changes
        last_status_changes = (
            select(
                RideStatusChange.ride_id,
                func.max(RideStatusChange.changed_at).label('last_status_change')
            )
            .where(RideStatusChange.changed_at >= start_utc)
            .group_by(RideStatusChange.ride_id)
        ).cte('last_status_changes')

        # Park-type aware is_down for main query
        is_down = case(
            (
                or_(Park.is_disney == True, Park.is_universal == True),
                RideStatusSnapshot.status == 'DOWN'
            ),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(
                    RideStatusSnapshot.status == None,
                    RideStatusSnapshot.computed_is_open == False
                )
            )
        )

        # Park-type aware is_down for current status (using ride_current_status CTE alias)
        current_is_down = case(
            (
                or_(Park.is_disney == True, Park.is_universal == True),
                ride_current_status.c.status == 'DOWN'
            ),
            else_=or_(
                ride_current_status.c.status.in_(['DOWN', 'CLOSED']),
                and_(
                    ride_current_status.c.status == None,
                    ride_current_status.c.computed_is_open == False
                )
            )
        )

        # Main SELECT query
        ride_rankings_select = (
            select(
                Ride.ride_id,
                Ride.park_id,
                Ride.queue_times_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),

                func.coalesce(RideClassification.tier, 3).label('tier'),
                func.coalesce(RideClassification.tier_weight, 2.0).label('tier_weight'),
                Ride.category,

                Park.is_disney,
                Park.is_universal,

                # Current status
                case(
                    (
                        func.coalesce(ride_current_status.c.park_appears_open, False) == False,
                        False
                    ),
                    (current_is_down, True),
                    else_=False
                ).label('is_down'),
                ride_current_status.c.current_status,
                ride_current_status.c.current_wait_time,
                last_status_changes.c.last_status_change,

                # Today's downtime hours
                func.round(
                    func.sum(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open == True,
                                    is_down
                                ),
                                5
                            ),
                            else_=0
                        )
                    ) / 60.0,
                    2
                ).label('downtime_hours'),

                # Downtime incidents (simplified)
                literal(0).label('downtime_incidents'),

                # Wait time stats
                func.round(
                    func.avg(
                        case(
                            (RideStatusSnapshot.wait_time > 0, RideStatusSnapshot.wait_time),
                            else_=None
                        )
                    ),
                    1
                ).label('avg_wait_time'),
                func.max(RideStatusSnapshot.wait_time).label('max_wait_time'),

                literal(calculated_at).label('calculated_at')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .outerjoin(ride_current_status, Ride.ride_id == ride_current_status.c.ride_id)
            .outerjoin(last_status_changes, Ride.ride_id == last_status_changes.c.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
            .where(Ride.last_operated_at >= func.utc_timestamp() - literal_column("INTERVAL 7 DAY"))
            .group_by(
                Ride.ride_id, Ride.name, Ride.park_id, Ride.queue_times_id, Ride.category,
                Park.name, Park.is_disney, Park.is_universal,
                RideClassification.tier, RideClassification.tier_weight,
                ride_current_status.c.computed_is_open, ride_current_status.c.current_status,
                ride_current_status.c.current_wait_time, ride_current_status.c.park_appears_open,
                last_status_changes.c.last_status_change
            )
            .having(
                or_(
                    func.round(
                        func.sum(
                            case(
                                (
                                    and_(
                                        ParkActivitySnapshot.park_appears_open == True,
                                        is_down
                                    ),
                                    5
                                ),
                                else_=0
                            )
                        ) / 60.0,
                        2
                    ) > 0,
                    case(
                        (
                            func.coalesce(ride_current_status.c.park_appears_open, False) == False,
                            False
                        ),
                        (current_is_down, True),
                        else_=False
                    ) == True
                )
            )
        )

        # Execute INSERT via compiled SQL (staging table has no ORM model)
        insert_sql = text(f"""
            INSERT INTO ride_live_rankings_staging (
                ride_id, park_id, queue_times_id, ride_name, park_name,
                tier, tier_weight, category,
                is_disney, is_universal,
                is_down, current_status, current_wait_time, last_status_change,
                downtime_hours, downtime_incidents, avg_wait_time, max_wait_time,
                calculated_at
            )
            {ride_rankings_select.compile(compile_kwargs={"literal_binds": True})}
        """)

        session.execute(insert_sql)

        # Get count before swap
        result = session.execute(text("SELECT COUNT(*) FROM ride_live_rankings_staging"))
        count = result.scalar()

        # Step 3: Atomic table swap (DDL - keep as text)
        session.execute(text("""
            RENAME TABLE
                ride_live_rankings TO ride_live_rankings_old,
                ride_live_rankings_staging TO ride_live_rankings
        """))

        # Step 4: Rename old table to become new staging table
        session.execute(text("""
            RENAME TABLE ride_live_rankings_old TO ride_live_rankings_staging
        """))

        self.stats["rides_aggregated"] = count
        self.stats["ride_time_seconds"] = time.time() - start
        logger.info(f"  Ride rankings: {count} rides in {self.stats['ride_time_seconds']:.1f}s")


def main():
    """Entry point for the aggregation script."""
    aggregator = LiveRankingsAggregator()
    try:
        stats = aggregator.run()
        if stats["errors"]:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error in aggregation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
