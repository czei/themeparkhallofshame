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
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from sqlalchemy import select, func, and_, or_, case, text, literal, literal_column, Integer, insert
from database.connection import get_db_session
from models import (
    Park, Ride, RideClassification,
    RideStatusSnapshot, ParkActivitySnapshot, RideStatusChange,
    ParkLiveRankingsStaging, RideLiveRankingsStaging
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
            .where(RideStatusSnapshot.recorded_at >= func.now() - timedelta(hours=LIVE_WINDOW_HOURS))
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
            .where(Ride.last_operated_at >= func.utc_timestamp() - timedelta(days=7))
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
            .where(Ride.last_operated_at >= func.utc_timestamp() - timedelta(days=7))
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

        # Execute INSERT...SELECT using ORM table with compiled SELECT
        # Note: insert().from_select() doesn't handle CTEs well with HAVING clauses,
        # so we use the ORM table's insert() with compiled SELECT
        park_columns = [
            ParkLiveRankingsStaging.park_id,
            ParkLiveRankingsStaging.queue_times_id,
            ParkLiveRankingsStaging.park_name,
            ParkLiveRankingsStaging.location,
            ParkLiveRankingsStaging.timezone,
            ParkLiveRankingsStaging.is_disney,
            ParkLiveRankingsStaging.is_universal,
            ParkLiveRankingsStaging.rides_down,
            ParkLiveRankingsStaging.total_rides,
            ParkLiveRankingsStaging.shame_score,
            ParkLiveRankingsStaging.park_is_open,
            ParkLiveRankingsStaging.total_downtime_hours,
            ParkLiveRankingsStaging.weighted_downtime_hours,
            ParkLiveRankingsStaging.total_park_weight,
            ParkLiveRankingsStaging.calculated_at,
        ]
        insert_stmt = insert(ParkLiveRankingsStaging.__table__).from_select(
            [c.key for c in park_columns], park_rankings_select
        )
        session.execute(insert_stmt)

        # Get count before swap using ORM
        count = session.query(func.count(ParkLiveRankingsStaging.park_id)).scalar()

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

        # Step 2: Execute INSERT...SELECT using raw SQL
        # Note: MySQL can't resolve CTE references in HAVING within insert().from_select().
        # Use raw SQL with proper parameter binding for this complex query.
        insert_sql = text("""
            INSERT INTO ride_live_rankings_staging
                (ride_id, park_id, queue_times_id, ride_name, park_name,
                 tier, tier_weight, category, is_disney, is_universal,
                 is_down, current_status, current_wait_time, last_status_change,
                 downtime_hours, downtime_incidents, avg_wait_time, max_wait_time,
                 calculated_at)
            WITH latest_snapshot AS (
                SELECT ride_id, MAX(recorded_at) AS latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc
                  AND recorded_at < :end_utc
                  AND recorded_at >= DATE_SUB(NOW(), INTERVAL :live_hours HOUR)
                GROUP BY ride_id
            ),
            ride_current_status AS (
                SELECT rss.ride_id, rss.status, rss.status AS current_status,
                       rss.wait_time AS current_wait_time, rss.computed_is_open,
                       pas.park_appears_open
                FROM ride_status_snapshots rss
                INNER JOIN latest_snapshot ls
                    ON rss.ride_id = ls.ride_id AND rss.recorded_at = ls.latest_recorded_at
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                LEFT OUTER JOIN park_activity_snapshots pas
                    ON pas.park_id = r.park_id
                   AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') =
                       DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
            ),
            last_status_changes AS (
                SELECT ride_id, MAX(changed_at) AS last_status_change
                FROM ride_status_changes
                WHERE changed_at >= :start_utc
                GROUP BY ride_id
            )
            SELECT
                r.ride_id, r.park_id, r.queue_times_id, r.name AS ride_name,
                p.name AS park_name,
                COALESCE(rc.tier, 3) AS tier,
                COALESCE(rc.tier_weight, 2.0) AS tier_weight,
                r.category,
                p.is_disney, p.is_universal,
                CASE
                    WHEN COALESCE(rcs.park_appears_open, FALSE) = FALSE THEN FALSE
                    WHEN CASE
                        WHEN p.is_disney = TRUE OR p.is_universal = TRUE
                            THEN rcs.status = 'DOWN'
                        ELSE rcs.status IN ('DOWN', 'CLOSED')
                            OR (rcs.status IS NULL AND rcs.computed_is_open = FALSE)
                    END THEN TRUE
                    ELSE FALSE
                END AS is_down,
                rcs.current_status,
                rcs.current_wait_time,
                lsc.last_status_change,
                ROUND(SUM(CASE
                    WHEN pas.park_appears_open = TRUE AND (
                        CASE
                            WHEN p.is_disney = TRUE OR p.is_universal = TRUE
                                THEN rss.status = 'DOWN'
                            ELSE rss.status IN ('DOWN', 'CLOSED')
                                OR (rss.status IS NULL AND rss.computed_is_open = FALSE)
                        END
                    ) THEN 5 ELSE 0
                END) / 60.0, 2) AS downtime_hours,
                0 AS downtime_incidents,
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 1) AS avg_wait_time,
                MAX(rss.wait_time) AS max_wait_time,
                :calculated_at AS calculated_at
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT OUTER JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas
                ON p.park_id = pas.park_id AND pas.recorded_at = rss.recorded_at
            LEFT OUTER JOIN ride_current_status rcs ON r.ride_id = rcs.ride_id
            LEFT OUTER JOIN last_status_changes lsc ON r.ride_id = lsc.ride_id
            WHERE rss.recorded_at >= :start_utc
              AND rss.recorded_at < :end_utc
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
              AND p.is_active = TRUE
              AND r.last_operated_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
            GROUP BY r.ride_id, r.name, r.park_id, r.queue_times_id, r.category,
                     p.name, p.is_disney, p.is_universal,
                     rc.tier, rc.tier_weight,
                     rcs.computed_is_open, rcs.current_status, rcs.current_wait_time,
                     rcs.park_appears_open, lsc.last_status_change
            HAVING ROUND(SUM(CASE
                WHEN pas.park_appears_open = TRUE AND (
                    CASE
                        WHEN p.is_disney = TRUE OR p.is_universal = TRUE
                            THEN rss.status = 'DOWN'
                        ELSE rss.status IN ('DOWN', 'CLOSED')
                            OR (rss.status IS NULL AND rss.computed_is_open = FALSE)
                    END
                ) THEN 5 ELSE 0
            END) / 60.0, 2) > 0
        """)
        session.execute(insert_sql, {
            'start_utc': start_utc,
            'end_utc': end_utc,
            'live_hours': LIVE_WINDOW_HOURS,
            'calculated_at': calculated_at
        })

        # Get count before swap using ORM
        count = session.query(func.count(RideLiveRankingsStaging.ride_id)).scalar()

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
