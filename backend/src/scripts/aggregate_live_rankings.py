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

from sqlalchemy import text
from database.connection import get_db_connection
from utils.logger import logger
from utils.timezone import get_today_pacific, get_pacific_day_range_utc
from utils.sql_helpers import RideStatusSQL, ParkStatusSQL


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
            with get_db_connection() as conn:
                # Aggregate parks
                self._aggregate_park_rankings(conn)

                # Aggregate rides
                self._aggregate_ride_rankings(conn)

                # Commit all changes
                conn.commit()

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

    def _aggregate_park_rankings(self, conn):
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

        # SQL helpers for consistent logic
        is_down_latest = RideStatusSQL.is_down("rss_latest", parks_alias="p")
        park_open_latest = ParkStatusSQL.park_appears_open_filter("pas_latest")
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")
        # Filter to exclude rides that never operated today (seasonal/weather closures)
        # CRITICAL: Must pass park_id_expr to check park_appears_open during operation
        has_operated = RideStatusSQL.has_operated_for_park_type("r_inner.ride_id", "p", park_id_expr="r_inner.park_id")

        # Step 1: Truncate staging table
        conn.execute(text("TRUNCATE TABLE park_live_rankings_staging"))

        # Step 2: Insert aggregated data into staging
        # This is the expensive query, but we only run it once per collection cycle
        insert_query = text(f"""
            INSERT INTO park_live_rankings_staging (
                park_id, queue_times_id, park_name, location, timezone,
                is_disney, is_universal,
                rides_down, total_rides, shame_score, park_is_open,
                total_downtime_hours, weighted_downtime_hours, total_park_weight,
                calculated_at
            )
            WITH
            latest_snapshot AS (
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            ),
            rides_currently_down AS (
                SELECT DISTINCT r_inner.ride_id, r_inner.park_id
                FROM rides r_inner
                INNER JOIN parks p ON r_inner.park_id = p.park_id
                INNER JOIN ride_status_snapshots rss_latest ON r_inner.ride_id = rss_latest.ride_id
                INNER JOIN latest_snapshot ls ON rss_latest.ride_id = ls.ride_id
                    AND rss_latest.recorded_at = ls.latest_recorded_at
                INNER JOIN park_activity_snapshots pas_latest ON r_inner.park_id = pas_latest.park_id
                    AND pas_latest.recorded_at = rss_latest.recorded_at
                WHERE r_inner.is_active = TRUE
                    AND r_inner.category = 'ATTRACTION'
                    AND {is_down_latest}
                    AND {park_open_latest}
                    AND {has_operated}
            ),
            park_weights AS (
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight,
                    COUNT(DISTINCT r.ride_id) AS total_rides
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                GROUP BY p.park_id
            ),
            current_down_weights AS (
                -- Sum of tier_weights for rides CURRENTLY down (for instantaneous shame score)
                SELECT
                    rcd.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS sum_down_weight
                FROM rides_currently_down rcd
                LEFT JOIN ride_classifications rc ON rcd.ride_id = rc.ride_id
                GROUP BY rcd.park_id
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                p.timezone,
                p.is_disney,
                p.is_universal,

                -- Rides currently down (live count)
                COUNT(DISTINCT rcd.ride_id) AS rides_down,

                -- Total rides
                pw.total_rides,

                -- Shame Score (INSTANTANEOUS - based on rides currently down)
                ROUND(
                    (COALESCE(cdw.sum_down_weight, 0)
                    / NULLIF(pw.total_park_weight, 0)) * 10,
                    1
                ) AS shame_score,

                -- Park is open
                {park_is_open_sq},

                -- Total downtime hours today
                ROUND(
                    SUM(CASE WHEN {park_open} AND {is_down} THEN 5 ELSE 0 END) / 60.0,
                    2
                ) AS total_downtime_hours,

                -- Weighted downtime hours today
                ROUND(
                    SUM(CASE WHEN {park_open} AND {is_down}
                        THEN 5 * COALESCE(rc.tier_weight, 2) ELSE 0
                    END) / 60.0,
                    2
                ) AS weighted_downtime_hours,

                pw.total_park_weight,
                :calculated_at AS calculated_at

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN rides_currently_down rcd ON r.ride_id = rcd.ride_id
            LEFT JOIN current_down_weights cdw ON p.park_id = cdw.park_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
            GROUP BY p.park_id, p.name, p.city, p.state_province, p.timezone,
                     p.queue_times_id, p.is_disney, p.is_universal,
                     pw.total_park_weight, pw.total_rides, cdw.sum_down_weight
        """)

        conn.execute(insert_query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "calculated_at": calculated_at,
        })

        # Get count before swap
        result = conn.execute(text("SELECT COUNT(*) FROM park_live_rankings_staging"))
        count = result.scalar()

        # Step 3: Atomic table swap
        # RENAME is atomic - API never sees incomplete data
        conn.execute(text("""
            RENAME TABLE
                park_live_rankings TO park_live_rankings_old,
                park_live_rankings_staging TO park_live_rankings
        """))

        # Step 4: Rename old table to become new staging table
        conn.execute(text("""
            RENAME TABLE park_live_rankings_old TO park_live_rankings_staging
        """))

        self.stats["parks_aggregated"] = count
        self.stats["park_time_seconds"] = time.time() - start
        logger.info(f"  Park rankings: {count} parks in {self.stats['park_time_seconds']:.1f}s")

    def _aggregate_ride_rankings(self, conn):
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

        # SQL helpers
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        # CRITICAL: Filter out rides that never operated today (seasonal closures)
        # Must pass park_id_expr to check park_appears_open during operation
        has_operated = RideStatusSQL.has_operated_for_park_type("r.ride_id", "p", park_id_expr="r.park_id")

        # Step 1: Truncate staging table
        conn.execute(text("TRUNCATE TABLE ride_live_rankings_staging"))

        # Step 2: Insert aggregated data into staging
        insert_query = text(f"""
            INSERT INTO ride_live_rankings_staging (
                ride_id, park_id, queue_times_id, ride_name, park_name,
                tier, tier_weight, category,
                is_disney, is_universal,
                is_down, current_status, current_wait_time, last_status_change,
                downtime_hours, downtime_incidents, avg_wait_time, max_wait_time,
                calculated_at
            )
            WITH
            latest_snapshot AS (
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            ),
            ride_current_status AS (
                SELECT
                    rss.ride_id,
                    rss.status AS current_status,
                    rss.wait_time AS current_wait_time,
                    rss.computed_is_open
                FROM ride_status_snapshots rss
                INNER JOIN latest_snapshot ls ON rss.ride_id = ls.ride_id
                    AND rss.recorded_at = ls.latest_recorded_at
            ),
            last_status_changes AS (
                SELECT ride_id, MAX(changed_at) as last_status_change
                FROM ride_status_changes
                WHERE changed_at >= :start_utc
                GROUP BY ride_id
            )
            SELECT
                r.ride_id,
                r.park_id,
                r.queue_times_id,
                r.name AS ride_name,
                p.name AS park_name,

                COALESCE(rc.tier, 3) AS tier,
                COALESCE(rc.tier_weight, 2.0) AS tier_weight,
                r.category,

                p.is_disney,
                p.is_universal,

                -- Current status (from latest snapshot)
                CASE WHEN rcs.computed_is_open = FALSE THEN TRUE ELSE FALSE END AS is_down,
                rcs.current_status,
                rcs.current_wait_time,
                lsc.last_status_change,

                -- Today's downtime hours
                ROUND(
                    SUM(CASE WHEN {park_open} AND {is_down} THEN 5 ELSE 0 END) / 60.0,
                    2
                ) AS downtime_hours,

                -- Count of downtime incidents (simplified - count status changes to down)
                0 AS downtime_incidents,

                -- Wait time stats
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 1) AS avg_wait_time,
                MAX(rss.wait_time) AS max_wait_time,

                :calculated_at AS calculated_at

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            LEFT JOIN ride_current_status rcs ON r.ride_id = rcs.ride_id
            LEFT JOIN last_status_changes lsc ON r.ride_id = lsc.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                AND {has_operated}
            GROUP BY r.ride_id, r.name, r.park_id, r.queue_times_id, r.category,
                     p.name, p.is_disney, p.is_universal,
                     rc.tier, rc.tier_weight,
                     rcs.computed_is_open, rcs.current_status, rcs.current_wait_time,
                     lsc.last_status_change
        """)

        conn.execute(insert_query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "calculated_at": calculated_at,
        })

        # Get count before swap
        result = conn.execute(text("SELECT COUNT(*) FROM ride_live_rankings_staging"))
        count = result.scalar()

        # Step 3: Atomic table swap
        conn.execute(text("""
            RENAME TABLE
                ride_live_rankings TO ride_live_rankings_old,
                ride_live_rankings_staging TO ride_live_rankings
        """))

        # Step 4: Rename old table to become new staging table
        conn.execute(text("""
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
