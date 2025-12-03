"""
Least Reliable Rides Query (Awards)
===================================

Endpoint: GET /api/trends/least-reliable
UI Location: Trends tab → Awards section

Returns top 10 rides ranked by total downtime hours.

Formula: COUNT(down_snapshots) * 5 / 60 = downtime hours
Only counts downtime when park is open.

CRITICAL: Only counts rides that have OPERATED during the period.
Rides that are DOWN all day (never operated) are excluded.
See RideStatusSQL.has_operated_subquery() in utils/sql_helpers.py.

Periods:
- today: Aggregates from ride_status_snapshots (midnight Pacific to now)
- 7days/30days: Aggregates from ride_daily_stats

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_status_snapshots + park_activity_snapshots (TODAY period)
- ride_daily_stats (7days/30days periods)
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc, get_today_pacific, get_yesterday_range_utc
from utils.sql_helpers import RideStatusSQL, ParkStatusSQL


class LeastReliableRidesQuery:
    """
    Query for rides with highest downtime hours.
    """

    # Snapshot interval in minutes
    SNAPSHOT_INTERVAL_MINUTES = 5

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get rides ranked by total downtime hours.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of rides with downtime hours and uptime percentage
        """
        if period == 'today':
            return self._get_today(filter_disney_universal, limit)
        elif period == 'yesterday':
            return self._get_yesterday(filter_disney_universal, limit)
        elif period == 'last_week' or period == '7days':
            return self._get_daily_aggregate(7, filter_disney_universal, limit)
        elif period == 'last_month' or period == '30days':
            return self._get_daily_aggregate(30, filter_disney_universal, limit)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_today(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get downtime hours from TODAY (snapshot data).

        Only counts downtime when park is open (park_appears_open = TRUE).
        Formula: COUNT(down snapshots while park open) * 5 / 60

        CRITICAL: Uses has_operated filter to exclude rides that are DOWN all day.
        Uses CTE-based query for better performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helpers for consistent status checks
        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        is_operating = RideStatusSQL.is_operating("rss")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":now_utc",
            filter_clause=filter_clause
        )

        sql = text(f"""
            WITH {rides_operated_cte},
            downtime_snapshots AS (
                -- PERFORMANCE: Pre-filter to only down snapshots for operated rides
                SELECT
                    r.ride_id,
                    r.name AS ride_name,
                    p.park_id,
                    p.name AS park_name,
                    CASE WHEN {is_down} AND {park_open} THEN 1 ELSE 0 END AS is_down_snapshot,
                    CASE WHEN {is_operating} THEN 1 ELSE 0 END AS is_operating_snapshot
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN park_activity_snapshots pas
                    ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at <= :now_utc
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
            )
            SELECT
                ride_id,
                ride_name,
                park_id,
                park_name,
                ROUND(SUM(is_down_snapshot) * {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0, 2) AS downtime_hours,
                SUM(is_down_snapshot) AS downtime_incidents,
                ROUND(100.0 * SUM(is_operating_snapshot) / NULLIF(COUNT(*), 0), 1) AS uptime_percentage
            FROM downtime_snapshots
            GROUP BY ride_id, ride_name, park_id, park_name
            HAVING SUM(is_down_snapshot) > 0
            ORDER BY downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'now_utc': now_utc,
            'end_utc': now_utc,  # has_operated_subquery uses :end_utc
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get downtime hours from YESTERDAY (snapshot data).

        Same logic as _get_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helpers for consistent status checks
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        is_operating = RideStatusSQL.is_operating("rss")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":end_utc",
            filter_clause=filter_clause
        )

        sql = text(f"""
            WITH {rides_operated_cte},
            downtime_snapshots AS (
                SELECT
                    r.ride_id,
                    r.name AS ride_name,
                    p.park_id,
                    p.name AS park_name,
                    CASE WHEN {is_down} AND {park_open} THEN 1 ELSE 0 END AS is_down_snapshot,
                    CASE WHEN {is_operating} THEN 1 ELSE 0 END AS is_operating_snapshot
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN park_activity_snapshots pas
                    ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at < :end_utc
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
            )
            SELECT
                ride_id,
                ride_name,
                park_id,
                park_name,
                ROUND(SUM(is_down_snapshot) * {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0, 2) AS downtime_hours,
                SUM(is_down_snapshot) AS downtime_incidents,
                ROUND(100.0 * SUM(is_operating_snapshot) / NULLIF(COUNT(*), 0), 1) AS uptime_percentage
            FROM downtime_snapshots
            GROUP BY ride_id, ride_name, park_id, park_name
            HAVING SUM(is_down_snapshot) > 0
            ORDER BY downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'end_utc': end_utc,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_daily_aggregate(
        self,
        days: int,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get downtime hours from daily stats (7days/30days).

        Uses ride_daily_stats.downtime_minutes for pre-aggregated data.
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        sql = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.park_id,
                p.name AS park_name,
                ROUND(SUM(rds.downtime_minutes) / 60.0, 2) AS downtime_hours,
                SUM(rds.status_changes) AS downtime_incidents,
                ROUND(AVG(rds.uptime_percentage), 1) AS uptime_percentage
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
            WHERE rds.stat_date >= :start_date
              AND rds.stat_date <= :end_date
              AND rds.downtime_minutes > 0
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY r.ride_id, r.name, p.park_id, p.name
            ORDER BY downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_date': start_date,
            'end_date': today,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    # ========================================
    # PARK-LEVEL RANKINGS
    # ========================================

    def get_park_rankings(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get parks ranked by total downtime hours (sum of all rides).

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of parks with total downtime hours
        """
        if period == 'today':
            return self._get_parks_today(filter_disney_universal, limit)
        elif period == 'yesterday':
            return self._get_parks_yesterday(filter_disney_universal, limit)
        elif period == 'last_week' or period == '7days':
            return self._get_parks_daily_aggregate(7, filter_disney_universal, limit)
        elif period == 'last_month' or period == '30days':
            return self._get_parks_daily_aggregate(30, filter_disney_universal, limit)
        else:
            raise ValueError(f"Invalid period: {period}. Must be 'today', 'yesterday', 'last_week', or 'last_month'")

    def _get_parks_today(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level reliability rankings from TODAY (snapshot data).

        Sorted by avg_shame_score (not downtime hours - that doesn't account for park size).
        Columns: avg_shame_score, uptime_percentage

        CRITICAL: Uses has_operated filter to exclude rides that are DOWN all day.
        Uses CTE-based query for better performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        filter_clause = ""
        park_filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"
            park_filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helpers for consistent status checks
        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        is_operating = RideStatusSQL.is_operating("rss")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":now_utc",
            filter_clause=filter_clause
        )

        sql = text(f"""
            WITH {rides_operated_cte},
            park_weights AS (
                -- Calculate total weight for each park (for shame score denominator)
                SELECT
                    r.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY r.park_id
            ),
            snapshot_shame AS (
                -- Calculate shame score at each snapshot
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    p.city,
                    p.state_province,
                    rss.recorded_at,
                    pw.total_park_weight,
                    SUM(CASE WHEN {is_down} AND {park_open} THEN COALESCE(rc.tier_weight, 2) ELSE 0 END) AS weighted_down,
                    SUM(CASE WHEN {is_operating} THEN 1 ELSE 0 END) AS operating_count,
                    COUNT(*) AS total_snapshots
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                LEFT JOIN park_activity_snapshots pas
                    ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                INNER JOIN park_weights pw ON p.park_id = pw.park_id
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at <= :now_utc
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY p.park_id, p.name, p.city, p.state_province, rss.recorded_at, pw.total_park_weight
            )
            SELECT
                park_id,
                park_name,
                CONCAT(city, ', ', COALESCE(state_province, '')) AS location,
                ROUND(AVG(weighted_down / NULLIF(total_park_weight, 0) * 10), 1) AS avg_shame_score,
                ROUND(100.0 * SUM(operating_count) / NULLIF(SUM(total_snapshots), 0), 1) AS uptime_percentage
            FROM snapshot_shame
            GROUP BY park_id, park_name, city, state_province
            HAVING AVG(weighted_down) > 0
            ORDER BY avg_shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'now_utc': now_utc,
            'end_utc': now_utc,  # has_operated_subquery uses :end_utc
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_parks_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level reliability rankings from YESTERDAY (snapshot data).

        Same logic as _get_parks_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        filter_clause = ""
        park_filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"
            park_filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helpers for consistent status checks
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        is_operating = RideStatusSQL.is_operating("rss")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":end_utc",
            filter_clause=filter_clause
        )

        sql = text(f"""
            WITH {rides_operated_cte},
            park_weights AS (
                -- Calculate total weight for each park (for shame score denominator)
                SELECT
                    r.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY r.park_id
            ),
            snapshot_shame AS (
                -- Calculate shame score at each snapshot
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    p.city,
                    p.state_province,
                    rss.recorded_at,
                    pw.total_park_weight,
                    SUM(CASE WHEN {is_down} AND {park_open} THEN COALESCE(rc.tier_weight, 2) ELSE 0 END) AS weighted_down,
                    SUM(CASE WHEN {is_operating} THEN 1 ELSE 0 END) AS operating_count,
                    COUNT(*) AS total_snapshots
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                LEFT JOIN park_activity_snapshots pas
                    ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                INNER JOIN park_weights pw ON p.park_id = pw.park_id
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at < :end_utc
                  AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY p.park_id, p.name, p.city, p.state_province, rss.recorded_at, pw.total_park_weight
            )
            SELECT
                park_id,
                park_name,
                CONCAT(city, ', ', COALESCE(state_province, '')) AS location,
                ROUND(AVG(weighted_down / NULLIF(total_park_weight, 0) * 10), 1) AS avg_shame_score,
                ROUND(100.0 * SUM(operating_count) / NULLIF(SUM(total_snapshots), 0), 1) AS uptime_percentage
            FROM snapshot_shame
            GROUP BY park_id, park_name, city, state_province
            HAVING AVG(weighted_down) > 0
            ORDER BY avg_shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'end_utc': end_utc,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_parks_daily_aggregate(
        self,
        days: int,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level reliability rankings from daily stats (7days/30days).

        Uses park_daily_stats which has pre-calculated shame_score and avg_uptime_percentage.
        Sorted by avg_shame_score (not downtime hours).
        """
        today = get_today_pacific()
        start_date = today - timedelta(days=days - 1)

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        sql = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', COALESCE(p.state_province, '')) AS location,
                ROUND(AVG(pds.shame_score), 1) AS avg_shame_score,
                ROUND(AVG(pds.avg_uptime_percentage), 1) AS uptime_percentage
            FROM parks p
            INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
            WHERE pds.stat_date >= :start_date
              AND pds.stat_date <= :end_date
              AND pds.shame_score > 0
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            ORDER BY avg_shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_date': start_date,
            'end_date': today,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]
