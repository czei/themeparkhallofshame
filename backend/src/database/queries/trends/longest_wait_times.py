"""
Longest Wait Times Query (Awards)
=================================

Endpoint: GET /api/trends/longest-wait-times
UI Location: Trends tab → Awards section

Returns top 10 rides ranked by average wait time (in minutes).

IMPORTANT: Rankings use avg_wait_time to match the Wait Times table.
This ensures consistency between Awards and the main rankings.

Uses CTE-based queries for performance on large snapshot tables.

Periods:
- today: Aggregates from ride_status_snapshots (midnight Pacific to now)
- last_week/last_month: Aggregates from ride_daily_stats

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_status_snapshots + park_activity_snapshots (TODAY period)
- ride_daily_stats (last_week/last_month periods)
"""

from datetime import timedelta
from typing import List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc, get_today_pacific, get_yesterday_range_utc
from utils.sql_helpers import ParkStatusSQL


class LongestWaitTimesQuery:
    """
    Query for rides with highest average wait times.

    IMPORTANT: Rankings use avg_wait_time (not cumulative_wait_hours)
    to match the Wait Times table for consistency.
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
        Get rides ranked by average wait time.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of rides with average wait time (ranked highest first)
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
        Get average wait times from TODAY (snapshot data).

        Ranked by avg_wait_time to match the Wait Times table.
        Uses park_appears_open for consistency with other queries.

        Uses CTE-based query for performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helper for park open check
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        sql = text(f"""
            WITH active_rides AS (
                -- PERFORMANCE: Pre-filter active attraction rides
                SELECT r.ride_id, r.name AS ride_name, p.park_id, p.name AS park_name
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  {filter_clause}
            ),
            wait_time_snapshots AS (
                -- PERFORMANCE: Only select snapshots with wait times for active rides
                -- Uses park_appears_open for consistency with Wait Times table
                SELECT
                    ar.ride_id,
                    ar.ride_name,
                    ar.park_id,
                    ar.park_name,
                    rss.wait_time
                FROM ride_status_snapshots rss
                INNER JOIN active_rides ar ON rss.ride_id = ar.ride_id
                INNER JOIN park_activity_snapshots pas
                    ON ar.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at <= :now_utc
                  AND rss.wait_time > 0
                  AND {park_open}
            )
            SELECT
                ride_id,
                ride_name,
                park_id,
                park_name,
                ROUND(AVG(wait_time), 0) AS avg_wait_time,
                MAX(wait_time) AS peak_wait_time,
                COUNT(*) AS snapshot_count
            FROM wait_time_snapshots
            GROUP BY ride_id, ride_name, park_id, park_name
            ORDER BY avg_wait_time DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'now_utc': now_utc,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get average wait times from YESTERDAY (snapshot data).

        Same logic as _get_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helper for park open check
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        sql = text(f"""
            WITH active_rides AS (
                -- PERFORMANCE: Pre-filter active attraction rides
                SELECT r.ride_id, r.name AS ride_name, p.park_id, p.name AS park_name
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  {filter_clause}
            ),
            wait_time_snapshots AS (
                -- PERFORMANCE: Only select snapshots with wait times for active rides
                SELECT
                    ar.ride_id,
                    ar.ride_name,
                    ar.park_id,
                    ar.park_name,
                    rss.wait_time
                FROM ride_status_snapshots rss
                INNER JOIN active_rides ar ON rss.ride_id = ar.ride_id
                INNER JOIN park_activity_snapshots pas
                    ON ar.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at < :end_utc
                  AND rss.wait_time > 0
                  AND {park_open}
            )
            SELECT
                ride_id,
                ride_name,
                park_id,
                park_name,
                ROUND(AVG(wait_time), 0) AS avg_wait_time,
                MAX(wait_time) AS peak_wait_time,
                COUNT(*) AS snapshot_count
            FROM wait_time_snapshots
            GROUP BY ride_id, ride_name, park_id, park_name
            ORDER BY avg_wait_time DESC
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
        Get average wait times from daily stats (last_week/last_month).

        Ranked by avg_wait_time to match the Wait Times table.
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
                ROUND(AVG(rds.avg_wait_time), 0) AS avg_wait_time,
                MAX(rds.peak_wait_time) AS peak_wait_time,
                COUNT(rds.stat_id) AS days_with_data
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
            WHERE rds.stat_date >= :start_date
              AND rds.stat_date <= :end_date
              AND rds.avg_wait_time > 0
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY r.ride_id, r.name, p.park_id, p.name
            ORDER BY avg_wait_time DESC
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
        Get parks ranked by average wait time.

        Args:
            period: 'today', 'yesterday', 'last_week', or 'last_month'
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results (default 10)

        Returns:
            List of parks with average wait time (ranked highest first)
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
        Get park-level wait time rankings from TODAY (snapshot data).

        Sorted by avg_wait_time to match the Wait Times table.
        Uses park_appears_open for consistency with other queries.
        Uses CTE-based query for performance on large snapshot tables.
        """
        start_utc, now_utc = get_today_range_to_now_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helper for park open check
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        sql = text(f"""
            WITH active_rides AS (
                -- PERFORMANCE: Pre-filter active attraction rides
                SELECT r.ride_id, p.park_id, p.name AS park_name, p.city, p.state_province
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  {filter_clause}
            ),
            wait_time_snapshots AS (
                -- PERFORMANCE: Only select snapshots with wait times for active rides
                -- Uses park_appears_open for consistency with Wait Times table
                SELECT
                    ar.park_id,
                    ar.park_name,
                    ar.city,
                    ar.state_province,
                    ar.ride_id,
                    rss.wait_time
                FROM ride_status_snapshots rss
                INNER JOIN active_rides ar ON rss.ride_id = ar.ride_id
                INNER JOIN park_activity_snapshots pas
                    ON ar.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at <= :now_utc
                  AND rss.wait_time > 0
                  AND {park_open}
            )
            SELECT
                park_id,
                park_name,
                CONCAT(city, ', ', COALESCE(state_province, '')) AS location,
                ROUND(AVG(wait_time), 0) AS avg_wait_time,
                COUNT(DISTINCT ride_id) AS rides_with_waits
            FROM wait_time_snapshots
            GROUP BY park_id, park_name, city, state_province
            ORDER BY avg_wait_time DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_utc': start_utc,
            'now_utc': now_utc,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]

    def _get_parks_yesterday(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait time rankings from YESTERDAY (snapshot data).

        Same logic as _get_parks_today() but for yesterday's full day UTC range.
        """
        start_utc, end_utc, _ = get_yesterday_range_utc()

        filter_clause = ""
        if filter_disney_universal:
            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"

        # Use centralized helper for park open check
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        sql = text(f"""
            WITH active_rides AS (
                -- PERFORMANCE: Pre-filter active attraction rides
                SELECT r.ride_id, p.park_id, p.name AS park_name, p.city, p.state_province
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
                  AND p.is_active = TRUE
                  {filter_clause}
            ),
            wait_time_snapshots AS (
                -- PERFORMANCE: Only select snapshots with wait times for active rides
                SELECT
                    ar.park_id,
                    ar.park_name,
                    ar.city,
                    ar.state_province,
                    ar.ride_id,
                    rss.wait_time
                FROM ride_status_snapshots rss
                INNER JOIN active_rides ar ON rss.ride_id = ar.ride_id
                INNER JOIN park_activity_snapshots pas
                    ON ar.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE rss.recorded_at >= :start_utc
                  AND rss.recorded_at < :end_utc
                  AND rss.wait_time > 0
                  AND {park_open}
            )
            SELECT
                park_id,
                park_name,
                CONCAT(city, ', ', COALESCE(state_province, '')) AS location,
                ROUND(AVG(wait_time), 0) AS avg_wait_time,
                COUNT(DISTINCT ride_id) AS rides_with_waits
            FROM wait_time_snapshots
            GROUP BY park_id, park_name, city, state_province
            ORDER BY avg_wait_time DESC
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
        Get park-level wait time rankings from daily stats (7days/30days).

        Sorted by avg_wait_time (not cumulative hours - that just shows parks with most rides).
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
                ROUND(AVG(rds.avg_wait_time), 0) AS avg_wait_time,
                COUNT(DISTINCT r.ride_id) AS rides_with_waits
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
            WHERE rds.stat_date >= :start_date
              AND rds.stat_date <= :end_date
              AND rds.avg_wait_time > 0
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
              AND p.is_active = TRUE
              {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            ORDER BY avg_wait_time DESC
            LIMIT :limit
        """)

        result = self.conn.execute(sql, {
            'start_date': start_date,
            'end_date': today,
            'limit': limit,
        })

        return [dict(row._mapping) for row in result]
