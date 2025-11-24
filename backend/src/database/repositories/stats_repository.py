"""
Theme Park Downtime Tracker - Statistics Repository
Provides data access layer for aggregate statistics tables.
"""

from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Connection

from ...utils.logger import logger


class StatsRepository:
    """
    Repository for statistics queries across all aggregate tables.

    Implements:
    - Park statistics queries (daily/weekly/monthly/yearly)
    - Ride statistics queries (daily/weekly/monthly/yearly)
    - Trend analysis queries
    - Operating session queries
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    # Park Statistics

    def get_park_daily_stats(
        self,
        park_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get daily statistics for a specific park.

        Args:
            park_id: Park ID
            start_date: Start date (YYYY-MM-DD) or None for 30 days ago
            end_date: End date (YYYY-MM-DD) or None for today
            limit: Maximum number of results

        Returns:
            List of daily statistics dictionaries
        """
        query = text("""
            SELECT
                stat_date,
                park_id,
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked,
                operating_hours_minutes
            FROM park_daily_stats
            WHERE park_id = :park_id
                AND stat_date >= COALESCE(:start_date, DATE_SUB(CURDATE(), INTERVAL 30 DAY))
                AND stat_date <= COALESCE(:end_date, CURDATE())
            ORDER BY stat_date DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_park_weekly_stats(
        self,
        park_id: int,
        year: Optional[int] = None,
        limit: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get weekly statistics for a specific park.

        Args:
            park_id: Park ID
            year: Year or None for current year
            limit: Maximum number of results

        Returns:
            List of weekly statistics dictionaries
        """
        query = text("""
            SELECT
                year,
                week_number,
                park_id,
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked,
                trend_vs_previous_week
            FROM park_weekly_stats
            WHERE park_id = :park_id
                AND year = COALESCE(:year, YEAR(CURDATE()))
            ORDER BY year DESC, week_number DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "year": year,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_park_monthly_stats(
        self,
        park_id: int,
        year: Optional[int] = None,
        limit: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get monthly statistics for a specific park.

        Args:
            park_id: Park ID
            year: Year or None for current year
            limit: Maximum number of results

        Returns:
            List of monthly statistics dictionaries
        """
        query = text("""
            SELECT
                year,
                month,
                park_id,
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked,
                trend_vs_previous_month
            FROM park_monthly_stats
            WHERE park_id = :park_id
                AND year = COALESCE(:year, YEAR(CURDATE()))
            ORDER BY year DESC, month DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "year": year,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    # Ride Statistics

    def get_ride_daily_stats(
        self,
        ride_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get daily statistics for a specific ride.

        Args:
            ride_id: Ride ID
            start_date: Start date (YYYY-MM-DD) or None for 30 days ago
            end_date: End date (YYYY-MM-DD) or None for today
            limit: Maximum number of results

        Returns:
            List of daily statistics dictionaries
        """
        query = text("""
            SELECT
                stat_date,
                ride_id,
                downtime_minutes,
                uptime_percentage,
                status_changes,
                avg_wait_time,
                peak_wait_time,
                operating_hours_minutes
            FROM ride_daily_stats
            WHERE ride_id = :ride_id
                AND stat_date >= COALESCE(:start_date, DATE_SUB(CURDATE(), INTERVAL 30 DAY))
                AND stat_date <= COALESCE(:end_date, CURDATE())
            ORDER BY stat_date DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_ride_weekly_stats(
        self,
        ride_id: int,
        year: Optional[int] = None,
        limit: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get weekly statistics for a specific ride.

        Args:
            ride_id: Ride ID
            year: Year or None for current year
            limit: Maximum number of results

        Returns:
            List of weekly statistics dictionaries
        """
        query = text("""
            SELECT
                year,
                week_number,
                ride_id,
                downtime_minutes,
                uptime_percentage,
                status_changes,
                avg_wait_time,
                peak_wait_time,
                trend_vs_previous_week
            FROM ride_weekly_stats
            WHERE ride_id = :ride_id
                AND year = COALESCE(:year, YEAR(CURDATE()))
            ORDER BY year DESC, week_number DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "year": year,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    # Operating Sessions

    def get_park_operating_sessions(
        self,
        park_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get operating sessions for a specific park.

        Args:
            park_id: Park ID
            start_date: Start date (YYYY-MM-DD) or None for 30 days ago
            end_date: End date (YYYY-MM-DD) or None for today
            limit: Maximum number of results

        Returns:
            List of operating session dictionaries
        """
        query = text("""
            SELECT
                session_id,
                park_id,
                session_date,
                session_start_utc,
                session_end_utc,
                operating_minutes
            FROM park_operating_sessions
            WHERE park_id = :park_id
                AND session_date >= COALESCE(:start_date, DATE_SUB(CURDATE(), INTERVAL 30 DAY))
                AND session_date <= COALESCE(:end_date, CURDATE())
            ORDER BY session_date DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    # Trend Analysis

    def get_park_downtime_trend(
        self,
        park_id: int,
        period: str = "daily",
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get downtime trend for a park over time.

        Args:
            park_id: Park ID
            period: "daily" or "weekly"
            days: Number of days to analyze

        Returns:
            List of trend data points
        """
        if period == "daily":
            query = text("""
                SELECT
                    stat_date AS period_key,
                    total_downtime_hours,
                    avg_uptime_percentage,
                    rides_with_downtime
                FROM park_daily_stats
                WHERE park_id = :park_id
                    AND stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY stat_date ASC
            """)
        else:  # weekly
            query = text("""
                SELECT
                    CONCAT(year, '-W', LPAD(week_number, 2, '0')) AS period_key,
                    total_downtime_hours,
                    avg_uptime_percentage,
                    rides_with_downtime
                FROM park_weekly_stats
                WHERE park_id = :park_id
                    AND CONCAT(year, '-', LPAD(week_number, 2, '0')) >=
                        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL :days DAY), '%Y-%U')
                ORDER BY year ASC, week_number ASC
            """)

        result = self.conn.execute(query, {"park_id": park_id, "days": days})
        return [dict(row._mapping) for row in result]

    def get_ride_downtime_trend(
        self,
        ride_id: int,
        period: str = "daily",
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get downtime trend for a ride over time.

        Args:
            ride_id: Ride ID
            period: "daily" or "weekly"
            days: Number of days to analyze

        Returns:
            List of trend data points
        """
        if period == "daily":
            query = text("""
                SELECT
                    stat_date AS period_key,
                    downtime_minutes,
                    uptime_percentage,
                    downtime_event_count,
                    avg_wait_time
                FROM ride_daily_stats
                WHERE ride_id = :ride_id
                    AND stat_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                ORDER BY stat_date ASC
            """)
        else:  # weekly
            query = text("""
                SELECT
                    CONCAT(year, '-W', LPAD(week_number, 2, '0')) AS period_key,
                    downtime_minutes,
                    uptime_percentage,
                    downtime_event_count,
                    avg_wait_time
                FROM ride_weekly_stats
                WHERE ride_id = :ride_id
                    AND CONCAT(year, '-', LPAD(week_number, 2, '0')) >=
                        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL :days DAY), '%Y-%U')
                ORDER BY year ASC, week_number ASC
            """)

        result = self.conn.execute(query, {"ride_id": ride_id, "days": days})
        return [dict(row._mapping) for row in result]

    # Comparative Analysis

    def get_park_comparison(
        self,
        park_ids: List[int],
        period: str = "weekly",
        stat_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Compare statistics across multiple parks.

        Args:
            park_ids: List of park IDs to compare
            period: "daily", "weekly", "monthly", or "yearly"
            stat_date: Date string (YYYY-MM-DD) or None for current period

        Returns:
            List of park comparison data
        """
        if period == "daily":
            query = text("""
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    pds.total_downtime_hours,
                    pds.avg_uptime_percentage,
                    pds.rides_with_downtime,
                    pds.total_rides_tracked
                FROM parks p
                INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
                WHERE p.park_id IN :park_ids
                    AND pds.stat_date = COALESCE(:stat_date, CURDATE())
                ORDER BY pds.total_downtime_hours DESC
            """)
        elif period == "weekly":
            query = text("""
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    pws.total_downtime_hours,
                    pws.avg_uptime_percentage,
                    pws.rides_with_downtime,
                    pws.total_rides_tracked
                FROM parks p
                INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
                WHERE p.park_id IN :park_ids
                    AND pws.year = YEAR(COALESCE(:stat_date, CURDATE()))
                    AND pws.week_number = WEEK(COALESCE(:stat_date, CURDATE()), 3)
                ORDER BY pws.total_downtime_hours DESC
            """)
        elif period == "monthly":
            query = text("""
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    pms.total_downtime_hours,
                    pms.avg_uptime_percentage,
                    pms.rides_with_downtime,
                    pms.total_rides_tracked
                FROM parks p
                INNER JOIN park_monthly_stats pms ON p.park_id = pms.park_id
                WHERE p.park_id IN :park_ids
                    AND pms.year = YEAR(COALESCE(:stat_date, CURDATE()))
                    AND pms.month = MONTH(COALESCE(:stat_date, CURDATE()))
                ORDER BY pms.total_downtime_hours DESC
            """)
        else:  # yearly
            query = text("""
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    pys.total_downtime_hours,
                    pys.avg_uptime_percentage,
                    pys.rides_with_downtime,
                    pys.total_rides_tracked
                FROM parks p
                INNER JOIN park_yearly_stats pys ON p.park_id = pys.park_id
                WHERE p.park_id IN :park_ids
                    AND pys.year = YEAR(COALESCE(:stat_date, CURDATE()))
                ORDER BY pys.total_downtime_hours DESC
            """)

        result = self.conn.execute(query, {
            "park_ids": tuple(park_ids),
            "stat_date": stat_date
        })
        return [dict(row._mapping) for row in result]

    # Aggregation Status

    def get_last_aggregation_status(
        self,
        aggregation_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get status of most recent aggregations.

        Args:
            aggregation_type: "daily", "weekly", "monthly", "yearly", or None for all

        Returns:
            List of aggregation log entries
        """
        type_filter = "AND aggregation_type = :aggregation_type" if aggregation_type else ""

        query = text(f"""
            SELECT
                log_id,
                aggregation_type,
                aggregated_from_ts,
                aggregated_until_ts,
                status,
                records_aggregated,
                error_message,
                created_at
            FROM aggregation_log
            WHERE 1=1 {type_filter}
            ORDER BY created_at DESC
            LIMIT 10
        """)

        params = {}
        if aggregation_type:
            params["aggregation_type"] = aggregation_type

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result]

    def check_aggregation_health(self) -> Dict[str, Any]:
        """
        Check aggregation health status.

        Returns:
            Dictionary with health metrics
        """
        query = text("""
            SELECT
                aggregation_type,
                MAX(aggregated_until_ts) AS last_aggregated_until,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
                MAX(created_at) AS last_run_at
            FROM aggregation_log
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY aggregation_type
        """)

        result = self.conn.execute(query)
        return {row.aggregation_type: dict(row._mapping) for row in result}

    # Park Rankings (User Story 1)

    def get_park_daily_rankings(
        self,
        stat_date,
        filter_disney_universal: bool = False,
        limit: int = 50,
        weighted: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings by downtime for a specific day.

        Args:
            stat_date: Date to rank (date object)
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Maximum results
            weighted: Use weighted scoring by ride tier

        Returns:
            List of park ranking dictionaries
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        if weighted:
            # Weighted ranking query (Query 1b from data-model.md)
            query = text(f"""
                WITH park_weights AS (
                    SELECT
                        p.park_id,
                        SUM(IFNULL(rc.tier_weight, 2)) AS total_park_weight,
                        COUNT(r.ride_id) AS total_rides
                    FROM parks p
                    INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                    WHERE p.is_active = TRUE
                        {disney_filter}
                    GROUP BY p.park_id
                ),
                weighted_downtime AS (
                    SELECT
                        p.park_id,
                        SUM(rds.downtime_minutes / 60.0 * IFNULL(rc.tier_weight, 2)) AS total_weighted_downtime_hours
                    FROM parks p
                    INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                    INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
                    WHERE rds.stat_date = :stat_date
                        AND p.is_active = TRUE
                        {disney_filter}
                    GROUP BY p.park_id
                )
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    wd.total_weighted_downtime_hours AS total_downtime_hours,
                    pds.rides_with_downtime AS affected_rides_count,
                    pds.avg_uptime_percentage AS uptime_percentage,
                    ROUND(
                        ((wd.total_weighted_downtime_hours - IFNULL(prev_wd.total_weighted_downtime_hours, 0)) /
                         NULLIF(prev_wd.total_weighted_downtime_hours, 0)) * 100,
                        2
                    ) AS trend_percentage
                FROM parks p
                INNER JOIN park_weights pw ON p.park_id = pw.park_id
                INNER JOIN weighted_downtime wd ON p.park_id = wd.park_id
                INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id AND pds.stat_date = :stat_date
                LEFT JOIN (
                    SELECT
                        p2.park_id,
                        SUM(rds2.downtime_minutes / 60.0 * IFNULL(rc2.tier_weight, 2)) AS total_weighted_downtime_hours
                    FROM parks p2
                    INNER JOIN rides r2 ON p2.park_id = r2.park_id AND r2.is_active = TRUE
                    LEFT JOIN ride_classifications rc2 ON r2.ride_id = rc2.ride_id
                    INNER JOIN ride_daily_stats rds2 ON r2.ride_id = rds2.ride_id
                    WHERE rds2.stat_date = DATE_SUB(:stat_date, INTERVAL 1 DAY)
                        AND p2.is_active = TRUE
                    GROUP BY p2.park_id
                ) prev_wd ON p.park_id = prev_wd.park_id
                WHERE p.is_active = TRUE
                ORDER BY wd.total_weighted_downtime_hours DESC
                LIMIT :limit
            """)
        else:
            # Standard ranking query (Query 1 from data-model.md)
            query = text(f"""
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    pds.total_downtime_hours,
                    pds.rides_with_downtime AS affected_rides_count,
                    pds.avg_uptime_percentage AS uptime_percentage,
                    prev.total_downtime_hours AS prev_day_downtime,
                    ROUND(
                        ((pds.total_downtime_hours - IFNULL(prev.total_downtime_hours, 0)) /
                         NULLIF(prev.total_downtime_hours, 0)) * 100,
                        2
                    ) AS trend_percentage
                FROM parks p
                INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
                LEFT JOIN park_daily_stats prev ON p.park_id = prev.park_id
                    AND prev.stat_date = DATE_SUB(pds.stat_date, INTERVAL 1 DAY)
                WHERE pds.stat_date = :stat_date
                    AND p.is_active = TRUE
                    {disney_filter}
                ORDER BY pds.total_downtime_hours DESC
                LIMIT :limit
            """)

        result = self.conn.execute(query, {
            "stat_date": stat_date,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_park_weekly_rankings(
        self,
        year: int,
        week_number: int,
        filter_disney_universal: bool = False,
        limit: int = 50,
        weighted: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings by downtime for a specific week.

        Args:
            year: Year
            week_number: ISO week number
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Maximum results
            weighted: Use weighted scoring by ride tier

        Returns:
            List of park ranking dictionaries
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Query from data-model.md Query 1 (7-day rankings)
        query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pws.total_downtime_hours,
                pws.rides_with_downtime AS affected_rides_count,
                pws.avg_uptime_percentage AS uptime_percentage,
                pws.trend_vs_previous_week AS trend_percentage
            FROM parks p
            INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
            WHERE pws.year = :year
                AND pws.week_number = :week_number
                AND p.is_active = TRUE
                {disney_filter}
            ORDER BY pws.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "year": year,
            "week_number": week_number,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_park_monthly_rankings(
        self,
        year: int,
        month: int,
        filter_disney_universal: bool = False,
        limit: int = 50,
        weighted: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings by downtime for a specific month.

        Args:
            year: Year
            month: Month (1-12)
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Maximum results
            weighted: Use weighted scoring by ride tier

        Returns:
            List of park ranking dictionaries
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pms.total_downtime_hours,
                pms.rides_with_downtime AS affected_rides_count,
                pms.avg_uptime_percentage AS uptime_percentage,
                pms.trend_vs_previous_month AS trend_percentage
            FROM parks p
            INNER JOIN park_monthly_stats pms ON p.park_id = pms.park_id
            WHERE pms.year = :year
                AND pms.month = :month
                AND p.is_active = TRUE
                {disney_filter}
            ORDER BY pms.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "year": year,
            "month": month,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]

    def get_aggregate_park_stats(
        self,
        period: str,
        filter_disney_universal: bool = False
    ) -> Dict[str, Any]:
        """
        Get aggregate statistics for all parks.

        Args:
            period: 'today', '7days', or '30days'
            filter_disney_universal: Filter to only Disney/Universal parks

        Returns:
            Dictionary with aggregate statistics
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        if period == 'today':
            query = text(f"""
                SELECT
                    COUNT(DISTINCT p.park_id) AS total_parks_tracked,
                    MAX(pds.total_downtime_hours) AS peak_downtime_hours,
                    SUM(pds.rides_with_downtime) AS currently_down_rides
                FROM parks p
                INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
                WHERE pds.stat_date = CURDATE()
                    AND p.is_active = TRUE
                    {disney_filter}
            """)
        elif period == '7days':
            query = text(f"""
                SELECT
                    COUNT(DISTINCT p.park_id) AS total_parks_tracked,
                    MAX(pws.total_downtime_hours) AS peak_downtime_hours,
                    SUM(pws.rides_with_downtime) AS currently_down_rides
                FROM parks p
                INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
                WHERE pws.year = YEAR(CURDATE())
                    AND pws.week_number = WEEK(CURDATE(), 3)
                    AND p.is_active = TRUE
                    {disney_filter}
            """)
        else:  # 30days
            query = text(f"""
                SELECT
                    COUNT(DISTINCT p.park_id) AS total_parks_tracked,
                    MAX(pms.total_downtime_hours) AS peak_downtime_hours,
                    SUM(pms.rides_with_downtime) AS currently_down_rides
                FROM parks p
                INNER JOIN park_monthly_stats pms ON p.park_id = pms.park_id
                WHERE pms.year = YEAR(CURDATE())
                    AND pms.month = MONTH(CURDATE())
                    AND p.is_active = TRUE
                    {disney_filter}
            """)

        result = self.conn.execute(query)
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return {
            "total_parks_tracked": 0,
            "peak_downtime_hours": 0.0,
            "currently_down_rides": 0
        }

    def get_park_tier_distribution(self, park_id: int) -> Dict[str, Any]:
        """
        Get ride tier distribution for a park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with tier counts
        """
        query = text("""
            SELECT
                COUNT(*) AS total_rides,
                SUM(CASE WHEN tier = 1 THEN 1 ELSE 0 END) AS tier1_count,
                SUM(CASE WHEN tier = 2 THEN 1 ELSE 0 END) AS tier2_count,
                SUM(CASE WHEN tier = 3 THEN 1 ELSE 0 END) AS tier3_count,
                SUM(CASE WHEN tier IS NULL THEN 1 ELSE 0 END) AS unclassified_count
            FROM rides
            WHERE park_id = :park_id
                AND is_active = TRUE
        """)

        result = self.conn.execute(query, {"park_id": park_id})
        row = result.fetchone()
        return dict(row._mapping) if row else {}

    def get_park_current_status(self, park_id: int) -> Dict[str, Any]:
        """
        Get current ride status summary for a park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with current status counts
        """
        query = text("""
            SELECT
                COUNT(DISTINCT r.ride_id) AS total_rides,
                SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS rides_open,
                SUM(CASE WHEN rss.computed_is_open = FALSE THEN 1 ELSE 0 END) AS rides_closed,
                MAX(rss.recorded_at) AS last_updated
            FROM rides r
            LEFT JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                AND rss.recorded_at = (
                    SELECT MAX(recorded_at)
                    FROM ride_status_snapshots
                    WHERE ride_id = r.ride_id
                )
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
        """)

        result = self.conn.execute(query, {"park_id": park_id})
        row = result.fetchone()
        return dict(row._mapping) if row else {}
