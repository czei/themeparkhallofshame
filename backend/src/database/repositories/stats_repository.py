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
                total_operating_hours
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
                downtime_event_count,
                avg_wait_time,
                peak_wait_time,
                total_operating_minutes
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
                downtime_event_count,
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
                operating_date,
                park_opened_at,
                park_closed_at,
                total_operating_hours,
                first_activity_detected_at,
                last_activity_detected_at
            FROM park_operating_sessions
            WHERE park_id = :park_id
                AND operating_date >= COALESCE(:start_date, DATE_SUB(CURDATE(), INTERVAL 30 DAY))
                AND operating_date <= COALESCE(:end_date, CURDATE())
            ORDER BY operating_date DESC
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
