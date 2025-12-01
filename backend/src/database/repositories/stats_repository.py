"""
Theme Park Downtime Tracker - Statistics Repository
Provides data access layer for aggregate statistics tables.

DEPRECATION NOTICE
==================
This file is being phased out in favor of modular query classes.

For new development, use the query classes in database/queries/ instead:

    Rankings (tables):
    - GET /parks/downtime?period=7days → queries/rankings/park_downtime_rankings.py
    - GET /rides/downtime?period=7days → queries/rankings/ride_downtime_rankings.py
    - GET /parks/waittimes            → queries/rankings/park_wait_time_rankings.py
    - GET /rides/waittimes            → queries/rankings/ride_wait_time_rankings.py

    Trends (improving/declining):
    - GET /trends?category=parks-improving → queries/trends/improving_parks.py
    - GET /trends?category=parks-declining → queries/trends/declining_parks.py
    - GET /trends?category=rides-improving → queries/trends/improving_rides.py
    - GET /trends?category=rides-declining → queries/trends/declining_rides.py

    Charts (time series):
    - GET /trends/chart-data?type=parks → queries/charts/park_shame_history.py
    - GET /trends/chart-data?type=rides → queries/charts/ride_downtime_history.py

    Live data (today):
    - GET /live/status-summary        → queries/live/status_summary.py
    - GET /parks/downtime?period=today → queries/live/live_park_rankings.py
    - GET /rides/downtime?period=today → queries/live/live_ride_rankings.py

Methods still in use (not yet migrated):
- get_aggregate_park_stats()
- get_park_tier_distribution()
- get_park_operating_sessions()
- get_park_current_status()

Migration Guide:
1. Import the specific query class you need
2. Instantiate with connection: query = ParkDowntimeRankingsQuery(conn)
3. Call the appropriate method: results = query.get_weekly(year, week)
"""

from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...utils.logger import logger
    from ...utils.timezone import get_today_pacific, get_pacific_day_range_utc
    from ...utils.sql_helpers import (
        RideStatusSQL, ParkStatusSQL, DowntimeSQL, UptimeSQL,
        RideFilterSQL, AffectedRidesSQL, ShameScoreSQL
    )
    from ...utils.metrics import (
        SNAPSHOT_INTERVAL_MINUTES,
        calculate_shame_score,
        calculate_instantaneous_shame_score,
        calculate_downtime_hours
    )
except ImportError:
    from utils.logger import logger
    from utils.timezone import get_today_pacific, get_pacific_day_range_utc
    from utils.sql_helpers import (
        RideStatusSQL, ParkStatusSQL, DowntimeSQL, UptimeSQL,
        RideFilterSQL, AffectedRidesSQL, ShameScoreSQL
    )
    from utils.metrics import (
        SNAPSHOT_INTERVAL_MINUTES,
        calculate_shame_score,
        calculate_instantaneous_shame_score,
        calculate_downtime_hours
    )


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
                        AND r.category = 'ATTRACTION'  -- Only include mechanical rides
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
                        AND r.category = 'ATTRACTION'  -- Only include mechanical rides
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
                    ROUND((wd.total_weighted_downtime_hours / pw.total_park_weight) * 10, 1) AS shame_score,
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
                        AND r2.category = 'ATTRACTION'  -- Only include mechanical rides
                    LEFT JOIN ride_classifications rc2 ON r2.ride_id = rc2.ride_id
                    INNER JOIN ride_daily_stats rds2 ON r2.ride_id = rds2.ride_id
                    WHERE rds2.stat_date = DATE_SUB(:stat_date, INTERVAL 1 DAY)
                        AND p2.is_active = TRUE
                    GROUP BY p2.park_id
                ) prev_wd ON p.park_id = prev_wd.park_id
                WHERE p.is_active = TRUE
                    AND wd.total_weighted_downtime_hours > 0  -- Only show parks with actual downtime (Hall of Shame)
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
                    AND pds.operating_hours_minutes > 0  -- Exclude closed parks
                    AND pds.total_downtime_hours > 0  -- Only show parks with actual downtime (Hall of Shame)
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
        Aggregates from daily stats instead of relying on weekly_stats table.

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

        # Calculate date range for the week (last 7 days including today)
        end_date = date.today()
        start_date = end_date - timedelta(days=6)

        # Aggregate from daily stats instead of weekly stats table
        # Include shame_score calculation using tier weights
        query = text(f"""
            WITH park_weights AS (
                -- Calculate total weight for each park based on tier classifications
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id
            ),
            weighted_downtime AS (
                -- Calculate weighted downtime from ride daily stats
                SELECT
                    p.park_id,
                    SUM(rds.downtime_minutes / 60.0 * COALESCE(rc.tier_weight, 2)) AS total_weighted_downtime_hours
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
                WHERE rds.stat_date BETWEEN :start_date AND :end_date
                    AND p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id
            )
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                ROUND(SUM(pds.total_downtime_hours), 2) AS total_downtime_hours,
                ROUND((wd.total_weighted_downtime_hours / pw.total_park_weight) * 10, 1) AS shame_score,
                MAX(pds.rides_with_downtime) AS affected_rides_count,
                ROUND(AVG(pds.avg_uptime_percentage), 2) AS uptime_percentage,
                NULL AS trend_percentage
            FROM parks p
            INNER JOIN park_daily_stats pds ON p.park_id = pds.park_id
            LEFT JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN weighted_downtime wd ON p.park_id = wd.park_id
            WHERE pds.stat_date BETWEEN :start_date AND :end_date
                AND p.is_active = TRUE
                AND pds.operating_hours_minutes > 0
                {disney_filter}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight, wd.total_weighted_downtime_hours
            HAVING SUM(pds.total_downtime_hours) > 0
            ORDER BY shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
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

        # Include shame_score calculation using tier weights from ride_daily_stats
        query = text(f"""
            WITH park_weights AS (
                -- Calculate total weight for each park based on tier classifications
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id
            ),
            weighted_downtime AS (
                -- Calculate weighted downtime from ride daily stats for the month
                SELECT
                    p.park_id,
                    SUM(rds.downtime_minutes / 60.0 * COALESCE(rc.tier_weight, 2)) AS total_weighted_downtime_hours
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
                WHERE YEAR(rds.stat_date) = :year AND MONTH(rds.stat_date) = :month
                    AND p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id
            )
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pms.total_downtime_hours,
                ROUND((wd.total_weighted_downtime_hours / NULLIF(pw.total_park_weight, 0)) * 10, 1) AS shame_score,
                pms.rides_with_downtime AS affected_rides_count,
                pms.avg_uptime_percentage AS uptime_percentage,
                pms.trend_vs_previous_month AS trend_percentage
            FROM parks p
            INNER JOIN park_monthly_stats pms ON p.park_id = pms.park_id
            LEFT JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN weighted_downtime wd ON p.park_id = wd.park_id
            WHERE pms.year = :year
                AND pms.month = :month
                AND p.is_active = TRUE
                {disney_filter}
            ORDER BY shame_score DESC
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

        Computes from LIVE data for real-time accuracy:
        - total_parks_tracked: Count of active parks
        - peak_downtime_hours: Peak ride downtime from recent data
        - currently_down_rides: Count of rides currently showing as down

        Args:
            period: 'today', '7days', or '30days'
            filter_disney_universal: Filter to only Disney/Universal parks

        Returns:
            Dictionary with aggregate statistics
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""
        disney_filter_pk = disney_filter.replace('p.', 'pk.')

        # Use Pacific day bounds for consistency with ride rankings table
        today_pacific = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today_pacific)

        if period == 'today':
            # Compute from LIVE data - active parks and current snapshots
            query = text(f"""
                SELECT
                    (SELECT COUNT(DISTINCT p.park_id)
                     FROM parks p
                     WHERE p.is_active = TRUE
                     {disney_filter}) AS total_parks_tracked,
                    COALESCE((
                        SELECT ROUND(MAX(rds.downtime_minutes) / 60.0, 2)
                        FROM ride_daily_stats rds
                        WHERE rds.stat_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                    ), 0) AS peak_downtime_hours,
                    COALESCE((
                        SELECT COUNT(DISTINCT r.ride_id)
                        FROM rides r
                        JOIN parks pk ON r.park_id = pk.park_id
                        WHERE r.is_active = TRUE
                            AND r.category = 'ATTRACTION'
                            -- Currently showing as DOWN (not CLOSED or REFURBISHMENT)
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss
                                WHERE rss.ride_id = r.ride_id
                                    AND (rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = FALSE))
                                    AND rss.recorded_at = (
                                        SELECT MAX(rss2.recorded_at)
                                        FROM ride_status_snapshots rss2
                                        WHERE rss2.ride_id = r.ride_id
                                    )
                            )
                            -- Park is currently operating
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss3
                                JOIN rides r3 ON rss3.ride_id = r3.ride_id
                                WHERE r3.park_id = pk.park_id
                                    AND rss3.wait_time > 0
                                    AND rss3.recorded_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
                            )
                            -- Ride had some uptime today (was running at some point - excludes all-day closures)
                            -- Uses Pacific day bounds for consistency with ride rankings table
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss4
                                JOIN park_activity_snapshots pas ON pas.park_id = pk.park_id
                                    AND pas.recorded_at = rss4.recorded_at
                                WHERE rss4.ride_id = r.ride_id
                                    AND (rss4.status = 'OPERATING' OR (rss4.status IS NULL AND rss4.computed_is_open = TRUE))
                                    AND pas.park_appears_open = TRUE
                                    AND rss4.recorded_at >= :start_utc
                                    AND rss4.recorded_at < :end_utc
                            )
                        {disney_filter_pk}
                    ), 0) AS currently_down_rides
            """)
        elif period == '7days':
            # Aggregate from park_daily_stats for last 7 days
            query = text(f"""
                SELECT
                    (SELECT COUNT(DISTINCT p.park_id)
                     FROM parks p
                     WHERE p.is_active = TRUE
                     {disney_filter}) AS total_parks_tracked,
                    COALESCE((
                        SELECT ROUND(MAX(rds.downtime_minutes) / 60.0, 2)
                        FROM ride_daily_stats rds
                        WHERE rds.stat_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    ), 0) AS peak_downtime_hours,
                    COALESCE((
                        SELECT COUNT(DISTINCT r.ride_id)
                        FROM rides r
                        JOIN parks pk ON r.park_id = pk.park_id
                        WHERE r.is_active = TRUE
                            AND r.category = 'ATTRACTION'
                            -- Currently showing as DOWN (not CLOSED or REFURBISHMENT)
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss
                                WHERE rss.ride_id = r.ride_id
                                    AND (rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = FALSE))
                                    AND rss.recorded_at = (
                                        SELECT MAX(rss2.recorded_at)
                                        FROM ride_status_snapshots rss2
                                        WHERE rss2.ride_id = r.ride_id
                                    )
                            )
                            -- Park is currently operating
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss3
                                JOIN rides r3 ON rss3.ride_id = r3.ride_id
                                WHERE r3.park_id = pk.park_id
                                    AND rss3.wait_time > 0
                                    AND rss3.recorded_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
                            )
                            -- Ride had some uptime today (was running at some point - excludes all-day closures)
                            -- Uses Pacific day bounds for consistency with ride rankings table
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss4
                                JOIN park_activity_snapshots pas ON pas.park_id = pk.park_id
                                    AND pas.recorded_at = rss4.recorded_at
                                WHERE rss4.ride_id = r.ride_id
                                    AND (rss4.status = 'OPERATING' OR (rss4.status IS NULL AND rss4.computed_is_open = TRUE))
                                    AND pas.park_appears_open = TRUE
                                    AND rss4.recorded_at >= :start_utc
                                    AND rss4.recorded_at < :end_utc
                            )
                        {disney_filter_pk}
                    ), 0) AS currently_down_rides
            """)
        else:  # 30days
            # Aggregate from park_daily_stats for last 30 days
            query = text(f"""
                SELECT
                    (SELECT COUNT(DISTINCT p.park_id)
                     FROM parks p
                     WHERE p.is_active = TRUE
                     {disney_filter}) AS total_parks_tracked,
                    COALESCE((
                        SELECT ROUND(MAX(rds.downtime_minutes) / 60.0, 2)
                        FROM ride_daily_stats rds
                        WHERE rds.stat_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    ), 0) AS peak_downtime_hours,
                    COALESCE((
                        SELECT COUNT(DISTINCT r.ride_id)
                        FROM rides r
                        JOIN parks pk ON r.park_id = pk.park_id
                        WHERE r.is_active = TRUE
                            AND r.category = 'ATTRACTION'
                            -- Currently showing as DOWN (not CLOSED or REFURBISHMENT)
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss
                                WHERE rss.ride_id = r.ride_id
                                    AND (rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = FALSE))
                                    AND rss.recorded_at = (
                                        SELECT MAX(rss2.recorded_at)
                                        FROM ride_status_snapshots rss2
                                        WHERE rss2.ride_id = r.ride_id
                                    )
                            )
                            -- Park is currently operating
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss3
                                JOIN rides r3 ON rss3.ride_id = r3.ride_id
                                WHERE r3.park_id = pk.park_id
                                    AND rss3.wait_time > 0
                                    AND rss3.recorded_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
                            )
                            -- Ride had some uptime today (was running at some point - excludes all-day closures)
                            -- Uses Pacific day bounds for consistency with ride rankings table
                            AND EXISTS (
                                SELECT 1 FROM ride_status_snapshots rss4
                                JOIN park_activity_snapshots pas ON pas.park_id = pk.park_id
                                    AND pas.recorded_at = rss4.recorded_at
                                WHERE rss4.ride_id = r.ride_id
                                    AND (rss4.status = 'OPERATING' OR (rss4.status IS NULL AND rss4.computed_is_open = TRUE))
                                    AND pas.park_appears_open = TRUE
                                    AND rss4.recorded_at >= :start_utc
                                    AND rss4.recorded_at < :end_utc
                            )
                        {disney_filter_pk}
                    ), 0) AS currently_down_rides
            """)

        result = self.conn.execute(query, {"start_utc": start_utc, "end_utc": end_utc})
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
                SUM(CASE WHEN rc.tier = 1 THEN 1 ELSE 0 END) AS tier_1_count,
                SUM(CASE WHEN rc.tier = 2 THEN 1 ELSE 0 END) AS tier_2_count,
                SUM(CASE WHEN rc.tier = 3 THEN 1 ELSE 0 END) AS tier_3_count,
                SUM(CASE WHEN rc.tier IS NULL THEN 1 ELSE 0 END) AS unclassified_count
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
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
                SUM(CASE
                    WHEN (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = TRUE))
                    THEN 1 ELSE 0
                END) AS rides_open,
                SUM(CASE
                    WHEN (rss.status IN ('DOWN', 'CLOSED', 'REFURBISHMENT') OR (rss.status IS NULL AND rss.computed_is_open = FALSE))
                    THEN 1 ELSE 0
                END) AS rides_closed,
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
                AND r.category = 'ATTRACTION'  -- Only include mechanical rides
        """)

        result = self.conn.execute(query, {"park_id": park_id})
        row = result.fetchone()
        return dict(row._mapping) if row else {}

    def get_park_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get detailed shame score breakdown for a park.

        Returns the list of rides currently down with their tier weights,
        total park weight, and the calculated shame score.

        CRITICAL: Shame score only counts rides CURRENTLY down (latest snapshot).

        Args:
            park_id: Park ID

        Returns:
            Dictionary with:
            - rides_down: List of rides currently down with tier info
            - total_park_weight: Sum of all tier weights
            - shame_score: Calculated shame score
            - park_is_open: Whether park appears open
        """
        from utils.timezone import get_today_pacific, get_pacific_day_range_utc
        from utils.sql_helpers import RideStatusSQL, ParkStatusSQL

        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Get total park weight - ONLY for rides that have operated today
        # This matches the logic in get_park_live_downtime_rankings()
        is_operating_check = RideStatusSQL.is_operating("rss_weight")
        weight_query = text(f"""
            WITH rides_that_operated_weight AS (
                SELECT DISTINCT rss_weight.ride_id
                FROM ride_status_snapshots rss_weight
                WHERE rss_weight.recorded_at >= :start_utc AND rss_weight.recorded_at < :end_utc
                    AND ({is_operating_check})
            )
            SELECT SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
            FROM rides r
            INNER JOIN rides_that_operated_weight rto ON r.ride_id = rto.ride_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
        """)
        weight_result = self.conn.execute(weight_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        weight_row = weight_result.fetchone()
        total_park_weight = float(weight_row.total_park_weight) if weight_row and weight_row.total_park_weight else 0

        # Check if park appears open (using 50% threshold)
        is_down = RideStatusSQL.is_down("rss")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        park_open_query = text(f"""
            SELECT
                (SELECT COUNT(*)
                 FROM rides r2
                 WHERE r2.park_id = :park_id
                   AND r2.is_active = TRUE
                   AND r2.category = 'ATTRACTION') as total_rides,
                pas.park_appears_open,
                {park_open} as meets_threshold
            FROM park_activity_snapshots pas
            WHERE pas.park_id = :park_id
                AND pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
            ORDER BY pas.recorded_at DESC
            LIMIT 1
        """)
        park_open_result = self.conn.execute(park_open_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        park_open_row = park_open_result.fetchone()
        park_is_open = bool(park_open_row and park_open_row.meets_threshold) if park_open_row else False

        # CRITICAL: Only count rides that have operated at some point today
        # This ensures consistency with get_park_live_downtime_rankings()
        # Rides that never showed OPERATING status are likely seasonal closures
        is_operating = RideStatusSQL.is_operating("rss_check")

        # Get rides currently down with tier info
        rides_down_query = text(f"""
            WITH latest_snapshot AS (
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            ),
            rides_that_operated AS (
                -- Only include rides that have shown OPERATING at some point today
                -- This excludes seasonal closures and scheduled maintenance
                SELECT DISTINCT rss_check.ride_id
                FROM ride_status_snapshots rss_check
                WHERE rss_check.recorded_at >= :start_utc AND rss_check.recorded_at < :end_utc
                    AND ({is_operating})
            )
            SELECT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier,
                COALESCE(rc.tier_weight, 2) AS tier_weight,
                rss.status,
                rss.recorded_at
            FROM rides r
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN latest_snapshot ls ON rss.ride_id = ls.ride_id
                AND rss.recorded_at = ls.latest_recorded_at
            INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            INNER JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND {is_down}
                AND {park_open}
            ORDER BY tier ASC, r.name ASC
        """)

        rides_down_result = self.conn.execute(rides_down_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })

        rides_down = []
        total_weighted_down = 0
        for row in rides_down_result:
            ride_data = {
                "ride_id": row.ride_id,
                "ride_name": row.ride_name,
                "tier": row.tier,
                "tier_weight": float(row.tier_weight),
                "status": row.status
            }
            rides_down.append(ride_data)
            total_weighted_down += float(row.tier_weight)

        # Calculate INSTANTANEOUS shame score using centralized function (single source of truth)
        # This is the same calculation used by the table rankings
        shame_score = calculate_instantaneous_shame_score(total_weighted_down, total_park_weight) or 0.0

        return {
            "rides_down": rides_down,
            "total_park_weight": total_park_weight,
            "total_weighted_down": total_weighted_down,
            "shame_score": shame_score,
            "park_is_open": park_is_open,
            "breakdown_type": "live",
            "tier_weights": {
                1: 5,  # Tier 1 (Flagship) = 5x weight
                2: 2,  # Tier 2 (Standard) = 2x weight
                3: 1   # Tier 3 (Minor) = 1x weight
            }
        }

    def get_park_today_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get CUMULATIVE shame score breakdown for a park (today period).

        Unlike get_park_shame_breakdown() which shows LIVE/instantaneous data,
        this returns cumulative downtime data for the entire day:
        - All rides that had ANY downtime today (not just currently down)
        - Total downtime hours per ride
        - Cumulative shame score based on weighted downtime

        This matches what the "Today" period shows in the rankings table.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with:
            - rides_with_downtime: List of rides that had downtime today
            - total_park_weight: Sum of all tier weights for operated rides
            - total_downtime_hours: Total cumulative downtime hours
            - weighted_downtime_hours: Total weighted downtime hours
            - shame_score: Cumulative shame score
            - park_is_open: Whether park is currently open
        """
        from utils.timezone import get_today_pacific, get_pacific_day_range_utc
        from utils.sql_helpers import RideStatusSQL, ParkStatusSQL
        from utils.metrics import SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION

        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Snapshot interval in minutes
        SNAPSHOT_INTERVAL_MINUTES = 5

        is_down = RideStatusSQL.is_down("rss")
        is_operating = RideStatusSQL.is_operating("rss_op")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Get total park weight - ONLY for rides that have operated today
        weight_query = text(f"""
            WITH rides_that_operated AS (
                SELECT DISTINCT rss_op.ride_id
                FROM ride_status_snapshots rss_op
                WHERE rss_op.recorded_at >= :start_utc AND rss_op.recorded_at < :end_utc
                    AND ({is_operating})
            )
            SELECT SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
            FROM rides r
            INNER JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
        """)
        weight_result = self.conn.execute(weight_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        weight_row = weight_result.fetchone()
        total_park_weight = float(weight_row.total_park_weight) if weight_row and weight_row.total_park_weight else 0

        # Check if park is currently open
        park_open_query = text(f"""
            SELECT p.park_id, {park_is_open_sq}
            FROM parks p
            WHERE p.park_id = :park_id
        """)
        park_open_result = self.conn.execute(park_open_query, {"park_id": park_id})
        park_open_row = park_open_result.fetchone()
        park_is_open = bool(park_open_row and park_open_row.park_is_open) if park_open_row else False

        # Get rides that had ANY downtime today with cumulative hours
        # CRITICAL: Only count rides that have operated at some point today
        rides_query = text(f"""
            WITH rides_that_operated AS (
                SELECT DISTINCT rss_op.ride_id
                FROM ride_status_snapshots rss_op
                WHERE rss_op.recorded_at >= :start_utc AND rss_op.recorded_at < :end_utc
                    AND ({is_operating})
            ),
            latest_snapshot AS (
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            )
            SELECT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier,
                COALESCE(rc.tier_weight, 2) AS tier_weight,
                -- Total downtime hours for this ride today
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open}
                        THEN {SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS downtime_hours,
                -- Weighted downtime hours
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open}
                        THEN ({SNAPSHOT_INTERVAL_MINUTES} / 60.0) * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END),
                    2
                ) AS weighted_downtime_hours,
                -- Is this ride currently down?
                MAX(CASE
                    WHEN rss.recorded_at = ls.latest_recorded_at AND {is_down}
                    THEN 1 ELSE 0
                END) AS is_currently_down,
                -- Latest status
                (SELECT rss2.status FROM ride_status_snapshots rss2
                 WHERE rss2.ride_id = r.ride_id
                 ORDER BY rss2.recorded_at DESC LIMIT 1) AS current_status
            FROM rides r
            INNER JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            LEFT JOIN latest_snapshot ls ON r.ride_id = ls.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
            GROUP BY r.ride_id, r.name, rc.tier, rc.tier_weight
            HAVING downtime_hours > 0
            ORDER BY weighted_downtime_hours DESC, downtime_hours DESC
        """)

        rides_result = self.conn.execute(rides_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })

        rides_with_downtime = []
        total_downtime_hours = 0
        total_weighted_downtime = 0
        for row in rides_result:
            ride_data = {
                "ride_id": row.ride_id,
                "ride_name": row.ride_name,
                "tier": row.tier,
                "tier_weight": float(row.tier_weight),
                "downtime_hours": float(row.downtime_hours),
                "weighted_downtime_hours": float(row.weighted_downtime_hours),
                "weighted_contribution": float(row.weighted_downtime_hours),  # Alias for frontend
                "is_currently_down": bool(row.is_currently_down),
                "current_status": row.current_status
            }
            rides_with_downtime.append(ride_data)
            total_downtime_hours += float(row.downtime_hours)
            total_weighted_downtime += float(row.weighted_downtime_hours)

        # Calculate CUMULATIVE shame score = weighted downtime / total weight * 10
        # This is different from instantaneous shame score
        shame_score = round(
            (total_weighted_downtime / total_park_weight * SHAME_SCORE_MULTIPLIER)
            if total_park_weight > 0 else 0,
            SHAME_SCORE_PRECISION
        )

        return {
            "rides_with_downtime": rides_with_downtime,
            "rides_affected_count": len(rides_with_downtime),
            "total_park_weight": total_park_weight,
            "total_downtime_hours": round(total_downtime_hours, 2),
            "weighted_downtime_hours": round(total_weighted_downtime, 2),
            "shame_score": shame_score,
            "park_is_open": park_is_open,
            "breakdown_type": "today",
            "explanation": "Cumulative downtime since midnight Pacific. Shows all rides that experienced any downtime today.",
            "tier_weights": {
                1: 5,  # Tier 1 (Flagship) = 5x weight
                2: 2,  # Tier 2 (Standard) = 2x weight
                3: 1   # Tier 3 (Minor) = 1x weight
            }
        }

    def get_ride_daily_rankings(
        self,
        stat_date: date,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ride downtime rankings for a specific day.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            stat_date: Date to get rankings for
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides ranked by downtime hours with current status
        """
        # Use centralized helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        # Pass park_id_expr to ensure rides at closed parks show PARK_CLOSED, not DOWN
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")

        query = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                rc.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rds.downtime_minutes / 60.0 AS downtime_hours,
                rds.uptime_percentage,
                rds.avg_wait_time,
                rds.peak_wait_time,
                -- Get current status using centralized helper
                {current_status_sq},
                -- Boolean for frontend compatibility using centralized helper
                {current_is_open_sq},
                -- Trend: compare to previous day
                CASE
                    WHEN prev_day.downtime_minutes > 0 THEN
                        ((rds.downtime_minutes - prev_day.downtime_minutes) / prev_day.downtime_minutes * 1.0) * 100
                    ELSE NULL
                END AS trend_percentage
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            LEFT JOIN ride_daily_stats prev_day ON rds.ride_id = prev_day.ride_id
                AND prev_day.stat_date = DATE_SUB(:stat_date, INTERVAL 1 DAY)
            WHERE rds.stat_date = :stat_date
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                AND rds.operating_hours_minutes > 0
                AND rds.downtime_minutes > 0
                {filter_clause}
            ORDER BY rds.downtime_minutes DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "stat_date": stat_date,
            "limit": limit
        })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_ride_weekly_rankings(
        self,
        year: int,
        week_number: int,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ride downtime rankings for a specific week.
        Aggregates from daily stats instead of relying on weekly_stats table.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            year: Year
            week_number: ISO week number
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides ranked by downtime hours with current status
        """
        # Use centralized helpers for consistent logic
        disney_filter = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        # Pass park_id_expr to ensure rides at closed parks show PARK_CLOSED, not DOWN
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")

        # Calculate date range for the week (last 7 days including today)
        end_date = date.today()
        start_date = end_date - timedelta(days=6)

        # Aggregate from daily stats instead of weekly stats table
        query = text(f"""
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                rc.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                ROUND(SUM(rds.downtime_minutes) / 60.0, 2) AS downtime_hours,
                ROUND(AVG(rds.uptime_percentage), 2) AS uptime_percentage,
                ROUND(AVG(rds.avg_wait_time), 2) AS avg_wait_time,
                MAX(rds.peak_wait_time) AS peak_wait_time,
                -- Get current status using centralized helper
                {current_status_sq},
                -- Boolean for frontend compatibility using centralized helper
                {current_is_open_sq},
                NULL AS trend_percentage
            FROM ride_daily_stats rds
            JOIN rides r ON rds.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE rds.stat_date BETWEEN :start_date AND :end_date
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                AND rds.operating_hours_minutes > 0
                {disney_filter}
            GROUP BY r.ride_id, r.queue_times_id, p.queue_times_id, r.name, rc.tier, p.park_id, p.name, p.city, p.state_province
            HAVING SUM(rds.downtime_minutes) > 0
            ORDER BY downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_ride_monthly_rankings(
        self,
        year: int,
        month: int,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ride downtime rankings for a specific month.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            year: Year
            month: Month (1-12)
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides ranked by downtime hours with current status
        """
        # Use centralized helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        # Pass park_id_expr to ensure rides at closed parks show PARK_CLOSED, not DOWN
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")

        query = text(f"""
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                rc.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rms.downtime_minutes / 60.0 AS downtime_hours,
                rms.uptime_percentage,
                rms.avg_wait_time,
                rms.peak_wait_time,
                -- Get current status using centralized helper
                {current_status_sq},
                -- Boolean for frontend compatibility using centralized helper
                {current_is_open_sq},
                -- Trend: compare to previous month
                CASE
                    WHEN prev_month.downtime_minutes > 0 THEN
                        ((rms.downtime_minutes - prev_month.downtime_minutes) / prev_month.downtime_minutes * 1.0) * 100
                    ELSE NULL
                END AS trend_percentage
            FROM ride_monthly_stats rms
            JOIN rides r ON rms.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            LEFT JOIN ride_monthly_stats prev_month ON rms.ride_id = prev_month.ride_id
                AND prev_month.year = :prev_year
                AND prev_month.month = :prev_month
            WHERE rms.year = :year
                AND rms.month = :month
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                {filter_clause}
            ORDER BY rms.downtime_minutes DESC
            LIMIT :limit
        """)

        # Calculate previous month
        prev_year = year if month > 1 else year - 1
        prev_month_num = month - 1 if month > 1 else 12

        result = self.conn.execute(query, {
            "year": year,
            "month": month,
            "prev_year": prev_year,
            "prev_month": prev_month_num,
            "limit": limit
        })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_live_wait_times(
        self,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get current live wait times from most recent snapshots.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides sorted by longest current wait times descending
        """
        query = text("""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                r.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rss.wait_time AS current_wait_minutes,
                rss.computed_is_open AS current_is_open,
                rss.recorded_at AS last_updated,
                -- Get 7-day average for trend comparison
                (
                    SELECT AVG(avg_wait_time)
                    FROM ride_weekly_stats
                    WHERE ride_id = r.ride_id
                        AND year = YEAR(CURDATE())
                        AND week_number = WEEK(CURDATE(), 3)
                ) AS avg_wait_7days,
                -- Trend percentage (current vs 7-day average)
                CASE
                    WHEN (
                        SELECT AVG(avg_wait_time)
                        FROM ride_weekly_stats
                        WHERE ride_id = r.ride_id
                            AND year = YEAR(CURDATE())
                            AND week_number = WEEK(CURDATE(), 3)
                    ) > 0 THEN
                        ((rss.wait_time - (
                            SELECT AVG(avg_wait_time)
                            FROM ride_weekly_stats
                            WHERE ride_id = r.ride_id
                                AND year = YEAR(CURDATE())
                                AND week_number = WEEK(CURDATE(), 3)
                        )) / (
                            SELECT AVG(avg_wait_time)
                            FROM ride_weekly_stats
                            WHERE ride_id = r.ride_id
                                AND year = YEAR(CURDATE())
                                AND week_number = WEEK(CURDATE(), 3)
                        )) * 100
                    ELSE NULL
                END AS trend_percentage
            FROM rides r
            JOIN parks p ON r.park_id = p.park_id
            JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            WHERE rss.recorded_at = (
                    SELECT MAX(recorded_at)
                    FROM ride_status_snapshots
                    WHERE ride_id = r.ride_id
                )
                AND (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = TRUE))
                AND rss.wait_time > 0
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                AND p.is_active = TRUE
                {:filter_clause}
            ORDER BY rss.wait_time DESC
            LIMIT :limit
        """.replace(
            "{:filter_clause}",
            "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""
        ))

        result = self.conn.execute(query, {"limit": limit})
        return [dict(row._mapping) for row in result.fetchall()]

    def get_average_wait_times(
        self,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get 7-day average wait times from weekly stats.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides sorted by longest average wait times descending
        """
        query = text("""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                r.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rws.avg_wait_time AS avg_wait_7days,
                rws.peak_wait_time AS peak_wait_7days,
                -- Get current status
                (
                    SELECT computed_is_open
                    FROM ride_status_snapshots
                    WHERE ride_id = r.ride_id
                    ORDER BY recorded_at DESC
                    LIMIT 1
                ) AS current_is_open,
                -- Trend: compare to previous week's average
                CASE
                    WHEN prev_week.avg_wait_time > 0 THEN
                        ((rws.avg_wait_time - prev_week.avg_wait_time * 1.0) / prev_week.avg_wait_time) * 100
                    ELSE NULL
                END AS trend_percentage
            FROM ride_weekly_stats rws
            JOIN rides r ON rws.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_weekly_stats prev_week ON rws.ride_id = prev_week.ride_id
                AND prev_week.year = :prev_year
                AND prev_week.week_number = :prev_week
            WHERE rws.year = :year
                AND rws.week_number = :week_number
                AND rws.avg_wait_time > 0
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                AND p.is_active = TRUE
                {:filter_clause}
            ORDER BY rws.avg_wait_time DESC
            LIMIT :limit
        """.replace(
            "{:filter_clause}",
            "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""
        ))

        # Calculate current and previous week
        from datetime import datetime, timedelta
        current_date = datetime.now()
        year = current_date.year
        week_number = current_date.isocalendar()[1]

        prev_week_date = current_date - timedelta(weeks=1)
        prev_year = prev_week_date.year
        prev_week_num = prev_week_date.isocalendar()[1]

        result = self.conn.execute(query, {
            "year": year,
            "week_number": week_number,
            "prev_year": prev_year,
            "prev_week": prev_week_num,
            "limit": limit
        })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_peak_wait_times(
        self,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get peak wait times from weekly stats.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides sorted by longest peak wait times descending
        """
        query = text("""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                r.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                rws.peak_wait_time AS peak_wait_7days,
                rws.avg_wait_time AS avg_wait_7days,
                -- Get current status
                (
                    SELECT computed_is_open
                    FROM ride_status_snapshots
                    WHERE ride_id = r.ride_id
                    ORDER BY recorded_at DESC
                    LIMIT 1
                ) AS current_is_open,
                -- Trend: compare to previous week's peak
                CASE
                    WHEN prev_week.peak_wait_time > 0 THEN
                        ((rws.peak_wait_time - prev_week.peak_wait_time * 1.0) / prev_week.peak_wait_time) * 100
                    ELSE NULL
                END AS trend_percentage
            FROM ride_weekly_stats rws
            JOIN rides r ON rws.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            LEFT JOIN ride_weekly_stats prev_week ON rws.ride_id = prev_week.ride_id
                AND prev_week.year = :prev_year
                AND prev_week.week_number = :prev_week
            WHERE rws.year = :year
                AND rws.week_number = :week_number
                AND rws.peak_wait_time > 0
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                AND p.is_active = TRUE
                {:filter_clause}
            ORDER BY rws.peak_wait_time DESC
            LIMIT :limit
        """.replace(
            "{:filter_clause}",
            "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""
        ))

        # Calculate current and previous week
        from datetime import datetime, timedelta
        current_date = datetime.now()
        year = current_date.year
        week_number = current_date.isocalendar()[1]

        prev_week_date = current_date - timedelta(weeks=1)
        prev_year = prev_week_date.year
        prev_week_num = prev_week_date.isocalendar()[1]

        result = self.conn.execute(query, {
            "year": year,
            "week_number": week_number,
            "prev_year": prev_year,
            "prev_week": prev_week_num,
            "limit": limit
        })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_wait_times_by_period(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get wait times for a specified period (today, 7days, 30days).

        Args:
            period: Time period - 'today', '7days', or '30days'
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides sorted by longest average wait times descending
        """
        from datetime import datetime, timedelta

        filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        if period == 'today':
            # Get Pacific day bounds in UTC - "today" means Pacific calendar day
            today_pacific = get_today_pacific()
            start_utc, end_utc = get_pacific_day_range_utc(today_pacific)
            yesterday_pacific = today_pacific - timedelta(days=1)

            # Query today's average wait times from LIVE snapshots (not ride_daily_stats)
            # This provides real-time data that updates every 10 minutes
            query = text(f"""
                SELECT
                    r.ride_id,
                    r.queue_times_id,
                    p.queue_times_id AS park_queue_times_id,
                    r.name AS ride_name,
                    r.tier,
                    p.park_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    ROUND(AVG(rss.wait_time), 0) AS avg_wait_minutes,
                    MAX(rss.wait_time) AS peak_wait_minutes,
                    -- Get current status from latest snapshot
                    (
                        SELECT computed_is_open
                        FROM ride_status_snapshots
                        WHERE ride_id = r.ride_id
                        ORDER BY recorded_at DESC
                        LIMIT 1
                    ) AS current_is_open,
                    -- Park is operating if ANY ride has wait_time > 0 (not just "open" flag)
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open,
                    -- Trend: compare to yesterday's daily stats
                    CASE
                        WHEN prev_day.avg_wait_time > 0 THEN
                            ((AVG(rss.wait_time) - prev_day.avg_wait_time) / prev_day.avg_wait_time) * 100
                        ELSE NULL
                    END AS trend_percentage
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                LEFT JOIN ride_daily_stats prev_day ON r.ride_id = prev_day.ride_id
                    AND prev_day.stat_date = :yesterday_pacific
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND rss.wait_time > 0
                    AND (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = TRUE))
                    AND pas.park_appears_open = TRUE
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                GROUP BY r.ride_id, r.queue_times_id, p.queue_times_id, r.name, r.tier, p.park_id, p.name, p.city, p.state_province, prev_day.avg_wait_time
                HAVING AVG(rss.wait_time) > 0
                ORDER BY avg_wait_minutes DESC
                LIMIT :limit
            """)
            params = {
                "limit": limit,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "yesterday_pacific": yesterday_pacific
            }

        elif period == '7days':
            # Query 7-day average from ride_weekly_stats
            current_date = datetime.now()
            year = current_date.year
            week_number = current_date.isocalendar()[1]
            prev_week_date = current_date - timedelta(weeks=1)
            prev_year = prev_week_date.year
            prev_week_num = prev_week_date.isocalendar()[1]

            query = text(f"""
                SELECT
                    r.ride_id,
                    r.queue_times_id,
                    p.queue_times_id AS park_queue_times_id,
                    r.name AS ride_name,
                    r.tier,
                    p.park_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    rws.avg_wait_time AS avg_wait_minutes,
                    rws.peak_wait_time AS peak_wait_minutes,
                    -- Get current status
                    (
                        SELECT computed_is_open
                        FROM ride_status_snapshots
                        WHERE ride_id = r.ride_id
                        ORDER BY recorded_at DESC
                        LIMIT 1
                    ) AS current_is_open,
                    -- Park is operating if ANY ride has wait_time > 0 (not just "open" flag)
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open,
                    -- Trend: compare to previous week's average
                    CASE
                        WHEN prev_week.avg_wait_time > 0 THEN
                            ((rws.avg_wait_time - prev_week.avg_wait_time) / prev_week.avg_wait_time) * 100
                        ELSE NULL
                    END AS trend_percentage
                FROM ride_weekly_stats rws
                JOIN rides r ON rws.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_weekly_stats prev_week ON rws.ride_id = prev_week.ride_id
                    AND prev_week.year = :prev_year
                    AND prev_week.week_number = :prev_week
                WHERE rws.year = :year
                    AND rws.week_number = :week_number
                    AND rws.avg_wait_time > 0
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                ORDER BY rws.avg_wait_time DESC
                LIMIT :limit
            """)
            params = {
                "year": year,
                "week_number": week_number,
                "prev_year": prev_year,
                "prev_week": prev_week_num,
                "limit": limit
            }

        else:  # 30days
            # Query 30-day average from ride_monthly_stats
            current_date = datetime.now()
            year = current_date.year
            month = current_date.month
            prev_month_date = current_date.replace(day=1) - timedelta(days=1)
            prev_year = prev_month_date.year
            prev_month = prev_month_date.month

            query = text(f"""
                SELECT
                    r.ride_id,
                    r.queue_times_id,
                    p.queue_times_id AS park_queue_times_id,
                    r.name AS ride_name,
                    r.tier,
                    p.park_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    rms.avg_wait_time AS avg_wait_minutes,
                    rms.peak_wait_time AS peak_wait_minutes,
                    -- Get current status
                    (
                        SELECT computed_is_open
                        FROM ride_status_snapshots
                        WHERE ride_id = r.ride_id
                        ORDER BY recorded_at DESC
                        LIMIT 1
                    ) AS current_is_open,
                    -- Park is operating if ANY ride has wait_time > 0 (not just "open" flag)
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open,
                    -- Trend: compare to previous month's average
                    CASE
                        WHEN prev_month.avg_wait_time > 0 THEN
                            ((rms.avg_wait_time - prev_month.avg_wait_time) / prev_month.avg_wait_time) * 100
                        ELSE NULL
                    END AS trend_percentage
                From ride_monthly_stats rms
                JOIN rides r ON rms.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_monthly_stats prev_month ON rms.ride_id = prev_month.ride_id
                    AND prev_month.year = :prev_year
                    AND prev_month.month = :prev_month
                WHERE rms.year = :year
                    AND rms.month = :month
                    AND rms.avg_wait_time > 0
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                ORDER BY rms.avg_wait_time DESC
                LIMIT :limit
            """)
            params = {
                "year": year,
                "month": month,
                "prev_year": prev_year,
                "prev_month": prev_month,
                "limit": limit
            }

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result.fetchall()]

    def get_park_wait_times_by_period(
        self,
        period: str = 'today',
        filter_disney_universal: bool = False,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get park-level wait times aggregated from ride data for a specified period.

        Args:
            period: Time period - 'today', '7days', or '30days'
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of parks to return

        Returns:
            List of parks sorted by longest average wait times descending
        """
        from datetime import datetime, timedelta

        filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        if period == 'today':
            # Get Pacific day bounds in UTC - "today" means Pacific calendar day
            today_pacific = get_today_pacific()
            start_utc, end_utc = get_pacific_day_range_utc(today_pacific)
            yesterday_pacific = today_pacific - timedelta(days=1)

            # Query today's average wait times from LIVE snapshots aggregated by park
            query = text(f"""
                SELECT
                    p.park_id,
                    p.queue_times_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    ROUND(AVG(rss.wait_time), 0) AS avg_wait_minutes,
                    MAX(rss.wait_time) AS peak_wait_minutes,
                    COUNT(DISTINCT r.ride_id) AS rides_reporting,
                    -- Trend: compare to yesterday's park daily stats
                    CASE
                        WHEN prev_day.avg_wait_time > 0 THEN
                            ((AVG(rss.wait_time) - prev_day.avg_wait_time) / prev_day.avg_wait_time) * 100
                        ELSE NULL
                    END AS trend_percentage,
                    -- Park is operating if ANY ride has wait_time > 0
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND r2.category = 'ATTRACTION'  -- Only include mechanical rides
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                LEFT JOIN park_daily_stats prev_day ON p.park_id = prev_day.park_id
                    AND prev_day.stat_date = :yesterday_pacific
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND rss.wait_time > 0
                    AND (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = TRUE))
                    AND pas.park_appears_open = TRUE
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                GROUP BY p.park_id, p.queue_times_id, p.name, p.city, p.state_province, prev_day.avg_wait_time
                HAVING AVG(rss.wait_time) > 0
                ORDER BY avg_wait_minutes DESC
                LIMIT :limit
            """)
            params = {
                "limit": limit,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "yesterday_pacific": yesterday_pacific
            }

        elif period == '7days':
            # Query 7-day average aggregated by park from daily stats
            end_date = date.today()
            start_date = end_date - timedelta(days=6)

            query = text(f"""
                SELECT
                    p.park_id,
                    p.queue_times_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    ROUND(AVG(rds.avg_wait_time), 0) AS avg_wait_minutes,
                    MAX(rds.peak_wait_time) AS peak_wait_minutes,
                    COUNT(DISTINCT r.ride_id) AS rides_reporting,
                    NULL AS trend_percentage,
                    -- Park is operating if ANY ride has wait_time > 0
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND r2.category = 'ATTRACTION'  -- Only include mechanical rides
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open
                FROM ride_daily_stats rds
                JOIN rides r ON rds.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                WHERE rds.stat_date BETWEEN :start_date AND :end_date
                    AND rds.avg_wait_time > 0
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                GROUP BY p.park_id, p.queue_times_id, p.name, p.city, p.state_province
                HAVING AVG(rds.avg_wait_time) > 0
                ORDER BY avg_wait_minutes DESC
                LIMIT :limit
            """)
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            }

        else:  # 30days
            # Query 30-day average aggregated by park from daily stats
            end_date = date.today()
            start_date = end_date - timedelta(days=29)

            query = text(f"""
                SELECT
                    p.park_id,
                    p.queue_times_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    ROUND(AVG(rds.avg_wait_time), 0) AS avg_wait_minutes,
                    MAX(rds.peak_wait_time) AS peak_wait_minutes,
                    COUNT(DISTINCT r.ride_id) AS rides_reporting,
                    NULL AS trend_percentage,
                    -- Park is operating if ANY ride has wait_time > 0
                    (
                        SELECT CASE WHEN MAX(rss2.wait_time) > 0 THEN 1 ELSE 0 END
                        FROM ride_status_snapshots rss2
                        JOIN rides r2 ON rss2.ride_id = r2.ride_id
                        WHERE r2.park_id = p.park_id
                        AND r2.category = 'ATTRACTION'  -- Only include mechanical rides
                        AND rss2.recorded_at = (
                            SELECT MAX(recorded_at)
                            FROM ride_status_snapshots
                            WHERE ride_id = rss2.ride_id
                        )
                    ) AS park_is_open
                FROM ride_daily_stats rds
                JOIN rides r ON rds.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                WHERE rds.stat_date BETWEEN :start_date AND :end_date
                    AND rds.avg_wait_time > 0
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'  -- Only include mechanical rides
                    AND p.is_active = TRUE
                    {filter_clause}
                GROUP BY p.park_id, p.queue_times_id, p.name, p.city, p.state_province
                HAVING AVG(rds.avg_wait_time) > 0
                ORDER BY avg_wait_minutes DESC
                LIMIT :limit
            """)
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit
            }

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result.fetchall()]

    # ========================================
    # Trend Analysis Methods (User Story 8)
    # ========================================

    def get_parks_improving(
        self,
        period: str = '7days',
        park_filter: str = 'all-parks',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get parks showing ≥5% uptime improvement (Query 8).

        Args:
            period: 'today', '7days', or '30days'
            park_filter: 'disney-universal' or 'all-parks'
            limit: Maximum number of results

        Returns:
            List of parks with improvement metrics
        """
        from datetime import datetime, timedelta

        today = datetime.now().date()

        if period == 'today':
            # Daily comparison: today vs yesterday
            current_date = today
            previous_date = today - timedelta(days=1)

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pds.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pds.avg_uptime_percentage,
                        pds.total_downtime_hours
                    FROM park_daily_stats pds
                    JOIN parks p ON pds.park_id = p.park_id
                    WHERE pds.stat_date = :current_date
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pds.park_id,
                        pds.avg_uptime_percentage,
                        pds.total_downtime_hours
                    FROM park_daily_stats pds
                    JOIN parks p ON pds.park_id = p.park_id
                    WHERE pds.stat_date = :previous_date
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (cp.avg_uptime_percentage - pp.avg_uptime_percentage) AS improvement_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (cp.avg_uptime_percentage - pp.avg_uptime_percentage) >= :threshold
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_date": current_date,
                "previous_date": previous_date,
                "threshold": 2.0,  # Lower threshold for daily comparisons
                "limit": limit
            })

        elif period == '7days':
            # Weekly comparison: current week vs previous week
            current_week = today.isocalendar()[1]
            current_year = today.year

            prev_week_date = today - timedelta(weeks=1)
            prev_week = prev_week_date.isocalendar()[1]
            prev_year = prev_week_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pws.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pws.avg_uptime_percentage,
                        pws.total_downtime_hours
                    FROM park_weekly_stats pws
                    JOIN parks p ON pws.park_id = p.park_id
                    WHERE pws.year = :current_year
                        AND pws.week_number = :current_week
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pws.park_id,
                        pws.avg_uptime_percentage,
                        pws.total_downtime_hours
                    FROM park_weekly_stats pws
                    JOIN parks p ON pws.park_id = p.park_id
                    WHERE pws.year = :prev_year
                        AND pws.week_number = :prev_week
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (cp.avg_uptime_percentage - pp.avg_uptime_percentage) AS improvement_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (cp.avg_uptime_percentage - pp.avg_uptime_percentage) >= 5.0
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_week": current_week,
                "prev_year": prev_year,
                "prev_week": prev_week,
                "limit": limit
            })

        elif period == '30days':
            # Monthly comparison: current month vs previous month
            current_month = today.month
            current_year = today.year

            prev_month_date = today - timedelta(days=30)
            prev_month = prev_month_date.month
            prev_year = prev_month_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pms.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pms.avg_uptime_percentage,
                        pms.total_downtime_hours
                    FROM park_monthly_stats pms
                    JOIN parks p ON pms.park_id = p.park_id
                    WHERE pms.year = :current_year
                        AND pms.month = :current_month
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pms.park_id,
                        pms.avg_uptime_percentage,
                        pms.total_downtime_hours
                    FROM park_monthly_stats pms
                    JOIN parks p ON pms.park_id = p.park_id
                    WHERE pms.year = :prev_year
                        AND pms.month = :prev_month
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (cp.avg_uptime_percentage - pp.avg_uptime_percentage) AS improvement_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (cp.avg_uptime_percentage - pp.avg_uptime_percentage) >= 5.0
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_month": current_month,
                "prev_year": prev_year,
                "prev_month": prev_month,
                "limit": limit
            })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_parks_declining(
        self,
        period: str = '7days',
        park_filter: str = 'all-parks',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get parks showing ≥5% uptime decline (Query 9).

        Args:
            period: 'today', '7days', or '30days'
            park_filter: 'disney-universal' or 'all-parks'
            limit: Maximum number of results

        Returns:
            List of parks with decline metrics
        """
        from datetime import datetime, timedelta

        today = datetime.now().date()

        if period == 'today':
            # Daily comparison: today vs yesterday
            current_date = today
            previous_date = today - timedelta(days=1)

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pds.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pds.avg_uptime_percentage,
                        pds.total_downtime_hours
                    FROM park_daily_stats pds
                    JOIN parks p ON pds.park_id = p.park_id
                    WHERE pds.stat_date = :current_date
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pds.park_id,
                        pds.avg_uptime_percentage,
                        pds.total_downtime_hours
                    FROM park_daily_stats pds
                    JOIN parks p ON pds.park_id = p.park_id
                    WHERE pds.stat_date = :previous_date
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (pp.avg_uptime_percentage - cp.avg_uptime_percentage) AS decline_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (pp.avg_uptime_percentage - cp.avg_uptime_percentage) >= :threshold
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_date": current_date,
                "previous_date": previous_date,
                "threshold": 2.0,  # Lower threshold for daily comparisons
                "limit": limit
            })

        elif period == '7days':
            # Weekly comparison: current week vs previous week
            current_week = today.isocalendar()[1]
            current_year = today.year

            prev_week_date = today - timedelta(weeks=1)
            prev_week = prev_week_date.isocalendar()[1]
            prev_year = prev_week_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pws.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pws.avg_uptime_percentage,
                        pws.total_downtime_hours
                    FROM park_weekly_stats pws
                    JOIN parks p ON pws.park_id = p.park_id
                    WHERE pws.year = :current_year
                        AND pws.week_number = :current_week
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pws.park_id,
                        pws.avg_uptime_percentage,
                        pws.total_downtime_hours
                    FROM park_weekly_stats pws
                    JOIN parks p ON pws.park_id = p.park_id
                    WHERE pws.year = :prev_year
                        AND pws.week_number = :prev_week
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (pp.avg_uptime_percentage - cp.avg_uptime_percentage) AS decline_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (pp.avg_uptime_percentage - cp.avg_uptime_percentage) >= 5.0
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_week": current_week,
                "prev_year": prev_year,
                "prev_week": prev_week,
                "limit": limit
            })

        elif period == '30days':
            # Monthly comparison: current month vs previous month
            current_month = today.month
            current_year = today.year

            prev_month_date = today - timedelta(days=30)
            prev_month = prev_month_date.month
            prev_year = prev_month_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        pms.park_id,
                        p.name AS park_name,
                        CONCAT(p.city, ', ', p.state_province) AS location,
                        pms.avg_uptime_percentage,
                        pms.total_downtime_hours
                    FROM park_monthly_stats pms
                    JOIN parks p ON pms.park_id = p.park_id
                    WHERE pms.year = :current_year
                        AND pms.month = :current_month
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        pms.park_id,
                        pms.avg_uptime_percentage,
                        pms.total_downtime_hours
                    FROM park_monthly_stats pms
                    JOIN parks p ON pms.park_id = p.park_id
                    WHERE pms.year = :prev_year
                        AND pms.month = :prev_month
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.park_id,
                    cp.park_name,
                    cp.location,
                    cp.avg_uptime_percentage AS current_uptime,
                    pp.avg_uptime_percentage AS previous_uptime,
                    (pp.avg_uptime_percentage - cp.avg_uptime_percentage) AS decline_percentage,
                    cp.total_downtime_hours AS current_downtime_hours,
                    pp.total_downtime_hours AS previous_downtime_hours
                FROM current_period cp
                JOIN previous_period pp ON cp.park_id = pp.park_id
                WHERE (pp.avg_uptime_percentage - cp.avg_uptime_percentage) >= 5.0
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_month": current_month,
                "prev_year": prev_year,
                "prev_month": prev_month,
                "limit": limit
            })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_rides_improving(
        self,
        period: str = '7days',
        park_filter: str = 'all-parks',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get rides showing ≥5% uptime improvement (Query 10).

        Args:
            period: 'today', '7days', or '30days'
            park_filter: 'disney-universal' or 'all-parks'
            limit: Maximum number of results

        Returns:
            List of rides with improvement metrics
        """
        from datetime import datetime, timedelta

        today = datetime.now().date()

        if period == 'today':
            # Daily comparison: today vs yesterday
            current_date = today
            previous_date = today - timedelta(days=1)

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rds.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rds.uptime_percentage,
                        rds.downtime_minutes
                    FROM ride_daily_stats rds
                    JOIN rides r ON rds.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rds.stat_date = :current_date
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rds.ride_id,
                        rds.uptime_percentage,
                        rds.downtime_minutes
                    FROM ride_daily_stats rds
                    JOIN rides r ON rds.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rds.stat_date = :previous_date
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (cp.uptime_percentage - pp.uptime_percentage) AS improvement_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (cp.uptime_percentage - pp.uptime_percentage) >= :threshold
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_date": current_date,
                "previous_date": previous_date,
                "threshold": 2.0,  # Lower threshold for daily comparisons
                "limit": limit
            })

        elif period == '7days':
            # Weekly comparison: current week vs previous week
            current_week = today.isocalendar()[1]
            current_year = today.year

            prev_week_date = today - timedelta(weeks=1)
            prev_week = prev_week_date.isocalendar()[1]
            prev_year = prev_week_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rws.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rws.uptime_percentage,
                        rws.downtime_minutes
                    FROM ride_weekly_stats rws
                    JOIN rides r ON rws.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rws.year = :current_year
                        AND rws.week_number = :current_week
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rws.ride_id,
                        rws.uptime_percentage,
                        rws.downtime_minutes
                    FROM ride_weekly_stats rws
                    JOIN rides r ON rws.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rws.year = :prev_year
                        AND rws.week_number = :prev_week
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (cp.uptime_percentage - pp.uptime_percentage) AS improvement_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (cp.uptime_percentage - pp.uptime_percentage) >= 5.0
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_week": current_week,
                "prev_year": prev_year,
                "prev_week": prev_week,
                "limit": limit
            })

        elif period == '30days':
            # Monthly comparison: current month vs previous month
            current_month = today.month
            current_year = today.year

            prev_month_date = today - timedelta(days=30)
            prev_month = prev_month_date.month
            prev_year = prev_month_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rms.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rms.uptime_percentage,
                        rms.downtime_minutes
                    FROM ride_monthly_stats rms
                    JOIN rides r ON rms.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rms.year = :current_year
                        AND rms.month = :current_month
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rms.ride_id,
                        rms.uptime_percentage,
                        rms.downtime_minutes
                    FROM ride_monthly_stats rms
                    JOIN rides r ON rms.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rms.year = :prev_year
                        AND rms.month = :prev_month
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (cp.uptime_percentage - pp.uptime_percentage) AS improvement_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (cp.uptime_percentage - pp.uptime_percentage) >= 5.0
                ORDER BY improvement_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_month": current_month,
                "prev_year": prev_year,
                "prev_month": prev_month,
                "limit": limit
            })

        return [dict(row._mapping) for row in result.fetchall()]

    def get_rides_declining(
        self,
        period: str = '7days',
        park_filter: str = 'all-parks',
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get rides showing ≥5% uptime decline (Query 11).

        Args:
            period: 'today', '7days', or '30days'
            park_filter: 'disney-universal' or 'all-parks'
            limit: Maximum number of results

        Returns:
            List of rides with decline metrics
        """
        from datetime import datetime, timedelta

        today = datetime.now().date()

        if period == 'today':
            # Daily comparison: today vs yesterday
            current_date = today
            previous_date = today - timedelta(days=1)

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rds.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rds.uptime_percentage,
                        rds.downtime_minutes
                    FROM ride_daily_stats rds
                    JOIN rides r ON rds.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rds.stat_date = :current_date
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rds.ride_id,
                        rds.uptime_percentage,
                        rds.downtime_minutes
                    FROM ride_daily_stats rds
                    JOIN rides r ON rds.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rds.stat_date = :previous_date
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (pp.uptime_percentage - cp.uptime_percentage) AS decline_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (pp.uptime_percentage - cp.uptime_percentage) >= :threshold
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_date": current_date,
                "previous_date": previous_date,
                "threshold": 2.0,  # Lower threshold for daily comparisons
                "limit": limit
            })

        elif period == '7days':
            # Weekly comparison: current week vs previous week
            current_week = today.isocalendar()[1]
            current_year = today.year

            prev_week_date = today - timedelta(weeks=1)
            prev_week = prev_week_date.isocalendar()[1]
            prev_year = prev_week_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rws.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rws.uptime_percentage,
                        rws.downtime_minutes
                    FROM ride_weekly_stats rws
                    JOIN rides r ON rws.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rws.year = :current_year
                        AND rws.week_number = :current_week
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rws.ride_id,
                        rws.uptime_percentage,
                        rws.downtime_minutes
                    FROM ride_weekly_stats rws
                    JOIN rides r ON rws.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rws.year = :prev_year
                        AND rws.week_number = :prev_week
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (pp.uptime_percentage - cp.uptime_percentage) AS decline_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (pp.uptime_percentage - cp.uptime_percentage) >= 5.0
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_week": current_week,
                "prev_year": prev_year,
                "prev_week": prev_week,
                "limit": limit
            })

        elif period == '30days':
            # Monthly comparison: current month vs previous month
            current_month = today.month
            current_year = today.year

            prev_month_date = today - timedelta(days=30)
            prev_month = prev_month_date.month
            prev_year = prev_month_date.year

            filter_clause = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if park_filter == 'disney-universal' else ""

            query = text(f"""
                WITH current_period AS (
                    SELECT
                        rms.ride_id,
                        r.name AS ride_name,
                        p.name AS park_name,
                        r.tier,
                        rms.uptime_percentage,
                        rms.downtime_minutes
                    FROM ride_monthly_stats rms
                    JOIN rides r ON rms.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rms.year = :current_year
                        AND rms.month = :current_month
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                ),
                previous_period AS (
                    SELECT
                        rms.ride_id,
                        rms.uptime_percentage,
                        rms.downtime_minutes
                    FROM ride_monthly_stats rms
                    JOIN rides r ON rms.ride_id = r.ride_id
                    JOIN parks p ON r.park_id = p.park_id
                    WHERE rms.year = :prev_year
                        AND rms.month = :prev_month
                        AND r.is_active = TRUE
                        AND p.is_active = TRUE
                        {filter_clause}
                )
                SELECT
                    cp.ride_id,
                    cp.ride_name,
                    cp.park_name,
                    cp.tier,
                    cp.uptime_percentage AS current_uptime,
                    pp.uptime_percentage AS previous_uptime,
                    (pp.uptime_percentage - cp.uptime_percentage) AS decline_percentage,
                    cp.downtime_minutes AS current_downtime_minutes,
                    pp.downtime_minutes AS previous_downtime_minutes
                FROM current_period cp
                JOIN previous_period pp ON cp.ride_id = pp.ride_id
                WHERE (pp.uptime_percentage - cp.uptime_percentage) >= 5.0
                ORDER BY decline_percentage DESC
                LIMIT :limit
            """)

            result = self.conn.execute(query, {
                "current_year": current_year,
                "current_month": current_month,
                "prev_year": prev_year,
                "prev_month": prev_month,
                "limit": limit
            })

        return [dict(row._mapping) for row in result.fetchall()]

    # =========================================================================
    # SORT ORDER HELPER
    # =========================================================================

    def _get_order_by_clause(self, sort_by: str) -> str:
        """
        Get the ORDER BY clause for park downtime rankings.

        Args:
            sort_by: Column to sort by

        Returns:
            SQL ORDER BY expression (column + direction)
        """
        # Map sort options to SQL expressions
        # Note: uptime_percentage sorts ASC (higher is better), others sort DESC (higher = worse)
        sort_mapping = {
            "shame_score": "shame_score DESC",
            "total_downtime_hours": "total_downtime_hours DESC",
            "uptime_percentage": "uptime_percentage ASC",  # Higher uptime is better
            "rides_down": "rides_down DESC",
        }
        return sort_mapping.get(sort_by, "shame_score DESC")

    def _get_ride_order_by_clause(self, sort_by: str) -> str:
        """
        Get the ORDER BY clause for ride downtime rankings.

        Args:
            sort_by: Column to sort by

        Returns:
            SQL ORDER BY expression (column + direction)
        """
        # Map sort options to SQL expressions
        # current_is_open ASC puts down rides (0) first
        # uptime_percentage ASC puts lowest uptime (worst) first
        sort_mapping = {
            "current_is_open": "current_is_open ASC, downtime_hours DESC",
            "downtime_hours": "downtime_hours DESC",
            "uptime_percentage": "uptime_percentage ASC",  # Lower uptime = worse
            "trend_percentage": "trend_percentage DESC",
        }
        return sort_mapping.get(sort_by, "downtime_hours DESC")

    # Live Downtime Rankings (for "Today" period - computed from snapshots)

    def get_park_live_downtime_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score"
    ) -> List[Dict[str, Any]]:
        """
        Get park downtime rankings calculated live from today's snapshots.

        This method computes downtime in real-time from ride_status_snapshots,
        providing up-to-the-minute accuracy for the "Today" period.

        CRITICAL: Shame score only counts rides that are CURRENTLY down.
        Rides that were down earlier but are now operating do NOT contribute
        to the shame score. "Rides Down" shows count of currently down rides.

        Uses centralized SQL helpers for consistent status logic.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of parks to return
            sort_by: Column to sort by - 'shame_score', 'total_downtime_hours',
                     'uptime_percentage', 'rides_down'

        Returns:
            List of parks ranked by specified column (descending, except uptime which is ascending)
        """
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Get Pacific day bounds in UTC - "today" means Pacific calendar day
        start_utc, end_utc = get_pacific_day_range_utc(get_today_pacific())

        # Use centralized helpers for consistent logic
        is_down = RideStatusSQL.is_down("rss")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        downtime_hours = DowntimeSQL.downtime_hours_rounded("rss", "pas")
        weighted_downtime = DowntimeSQL.weighted_downtime_hours("rss", "pas", "COALESCE(rc.tier_weight, 2)")
        uptime_pct = UptimeSQL.uptime_percentage("rss", "pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # INSTANTANEOUS shame score - uses centralized helper for single source of truth
        shame_score_sql = ShameScoreSQL.instantaneous_shame_score()

        # CRITICAL: Only count downtime for rides that have operated at some point today
        # Rides that have NEVER been OPERATING during the period are likely seasonal closures
        # or scheduled maintenance, not unplanned outages
        has_operated = RideStatusSQL.has_operated_subquery("r.ride_id")

        # For latest snapshot check - used in rides_currently_down CTE
        is_down_latest = RideStatusSQL.is_down("rss_latest")
        park_open_latest = ParkStatusSQL.park_appears_open_filter("pas_latest")

        # CRITICAL: Also need is_operating for rides_that_operated CTE
        is_operating_cte = RideStatusSQL.is_operating("rss_op")

        query = text(f"""
            WITH
            latest_snapshot AS (
                -- Find the most recent snapshot timestamp for each ride today
                SELECT ride_id, MAX(recorded_at) as latest_recorded_at
                FROM ride_status_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY ride_id
            ),
            rides_that_operated AS (
                -- Only include rides that showed OPERATING status at some point today
                -- This excludes seasonal closures and scheduled maintenance
                SELECT DISTINCT rss_op.ride_id
                FROM ride_status_snapshots rss_op
                WHERE rss_op.recorded_at >= :start_utc AND rss_op.recorded_at < :end_utc
                    AND ({is_operating_cte})
            ),
            rides_currently_down AS (
                -- Identify rides that are DOWN in their latest snapshot
                -- CRITICAL: Only count rides that have operated at some point today
                SELECT DISTINCT r_inner.ride_id, r_inner.park_id
                FROM rides r_inner
                INNER JOIN ride_status_snapshots rss_latest ON r_inner.ride_id = rss_latest.ride_id
                INNER JOIN latest_snapshot ls ON rss_latest.ride_id = ls.ride_id
                    AND rss_latest.recorded_at = ls.latest_recorded_at
                INNER JOIN park_activity_snapshots pas_latest ON r_inner.park_id = pas_latest.park_id
                    AND pas_latest.recorded_at = rss_latest.recorded_at
                INNER JOIN rides_that_operated rto ON r_inner.ride_id = rto.ride_id
                WHERE r_inner.is_active = TRUE
                    AND r_inner.category = 'ATTRACTION'
                    AND {is_down_latest}
                    AND {park_open_latest}
            ),
            park_weights AS (
                -- Calculate total weight for each park based on tier classifications
                -- Only count rides that have actually operated today
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    AND {has_operated}
                    {filter_clause}
                GROUP BY p.park_id
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Calculate total downtime hours using centralized helper
                {downtime_hours} AS total_downtime_hours,

                -- Calculate weighted downtime hours using centralized helper
                {weighted_downtime} AS weighted_downtime_hours,

                -- Shame Score = INSTANTANEOUS (rides currently down, not cumulative)
                -- Uses centralized ShameScoreSQL helper for single source of truth
                {shame_score_sql} AS shame_score,

                -- Count of rides CURRENTLY down (not cumulative)
                COUNT(DISTINCT rcd.ride_id) AS rides_down,

                -- Calculate uptime percentage using centralized helper
                {uptime_pct} AS uptime_percentage,

                -- Trend: NULL for live data (no historical comparison needed for "Today")
                NULL AS trend_percentage,

                -- Park operating status using centralized helper
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN rides_currently_down rcd ON r.ride_id = rcd.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
                AND {has_operated}
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight
            HAVING total_downtime_hours > 0  -- Hall of Shame: only parks with actual downtime
            ORDER BY {self._get_order_by_clause(sort_by)}
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "limit": limit,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result.fetchall()]

    def get_ride_live_downtime_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 100,
        sort_by: str = "downtime_hours"
    ) -> List[Dict[str, Any]]:
        """
        Get ride downtime rankings calculated live from today's snapshots.

        This method computes downtime in real-time from ride_status_snapshots,
        providing up-to-the-minute accuracy for the "Today" period.

        Uses centralized SQL helpers to ensure current_status matches the
        status summary panel counts.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return
            sort_by: Column to sort by (current_is_open, downtime_hours, uptime_percentage, trend_percentage)

        Returns:
            List of rides ranked by specified sort column with current status
        """
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        order_by_clause = self._get_ride_order_by_clause(sort_by)

        # Get Pacific day bounds in UTC - "today" means Pacific calendar day
        today_pacific = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today_pacific)

        # Use centralized helpers for consistent logic across all queries
        is_down = RideStatusSQL.is_down("rss")
        is_operating = RideStatusSQL.is_operating("rss")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        active_filter = RideFilterSQL.active_attractions_filter("r", "p")
        downtime_hours = DowntimeSQL.downtime_hours_rounded("rss", "pas")
        uptime_pct = UptimeSQL.uptime_percentage("rss", "pas")

        # CRITICAL: Only count downtime for rides that have operated at some point today
        # Rides that have NEVER been OPERATING during the period are likely seasonal closures
        # or scheduled maintenance, not unplanned outages
        has_operated = RideStatusSQL.has_operated_subquery("r.ride_id")

        # CRITICAL: Use helper subqueries that include time window filter
        # This ensures current_status matches the status summary panel
        # Pass park_id_expr to ensure rides at closed parks show PARK_CLOSED, not DOWN
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                rc.tier,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Calculate downtime hours from today's snapshots using centralized helper
                {downtime_hours} AS downtime_hours,

                -- Calculate uptime percentage using centralized helper
                {uptime_pct} AS uptime_percentage,

                -- Wait time stats from today's snapshots
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 2) AS avg_wait_time,
                MAX(rss.wait_time) AS peak_wait_time,

                -- Get current status using centralized helper (includes time window for consistency)
                {current_status_sq},

                -- Boolean for frontend compatibility using centralized helper
                {current_is_open_sq},

                -- Park operating status using centralized helper
                {park_is_open_sq},

                -- Trend: compare to yesterday's aggregated stats
                CASE
                    WHEN prev_day.downtime_minutes > 0 THEN
                        ROUND(
                            ((SUM(CASE
                                WHEN {park_open} AND {is_down}
                                THEN 5
                                ELSE 0
                            END) - prev_day.downtime_minutes) / prev_day.downtime_minutes) * 100,
                            2
                        )
                    ELSE NULL
                END AS trend_percentage

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            LEFT JOIN ride_daily_stats prev_day ON r.ride_id = prev_day.ride_id
                AND prev_day.stat_date = :yesterday_pacific
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND {active_filter}
                AND {has_operated}
                {filter_clause}
            GROUP BY r.ride_id, r.name, rc.tier, p.park_id, p.name, p.city, p.state_province, prev_day.downtime_minutes
            HAVING (downtime_hours > 0 AND uptime_percentage > 0) OR current_status = 'DOWN'  -- Include rides with downtime OR currently down
            ORDER BY {order_by_clause}
            LIMIT :limit
        """)

        from datetime import timedelta
        yesterday_pacific = today_pacific - timedelta(days=1)
        result = self.conn.execute(query, {
            "limit": limit,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "yesterday_pacific": yesterday_pacific
        })
        return [dict(row._mapping) for row in result.fetchall()]

    def get_live_status_summary(
        self,
        filter_disney_universal: bool = False,
        park_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get live status summary counts by status type for rides at OPEN parks only.

        Uses the most recent snapshot for each ride to count:
        - OPERATING: Rides currently running
        - DOWN: Rides experiencing unscheduled breakdowns
        - CLOSED: Rides on scheduled closure (at open parks)
        - REFURBISHMENT: Rides on extended maintenance

        For parks without ThemeParks.wiki data (status is NULL),
        maps computed_is_open to OPERATING/DOWN.

        Only includes rides at parks that are currently operating
        (park_appears_open = TRUE in latest park_activity_snapshot).

        Uses centralized SQL helpers for consistent status logic.

        Args:
            filter_disney_universal: If True, only include Disney/Universal parks
            park_id: Optional park ID to filter to a single park

        Returns:
            Dictionary with status counts and totals
        """
        filter_clause = ""
        params = {}

        if filter_disney_universal:
            filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}"
        if park_id:
            filter_clause += " AND p.park_id = :park_id"
            params["park_id"] = park_id

        # Use centralized helpers for consistent logic
        time_window = RideFilterSQL.live_time_window_filter("pas.recorded_at")
        time_window_rss = RideFilterSQL.live_time_window_filter("rss.recorded_at")
        active_filter = RideFilterSQL.active_attractions_filter("r", "p")
        status_expr = RideStatusSQL.status_expression("ls")

        query = text(f"""
            WITH latest_park_status AS (
                -- Get the latest park activity snapshot for each park
                SELECT
                    pas.park_id,
                    pas.park_appears_open,
                    ROW_NUMBER() OVER (PARTITION BY pas.park_id ORDER BY pas.recorded_at DESC) as rn
                FROM park_activity_snapshots pas
                WHERE {time_window}
            ),
            latest_snapshots AS (
                SELECT
                    rss.ride_id,
                    rss.status,
                    rss.computed_is_open,
                    rss.recorded_at,
                    r.park_id,
                    ROW_NUMBER() OVER (PARTITION BY rss.ride_id ORDER BY rss.recorded_at DESC) as rn
                FROM ride_status_snapshots rss
                JOIN rides r ON rss.ride_id = r.ride_id
                JOIN parks p ON r.park_id = p.park_id
                WHERE {time_window_rss}
                    AND {active_filter}
                    {filter_clause}
            )
            SELECT
                -- Map NULL status to OPERATING/DOWN based on computed_is_open
                {status_expr} as status_type,
                COUNT(*) as count
            FROM latest_snapshots ls
            JOIN latest_park_status lps ON ls.park_id = lps.park_id AND lps.rn = 1
            WHERE ls.rn = 1
                AND lps.park_appears_open = TRUE  -- Only count rides at open parks
            GROUP BY status_type
        """)

        result = self.conn.execute(query, params)
        rows = result.fetchall()

        # Build status summary with defaults
        summary = {
            "OPERATING": 0,
            "DOWN": 0,
            "CLOSED": 0,
            "REFURBISHMENT": 0,
            "total": 0
        }

        for row in rows:
            status = row[0]
            count = row[1]
            if status in summary:
                summary[status] = count
            summary["total"] += count

        return summary

    # Chart Data Methods for Trends Visualization

    def get_park_shame_score_history(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get historical shame scores for top parks over a date range.

        Returns data formatted for Chart.js line charts.

        Args:
            start_date: Start date for data
            end_date: End date for data
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Number of parks to include (default 10)

        Returns:
            Dict with 'labels' (dates) and 'datasets' (park data series)
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # First, identify the top parks by total shame score in the period
        top_parks_query = text(f"""
            WITH park_weights AS (
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id
            )
            SELECT
                pds.park_id,
                p.name AS park_name,
                SUM(pds.shame_score) AS total_shame_score
            FROM park_daily_stats pds
            INNER JOIN parks p ON pds.park_id = p.park_id
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            WHERE pds.stat_date BETWEEN :start_date AND :end_date
                AND p.is_active = TRUE
                AND pds.operating_hours_minutes > 0
                AND pds.shame_score IS NOT NULL
                {disney_filter}
            GROUP BY pds.park_id, p.name
            ORDER BY total_shame_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_parks_query, {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })
        top_parks = [dict(row._mapping) for row in result]

        if not top_parks:
            return {"labels": [], "datasets": []}

        # Get the park IDs
        park_ids = [p['park_id'] for p in top_parks]

        # Generate all dates in range for labels
        labels = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime('%b %d'))
            current += timedelta(days=1)

        # Get daily shame scores for each park
        datasets = []
        for park in top_parks:
            history_query = text("""
                SELECT
                    stat_date,
                    COALESCE(shame_score, 0) AS shame_score
                FROM park_daily_stats
                WHERE park_id = :park_id
                    AND stat_date BETWEEN :start_date AND :end_date
                ORDER BY stat_date ASC
            """)

            result = self.conn.execute(history_query, {
                "park_id": park['park_id'],
                "start_date": start_date,
                "end_date": end_date
            })
            scores = {row.stat_date: float(row.shame_score) for row in result}

            # Build data array with None for missing dates
            data = []
            current = start_date
            while current <= end_date:
                if current in scores:
                    data.append(round(scores[current], 2))
                else:
                    data.append(None)
                current += timedelta(days=1)

            datasets.append({
                "label": park['park_name'],
                "data": data
            })

        return {
            "labels": labels,
            "datasets": datasets
        }

    def get_ride_downtime_history(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get historical downtime percentages for top rides over a date range.

        Returns data formatted for Chart.js line charts.

        Args:
            start_date: Start date for data
            end_date: End date for data
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Number of rides to include (default 10)

        Returns:
            Dict with 'labels' (dates) and 'datasets' (ride data series)
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # First, identify the top rides by total downtime in the period
        top_rides_query = text(f"""
            SELECT
                rds.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                SUM(rds.downtime_minutes) AS total_downtime
            FROM ride_daily_stats rds
            INNER JOIN rides r ON rds.ride_id = r.ride_id
            INNER JOIN parks p ON r.park_id = p.park_id
            WHERE rds.stat_date BETWEEN :start_date AND :end_date
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                AND rds.operating_hours_minutes > 0
                {disney_filter}
            GROUP BY rds.ride_id, r.name, p.name
            ORDER BY total_downtime DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_rides_query, {
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit
        })
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": [], "datasets": []}

        # Generate all dates in range for labels
        labels = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime('%b %d'))
            current += timedelta(days=1)

        # Get daily downtime percentages for each ride
        datasets = []
        for ride in top_rides:
            history_query = text("""
                SELECT
                    stat_date,
                    CASE
                        WHEN operating_hours_minutes > 0
                        THEN ROUND((100.0 - uptime_percentage), 1)
                        ELSE 0
                    END AS downtime_percentage
                FROM ride_daily_stats
                WHERE ride_id = :ride_id
                    AND stat_date BETWEEN :start_date AND :end_date
                ORDER BY stat_date ASC
            """)

            result = self.conn.execute(history_query, {
                "ride_id": ride['ride_id'],
                "start_date": start_date,
                "end_date": end_date
            })
            percentages = {row.stat_date: float(row.downtime_percentage) for row in result}

            # Build data array with None for missing dates
            data = []
            current = start_date
            while current <= end_date:
                if current in percentages:
                    data.append(percentages[current])
                else:
                    data.append(None)
                current += timedelta(days=1)

            datasets.append({
                "label": ride['ride_name'],
                "park": ride['park_name'],
                "data": data
            })

        return {
            "labels": labels,
            "datasets": datasets
        }

    def get_park_hourly_shame_scores(
        self,
        target_date: date,
        filter_disney_universal: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get hourly shame scores for top parks on a specific date.

        Calculates shame score per hour by querying ride_status_snapshots
        and computing weighted downtime for each hour of the day.

        Args:
            target_date: The date to get hourly data for (usually today)
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Number of parks to include (default 10)

        Returns:
            Dict with 'labels' (hours) and 'datasets' (park data series)
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Convert target_date to UTC datetime range using existing utility
        utc_start, utc_end = get_pacific_day_range_utc(target_date)

        # First, identify the top parks by weighted downtime today
        # Excludes parks that are completely closed (no rides open at all = seasonal closure)
        top_parks_query = text(f"""
            WITH park_weights AS (
                SELECT
                    p.park_id,
                    p.name AS park_name,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    {disney_filter}
                GROUP BY p.park_id, p.name
            ),
            park_activity AS (
                SELECT
                    r.park_id,
                    SUM(CASE WHEN rss.computed_is_open = FALSE THEN COALESCE(rc.tier_weight, 2) ELSE 0 END) AS weighted_downtime_count,
                    SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS open_snapshots
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE rss.recorded_at >= :utc_start
                    AND rss.recorded_at <= :utc_end
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                GROUP BY r.park_id
            )
            SELECT
                pw.park_id,
                pw.park_name,
                pw.total_park_weight,
                COALESCE(pa.weighted_downtime_count, 0) AS weighted_downtime_count
            FROM park_weights pw
            INNER JOIN park_activity pa ON pw.park_id = pa.park_id
            WHERE pa.open_snapshots > 0
            ORDER BY COALESCE(pa.weighted_downtime_count, 0) DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_parks_query, {
            "utc_start": utc_start,
            "utc_end": utc_end,
            "limit": limit
        })
        top_parks = [dict(row._mapping) for row in result]

        if not top_parks:
            return {"labels": [], "datasets": []}

        # Generate hourly labels (6am to 11pm Pacific - typical park hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get hourly data for each park
        datasets = []
        for park in top_parks:
            # Note: Using DATE_SUB with INTERVAL 8 HOUR for PST (winter)
            # MySQL timezone tables aren't loaded so CONVERT_TZ returns NULL
            hourly_query = text("""
                SELECT
                    HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour_of_day,
                    COUNT(*) AS total_snapshots,
                    SUM(CASE WHEN rss.computed_is_open = FALSE THEN COALESCE(rc.tier_weight, 2) ELSE 0 END) AS weighted_downtime,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_weight
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE rss.recorded_at >= :utc_start
                    AND rss.recorded_at <= :utc_end
                    AND r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)

            result = self.conn.execute(hourly_query, {
                "utc_start": utc_start,
                "utc_end": utc_end,
                "park_id": park['park_id']
            })

            hourly_scores = {}
            for row in result:
                if row.hour_of_day is None:
                    continue
                hour = int(row.hour_of_day)
                if row.total_snapshots and row.total_snapshots > 0:
                    # Use centralized metrics for consistent calculations
                    # Convert weighted downtime snapshots to hours using SNAPSHOT_INTERVAL_MINUTES
                    weighted_downtime_hours = float(row.weighted_downtime) * (SNAPSHOT_INTERVAL_MINUTES / 60.0)
                    # Calculate shame score using centralized function
                    hourly_shame = calculate_shame_score(
                        weighted_downtime_hours,
                        float(park['total_park_weight'])
                    )
                    if hourly_shame is not None:
                        hourly_scores[hour] = hourly_shame

            # Build CUMULATIVE data array for hours 6-23
            # Shame score grows throughout the day as downtime accumulates
            data = []
            cumulative = 0.0
            first_data_seen = False
            for h in range(6, 24):
                if h in hourly_scores:
                    first_data_seen = True
                    cumulative += hourly_scores[h]
                    data.append(round(cumulative, 2))
                elif first_data_seen:
                    # After first data, show cumulative even if no new downtime this hour
                    data.append(round(cumulative, 2))
                else:
                    # Before first data (park not open yet), show null
                    data.append(None)

            datasets.append({
                "label": park['park_name'],
                "data": data
            })

        return {
            "labels": labels,
            "datasets": datasets
        }

    def get_ride_hourly_downtime(
        self,
        target_date: date,
        filter_disney_universal: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get hourly downtime percentages for top rides on a specific date.

        Args:
            target_date: The date to get hourly data for (usually today)
            filter_disney_universal: Filter to only Disney/Universal parks
            limit: Number of rides to include (default 10)

        Returns:
            Dict with 'labels' (hours) and 'datasets' (ride data series)
        """
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Convert target_date to UTC datetime range using existing utility
        utc_start, utc_end = get_pacific_day_range_utc(target_date)

        # First, identify the top rides by downtime today
        # Excludes rides at closed parks (rides with 0 open snapshots = seasonal closure)
        top_rides_query = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                COUNT(*) AS total_snapshots,
                SUM(CASE WHEN rss.computed_is_open = FALSE THEN 1 ELSE 0 END) AS downtime_snapshots,
                SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS open_snapshots
            FROM ride_status_snapshots rss
            INNER JOIN rides r ON rss.ride_id = r.ride_id
            INNER JOIN parks p ON r.park_id = p.park_id
            WHERE rss.recorded_at >= :utc_start
                AND rss.recorded_at <= :utc_end
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                {disney_filter}
            GROUP BY r.ride_id, r.name, p.name
            HAVING downtime_snapshots > 0 AND open_snapshots > 0
            ORDER BY downtime_snapshots DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_rides_query, {
            "utc_start": utc_start,
            "utc_end": utc_end,
            "limit": limit
        })
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": [], "datasets": []}

        # Generate hourly labels (6am to 11pm Pacific - typical park hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get hourly data for each ride
        datasets = []
        for ride in top_rides:
            # Note: Using DATE_SUB with INTERVAL 8 HOUR for PST (winter)
            # MySQL timezone tables aren't loaded so CONVERT_TZ returns NULL
            hourly_query = text("""
                SELECT
                    HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour_of_day,
                    COUNT(*) AS total_snapshots,
                    SUM(CASE WHEN rss.computed_is_open = FALSE THEN 1 ELSE 0 END) AS downtime_snapshots
                FROM ride_status_snapshots rss
                WHERE rss.recorded_at >= :utc_start
                    AND rss.recorded_at <= :utc_end
                    AND rss.ride_id = :ride_id
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)

            result = self.conn.execute(hourly_query, {
                "utc_start": utc_start,
                "utc_end": utc_end,
                "ride_id": ride['ride_id']
            })

            hourly_downtime = {}
            for row in result:
                if row.hour_of_day is None:
                    continue
                hour = int(row.hour_of_day)
                if row.total_snapshots and row.total_snapshots > 0:
                    # Use centralized metrics for consistent calculations
                    # This matches the downtime_hours metric shown in tables
                    downtime_hours = calculate_downtime_hours(int(row.downtime_snapshots))
                    hourly_downtime[hour] = downtime_hours

            # Build CUMULATIVE data array for hours 6-23
            # Downtime grows throughout the day as issues accumulate
            data = []
            cumulative = 0.0
            first_data_seen = False
            for h in range(6, 24):
                if h in hourly_downtime:
                    first_data_seen = True
                    cumulative += hourly_downtime[h]
                    data.append(round(cumulative, 2))
                elif first_data_seen:
                    # After first data, show cumulative even if no downtime this hour
                    data.append(round(cumulative, 2))
                else:
                    # Before first data (ride not tracked yet), show null
                    data.append(None)

            datasets.append({
                "label": ride['ride_name'],
                "park": ride['park_name'],
                "data": data
            })

        return {
            "labels": labels,
            "datasets": datasets
        }

    # Live Wait Time Rankings Methods

    def get_ride_live_wait_time_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ride wait time rankings calculated live from today's snapshots.

        This method computes wait times in real-time from ride_status_snapshots,
        providing up-to-the-minute accuracy for the "Today" period.

        CONSISTENCY: Uses the same filtering as downtime rankings:
        - park_activity_snapshots join for park_appears_open
        - has_operated check (ride must have operated at least once)
        - active_filter for is_active and ATTRACTION category

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of rides to return

        Returns:
            List of rides ranked by average wait time (descending)
        """
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Get Pacific day bounds in UTC - "today" means Pacific calendar day
        today_pacific = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today_pacific)

        # Calculate yesterday's date for trend comparison
        from datetime import timedelta
        yesterday_pacific = today_pacific - timedelta(days=1)

        # Use centralized helpers for consistent logic across all queries
        # CRITICAL: Must match downtime rankings filtering for consistency
        active_filter = RideFilterSQL.active_attractions_filter("r", "p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        has_operated = RideStatusSQL.has_operated_subquery("r.ride_id")

        # CRITICAL: Use helper subqueries that include time window filter
        # This ensures current_status matches the status summary panel
        # Pass park_id_expr to ensure rides at closed parks show PARK_CLOSED, not DOWN
        current_status_sq = RideStatusSQL.current_status_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        current_is_open_sq = RideStatusSQL.current_is_open_subquery("r.ride_id", include_time_window=True, park_id_expr="r.park_id")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                r.ride_id,
                r.queue_times_id,
                p.queue_times_id AS park_queue_times_id,
                r.name AS ride_name,
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Wait time stats from today's snapshots (field names match frontend expectations)
                -- Only count wait times when park is open
                ROUND(AVG(CASE WHEN {park_open} AND rss.wait_time > 0 THEN rss.wait_time END), 1) AS avg_wait_minutes,
                MAX(CASE WHEN {park_open} THEN rss.wait_time END) AS peak_wait_minutes,

                -- Ride tier from classifications (frontend displays tier badge)
                COALESCE(rc.tier, 3) AS tier,

                -- Trend percentage: compare today's avg to yesterday's avg from ride_daily_stats
                -- Formula: ((today - yesterday) / yesterday) * 100
                -- NULL if yesterday has no data (ride was closed, new ride, etc.)
                CASE
                    WHEN yesterday_stats.avg_wait_time IS NOT NULL AND yesterday_stats.avg_wait_time > 0
                    THEN ROUND(
                        ((AVG(CASE WHEN {park_open} AND rss.wait_time > 0 THEN rss.wait_time END) - yesterday_stats.avg_wait_time)
                         / yesterday_stats.avg_wait_time) * 100,
                        1
                    )
                    ELSE NULL
                END AS trend_percentage,

                -- Get current status using centralized helper (includes time window for consistency)
                {current_status_sq},

                -- Boolean for frontend compatibility using centralized helper
                {current_is_open_sq},

                -- Park operating status using centralized helper
                {park_is_open_sq}

            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            -- CRITICAL: Join park_activity_snapshots for consistent park filtering
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            -- LEFT JOIN yesterday's stats for trend calculation
            LEFT JOIN ride_daily_stats yesterday_stats
                ON r.ride_id = yesterday_stats.ride_id
                AND yesterday_stats.stat_date = :yesterday_date
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND {active_filter}
                AND {has_operated}
                AND rss.wait_time IS NOT NULL
                AND rss.wait_time > 0
                {filter_clause}
            GROUP BY r.ride_id, r.name, p.park_id, p.name, p.city, p.state_province,
                     rc.tier, yesterday_stats.avg_wait_time
            HAVING avg_wait_minutes > 0
            ORDER BY avg_wait_minutes DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "limit": limit,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "yesterday_date": yesterday_pacific
        })
        return [dict(row._mapping) for row in result.fetchall()]

    def get_park_live_wait_time_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get park wait time rankings calculated live from today's snapshots.

        This method computes wait times in real-time from ride_status_snapshots,
        providing up-to-the-minute accuracy for the "Today" period.

        Args:
            filter_disney_universal: If True, only include Disney & Universal parks
            limit: Maximum number of parks to return

        Returns:
            List of parks ranked by average wait time (descending)
        """
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""

        # Get Pacific day bounds in UTC - "today" means Pacific calendar day
        today_pacific = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today_pacific)

        # Calculate yesterday's date for trend comparison
        from datetime import timedelta
        yesterday_pacific = today_pacific - timedelta(days=1)

        # Park operating status
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Park-level wait time stats from today's snapshots (field names match frontend)
                ROUND(AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END), 1) AS avg_wait_minutes,
                MAX(rss.wait_time) AS peak_wait_minutes,

                -- Count of rides reporting wait times (frontend displays in Rides column)
                COUNT(DISTINCT r.ride_id) AS rides_reporting,

                -- Trend percentage: compare today's avg to yesterday's avg from park_daily_stats
                -- Formula: ((today - yesterday) / yesterday) * 100
                -- NULL if yesterday has no data (park was closed, new park, etc.)
                CASE
                    WHEN yesterday_stats.avg_wait_time IS NOT NULL AND yesterday_stats.avg_wait_time > 0
                    THEN ROUND(
                        ((AVG(CASE WHEN rss.wait_time > 0 THEN rss.wait_time END) - yesterday_stats.avg_wait_time)
                         / yesterday_stats.avg_wait_time) * 100,
                        1
                    )
                    ELSE NULL
                END AS trend_percentage,

                -- Park operating status using centralized helper
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            -- LEFT JOIN yesterday's stats for trend calculation
            LEFT JOIN park_daily_stats yesterday_stats
                ON p.park_id = yesterday_stats.park_id
                AND yesterday_stats.stat_date = :yesterday_date
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
                AND rss.wait_time IS NOT NULL
                AND rss.wait_time > 0
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, yesterday_stats.avg_wait_time
            HAVING avg_wait_minutes > 0
                AND park_is_open = 1
            ORDER BY avg_wait_minutes DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "limit": limit,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "yesterday_date": yesterday_pacific
        })
        return [dict(row._mapping) for row in result.fetchall()]
