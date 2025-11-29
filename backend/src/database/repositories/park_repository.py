"""
Theme Park Downtime Tracker - Park Repository
Provides data access layer for parks table with CRUD operations and rankings queries.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...models.park import Park
    from ...utils.logger import logger, log_database_error
except ImportError:
    from models.park import Park
    from utils.logger import logger, log_database_error


class ParkRepository:
    """
    Repository for park entity operations.

    Implements:
    - CRUD operations for parks
    - Park rankings by downtime (FR-010)
    - Park rankings by weighted downtime score (FR-024)
    - Disney & Universal filtering
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def get_by_id(self, park_id: int) -> Optional[Park]:
        """
        Fetch park by ID.

        Args:
            park_id: Park ID

        Returns:
            Park object or None if not found
        """
        query = text("""
            SELECT park_id, queue_times_id, themeparks_wiki_id, name, city, state_province, country,
                   latitude, longitude, timezone, operator, is_disney, is_universal,
                   is_active, created_at, updated_at
            FROM parks
            WHERE park_id = :park_id
        """)

        result = self.conn.execute(query, {"park_id": park_id})
        row = result.fetchone()

        if row is None:
            return None

        return self._row_to_park(row)

    def get_by_queue_times_id(self, queue_times_id: int) -> Optional[Park]:
        """
        Fetch park by Queue-Times.com external ID.

        Args:
            queue_times_id: Queue-Times.com park ID

        Returns:
            Park object or None if not found
        """
        query = text("""
            SELECT park_id, queue_times_id, themeparks_wiki_id, name, city, state_province, country,
                   latitude, longitude, timezone, operator, is_disney, is_universal,
                   is_active, created_at, updated_at
            FROM parks
            WHERE queue_times_id = :queue_times_id
        """)

        result = self.conn.execute(query, {"queue_times_id": queue_times_id})
        row = result.fetchone()

        if row is None:
            return None

        return self._row_to_park(row)

    def get_all_active(self) -> List[Park]:
        """
        Fetch all active parks.

        Returns:
            List of Park objects
        """
        query = text("""
            SELECT park_id, queue_times_id, themeparks_wiki_id, name, city, state_province, country,
                   latitude, longitude, timezone, operator, is_disney, is_universal,
                   is_active, created_at, updated_at
            FROM parks
            WHERE is_active = TRUE
            ORDER BY name
        """)

        result = self.conn.execute(query)
        return [self._row_to_park(row) for row in result]

    def get_disney_universal_parks(self) -> List[Park]:
        """
        Fetch all active Disney and Universal parks (FR-008).

        Returns:
            List of Park objects for Disney/Universal parks
        """
        query = text("""
            SELECT park_id, queue_times_id, themeparks_wiki_id, name, city, state_province, country,
                   latitude, longitude, timezone, operator, is_disney, is_universal,
                   is_active, created_at, updated_at
            FROM parks
            WHERE (is_disney = TRUE OR is_universal = TRUE)
                AND is_active = TRUE
            ORDER BY operator, name
        """)

        result = self.conn.execute(query)
        return [self._row_to_park(row) for row in result]

    def create(self, park_data: Dict[str, Any]) -> Park:
        """
        Create new park record.

        Args:
            park_data: Dictionary with park fields

        Returns:
            Created Park object

        Raises:
            DatabaseError: If creation fails
        """
        query = text("""
            INSERT INTO parks (
                queue_times_id, name, city, state_province, country,
                latitude, longitude, timezone, operator, is_disney, is_universal
            )
            VALUES (
                :queue_times_id, :name, :city, :state_province, :country,
                :latitude, :longitude, :timezone, :operator, :is_disney, :is_universal
            )
        """)

        try:
            result = self.conn.execute(query, park_data)
            park_id = result.lastrowid

            logger.info(f"Created park: {park_data['name']} (ID: {park_id})")

            # Fetch and return the created park
            return self.get_by_id(park_id)

        except Exception as e:
            log_database_error(e, "Failed to create park")
            raise

    def update(self, park_id: int, park_data: Dict[str, Any]) -> Optional[Park]:
        """
        Update existing park record.

        Args:
            park_id: Park ID to update
            park_data: Dictionary with fields to update

        Returns:
            Updated Park object or None if not found
        """
        # Build dynamic SET clause from provided fields
        set_clauses = []
        params = {"park_id": park_id}

        for field, value in park_data.items():
            set_clauses.append(f"{field} = :{field}")
            params[field] = value

        if not set_clauses:
            # No fields to update
            return self.get_by_id(park_id)

        query = text(f"""
            UPDATE parks
            SET {', '.join(set_clauses)}
            WHERE park_id = :park_id
        """)

        try:
            result = self.conn.execute(query, params)

            if result.rowcount == 0:
                return None

            logger.info(f"Updated park ID {park_id}")
            return self.get_by_id(park_id)

        except Exception as e:
            log_database_error(e, f"Failed to update park ID {park_id}")
            raise

    def get_rankings_by_downtime(
        self,
        period: str = "daily",
        stat_date: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings by total downtime (FR-010).

        Args:
            period: "daily", "weekly", "monthly", or "yearly"
            stat_date: Date string (YYYY-MM-DD) or None for current period
            limit: Maximum number of results

        Returns:
            List of dictionaries with park ranking data
        """
        if period == "daily":
            return self._get_daily_rankings(stat_date, limit)
        elif period == "weekly":
            return self._get_weekly_rankings(stat_date, limit)
        elif period == "monthly":
            return self._get_monthly_rankings(stat_date, limit)
        elif period == "yearly":
            return self._get_yearly_rankings(stat_date, limit)
        else:
            raise ValueError(f"Invalid period: {period}")

    def _get_daily_rankings(
        self,
        stat_date: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get daily park rankings by downtime."""
        query = text("""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pds.total_downtime_hours,
                pds.rides_with_downtime AS affected_rides,
                pds.avg_uptime_percentage,
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
            WHERE pds.stat_date = COALESCE(:stat_date, CURDATE())
                AND p.is_active = TRUE
            ORDER BY pds.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"stat_date": stat_date, "limit": limit})
        return [dict(row._mapping) for row in result]

    def _get_weekly_rankings(
        self,
        stat_date: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get weekly park rankings by downtime."""
        query = text("""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pws.total_downtime_hours,
                pws.rides_with_downtime AS affected_rides,
                pws.avg_uptime_percentage,
                pws.trend_vs_previous_week AS trend_percentage
            FROM parks p
            INNER JOIN park_weekly_stats pws ON p.park_id = pws.park_id
            WHERE pws.year = YEAR(COALESCE(:stat_date, CURDATE()))
                AND pws.week_number = WEEK(COALESCE(:stat_date, CURDATE()), 3)
                AND p.is_active = TRUE
            ORDER BY pws.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"stat_date": stat_date, "limit": limit})
        return [dict(row._mapping) for row in result]

    def _get_monthly_rankings(
        self,
        stat_date: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get monthly park rankings by downtime."""
        query = text("""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pms.total_downtime_hours,
                pms.rides_with_downtime AS affected_rides,
                pms.avg_uptime_percentage,
                pms.trend_vs_previous_month AS trend_percentage
            FROM parks p
            INNER JOIN park_monthly_stats pms ON p.park_id = pms.park_id
            WHERE pms.year = YEAR(COALESCE(:stat_date, CURDATE()))
                AND pms.month = MONTH(COALESCE(:stat_date, CURDATE()))
                AND p.is_active = TRUE
            ORDER BY pms.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"stat_date": stat_date, "limit": limit})
        return [dict(row._mapping) for row in result]

    def _get_yearly_rankings(
        self,
        stat_date: Optional[str],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Get yearly park rankings by downtime."""
        query = text("""
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pys.total_downtime_hours,
                pys.rides_with_downtime AS affected_rides,
                pys.avg_uptime_percentage
            FROM parks p
            INNER JOIN park_yearly_stats pys ON p.park_id = pys.park_id
            WHERE pys.year = YEAR(COALESCE(:stat_date, CURDATE()))
                AND p.is_active = TRUE
            ORDER BY pys.total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"stat_date": stat_date, "limit": limit})
        return [dict(row._mapping) for row in result]

    def get_rankings_by_weighted_downtime(
        self,
        period: str = "weekly",
        stat_date: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings by weighted downtime score (FR-024).

        Weighted score accounts for ride tier importance:
        - Tier 1 (major attractions): 3x weight
        - Tier 2 (standard rides): 2x weight
        - Tier 3 (minor rides): 1x weight

        Args:
            period: "weekly", "monthly", or "yearly" (daily not meaningful for weighted scores)
            stat_date: Date string (YYYY-MM-DD) or None for current period
            limit: Maximum number of results

        Returns:
            List of dictionaries with weighted ranking data
        """
        if period == "weekly":
            stats_table = "ride_weekly_stats"
            date_condition = "rws.year = YEAR(COALESCE(:stat_date, CURDATE())) AND rws.week_number = WEEK(COALESCE(:stat_date, CURDATE()), 3)"
        elif period == "monthly":
            stats_table = "ride_monthly_stats"
            date_condition = "rms.year = YEAR(COALESCE(:stat_date, CURDATE())) AND rms.month = MONTH(COALESCE(:stat_date, CURDATE()))"
        elif period == "yearly":
            stats_table = "ride_yearly_stats"
            date_condition = "rys.year = YEAR(COALESCE(:stat_date, CURDATE()))"
        else:
            raise ValueError(f"Invalid period for weighted rankings: {period}")

        # Adjust table alias in date condition
        stats_alias = stats_table[0:3]  # rws, rms, or rys

        query = text(f"""
            WITH park_weights AS (
                SELECT
                    p.park_id,
                    SUM(IFNULL(rc.tier_weight, 2)) AS total_park_weight,
                    COUNT(r.ride_id) AS total_rides,
                    SUM(CASE WHEN r.tier = 1 THEN 1 ELSE 0 END) AS tier1_count,
                    SUM(CASE WHEN r.tier = 2 THEN 1 ELSE 0 END) AS tier2_count,
                    SUM(CASE WHEN r.tier = 3 THEN 1 ELSE 0 END) AS tier3_count
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                GROUP BY p.park_id
            ),
            weighted_downtime AS (
                SELECT
                    p.park_id,
                    SUM({stats_alias}.downtime_minutes / 60.0 * IFNULL(rc.tier_weight, 2)) AS total_weighted_downtime_hours
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                INNER JOIN {stats_table} {stats_alias} ON r.ride_id = {stats_alias}.ride_id
                WHERE {date_condition}
                    AND p.is_active = TRUE
                GROUP BY p.park_id
            )
            SELECT
                p.park_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                pw.total_park_weight,
                pw.tier1_count,
                pw.tier2_count,
                pw.tier3_count,
                wd.total_weighted_downtime_hours,
                ROUND(wd.total_weighted_downtime_hours / pw.total_park_weight, 4) AS weighted_downtime_score,
                ROUND((wd.total_weighted_downtime_hours / pw.total_park_weight) * 100, 2) AS score_percentage
            FROM parks p
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            INNER JOIN weighted_downtime wd ON p.park_id = wd.park_id
            WHERE p.is_active = TRUE
            ORDER BY weighted_downtime_score DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {"stat_date": stat_date, "limit": limit})
        return [dict(row._mapping) for row in result]

    def _row_to_park(self, row) -> Park:
        """Convert database row to Park object."""
        return Park(
            park_id=row.park_id,
            queue_times_id=row.queue_times_id,
            name=row.name,
            city=row.city,
            state_province=row.state_province,
            country=row.country,
            latitude=float(row.latitude) if row.latitude is not None else None,
            longitude=float(row.longitude) if row.longitude is not None else None,
            timezone=row.timezone,
            operator=row.operator,
            is_disney=row.is_disney,
            is_universal=row.is_universal,
            is_active=row.is_active,
            created_at=row.created_at,
            updated_at=row.updated_at,
            themeparks_wiki_id=getattr(row, 'themeparks_wiki_id', None)
        )
