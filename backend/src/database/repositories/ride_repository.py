"""
Theme Park Downtime Tracker - Ride Repository
Provides data access layer for rides table with CRUD operations and performance queries.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...models.ride import Ride
    from ...utils.logger import logger, log_database_error
except ImportError:
    from models.ride import Ride
    from utils.logger import logger, log_database_error


class RideRepository:
    """
    Repository for ride entity operations.

    Implements:
    - CRUD operations for rides
    - Ride performance rankings (FR-014)
    - Current wait times query (FR-017)
    - Ride status filtering
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def get_by_id(self, ride_id: int) -> Optional[Ride]:
        """
        Fetch ride by ID.

        Args:
            ride_id: Ride ID

        Returns:
            Ride object or None if not found
        """
        query = text("""
            SELECT ride_id, queue_times_id, park_id, name, land_area, tier,
                   is_active, created_at, updated_at
            FROM rides
            WHERE ride_id = :ride_id
        """)

        result = self.conn.execute(query, {"ride_id": ride_id})
        row = result.fetchone()

        if row is None:
            return None

        return self._row_to_ride(row)

    def get_by_queue_times_id(self, queue_times_id: int) -> Optional[Ride]:
        """
        Fetch ride by Queue-Times.com external ID.

        Args:
            queue_times_id: Queue-Times.com ride ID

        Returns:
            Ride object or None if not found
        """
        query = text("""
            SELECT ride_id, queue_times_id, park_id, name, land_area, tier,
                   is_active, created_at, updated_at
            FROM rides
            WHERE queue_times_id = :queue_times_id
        """)

        result = self.conn.execute(query, {"queue_times_id": queue_times_id})
        row = result.fetchone()

        if row is None:
            return None

        return self._row_to_ride(row)

    def get_by_park_id(self, park_id: int, active_only: bool = True) -> List[Ride]:
        """
        Fetch all rides for a specific park.

        Args:
            park_id: Park ID
            active_only: If True, only return active rides

        Returns:
            List of Ride objects
        """
        query = text("""
            SELECT ride_id, queue_times_id, park_id, name, land_area, tier,
                   is_active, created_at, updated_at
            FROM rides
            WHERE park_id = :park_id
                AND (:active_only = FALSE OR is_active = TRUE)
            ORDER BY name
        """)

        result = self.conn.execute(query, {"park_id": park_id, "active_only": active_only})
        return [self._row_to_ride(row) for row in result]

    def get_all_active(self) -> List[Ride]:
        """
        Fetch all active rides.

        Returns:
            List of Ride objects
        """
        query = text("""
            SELECT ride_id, queue_times_id, park_id, name, land_area, tier,
                   is_active, created_at, updated_at
            FROM rides
            WHERE is_active = TRUE
            ORDER BY park_id, name
        """)

        result = self.conn.execute(query)
        return [self._row_to_ride(row) for row in result]

    def get_unclassified_rides(self) -> List[Ride]:
        """
        Fetch rides that have no tier classification yet.

        Returns:
            List of Ride objects without tier classification
        """
        query = text("""
            SELECT r.ride_id, r.queue_times_id, r.park_id, r.name, r.land_area, r.tier,
                   r.is_active, r.created_at, r.updated_at
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.is_active = TRUE
                AND rc.classification_id IS NULL
            ORDER BY r.park_id, r.name
        """)

        result = self.conn.execute(query)
        return [self._row_to_ride(row) for row in result]

    def create(self, ride_data: Dict[str, Any]) -> Ride:
        """
        Create new ride record.

        Args:
            ride_data: Dictionary with ride fields

        Returns:
            Created Ride object

        Raises:
            DatabaseError: If creation fails
        """
        query = text("""
            INSERT INTO rides (
                queue_times_id, park_id, name, land_area, tier
            )
            VALUES (
                :queue_times_id, :park_id, :name, :land_area, :tier
            )
        """)

        try:
            result = self.conn.execute(query, ride_data)
            ride_id = result.lastrowid

            logger.info(f"Created ride: {ride_data['name']} (ID: {ride_id})")

            return self.get_by_id(ride_id)

        except Exception as e:
            log_database_error(e, "Failed to create ride")
            raise

    def update(self, ride_id: int, ride_data: Dict[str, Any]) -> Optional[Ride]:
        """
        Update existing ride record.

        Args:
            ride_id: Ride ID to update
            ride_data: Dictionary with fields to update

        Returns:
            Updated Ride object or None if not found
        """
        set_clauses = []
        params = {"ride_id": ride_id}

        for field, value in ride_data.items():
            set_clauses.append(f"{field} = :{field}")
            params[field] = value

        if not set_clauses:
            return self.get_by_id(ride_id)

        query = text(f"""
            UPDATE rides
            SET {', '.join(set_clauses)}
            WHERE ride_id = :ride_id
        """)

        try:
            result = self.conn.execute(query, params)

            if result.rowcount == 0:
                return None

            logger.info(f"Updated ride ID {ride_id}")
            return self.get_by_id(ride_id)

        except Exception as e:
            log_database_error(e, f"Failed to update ride ID {ride_id}")
            raise

    def get_performance_rankings(
        self,
        period: str = "weekly",
        stat_date: Optional[str] = None,
        park_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get ride performance rankings by downtime (FR-014).

        Args:
            period: "daily", "weekly", "monthly", or "yearly"
            stat_date: Date string (YYYY-MM-DD) or None for current period
            park_id: Optional park ID to filter results
            limit: Maximum number of results

        Returns:
            List of dictionaries with ride performance data
        """
        if period == "daily":
            stats_table = "ride_daily_stats"
            date_condition = "rds.stat_date = COALESCE(:stat_date, CURDATE())"
            stats_alias = "rds"
        elif period == "weekly":
            stats_table = "ride_weekly_stats"
            date_condition = "rws.year = YEAR(COALESCE(:stat_date, CURDATE())) AND rws.week_number = WEEK(COALESCE(:stat_date, CURDATE()), 3)"
            stats_alias = "rws"
        elif period == "monthly":
            stats_table = "ride_monthly_stats"
            date_condition = "rms.year = YEAR(COALESCE(:stat_date, CURDATE())) AND rms.month = MONTH(COALESCE(:stat_date, CURDATE()))"
            stats_alias = "rms"
        elif period == "yearly":
            stats_table = "ride_yearly_stats"
            date_condition = "rys.year = YEAR(COALESCE(:stat_date, CURDATE()))"
            stats_alias = "rys"
        else:
            raise ValueError(f"Invalid period: {period}")

        park_filter = "AND p.park_id = :park_id" if park_id else ""

        query = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                p.park_id,
                {stats_alias}.downtime_minutes / 60.0 AS downtime_hours,
                {stats_alias}.uptime_percentage,
                {stats_alias}.avg_wait_time,
                {stats_alias}.peak_wait_time,
                (SELECT computed_is_open
                 FROM ride_status_snapshots
                 WHERE ride_id = r.ride_id
                 ORDER BY recorded_at DESC
                 LIMIT 1) AS current_status
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN {stats_table} {stats_alias} ON r.ride_id = {stats_alias}.ride_id
            WHERE {date_condition}
                AND r.is_active = TRUE
                AND p.is_active = TRUE
                {park_filter}
            ORDER BY {stats_alias}.downtime_minutes DESC
            LIMIT :limit
        """)

        params = {"stat_date": stat_date, "limit": limit}
        if park_id:
            params["park_id"] = park_id

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result]

    def get_current_wait_times(
        self,
        park_id: Optional[int] = None,
        open_only: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get current wait times for rides (FR-017).

        Args:
            park_id: Optional park ID to filter results
            open_only: If True, only show open rides
            limit: Maximum number of results

        Returns:
            List of dictionaries with current wait time data
        """
        park_filter = "AND p.park_id = :park_id" if park_id else ""
        open_filter = "AND rss.computed_is_open = TRUE" if open_only else ""

        query = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                p.park_id,
                rss.wait_time AS current_wait,
                rws.avg_wait_time AS seven_day_avg,
                rws.peak_wait_time,
                rss.computed_is_open AS is_currently_open,
                rss.recorded_at AS last_updated,
                ROUND(
                    ((rss.wait_time - rws.avg_wait_time) / NULLIF(rws.avg_wait_time, 0)) * 100,
                    2
                ) AS trend_percentage
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN ride_weekly_stats rws ON r.ride_id = rws.ride_id
            WHERE rss.snapshot_id IN (
                -- Get most recent snapshot per ride
                SELECT MAX(snapshot_id)
                FROM ride_status_snapshots
                WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                GROUP BY ride_id
            )
            AND rws.year = YEAR(CURDATE())
            AND rws.week_number = WEEK(CURDATE(), 3)
            AND r.is_active = TRUE
            AND p.is_active = TRUE
            {park_filter}
            {open_filter}
            ORDER BY rss.wait_time DESC
            LIMIT :limit
        """)

        params = {"limit": limit}
        if park_id:
            params["park_id"] = park_id

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result]

    def get_ride_status_history(
        self,
        ride_id: int,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get status history for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back (default 24)

        Returns:
            List of dictionaries with status snapshots
        """
        query = text("""
            SELECT
                snapshot_id,
                ride_id,
                recorded_at,
                is_open,
                wait_time,
                computed_is_open
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
                AND recorded_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
            ORDER BY recorded_at DESC
        """)

        result = self.conn.execute(query, {"ride_id": ride_id, "hours": hours})
        return [dict(row._mapping) for row in result]

    def get_downtime_changes(
        self,
        ride_id: int,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get downtime change events for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back (default 24)

        Returns:
            List of dictionaries with status change events
        """
        query = text("""
            SELECT
                change_id,
                ride_id,
                previous_status,
                new_status,
                changed_at,
                duration_in_previous_status
            FROM ride_status_changes
            WHERE ride_id = :ride_id
                AND changed_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
                AND new_status = FALSE
            ORDER BY changed_at DESC
        """)

        result = self.conn.execute(query, {"ride_id": ride_id, "hours": hours})
        return [dict(row._mapping) for row in result]

    def _row_to_ride(self, row) -> Ride:
        """Convert database row to Ride object."""
        return Ride(
            ride_id=row.ride_id,
            queue_times_id=row.queue_times_id,
            park_id=row.park_id,
            name=row.name,
            land_area=row.land_area,
            tier=row.tier,
            is_active=row.is_active,
            created_at=row.created_at,
            updated_at=row.updated_at
        )
