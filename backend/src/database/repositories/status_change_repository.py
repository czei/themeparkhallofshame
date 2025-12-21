"""
Theme Park Downtime Tracker - Status Change Repository
Provides data access layer for ride status change events (up/down transitions).
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...utils.logger import logger, log_database_error
except ImportError:
    from utils.logger import log_database_error


class RideStatusChangeRepository:
    """
    Repository for ride status change operations.

    Implements:
    - CRUD operations for ride_status_changes table
    - Status change detection and tracking
    - Downtime duration queries
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def insert(self, change_data: Dict[str, Any]) -> int:
        """
        Insert a new ride status change event.

        Args:
            change_data: Dictionary with change fields

        Returns:
            change_id of inserted record

        Raises:
            DatabaseError: If insertion fails
        """
        query = text("""
            INSERT INTO ride_status_changes (
                ride_id, changed_at, previous_status, new_status,
                duration_in_previous_status, wait_time_at_change
            )
            VALUES (
                :ride_id, :changed_at, :previous_status, :new_status,
                :duration_in_previous_status, :wait_time_at_change
            )
        """)

        try:
            result = self.conn.execute(query, change_data)
            change_id = result.lastrowid
            return change_id

        except Exception as e:
            log_database_error(e, "Failed to insert ride status change")
            raise

    def get_latest_by_ride(self, ride_id: int) -> Optional[Dict[str, Any]]:
        """
        Get most recent status change for a specific ride.

        Args:
            ride_id: Ride ID

        Returns:
            Dictionary with change data or None if not found
        """
        query = text("""
            SELECT change_id, ride_id, changed_at, previous_status,
                   new_status, duration_in_previous_status, wait_time_at_change
            FROM ride_status_changes
            WHERE ride_id = :ride_id
            ORDER BY changed_at DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {"ride_id": ride_id})
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)

    def get_history(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical status changes for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with change data
        """
        query = text("""
            SELECT change_id, ride_id, changed_at, previous_status,
                   new_status, duration_in_previous_status, wait_time_at_change
            FROM ride_status_changes
            WHERE ride_id = :ride_id
                AND changed_at >= :cutoff_time
            ORDER BY changed_at DESC
        """)

        cutoff_time = datetime.now() - timedelta(hours=hours)
        result = self.conn.execute(query, {"ride_id": ride_id, "cutoff_time": cutoff_time})
        return [dict(row._mapping) for row in result]

    def get_downtime_events(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get downtime events (transitions to closed status) for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with downtime events
        """
        query = text("""
            SELECT change_id, ride_id, changed_at, previous_status,
                   new_status, duration_in_previous_status, wait_time_at_change
            FROM ride_status_changes
            WHERE ride_id = :ride_id
                AND new_status = FALSE
                AND changed_at >= :cutoff_time
            ORDER BY changed_at DESC
        """)

        cutoff_time = datetime.now() - timedelta(hours=hours)
        result = self.conn.execute(query, {"ride_id": ride_id, "cutoff_time": cutoff_time})
        return [dict(row._mapping) for row in result]

    def get_uptime_events(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get uptime events (transitions to open status) for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with uptime events
        """
        query = text("""
            SELECT change_id, ride_id, changed_at, previous_status,
                   new_status, duration_in_previous_status, wait_time_at_change
            FROM ride_status_changes
            WHERE ride_id = :ride_id
                AND new_status = TRUE
                AND changed_at >= :cutoff_time
            ORDER BY changed_at DESC
        """)

        cutoff_time = datetime.now() - timedelta(hours=hours)
        result = self.conn.execute(query, {"ride_id": ride_id, "cutoff_time": cutoff_time})
        return [dict(row._mapping) for row in result]

    def get_recent_changes_all_rides(
        self,
        park_id: Optional[int] = None,
        hours: int = 24,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get recent status changes across all rides, optionally filtered by park.

        Args:
            park_id: Optional park ID to filter results
            hours: Number of hours to look back
            limit: Maximum number of results

        Returns:
            List of dictionaries with change data
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        if park_id:
            query = text("""
                SELECT rsc.change_id, rsc.ride_id, rsc.changed_at,
                       rsc.previous_status, rsc.new_status,
                       rsc.duration_in_previous_status,
                       r.name as ride_name, r.park_id,
                       p.name as park_name
                FROM ride_status_changes rsc
                INNER JOIN rides r ON rsc.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE rsc.changed_at >= :cutoff_time
                    AND r.park_id = :park_id
                ORDER BY rsc.changed_at DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {
                "park_id": park_id,
                "cutoff_time": cutoff_time,
                "limit": limit
            })
        else:
            query = text("""
                SELECT rsc.change_id, rsc.ride_id, rsc.changed_at,
                       rsc.previous_status, rsc.new_status,
                       rsc.duration_in_previous_status,
                       r.name as ride_name, r.park_id,
                       p.name as park_name
                FROM ride_status_changes rsc
                INNER JOIN rides r ON rsc.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE rsc.changed_at >= :cutoff_time
                ORDER BY rsc.changed_at DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {"cutoff_time": cutoff_time, "limit": limit})

        return [dict(row._mapping) for row in result]

    def get_longest_downtimes(
        self,
        park_id: Optional[int] = None,
        hours: int = 24,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get rides with longest downtime durations in the specified period.

        Args:
            park_id: Optional park ID to filter results
            hours: Number of hours to look back
            limit: Maximum number of results

        Returns:
            List of dictionaries with longest downtime events
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        if park_id:
            query = text("""
                SELECT rsc.change_id, rsc.ride_id, rsc.changed_at,
                       rsc.duration_in_previous_status,
                       r.name as ride_name, r.park_id,
                       p.name as park_name
                FROM ride_status_changes rsc
                INNER JOIN rides r ON rsc.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE rsc.changed_at >= :cutoff_time
                    AND rsc.new_status = FALSE
                    AND rsc.duration_in_previous_status IS NOT NULL
                    AND r.park_id = :park_id
                ORDER BY rsc.duration_in_previous_status DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {
                "park_id": park_id,
                "cutoff_time": cutoff_time,
                "limit": limit
            })
        else:
            query = text("""
                SELECT rsc.change_id, rsc.ride_id, rsc.changed_at,
                       rsc.duration_in_previous_status,
                       r.name as ride_name, r.park_id,
                       p.name as park_name
                FROM ride_status_changes rsc
                INNER JOIN rides r ON rsc.ride_id = r.ride_id
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE rsc.changed_at >= :cutoff_time
                    AND rsc.new_status = FALSE
                    AND rsc.duration_in_previous_status IS NOT NULL
                ORDER BY rsc.duration_in_previous_status DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {"cutoff_time": cutoff_time, "limit": limit})

        return [dict(row._mapping) for row in result]

    def count_changes_by_ride(
        self,
        ride_id: int,
        hours: int = 24
    ) -> Dict[str, int]:
        """
        Count status changes for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            Dictionary with counts {total, to_open, to_closed}
        """
        query = text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN new_status = TRUE THEN 1 ELSE 0 END) as to_open,
                SUM(CASE WHEN new_status = FALSE THEN 1 ELSE 0 END) as to_closed
            FROM ride_status_changes
            WHERE ride_id = :ride_id
                AND changed_at >= :cutoff_time
        """)

        cutoff_time = datetime.now() - timedelta(hours=hours)
        result = self.conn.execute(query, {"ride_id": ride_id, "cutoff_time": cutoff_time})
        row = result.fetchone()

        if row is None:
            return {"total": 0, "to_open": 0, "to_closed": 0}

        return {
            "total": row.total or 0,
            "to_open": row.to_open or 0,
            "to_closed": row.to_closed or 0
        }
