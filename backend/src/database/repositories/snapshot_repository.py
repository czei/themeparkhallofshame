"""
Theme Park Downtime Tracker - Snapshot Repositories
Provides data access layer for ride status and park activity snapshots.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.utils.logger import logger, log_database_error


class RideStatusSnapshotRepository:
    """
    Repository for ride status snapshot operations.

    Implements:
    - CRUD operations for ride_status_snapshots table
    - Latest snapshot queries for current status
    - Historical snapshot queries
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def insert(self, snapshot_data: Dict[str, Any]) -> int:
        """
        Insert a new ride status snapshot.

        Args:
            snapshot_data: Dictionary with snapshot fields

        Returns:
            snapshot_id of inserted record

        Raises:
            DatabaseError: If insertion fails
        """
        query = text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api
            )
            VALUES (
                :ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open, :status, :last_updated_api
            )
        """)

        try:
            # Parse ISO 8601 timestamp if provided as string (e.g., '2024-03-19T03:04:01Z')
            if 'last_updated_api' in snapshot_data and isinstance(snapshot_data['last_updated_api'], str):
                ts = snapshot_data['last_updated_api']
                if ts:
                    # Remove 'Z' suffix and parse ISO format
                    ts = ts.replace('Z', '+00:00')
                    snapshot_data['last_updated_api'] = datetime.fromisoformat(ts).replace(tzinfo=None)

            result = self.conn.execute(query, snapshot_data)
            snapshot_id = result.lastrowid
            return snapshot_id

        except Exception as e:
            log_database_error(e, "Failed to insert ride status snapshot")
            raise

    def get_latest_by_ride(self, ride_id: int) -> Optional[Dict[str, Any]]:
        """
        Get most recent snapshot for a specific ride.

        Args:
            ride_id: Ride ID

        Returns:
            Dictionary with snapshot data or None if not found
        """
        query = text("""
            SELECT snapshot_id, ride_id, recorded_at, wait_time,
                   is_open, computed_is_open, status, last_updated_api
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
            ORDER BY recorded_at DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {"ride_id": ride_id})
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)

    def get_latest_all_rides(self, park_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get most recent snapshot for each ride, optionally filtered by park.

        Args:
            park_id: Optional park ID to filter results

        Returns:
            List of dictionaries with snapshot data
        """
        if park_id:
            query = text("""
                SELECT rss.snapshot_id, rss.ride_id, rss.recorded_at,
                       rss.wait_time, rss.is_open, rss.computed_is_open,
                       r.name as ride_name, r.park_id
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                WHERE rss.snapshot_id IN (
                    SELECT MAX(snapshot_id)
                    FROM ride_status_snapshots
                    WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    GROUP BY ride_id
                )
                AND r.park_id = :park_id
                ORDER BY rss.wait_time DESC
            """)
            result = self.conn.execute(query, {"park_id": park_id})
        else:
            query = text("""
                SELECT rss.snapshot_id, rss.ride_id, rss.recorded_at,
                       rss.wait_time, rss.is_open, rss.computed_is_open,
                       r.name as ride_name, r.park_id
                FROM ride_status_snapshots rss
                INNER JOIN rides r ON rss.ride_id = r.ride_id
                WHERE rss.snapshot_id IN (
                    SELECT MAX(snapshot_id)
                    FROM ride_status_snapshots
                    WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    GROUP BY ride_id
                )
                ORDER BY rss.wait_time DESC
            """)
            result = self.conn.execute(query)

        return [dict(row._mapping) for row in result]

    def get_history(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical snapshots for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with snapshot data
        """
        query = text("""
            SELECT snapshot_id, ride_id, recorded_at, wait_time,
                   is_open, computed_is_open, last_updated_api
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
                AND recorded_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
            ORDER BY recorded_at DESC
        """)

        result = self.conn.execute(query, {"ride_id": ride_id, "hours": hours})
        return [dict(row._mapping) for row in result]


class ParkActivitySnapshotRepository:
    """
    Repository for park activity snapshot operations.

    Implements:
    - CRUD operations for park_activity_snapshots table
    - Park operating status queries
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def insert(self, activity_data: Dict[str, Any]) -> int:
        """
        Insert a new park activity snapshot.

        Args:
            activity_data: Dictionary with activity fields

        Returns:
            activity_id of inserted record

        Raises:
            DatabaseError: If insertion fails
        """
        query = text("""
            INSERT INTO park_activity_snapshots (
                park_id, recorded_at, total_rides_tracked,
                rides_open, rides_closed, avg_wait_time, max_wait_time,
                park_appears_open, shame_score
            )
            VALUES (
                :park_id, :recorded_at, :total_rides_tracked,
                :rides_open, :rides_closed, :avg_wait_time, :max_wait_time,
                :park_appears_open, :shame_score
            )
        """)

        try:
            result = self.conn.execute(query, activity_data)
            activity_id = result.lastrowid
            return activity_id

        except Exception as e:
            log_database_error(e, "Failed to insert park activity snapshot")
            raise

    def get_latest_by_park(self, park_id: int) -> Optional[Dict[str, Any]]:
        """
        Get most recent activity snapshot for a specific park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with activity data or None if not found
        """
        query = text("""
            SELECT snapshot_id, park_id, recorded_at, total_rides_tracked,
                   rides_open, rides_closed, avg_wait_time, max_wait_time,
                   park_appears_open
            FROM park_activity_snapshots
            WHERE park_id = :park_id
            ORDER BY recorded_at DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {"park_id": park_id})
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)

    def get_history(self, park_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical activity snapshots for a specific park.

        Args:
            park_id: Park ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with activity data
        """
        query = text("""
            SELECT snapshot_id, park_id, recorded_at, total_rides_tracked,
                   rides_open, rides_closed, avg_wait_time, max_wait_time,
                   park_appears_open
            FROM park_activity_snapshots
            WHERE park_id = :park_id
                AND recorded_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
            ORDER BY recorded_at DESC
        """)

        result = self.conn.execute(query, {"park_id": park_id, "hours": hours})
        return [dict(row._mapping) for row in result]

    def get_all_latest(self) -> List[Dict[str, Any]]:
        """
        Get most recent activity snapshot for all parks.

        Returns:
            List of dictionaries with activity data
        """
        query = text("""
            SELECT pas.snapshot_id, pas.park_id, pas.recorded_at,
                   pas.total_rides_tracked, pas.rides_open, pas.rides_closed,
                   pas.avg_wait_time, pas.max_wait_time, pas.park_appears_open,
                   p.name as park_name
            FROM park_activity_snapshots pas
            INNER JOIN parks p ON pas.park_id = p.park_id
            WHERE pas.snapshot_id IN (
                SELECT MAX(snapshot_id)
                FROM park_activity_snapshots
                WHERE recorded_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                GROUP BY park_id
            )
            ORDER BY p.name
        """)

        result = self.conn.execute(query)
        return [dict(row._mapping) for row in result]
