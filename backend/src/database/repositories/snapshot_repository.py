"""
Theme Park Downtime Tracker - Snapshot Repositories
Provides data access layer for ride status and park activity snapshots using SQLAlchemy ORM.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func

from models import RideStatusSnapshot, ParkActivitySnapshot, Ride, Park
from utils.logger import logger, log_database_error


class RideStatusSnapshotRepository:
    """
    Repository for ride status snapshot operations using SQLAlchemy ORM.

    Implements:
    - CRUD operations for ride_status_snapshots table
    - Latest snapshot queries for current status
    - Historical snapshot queries
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

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
        try:
            # Parse ISO 8601 timestamp if provided as string (e.g., '2024-03-19T03:04:01Z')
            last_updated_api = snapshot_data.get('last_updated_api')
            if last_updated_api and isinstance(last_updated_api, str):
                # Remove 'Z' suffix and parse ISO format
                ts = last_updated_api.replace('Z', '+00:00')
                last_updated_api = datetime.fromisoformat(ts).replace(tzinfo=None)

            snapshot = RideStatusSnapshot(
                ride_id=snapshot_data['ride_id'],
                recorded_at=snapshot_data['recorded_at'],
                wait_time=snapshot_data.get('wait_time'),
                is_open=snapshot_data.get('is_open'),
                computed_is_open=snapshot_data.get('computed_is_open', False),
                status=snapshot_data.get('status'),
                last_updated_api=last_updated_api or datetime.utcnow(),
                data_source=snapshot_data.get('data_source', 'LIVE')
            )

            self.session.add(snapshot)
            self.session.flush()  # Get snapshot_id without committing
            return snapshot.snapshot_id

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
        stmt = (
            select(RideStatusSnapshot)
            .where(RideStatusSnapshot.ride_id == ride_id)
            .order_by(RideStatusSnapshot.recorded_at.desc())
            .limit(1)
        )

        result = self.session.execute(stmt).scalar_one_or_none()

        if result is None:
            return None

        return {
            'snapshot_id': result.snapshot_id,
            'ride_id': result.ride_id,
            'recorded_at': result.recorded_at,
            'wait_time': result.wait_time,
            'is_open': result.is_open,
            'computed_is_open': result.computed_is_open,
            'status': result.status,
            'last_updated_api': result.last_updated_api
        }

    def get_latest_all_rides(self, park_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get most recent snapshot for each ride, optionally filtered by park.

        Args:
            park_id: Optional park ID to filter results

        Returns:
            List of dictionaries with snapshot data
        """
        # Subquery to get latest snapshot_id per ride from the last hour
        cutoff_time = datetime.utcnow() - timedelta(hours=1)

        latest_snapshot_subquery = (
            select(func.max(RideStatusSnapshot.snapshot_id).label('max_snapshot_id'))
            .where(RideStatusSnapshot.recorded_at >= cutoff_time)
            .group_by(RideStatusSnapshot.ride_id)
            .subquery()
        )

        # Main query
        stmt = (
            select(
                RideStatusSnapshot.snapshot_id,
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.recorded_at,
                RideStatusSnapshot.wait_time,
                RideStatusSnapshot.is_open,
                RideStatusSnapshot.computed_is_open,
                Ride.name.label('ride_name'),
                Ride.park_id
            )
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .where(RideStatusSnapshot.snapshot_id.in_(select(latest_snapshot_subquery.c.max_snapshot_id)))
        )

        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)

        stmt = stmt.order_by(RideStatusSnapshot.wait_time.desc())

        results = self.session.execute(stmt).all()
        return [dict(row._mapping) for row in results]

    def get_history(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical snapshots for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with snapshot data
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        stmt = (
            select(
                RideStatusSnapshot.snapshot_id,
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.recorded_at,
                RideStatusSnapshot.wait_time,
                RideStatusSnapshot.is_open,
                RideStatusSnapshot.computed_is_open,
                RideStatusSnapshot.last_updated_api
            )
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= cutoff_time
                )
            )
            .order_by(RideStatusSnapshot.recorded_at.desc())
        )

        results = self.session.execute(stmt).all()
        return [dict(row._mapping) for row in results]


class ParkActivitySnapshotRepository:
    """
    Repository for park activity snapshot operations using SQLAlchemy ORM.

    Implements:
    - CRUD operations for park_activity_snapshots table
    - Park operating status queries
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

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
        try:
            snapshot = ParkActivitySnapshot(
                park_id=activity_data['park_id'],
                recorded_at=activity_data['recorded_at'],
                total_rides_tracked=activity_data.get('total_rides_tracked', 0),
                rides_open=activity_data.get('rides_open', 0),
                rides_closed=activity_data.get('rides_closed', 0),
                avg_wait_time=activity_data.get('avg_wait_time'),
                max_wait_time=activity_data.get('max_wait_time'),
                park_appears_open=activity_data.get('park_appears_open', False),
                shame_score=activity_data.get('shame_score')
            )

            self.session.add(snapshot)
            self.session.flush()  # Get snapshot_id without committing
            return snapshot.snapshot_id

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
        stmt = (
            select(ParkActivitySnapshot)
            .where(ParkActivitySnapshot.park_id == park_id)
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .limit(1)
        )

        result = self.session.execute(stmt).scalar_one_or_none()

        if result is None:
            return None

        return {
            'snapshot_id': result.snapshot_id,
            'park_id': result.park_id,
            'recorded_at': result.recorded_at,
            'total_rides_tracked': result.total_rides_tracked,
            'rides_open': result.rides_open,
            'rides_closed': result.rides_closed,
            'avg_wait_time': result.avg_wait_time,
            'max_wait_time': result.max_wait_time,
            'park_appears_open': result.park_appears_open
        }

    def get_history(self, park_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical activity snapshots for a specific park.

        Args:
            park_id: Park ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with activity data
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        stmt = (
            select(
                ParkActivitySnapshot.snapshot_id,
                ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.recorded_at,
                ParkActivitySnapshot.total_rides_tracked,
                ParkActivitySnapshot.rides_open,
                ParkActivitySnapshot.rides_closed,
                ParkActivitySnapshot.avg_wait_time,
                ParkActivitySnapshot.max_wait_time,
                ParkActivitySnapshot.park_appears_open
            )
            .where(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= cutoff_time
                )
            )
            .order_by(ParkActivitySnapshot.recorded_at.desc())
        )

        results = self.session.execute(stmt).all()
        return [dict(row._mapping) for row in results]

    def get_all_latest(self) -> List[Dict[str, Any]]:
        """
        Get most recent activity snapshot for all parks.

        Returns:
            List of dictionaries with activity data
        """
        # Subquery to get latest snapshot_id per park from the last hour
        cutoff_time = datetime.utcnow() - timedelta(hours=1)

        latest_snapshot_subquery = (
            select(func.max(ParkActivitySnapshot.snapshot_id).label('max_snapshot_id'))
            .where(ParkActivitySnapshot.recorded_at >= cutoff_time)
            .group_by(ParkActivitySnapshot.park_id)
            .subquery()
        )

        # Main query
        stmt = (
            select(
                ParkActivitySnapshot.snapshot_id,
                ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.recorded_at,
                ParkActivitySnapshot.total_rides_tracked,
                ParkActivitySnapshot.rides_open,
                ParkActivitySnapshot.rides_closed,
                ParkActivitySnapshot.avg_wait_time,
                ParkActivitySnapshot.max_wait_time,
                ParkActivitySnapshot.park_appears_open,
                Park.name.label('park_name')
            )
            .join(Park, ParkActivitySnapshot.park_id == Park.park_id)
            .where(ParkActivitySnapshot.snapshot_id.in_(select(latest_snapshot_subquery.c.max_snapshot_id)))
            .order_by(Park.name)
        )

        results = self.session.execute(stmt).all()
        return [dict(row._mapping) for row in results]
