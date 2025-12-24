"""
Theme Park Downtime Tracker - Status Change Repository
Provides data access layer for ride status change events (up/down transitions).
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func, case, and_, desc

from src.models import RideStatusChange, Ride, Park
from src.utils.logger import logger, log_database_error


def _to_dict(obj) -> Dict[str, Any]:
    """Convert ORM object or Row to dict, handling attribute access"""
    if hasattr(obj, '__dict__'):
        # ORM object - extract non-private attributes
        return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
    else:
        # Row object from joined query
        return dict(obj._mapping)


class RideStatusChangeRepository:
    """
    Repository for ride status change operations.

    Implements:
    - CRUD operations for ride_status_changes table
    - Status change detection and tracking
    - Downtime duration queries
    """

    def __init__(self, session: Session):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy Session object
        """
        self.session = session

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
        try:
            change = RideStatusChange(**change_data)
            self.session.add(change)
            self.session.flush()
            return change.change_id

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
        stmt = (
            select(RideStatusChange)
            .where(RideStatusChange.ride_id == ride_id)
            .order_by(desc(RideStatusChange.changed_at))
            .limit(1)
        )

        result = self.session.execute(stmt).scalars().first()

        if result is None:
            return None

        return {
            'change_id': result.change_id,
            'ride_id': result.ride_id,
            'changed_at': result.changed_at,
            'previous_status': result.previous_status,
            'new_status': result.new_status,
            'duration_in_previous_status': result.duration_in_previous_status,
            'wait_time_at_change': result.wait_time_at_change
        }

    def get_history(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical status changes for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with change data
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        stmt = (
            select(RideStatusChange)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.changed_at >= cutoff_time
                )
            )
            .order_by(desc(RideStatusChange.changed_at))
        )

        results = self.session.execute(stmt).scalars().all()

        return [
            {
                'change_id': r.change_id,
                'ride_id': r.ride_id,
                'changed_at': r.changed_at,
                'previous_status': r.previous_status,
                'new_status': r.new_status,
                'duration_in_previous_status': r.duration_in_previous_status,
                'wait_time_at_change': r.wait_time_at_change
            }
            for r in results
        ]

    def get_downtime_events(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get downtime events (transitions to closed status) for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with downtime events
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        stmt = (
            select(RideStatusChange)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.new_status == False,
                    RideStatusChange.changed_at >= cutoff_time
                )
            )
            .order_by(desc(RideStatusChange.changed_at))
        )

        results = self.session.execute(stmt).scalars().all()

        return [
            {
                'change_id': r.change_id,
                'ride_id': r.ride_id,
                'changed_at': r.changed_at,
                'previous_status': r.previous_status,
                'new_status': r.new_status,
                'duration_in_previous_status': r.duration_in_previous_status,
                'wait_time_at_change': r.wait_time_at_change
            }
            for r in results
        ]

    def get_uptime_events(self, ride_id: int, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get uptime events (transitions to open status) for a specific ride.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back

        Returns:
            List of dictionaries with uptime events
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        stmt = (
            select(RideStatusChange)
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.new_status == True,
                    RideStatusChange.changed_at >= cutoff_time
                )
            )
            .order_by(desc(RideStatusChange.changed_at))
        )

        results = self.session.execute(stmt).scalars().all()

        return [
            {
                'change_id': r.change_id,
                'ride_id': r.ride_id,
                'changed_at': r.changed_at,
                'previous_status': r.previous_status,
                'new_status': r.new_status,
                'duration_in_previous_status': r.duration_in_previous_status,
                'wait_time_at_change': r.wait_time_at_change
            }
            for r in results
        ]

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

        # Build query with joins
        stmt = (
            select(
                RideStatusChange.change_id,
                RideStatusChange.ride_id,
                RideStatusChange.changed_at,
                RideStatusChange.previous_status,
                RideStatusChange.new_status,
                RideStatusChange.duration_in_previous_status,
                Ride.name.label('ride_name'),
                Ride.park_id,
                Park.name.label('park_name')
            )
            .join(Ride, RideStatusChange.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .where(RideStatusChange.changed_at >= cutoff_time)
        )

        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)

        stmt = stmt.order_by(desc(RideStatusChange.changed_at)).limit(limit)

        results = self.session.execute(stmt).all()

        return [_to_dict(row) for row in results]

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

        # Build query with joins
        stmt = (
            select(
                RideStatusChange.change_id,
                RideStatusChange.ride_id,
                RideStatusChange.changed_at,
                RideStatusChange.duration_in_previous_status,
                Ride.name.label('ride_name'),
                Ride.park_id,
                Park.name.label('park_name')
            )
            .join(Ride, RideStatusChange.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .where(
                and_(
                    RideStatusChange.changed_at >= cutoff_time,
                    RideStatusChange.new_status == False,
                    RideStatusChange.duration_in_previous_status.isnot(None)
                )
            )
        )

        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)

        stmt = stmt.order_by(desc(RideStatusChange.duration_in_previous_status)).limit(limit)

        results = self.session.execute(stmt).all()

        return [_to_dict(row) for row in results]

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
        cutoff_time = datetime.now() - timedelta(hours=hours)

        stmt = (
            select(
                func.count(RideStatusChange.change_id).label('total'),
                func.sum(
                    case(
                        (RideStatusChange.new_status == True, 1),
                        else_=0
                    )
                ).label('to_open'),
                func.sum(
                    case(
                        (RideStatusChange.new_status == False, 1),
                        else_=0
                    )
                ).label('to_closed')
            )
            .where(
                and_(
                    RideStatusChange.ride_id == ride_id,
                    RideStatusChange.changed_at >= cutoff_time
                )
            )
        )

        result = self.session.execute(stmt).first()

        if result is None:
            return {"total": 0, "to_open": 0, "to_closed": 0}

        return {
            "total": result.total or 0,
            "to_open": result.to_open or 0,
            "to_closed": result.to_closed or 0
        }
