"""
Theme Park Downtime Tracker - Ride Repository (ORM Version)
Provides data access layer for rides table using SQLAlchemy ORM.
Maintains API compatibility by returning dataclass Ride objects.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, func, case, text
from datetime import datetime, timedelta

try:
    from ...models.ride import Ride as RideDataclass
    from ...models.orm_ride import Ride as RideORM
    from ...models.orm_park import Park as ParkORM
    from ...models.orm_snapshots import RideStatusSnapshot
    from ...utils.logger import logger, log_database_error
except ImportError:
    from models.ride import Ride as RideDataclass
    from models.orm_ride import Ride as RideORM
    from models.orm_park import Park as ParkORM
    from models.orm_snapshots import RideStatusSnapshot
    from utils.logger import logger, log_database_error


class RideRepository:
    """
    Repository for ride entity operations using SQLAlchemy ORM.

    Implements:
    - CRUD operations for rides
    - Ride performance rankings (FR-014)
    - Current wait times query (FR-017)
    - Ride status filtering

    Migration Note: Uses ORM internally but returns dataclass objects
    to maintain API compatibility with existing code.
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def get_by_id(self, ride_id: int) -> Optional[RideDataclass]:
        """
        Fetch ride by ID.

        Args:
            ride_id: Ride ID

        Returns:
            Ride dataclass object or None if not found
        """
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(RideORM.ride_id == ride_id)
        )

        result = self.session.execute(query).first()

        if result is None:
            return None

        ride_orm, park_qtid = result
        return self._orm_to_dataclass(ride_orm, park_qtid)

    def get_by_queue_times_id(self, queue_times_id: int) -> Optional[RideDataclass]:
        """
        Fetch ride by Queue-Times.com external ID.

        Args:
            queue_times_id: Queue-Times.com ride ID

        Returns:
            Ride dataclass object or None if not found
        """
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(RideORM.queue_times_id == queue_times_id)
        )

        result = self.session.execute(query).first()

        if result is None:
            return None

        ride_orm, park_qtid = result
        return self._orm_to_dataclass(ride_orm, park_qtid)

    def get_by_themeparks_wiki_id(self, wiki_id: str) -> Optional[RideDataclass]:
        """
        Fetch ride by ThemeParks.wiki entity UUID.

        Args:
            wiki_id: ThemeParks.wiki entity UUID

        Returns:
            Ride dataclass object or None if not found
        """
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(RideORM.themeparks_wiki_id == wiki_id)
        )

        result = self.session.execute(query).first()

        if result is None:
            return None

        ride_orm, park_qtid = result
        return self._orm_to_dataclass(ride_orm, park_qtid)

    def get_by_park_id(self, park_id: int, active_only: bool = True) -> List[RideDataclass]:
        """
        Fetch all rides for a specific park.

        Args:
            park_id: Park ID
            active_only: If True, only return active rides

        Returns:
            List of Ride dataclass objects
        """
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(RideORM.park_id == park_id)
        )

        if active_only:
            query = query.where(RideORM.is_active.is_(True))

        query = query.order_by(RideORM.name)

        results = self.session.execute(query).all()
        return [self._orm_to_dataclass(ride_orm, park_qtid) for ride_orm, park_qtid in results]

    def get_all_active(self) -> List[RideDataclass]:
        """
        Fetch all active rides.

        Returns:
            List of Ride dataclass objects
        """
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(RideORM.is_active.is_(True))
            .order_by(RideORM.park_id, RideORM.name)
        )

        results = self.session.execute(query).all()
        return [self._orm_to_dataclass(ride_orm, park_qtid) for ride_orm, park_qtid in results]

    def get_unclassified_rides(self) -> List[RideDataclass]:
        """
        Fetch rides that have no tier classification yet.

        Note: This uses raw SQL since ride_classifications table
        doesn't have an ORM model yet.

        Returns:
            List of Ride dataclass objects without tier classification
        """
        # For now, return rides with tier=NULL as proxy for unclassified
        query = (
            select(RideORM, ParkORM.queue_times_id.label('park_queue_times_id'))
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .where(
                and_(
                    RideORM.is_active.is_(True),
                    RideORM.tier.is_(None)
                )
            )
            .order_by(RideORM.park_id, RideORM.name)
        )

        results = self.session.execute(query).all()
        return [self._orm_to_dataclass(ride_orm, park_qtid) for ride_orm, park_qtid in results]

    def create(self, ride_data: Dict[str, Any]) -> RideDataclass:
        """
        Create new ride record.

        Args:
            ride_data: Dictionary with ride fields

        Returns:
            Created Ride dataclass object

        Raises:
            DatabaseError: If creation fails
        """
        try:
            ride_orm = RideORM(
                queue_times_id=ride_data['queue_times_id'],
                park_id=ride_data['park_id'],
                name=ride_data['name'],
                land_area=ride_data.get('land_area'),
                tier=ride_data.get('tier')
            )

            self.session.add(ride_orm)
            self.session.flush()  # Get the ride_id without committing

            logger.info(f"Created ride: {ride_data['name']} (ID: {ride_orm.ride_id})")

            return self.get_by_id(ride_orm.ride_id)

        except Exception as e:
            log_database_error(e, "Failed to create ride")
            self.session.rollback()
            raise

    def update(self, ride_id: int, ride_data: Dict[str, Any]) -> Optional[RideDataclass]:
        """
        Update existing ride record.

        Args:
            ride_id: Ride ID to update
            ride_data: Dictionary with fields to update

        Returns:
            Updated Ride dataclass object or None if not found
        """
        try:
            ride_orm = self.session.query(RideORM).filter(RideORM.ride_id == ride_id).first()

            if ride_orm is None:
                return None

            for field, value in ride_data.items():
                if hasattr(ride_orm, field):
                    setattr(ride_orm, field, value)

            self.session.flush()

            logger.info(f"Updated ride ID {ride_id}")
            return self.get_by_id(ride_id)

        except Exception as e:
            log_database_error(e, f"Failed to update ride ID {ride_id}")
            self.session.rollback()
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

        Note: This method uses daily stats only (weekly/monthly/yearly
        aggregation tables don't have ORM models yet).

        Args:
            period: "daily" (others not yet supported in ORM)
            stat_date: Date string (YYYY-MM-DD) or None for current date
            park_id: Optional park ID to filter results
            limit: Maximum number of results

        Returns:
            List of dictionaries with ride performance data
        """
        from src.models.orm_stats import RideDailyStats

        # For now, only support daily (other periods need weekly/monthly ORM models)
        if period != "daily":
            raise NotImplementedError(f"Period '{period}' not yet supported in ORM migration")

        # Get current status subquery
        current_status_subquery = (
            select(RideStatusSnapshot.computed_is_open)
            .where(RideStatusSnapshot.ride_id == RideORM.ride_id)
            .order_by(RideStatusSnapshot.recorded_at.desc())
            .limit(1)
            .scalar_subquery()
        )

        query = (
            select(
                RideORM.ride_id,
                RideORM.name.label('ride_name'),
                ParkORM.name.label('park_name'),
                ParkORM.park_id,
                (RideDailyStats.downtime_minutes / 60.0).label('downtime_hours'),
                RideDailyStats.uptime_percentage,
                RideDailyStats.avg_wait_time,
                RideDailyStats.peak_wait_time,
                current_status_subquery.label('current_status')
            )
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .join(RideDailyStats, RideORM.ride_id == RideDailyStats.ride_id)
            .where(
                and_(
                    RideDailyStats.stat_date == (stat_date or func.curdate()),
                    RideORM.is_active.is_(True),
                    ParkORM.is_active.is_(True)
                )
            )
        )

        if park_id:
            query = query.where(ParkORM.park_id == park_id)

        query = query.order_by(RideDailyStats.downtime_minutes.desc()).limit(limit)

        results = self.session.execute(query).all()
        return [dict(row._mapping) for row in results]

    def get_current_wait_times(
        self,
        park_id: Optional[int] = None,
        open_only: bool = False,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get current wait times for rides (FR-017).

        Note: Weekly stats aggregation not yet implemented in ORM.

        Args:
            park_id: Optional park ID to filter results
            open_only: If True, only show open rides
            limit: Maximum number of results

        Returns:
            List of dictionaries with current wait time data
        """
        # Subquery for most recent snapshot per ride (within last hour)
        # Use ORDER BY recorded_at DESC to get the actual latest snapshot
        latest_snapshot_subquery = (
            select(RideStatusSnapshot.snapshot_id)
            .where(
                and_(
                    RideStatusSnapshot.ride_id == RideORM.ride_id,
                    RideStatusSnapshot.recorded_at >= func.date_sub(func.now(), text("INTERVAL 1 HOUR"))
                )
            )
            .order_by(RideStatusSnapshot.recorded_at.desc(), RideStatusSnapshot.snapshot_id.desc())
            .limit(1)
            .scalar_subquery()
        )

        query = (
            select(
                RideORM.ride_id,
                RideORM.name.label('ride_name'),
                ParkORM.name.label('park_name'),
                ParkORM.park_id,
                RideStatusSnapshot.wait_time.label('current_wait'),
                RideStatusSnapshot.computed_is_open.label('is_currently_open'),
                RideStatusSnapshot.recorded_at.label('last_updated')
            )
            .join(ParkORM, RideORM.park_id == ParkORM.park_id)
            .join(RideStatusSnapshot, RideORM.ride_id == RideStatusSnapshot.ride_id)
            .where(
                and_(
                    RideStatusSnapshot.snapshot_id == latest_snapshot_subquery,
                    RideORM.is_active.is_(True),
                    ParkORM.is_active.is_(True)
                )
            )
        )

        if park_id:
            query = query.where(ParkORM.park_id == park_id)

        if open_only:
            query = query.where(RideStatusSnapshot.computed_is_open.is_(True))

        query = query.order_by(RideStatusSnapshot.wait_time.desc()).limit(limit)

        results = self.session.execute(query).all()
        return [dict(row._mapping) for row in results]

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
        query = (
            select(
                RideStatusSnapshot.snapshot_id,
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.recorded_at,
                RideStatusSnapshot.is_open,
                RideStatusSnapshot.wait_time,
                RideStatusSnapshot.computed_is_open
            )
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= (func.now() - timedelta(hours=hours))
                )
            )
            .order_by(RideStatusSnapshot.recorded_at.desc())
        )

        results = self.session.execute(query).all()
        return [dict(row._mapping) for row in results]

    def get_downtime_changes(
        self,
        ride_id: int,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get downtime change events for a specific ride.

        Note: ride_status_changes table doesn't have ORM model yet.
        Returns empty list until implemented.

        Args:
            ride_id: Ride ID
            hours: Number of hours to look back (default 24)

        Returns:
            List of dictionaries with status change events
        """
        # TODO: Implement when ride_status_changes ORM model is created
        logger.warning("get_downtime_changes not yet implemented in ORM migration")
        return []

    def _orm_to_dataclass(self, ride_orm: RideORM, park_queue_times_id: Optional[int] = None) -> RideDataclass:
        """
        Convert ORM Ride object to dataclass Ride object.

        Args:
            ride_orm: ORM Ride instance
            park_queue_times_id: Park's queue_times_id (from join)

        Returns:
            Dataclass Ride instance
        """
        return RideDataclass(
            ride_id=ride_orm.ride_id,
            queue_times_id=ride_orm.queue_times_id,
            park_id=ride_orm.park_id,
            name=ride_orm.name,
            land_area=ride_orm.land_area,
            tier=ride_orm.tier,
            category=ride_orm.category,
            is_active=ride_orm.is_active,
            created_at=ride_orm.created_at,
            updated_at=ride_orm.updated_at,
            park_queue_times_id=park_queue_times_id
        )
