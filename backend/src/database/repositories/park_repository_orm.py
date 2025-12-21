"""
Theme Park Downtime Tracker - Park Repository (ORM Version)
Provides data access layer for parks table using SQLAlchemy ORM.
Maintains API compatibility by returning dataclass Park objects.
"""

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func

try:
    from ...models.park import Park as ParkDataclass
    from ...models.orm_park import Park as ParkORM
    from ...utils.logger import logger, log_database_error
except ImportError:
    from models.park import Park as ParkDataclass
    from models.orm_park import Park as ParkORM
    from utils.logger import logger, log_database_error


class ParkRepository:
    """
    Repository for park entity operations using SQLAlchemy ORM.

    Implements:
    - CRUD operations for parks
    - Park filtering by type (Disney/Universal)

    Migration Note: Uses ORM internally but returns dataclass objects
    to maintain API compatibility with existing code.

    Ranking methods have been migrated to modular query classes in database/queries/.
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def get_by_id(self, park_id: int) -> Optional[ParkDataclass]:
        """
        Fetch park by ID.

        Args:
            park_id: Park ID

        Returns:
            Park dataclass object or None if not found
        """
        park_orm = self.session.query(ParkORM).filter(ParkORM.park_id == park_id).first()

        if park_orm is None:
            return None

        return self._orm_to_dataclass(park_orm)

    def get_by_queue_times_id(self, queue_times_id: int) -> Optional[ParkDataclass]:
        """
        Fetch park by Queue-Times.com external ID.

        Args:
            queue_times_id: Queue-Times.com park ID

        Returns:
            Park dataclass object or None if not found
        """
        park_orm = self.session.query(ParkORM).filter(
            ParkORM.queue_times_id == queue_times_id
        ).first()

        if park_orm is None:
            return None

        return self._orm_to_dataclass(park_orm)

    def get_all_active(self) -> List[ParkDataclass]:
        """
        Fetch all active parks.

        Returns:
            List of Park dataclass objects
        """
        parks_orm = (
            self.session.query(ParkORM)
            .filter(ParkORM.is_active.is_(True))
            .order_by(ParkORM.name)
            .all()
        )

        return [self._orm_to_dataclass(park) for park in parks_orm]

    def get_disney_universal_parks(self) -> List[ParkDataclass]:
        """
        Fetch all Disney and Universal parks.

        Returns:
            List of Park dataclass objects for Disney/Universal parks
        """
        parks_orm = (
            self.session.query(ParkORM)
            .filter(
                and_(
                    ParkORM.is_active.is_(True),
                    (ParkORM.is_disney.is_(True)) | (ParkORM.is_universal.is_(True))
                )
            )
            .order_by(ParkORM.name)
            .all()
        )

        return [self._orm_to_dataclass(park) for park in parks_orm]

    def create(self, park_data: Dict[str, Any]) -> ParkDataclass:
        """
        Create new park record.

        Args:
            park_data: Dictionary with park fields

        Returns:
            Created Park dataclass object

        Raises:
            DatabaseError: If creation fails
        """
        try:
            park_orm = ParkORM(
                queue_times_id=park_data['queue_times_id'],
                name=park_data['name'],
                city=park_data.get('city'),
                state_province=park_data.get('state_province'),
                country=park_data.get('country'),
                latitude=park_data.get('latitude'),
                longitude=park_data.get('longitude'),
                timezone=park_data.get('timezone'),
                operator=park_data.get('operator'),
                is_disney=park_data.get('is_disney', False),
                is_universal=park_data.get('is_universal', False),
                themeparks_wiki_id=park_data.get('themeparks_wiki_id')
            )

            self.session.add(park_orm)
            self.session.flush()  # Get the park_id without committing

            logger.info(f"Created park: {park_data['name']} (ID: {park_orm.park_id})")

            return self.get_by_id(park_orm.park_id)

        except Exception as e:
            log_database_error(e, "Failed to create park")
            self.session.rollback()
            raise

    def update(self, park_id: int, park_data: Dict[str, Any]) -> Optional[ParkDataclass]:
        """
        Update existing park record.

        Args:
            park_id: Park ID to update
            park_data: Dictionary with fields to update

        Returns:
            Updated Park dataclass object or None if not found
        """
        try:
            park_orm = self.session.query(ParkORM).filter(ParkORM.park_id == park_id).first()

            if park_orm is None:
                return None

            for field, value in park_data.items():
                if hasattr(park_orm, field):
                    setattr(park_orm, field, value)

            self.session.flush()

            logger.info(f"Updated park ID {park_id}")
            return self.get_by_id(park_id)

        except Exception as e:
            log_database_error(e, f"Failed to update park ID {park_id}")
            self.session.rollback()
            raise

    def _orm_to_dataclass(self, park_orm: ParkORM) -> ParkDataclass:
        """
        Convert ORM Park object to dataclass Park object.

        Args:
            park_orm: ORM Park instance

        Returns:
            Dataclass Park instance
        """
        return ParkDataclass(
            park_id=park_orm.park_id,
            queue_times_id=park_orm.queue_times_id,
            themeparks_wiki_id=park_orm.themeparks_wiki_id,
            name=park_orm.name,
            city=park_orm.city,
            state_province=park_orm.state_province,
            country=park_orm.country,
            latitude=float(park_orm.latitude) if park_orm.latitude else None,
            longitude=float(park_orm.longitude) if park_orm.longitude else None,
            timezone=park_orm.timezone,
            operator=park_orm.operator,
            is_disney=park_orm.is_disney,
            is_universal=park_orm.is_universal,
            is_active=park_orm.is_active,
            created_at=park_orm.created_at,
            updated_at=park_orm.updated_at
        )

    # === DEPRECATED METHODS ===
    # Ranking methods have been migrated to modular query classes.

    def get_rankings_by_downtime(self, *args, **kwargs):
        """DEPRECATED: Use queries/rankings/park_downtime_rankings.py instead."""
        raise NotImplementedError(
            "get_rankings_by_downtime() is deprecated. "
            "Use ParkDowntimeRankingsQuery from queries/rankings/park_downtime_rankings.py"
        )

    def get_rankings_by_weighted_downtime(self, *args, **kwargs):
        """DEPRECATED: Use queries/rankings/park_downtime_rankings.py instead."""
        raise NotImplementedError(
            "get_rankings_by_weighted_downtime() is deprecated. "
            "Use ParkDowntimeRankingsQuery from queries/rankings/park_downtime_rankings.py"
        )
