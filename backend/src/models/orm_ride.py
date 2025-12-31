"""
SQLAlchemy ORM Model: Ride
Represents theme park ride/attraction data.
"""

from sqlalchemy import String, Boolean, Integer, ForeignKey, DateTime, Enum, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime
from typing import List, Optional


class Ride(Base):
    __tablename__ = "rides"
    __table_args__ = {'extend_existing': True}

    # Primary Key
    ride_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False
    )

    # Queue-Times.com Integration
    queue_times_id: Mapped[int] = mapped_column(
        nullable=False,
        unique=True,
        comment="External ID from Queue-Times.com API"
    )

    # Basic Information
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[Optional[str]] = mapped_column(
        Enum('ATTRACTION', 'SHOW', 'RESTAURANT', name='entity_type_enum'),
        default='ATTRACTION'
    )
    category: Mapped[Optional[str]] = mapped_column(
        Enum('ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE', name='category_enum'),
        default='ATTRACTION'
    )
    land_area: Mapped[Optional[str]] = mapped_column(
        String(100),
        comment="Themed land/area within park (e.g., Fantasyland)"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="FALSE if ride permanently closed or removed"
    )

    # Ride Classification
    tier: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Ride demand tier: 1=flagship/high-demand, 2=moderate, 3=low-demand/filler"
    )

    # ThemeParks.wiki Integration (optional)
    themeparks_wiki_id: Mapped[Optional[str]] = mapped_column(String(36))

    # Disney Integration (optional)
    disney_entity_id: Mapped[Optional[str]] = mapped_column(String(50))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )
    last_operated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        comment="Last time ride was observed operating (populated by collector/aggregator scripts)"
    )

    # Relationships
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="rides"
    )
    ride_snapshots: Mapped[List["RideStatusSnapshot"]] = relationship(
        "RideStatusSnapshot",
        back_populates="ride",
        lazy="select"
    )
    daily_stats: Mapped[List["RideDailyStats"]] = relationship(
        "RideDailyStats",
        back_populates="ride",
        lazy="select"
    )
    weekly_stats: Mapped[List["RideWeeklyStats"]] = relationship(
        "RideWeeklyStats",
        back_populates="ride",
        lazy="select"
    )
    monthly_stats: Mapped[List["RideMonthlyStats"]] = relationship(
        "RideMonthlyStats",
        back_populates="ride",
        lazy="select"
    )
    classification: Mapped[Optional["RideClassification"]] = relationship(
        "RideClassification",
        back_populates="ride",
        lazy="select",
        uselist=False
    )

    # Model Methods (Business Logic)
    def get_current_status(self, session: Optional["Session"] = None):
        """
        Get most recent status snapshot for this ride.

        Args:
            session: Optional SQLAlchemy session (uses object_session if not provided)

        Returns:
            RideStatusSnapshot instance or None if no snapshots exist
        """
        from sqlalchemy.orm import Session, object_session
        from models.orm_snapshots import RideStatusSnapshot

        session = session or object_session(self)
        if session is None:
            raise RuntimeError("Ride.get_current_status requires an active Session")

        snapshot = (
            session.query(RideStatusSnapshot)
            .filter(RideStatusSnapshot.ride_id == self.ride_id)
            .order_by(RideStatusSnapshot.recorded_at.desc())
            .first()
        )

        return snapshot

    def calculate_uptime(self, start_time: datetime, end_time: datetime, session: Optional["Session"] = None) -> float:
        """
        Calculate uptime percentage for this ride over a time period.

        Args:
            start_time: Period start (UTC)
            end_time: Period end (UTC)
            session: Optional SQLAlchemy session (uses object_session if not provided)

        Returns:
            Uptime percentage (0.0 to 100.0)
        """
        from sqlalchemy.orm import Session, object_session
        from models.orm_snapshots import RideStatusSnapshot
        from sqlalchemy import func, case

        session = session or object_session(self)
        if session is None:
            raise RuntimeError("Ride.calculate_uptime requires an active Session")

        # Use explicit SQL expression for hybrid method
        operating_expr = RideStatusSnapshot.is_operating()

        # Count total snapshots and operating snapshots
        result = (
            session.query(
                func.count(RideStatusSnapshot.snapshot_id).label('total'),
                func.sum(
                    case(
                        (operating_expr, 1),
                        else_=0
                    )
                ).label('operating')
            )
            .filter(RideStatusSnapshot.ride_id == self.ride_id)
            .filter(RideStatusSnapshot.recorded_at.between(start_time, end_time))
            .filter(RideStatusSnapshot.park_appears_open == True)
            .first()
        )

        if not result or not result.total or result.total == 0:
            return 0.0

        return (result.operating / result.total) * 100.0

    def __repr__(self) -> str:
        return f"<Ride(ride_id={self.ride_id}, name='{self.name}', park_id={self.park_id})>"
