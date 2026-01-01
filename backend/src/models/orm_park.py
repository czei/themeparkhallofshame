"""
SQLAlchemy ORM Model: Park
Represents theme park master data.
"""

from sqlalchemy import String, Boolean, Float, DateTime, func, Enum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime
from typing import List, Optional


class Park(Base):
    __tablename__ = "parks"
    __table_args__ = {'extend_existing': True}

    # Primary Key
    park_id: Mapped[int] = mapped_column(primary_key=True)

    # Queue-Times.com Integration
    queue_times_id: Mapped[int] = mapped_column(
        nullable=False,
        unique=True,
        comment="External ID from Queue-Times.com API"
    )

    # Basic Information
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    state_province: Mapped[Optional[str]] = mapped_column(String(100))
    country: Mapped[str] = mapped_column(String(2), nullable=False, comment="ISO 3166-1 alpha-2 country code")

    # Geographic Coordinates
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)

    # Operational Details
    timezone: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default='America/Los_Angeles',
        comment="Park local timezone for operating hours calculation"
    )
    operator: Mapped[Optional[str]] = mapped_column(String(100))

    # Park Classification
    is_disney: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Disney parks handle CLOSED status differently"
    )
    is_universal: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Universal parks handle CLOSED status differently"
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="FALSE if park permanently closed"
    )

    # ThemeParks.wiki Integration (optional)
    themeparks_wiki_id: Mapped[Optional[str]] = mapped_column(String(36))

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

    # Relationships
    rides: Mapped[List["Ride"]] = relationship(
        "Ride",
        back_populates="park",
        lazy="select",  # Default: lazy load, use joinedload() for hot paths
        cascade="all, delete-orphan"
    )
    schedules: Mapped[List["ParkSchedule"]] = relationship(
        "ParkSchedule",
        back_populates="park",
        lazy="select"
    )
    park_snapshots: Mapped[List["ParkActivitySnapshot"]] = relationship(
        "ParkActivitySnapshot",
        back_populates="park",
        lazy="select"
    )
    daily_stats: Mapped[List["ParkDailyStats"]] = relationship(
        "ParkDailyStats",
        back_populates="park",
        lazy="select"
    )
    weekly_stats: Mapped[List["ParkWeeklyStats"]] = relationship(
        "ParkWeeklyStats",
        back_populates="park",
        lazy="select"
    )
    monthly_stats: Mapped[List["ParkMonthlyStats"]] = relationship(
        "ParkMonthlyStats",
        back_populates="park",
        lazy="select"
    )
    operating_sessions: Mapped[List["ParkOperatingSession"]] = relationship(
        "ParkOperatingSession",
        back_populates="park",
        lazy="select"
    )
    import_checkpoints: Mapped[List["ImportCheckpoint"]] = relationship(
        "ImportCheckpoint",
        back_populates="park",
        lazy="select"
    )

    # Model Methods (Business Logic)
    def is_operating_at(self, timestamp: datetime, session: Optional["Session"] = None) -> bool:
        """
        Check if park is operating at given timestamp.
        Queries park_activity_snapshots for park_appears_open=TRUE.

        Args:
            timestamp: UTC datetime to check
            session: Optional SQLAlchemy session (uses object_session if not provided)

        Returns:
            True if park appears open at the given timestamp
        """
        from sqlalchemy.orm import Session, object_session
        from models.orm_snapshots import ParkActivitySnapshot

        session = session or object_session(self)
        if session is None:
            raise RuntimeError("Park.is_operating_at requires an active Session")

        snapshot = (
            session.query(ParkActivitySnapshot)
            .filter(ParkActivitySnapshot.park_id == self.park_id)
            .filter(ParkActivitySnapshot.recorded_at <= timestamp)
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .first()
        )

        return snapshot.park_appears_open if snapshot else False

    @property
    def park_type(self) -> str:
        """
        Get park type classification for downtime logic.

        Returns:
            'disney', 'universal', or 'other'
        """
        if self.is_disney:
            return 'disney'
        elif self.is_universal:
            return 'universal'
        else:
            return 'other'

    def __repr__(self) -> str:
        return f"<Park(park_id={self.park_id}, name='{self.name}', type='{self.park_type}')>"
