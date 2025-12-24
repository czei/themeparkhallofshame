"""
SQLAlchemy ORM Model: ParkOperatingSession
Represents daily operating hours for theme parks.
"""

from sqlalchemy import Integer, ForeignKey, Date, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime, date
from typing import Optional


class ParkOperatingSession(Base):
    """
    Daily operating hours session for a theme park.

    Records first/last ride activity to infer park open/close times.
    Used by aggregation service to determine valid snapshot windows.
    """
    __tablename__ = "park_operating_sessions"
    __table_args__ = {'extend_existing': True}

    # Primary Key
    session_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    # Foreign Keys
    park_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("parks.park_id", ondelete="SET NULL"),
        nullable=True
    )

    # Session Date and Times
    session_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Operating date in park's local timezone"
    )
    session_start_utc: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="First ride activity timestamp (UTC)"
    )
    session_end_utc: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="Last ride activity timestamp (UTC)"
    )

    # Duration
    operating_minutes: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Total operating duration in minutes"
    )

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
    park: Mapped[Optional["Park"]] = relationship(
        "Park",
        back_populates="operating_sessions"
    )

    def __repr__(self) -> str:
        return (
            f"<ParkOperatingSession(session_id={self.session_id}, "
            f"park_id={self.park_id}, date={self.session_date}, "
            f"duration={self.operating_minutes}min)>"
        )
