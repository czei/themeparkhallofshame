"""
SQLAlchemy ORM Model: ParkSchedule
Represents theme park operating schedule data.
"""

from sqlalchemy import String, Date, DateTime, Integer, ForeignKey, UniqueConstraint, func, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime, date
from typing import Optional


class ParkSchedule(Base):
    __tablename__ = "park_schedules"
    __table_args__ = (
        UniqueConstraint('park_id', 'schedule_date', name='park_schedule_unique'),
        {'extend_existing': True}
    )

    # Primary Key
    schedule_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key
    park_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey('parks.park_id'),
        nullable=False,
        comment="Reference to parks table"
    )

    # Schedule Data
    schedule_date: Mapped[date] = mapped_column(Date, nullable=False, comment="Date for this schedule entry")
    opening_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, comment="Park opening time (UTC)")
    closing_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, comment="Park closing time (UTC)")

    schedule_type: Mapped[str] = mapped_column(
        SQLEnum('OPERATING', 'TICKETED_EVENT', 'PRIVATE_EVENT', 'EXTRA_HOURS', 'INFO', name='schedule_type_enum'),
        nullable=False,
        default='OPERATING',
        comment="Type of schedule entry"
    )

    # Metadata
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="When this schedule data was fetched from API"
    )
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
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="schedules",
        lazy="select"
    )

    def __repr__(self) -> str:
        return (
            f"<ParkSchedule(schedule_id={self.schedule_id}, park_id={self.park_id}, "
            f"date={self.schedule_date}, type='{self.schedule_type}')>"
        )
