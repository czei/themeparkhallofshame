"""
SQLAlchemy ORM Model: RideStatusChange
Represents ride status change events (up/down transitions).
"""

from sqlalchemy import Boolean, Integer, ForeignKey, DateTime, BigInteger, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime
from typing import Optional


class RideStatusChange(Base):
    """
    Ride status change event tracking.
    Records when rides transition between operating and closed states.
    """
    __tablename__ = "ride_status_changes"

    # Primary Key
    change_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Foreign Keys
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE"),
        nullable=False
    )

    # Change Metadata
    changed_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="UTC timestamp of status change"
    )

    # Status Transition
    previous_status: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="Previous computed_is_open value"
    )
    new_status: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        comment="New computed_is_open value"
    )

    # Duration Tracking
    duration_in_previous_status: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Minutes spent in previous status"
    )

    # Context
    wait_time_at_change: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Wait time when change occurred"
    )

    # Composite Indexes for Performance (already exist in database)
    __table_args__ = (
        Index('idx_ride_changed', 'ride_id', 'changed_at', mysql_length={'changed_at': None}),
        Index('idx_changed_at', 'changed_at'),
        Index('idx_downtime', 'ride_id', 'new_status', 'changed_at'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        foreign_keys=[ride_id]
    )

    def __repr__(self) -> str:
        status_change = f"{self.previous_status} -> {self.new_status}"
        return f"<RideStatusChange(change_id={self.change_id}, ride_id={self.ride_id}, {status_change}, time={self.changed_at})>"
