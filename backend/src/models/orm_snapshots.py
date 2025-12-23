"""
SQLAlchemy ORM Models: Snapshot Tables
RideStatusSnapshot and ParkActivitySnapshot for time-series data.
"""

from sqlalchemy import String, Boolean, Integer, ForeignKey, DateTime, Float, Index, Enum, or_, BigInteger, Numeric, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from src.models.base import Base
from datetime import datetime
from typing import Optional
from decimal import Decimal


class RideStatusSnapshot(Base):
    """
    Ride status point-in-time snapshot from Queue-Times.com API.
    Collected every 10 minutes, retained for 24 hours before aggregation.
    """
    __tablename__ = "ride_status_snapshots"

    # Primary Key
    snapshot_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Foreign Keys
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE"),
        nullable=False
    )

    # Snapshot Metadata
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="UTC timestamp when snapshot was collected"
    )

    # Queue-Times.com Raw Data
    is_open: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        comment="Raw is_open flag from Queue-Times.com API"
    )
    status: Mapped[Optional[str]] = mapped_column(
        Enum('OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT', name='ride_status_enum'),
        comment="Raw status from API: OPERATING, DOWN, CLOSED, REFURBISHMENT, etc."
    )
    wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Wait time in minutes (NULL if not operating)"
    )
    last_updated_api: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last update timestamp from Queue-Times.com"
    )

    # Computed Fields (Business Logic)
    computed_is_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="TRUE if ride is operating (status='OPERATING' OR wait_time > 0)"
    )
    park_appears_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="TRUE if park appears to be operating at snapshot time"
    )

    # Composite Indexes for Performance (already exist in database)
    __table_args__ = (
        Index('idx_ride_recorded', 'ride_id', 'recorded_at'),
        Index('idx_ride_status_snapshots_recorded_at', 'recorded_at'),
        Index('idx_computed_status', 'computed_is_open', 'recorded_at'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        back_populates="ride_snapshots"
    )

    # Hybrid Methods (Business Logic - work in both Python and SQL)
    @hybrid_method
    def is_operating(self):
        """
        Ride is operating if status='OPERATING' OR computed_is_open=TRUE.

        Returns:
            True if ride is operating
        """
        return (self.status == 'OPERATING') or (self.computed_is_open == True)

    @is_operating.expression
    def is_operating(cls):
        """SQL expression for is_operating (for WHERE clauses)"""
        return or_(cls.status == 'OPERATING', cls.computed_is_open == True)

    @hybrid_method
    def is_down(self):
        """
        Ride is down if status='DOWN' (or 'CLOSED' for non-Disney/Universal parks).

        Note: This is a simplified version. Full logic requires park_type
        from the related park for Disney/Universal distinction.

        Returns:
            True if ride is down
        """
        return self.status == 'DOWN'

    @is_down.expression
    def is_down(cls):
        """SQL expression for is_down (for WHERE clauses)"""
        return cls.status == 'DOWN'

    def __repr__(self) -> str:
        return f"<RideStatusSnapshot(snapshot_id={self.snapshot_id}, ride_id={self.ride_id}, status='{self.status}', time={self.recorded_at})>"


class ParkActivitySnapshot(Base):
    """
    Park operating hours snapshot derived from ride activity.
    Used to determine when parks are actually open (vs. scheduled hours).
    """
    __tablename__ = "park_activity_snapshots"

    # Primary Key
    snapshot_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False
    )

    # Snapshot Metadata
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="UTC timestamp when snapshot was analyzed"
    )

    # Park Activity Metrics
    total_rides_tracked: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total number of rides being tracked for this park"
    )
    rides_open: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of rides operating at snapshot time"
    )
    rides_closed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of rides closed at snapshot time"
    )
    avg_wait_time: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Average wait time across all operating rides"
    )
    max_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Maximum wait time across all rides"
    )

    # Park Operating Status (Computed)
    park_appears_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="TRUE if park appears to be operating (rides are active)"
    )

    # Shame Score (for trending)
    shame_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(4, 1),
        comment="Aggregate shame score for park at this snapshot"
    )

    # Composite Indexes for Performance (already exist in database)
    __table_args__ = (
        Index('idx_park_recorded', 'park_id', 'recorded_at'),
        Index('idx_park_activity_snapshots_recorded_at', 'recorded_at'),
        Index('idx_park_open', 'park_id', 'park_appears_open', 'recorded_at'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="park_snapshots"
    )

    def __repr__(self) -> str:
        return f"<ParkActivitySnapshot(snapshot_id={self.snapshot_id}, park_id={self.park_id}, open={self.park_appears_open}, time={self.recorded_at})>"
