"""
SQLAlchemy ORM Models: Stats Tables
RideDailyStats and ParkDailyStats aggregated daily statistics.
"""

from sqlalchemy import Integer, ForeignKey, Date, Numeric, DateTime, Index, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import date, datetime
from typing import Optional
from decimal import Decimal


class RideDailyStats(Base):
    """
    Daily aggregated statistics for ride performance.
    Calculated from ride_status_snapshots and retained permanently.
    """
    __tablename__ = "ride_daily_stats"

    # Primary Key
    stat_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    ride_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE")
    )

    # Date (Pacific Timezone)
    stat_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
        comment="Calendar date in Pacific timezone"
    )

    # Aggregated Metrics
    uptime_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total minutes ride was operating during park operating hours"
    )
    downtime_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        index=True,
        comment="Total minutes ride was down during park operating hours"
    )
    uptime_percentage: Mapped[Decimal] = mapped_column(
        Numeric(5, 2),
        nullable=False,
        default=0.0,
        comment="Percentage of time ride was operating (0.0-100.0)"
    )
    operating_hours_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total minutes park was open"
    )

    # Wait Time Statistics
    avg_wait_time: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Average wait time in minutes"
    )
    min_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Minimum wait time observed"
    )
    max_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Maximum wait time observed"
    )
    peak_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Peak wait time during busiest hour"
    )

    # Downtime Details
    status_changes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of status changes (operating <-> down)"
    )
    longest_downtime_minutes: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Longest continuous downtime period in minutes"
    )

    # Calculation Metadata
    metrics_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        comment="Calculation version for side-by-side comparison during bug fixes"
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_ride_daily_stats_ride', 'ride_id'),
        Index('idx_ride_daily_stats_date', 'stat_date'),
        Index('idx_ride_daily_ranking', 'stat_date', 'downtime_minutes'),
        Index('idx_ride_daily_stats_version', 'metrics_version'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        back_populates="daily_stats"
    )

    def __repr__(self) -> str:
        return f"<RideDailyStats(stat_id={self.stat_id}, ride_id={self.ride_id}, date={self.stat_date}, downtime={self.downtime_minutes}min)>"


class ParkDailyStats(Base):
    """
    Daily aggregated statistics for park performance.
    Calculated from park_activity_snapshots and ride stats.
    """
    __tablename__ = "park_daily_stats"

    # Primary Key
    stat_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE")
    )

    # Date (Pacific Timezone)
    stat_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
        comment="Calendar date in Pacific timezone"
    )

    # Aggregated Metrics
    total_rides_tracked: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of rides tracked for this park"
    )
    avg_uptime_percentage: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Average uptime percentage across all rides"
    )
    shame_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 3),
        comment="Weighted downtime score for park"
    )
    total_downtime_hours: Mapped[Decimal] = mapped_column(
        Numeric(8, 2),
        nullable=False,
        default=0.0,
        index=True,
        comment="Total downtime hours across all rides"
    )
    rides_with_downtime: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of rides that experienced downtime"
    )

    # Wait Time Statistics
    avg_wait_time: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Average wait time across all rides"
    )
    peak_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Peak wait time across all rides"
    )

    # Operating Hours
    operating_hours_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total minutes park was open"
    )

    # Calculation Metadata
    metrics_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        comment="Calculation version for side-by-side comparison during bug fixes"
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_park_daily_stats_park', 'park_id'),
        Index('idx_park_daily_stats_date', 'stat_date'),
        Index('idx_park_daily_ranking', 'stat_date', 'total_downtime_hours'),
        Index('idx_park_daily_stats_version', 'metrics_version'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="daily_stats"
    )

    def __repr__(self) -> str:
        return f"<ParkDailyStats(stat_id={self.stat_id}, park_id={self.park_id}, date={self.stat_date}, shame_score={self.shame_score})>"
