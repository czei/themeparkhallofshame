"""
SQLAlchemy ORM Models: Stats Tables
RideDailyStats, ParkDailyStats, and ParkWeeklyStats aggregated statistics.
"""

from sqlalchemy import Integer, ForeignKey, Date, Numeric, DateTime, Index, UniqueConstraint, func, String, Boolean
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
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="daily_stats"
    )

    def __repr__(self) -> str:
        return f"<ParkDailyStats(stat_id={self.stat_id}, park_id={self.park_id}, date={self.stat_date}, shame_score={self.shame_score})>"


class ParkWeeklyStats(Base):
    """
    Weekly aggregated statistics for park performance.
    Calculated from park_daily_stats and retained permanently.
    """
    __tablename__ = "park_weekly_stats"

    # Primary Key
    stat_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("parks.park_id", ondelete="SET NULL")
    )

    # Week Identification
    year: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="ISO year number"
    )
    week_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="ISO week number (1-53)"
    )
    week_start_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Monday of the week (Pacific timezone)"
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
    total_downtime_hours: Mapped[Decimal] = mapped_column(
        Numeric(8, 2),
        nullable=False,
        default=0.0,
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

    # Trend Analysis
    trend_vs_previous_week: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Percentage change in downtime vs previous week (positive = worse)"
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )

    # Composite Indexes and Constraints for Performance
    __table_args__ = (
        UniqueConstraint('park_id', 'year', 'week_number', name='unique_park_week'),
        Index('idx_pws_park_week', 'park_id', 'year', 'week_number'),
        Index('idx_pws_week', 'year', 'week_number'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship(
        "Park",
        back_populates="weekly_stats"
    )

    def __repr__(self) -> str:
        return f"<ParkWeeklyStats(stat_id={self.stat_id}, park_id={self.park_id}, year={self.year}, week={self.week_number})>"


class ParkHourlyStats(Base):
    """
    Hourly aggregated statistics for park performance.
    Used for fast time-series queries and "today" cumulative data.
    """
    __tablename__ = "park_hourly_stats"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Hour (UTC)
    hour_start_utc: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="Start of the hour in UTC"
    )

    # Aggregated Metrics
    shame_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(3, 1),
        comment="Shame score for this hour (0-10 scale)"
    )
    avg_wait_time_minutes: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Average wait time in minutes"
    )
    rides_operating: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Number of rides operating"
    )
    rides_down: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Number of rides down"
    )
    total_downtime_hours: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 2),
        comment="Total downtime hours across all rides"
    )
    weighted_downtime_hours: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 2),
        comment="Tier-weighted downtime hours"
    )
    effective_park_weight: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        comment="Sum of tier weights for active rides"
    )
    snapshot_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Number of snapshots aggregated"
    )
    park_was_open: Mapped[bool] = mapped_column(
        nullable=False,
        comment="Whether park was open during this hour"
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

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_park_hourly_stats_park_hour', 'park_id', 'hour_start_utc'),
        Index('idx_park_hourly_stats_hour', 'hour_start_utc'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship("Park")

    def __repr__(self) -> str:
        return f"<ParkHourlyStats(id={self.id}, park_id={self.park_id}, hour={self.hour_start_utc}, shame={self.shame_score})>"


class RideHourlyStats(Base):
    """
    Hourly aggregated statistics for ride performance.
    Used for fast time-series queries and "today" cumulative data.
    """
    __tablename__ = "ride_hourly_stats"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Hour (UTC)
    hour_start_utc: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="Start of the hour in UTC"
    )

    # Aggregated Metrics
    avg_wait_time_minutes: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Average wait time in minutes"
    )
    operating_snapshots: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Number of snapshots where ride was operating"
    )
    down_snapshots: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Number of snapshots where ride was down"
    )
    downtime_hours: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Total downtime hours"
    )
    uptime_percentage: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Percentage of time operating"
    )

    # Quality Metadata
    snapshot_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Number of snapshots aggregated"
    )
    ride_operated: Mapped[bool] = mapped_column(
        nullable=False,
        comment="Whether ride operated during this hour"
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

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_ride_hourly_stats_ride_hour', 'ride_id', 'hour_start_utc'),
        Index('idx_ride_hourly_stats_hour', 'hour_start_utc'),
        Index('idx_ride_hourly_stats_park_hour', 'park_id', 'hour_start_utc'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship("Ride")
    park: Mapped["Park"] = relationship("Park")

    def __repr__(self) -> str:
        return f"<RideHourlyStats(id={self.id}, ride_id={self.ride_id}, hour={self.hour_start_utc}, downtime={self.downtime_hours})>"


class RideWeeklyStats(Base):
    """
    Weekly aggregated statistics for ride performance.
    Calculated from ride_status_snapshots and retained permanently.
    """
    __tablename__ = "ride_weekly_stats"

    # Primary Key
    stat_id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    ride_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="CASCADE")
    )

    # Week Identification
    year: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="ISO year"
    )
    week_number: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="ISO week number (1-53)"
    )
    week_start_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Start date of the ISO week"
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
        comment="Total minutes park was open during the week"
    )

    # Wait Time Statistics
    avg_wait_time: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Average wait time in minutes"
    )
    peak_wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Peak wait time during the week"
    )

    # Downtime Details
    status_changes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of status changes (operating <-> down)"
    )

    # Trend Metrics
    trend_vs_previous_week: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Percentage change in uptime vs previous week"
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_ride_weekly_stats_ride_week', 'ride_id', 'year', 'week_number'),
        Index('idx_ride_weekly_stats_week', 'year', 'week_number'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        back_populates="weekly_stats"
    )

    def __repr__(self) -> str:
        return f"<RideWeeklyStats(stat_id={self.stat_id}, ride_id={self.ride_id}, year={self.year}, week={self.week_number}, uptime={self.uptime_percentage}%)>"


class ParkLiveRankings(Base):
    """
    Live park rankings table - updated in real-time.
    Contains current shame scores and downtime for all parks.
    """
    __tablename__ = "park_live_rankings"

    # Primary Key
    park_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        comment="Park ID from parks table"
    )

    # Park Info (denormalized for performance)
    queue_times_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Queue-Times.com park ID"
    )
    park_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Park name"
    )
    location: Mapped[Optional[str]] = mapped_column(
        String(255),
        comment="Park location"
    )
    timezone: Mapped[Optional[str]] = mapped_column(
        String(50),
        comment="Park timezone"
    )
    is_disney: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        index=True,
        comment="Is Disney park"
    )
    is_universal: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Is Universal park"
    )

    # Ride Counts
    rides_down: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        index=True,
        comment="Number of rides currently down"
    )
    total_rides: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total number of rides tracked"
    )

    # Metrics
    shame_score: Mapped[Decimal] = mapped_column(
        Numeric(4, 1),
        nullable=False,
        default=0.0,
        index=True,
        comment="Current shame score"
    )
    park_is_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Is park currently open"
    )
    total_downtime_hours: Mapped[Decimal] = mapped_column(
        Numeric(6, 2),
        nullable=False,
        default=0.0,
        comment="Total downtime hours"
    )
    weighted_downtime_hours: Mapped[Decimal] = mapped_column(
        Numeric(6, 2),
        nullable=False,
        default=0.0,
        comment="Tier-weighted downtime hours"
    )
    total_park_weight: Mapped[Decimal] = mapped_column(
        Numeric(8, 2),
        nullable=False,
        default=0.0,
        comment="Sum of tier weights for all rides"
    )

    # Timestamp
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="When rankings were calculated"
    )

    # Indexes
    __table_args__ = (
        Index('idx_plr_shame', 'shame_score'),
        Index('idx_plr_disney', 'is_disney'),
        Index('idx_plr_calculated', 'calculated_at'),
        {'extend_existing': True}
    )

    def __repr__(self) -> str:
        return f"<ParkLiveRankings(park_id={self.park_id}, park_name={self.park_name}, shame_score={self.shame_score})>"
