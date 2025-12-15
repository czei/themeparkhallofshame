"""
Aggregated Statistics Tables
============================

Pre-computed statistics at daily, weekly, monthly, and yearly granularity.
These are populated by the aggregation service and used for historical queries.

Tables:
- aggregation_log: Tracks aggregation job status
- park_operating_sessions: Daily operating hours per park
- ride_daily_stats, ride_weekly_stats, etc.: Per-ride statistics
- park_daily_stats, park_weekly_stats, etc.: Per-park statistics

Database: MySQL/MariaDB
Source: migrations/003_aggregates_tables.sql
"""

from sqlalchemy import (
    Table,
    Column,
    Integer,
    DateTime,
    Date,
    Numeric,
    Text,
    Enum,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.sql import func
from .metadata import metadata


# =============================================================================
# AGGREGATION_LOG TABLE
# =============================================================================
# Tracks aggregation job execution and status.
#
# Used to:
#   - Prevent duplicate aggregation runs
#   - Debug failed aggregations
#   - Track data completeness
# =============================================================================

aggregation_log = Table(
    "aggregation_log",
    metadata,
    Column("log_id", Integer, primary_key=True, autoincrement=True),
    Column("aggregation_date", Date, nullable=False),
    Column(
        "aggregation_type",
        Enum("daily", "weekly", "monthly", "yearly", name="aggregation_type_enum"),
        nullable=False,
    ),
    Column("started_at", DateTime, nullable=False),
    Column("completed_at", DateTime, nullable=True),
    Column(
        "status",
        Enum("running", "success", "failed", name="aggregation_status_enum"),
        nullable=False,
        server_default="running",
    ),
    Column("aggregated_until_ts", DateTime, nullable=True),
    Column("error_message", Text, nullable=True),
    Column("parks_processed", Integer, server_default="0"),
    Column("rides_processed", Integer, server_default="0"),
    # Constraints and indexes
    UniqueConstraint("aggregation_date", "aggregation_type", name="unique_aggregation"),
    Index("idx_agg_status", "status", "aggregation_date"),
    Index("idx_agg_completed", "completed_at"),
)


# =============================================================================
# PARK_OPERATING_SESSIONS TABLE
# =============================================================================
# Records daily operating hours for each park.
#
# Key columns:
#   - session_start_utc / session_end_utc: UTC boundaries of operation
#   - operating_minutes: Total duration
#
# Used by:
#   - Aggregation service to determine valid snapshot window
#   - Downtime calculations (only count when park is open)
# =============================================================================

park_operating_sessions = Table(
    "park_operating_sessions",
    metadata,
    Column("session_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="SET NULL"), nullable=True),
    Column("session_date", Date, nullable=False),
    Column("session_start_utc", DateTime, nullable=True),
    Column("session_end_utc", DateTime, nullable=True),
    Column("operating_minutes", Integer, nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
    # Constraints and indexes
    UniqueConstraint("park_id", "session_date", name="unique_park_session"),
    Index("idx_park_date", "park_id", "session_date"),
    Index("idx_session_date", "session_date"),
)


# =============================================================================
# RIDE STATISTICS TABLES
# =============================================================================
# Per-ride aggregated statistics at different time granularities.
#
# Key columns (shared):
#   - uptime_minutes / downtime_minutes: Minutes in each state
#   - uptime_percentage: (uptime / operating_hours) * 100
#   - status_changes: Number of open/closed transitions
#   - longest_downtime_minutes: Longest single downtime period (daily only)
#   - trend_vs_previous_*: Percentage change vs previous period
#
# How to Add a Column:
#   1. Add to migration (003_aggregates_tables.sql or new migration)
#   2. Add Column definition below
#   3. Update aggregation_service.py to populate it
#   4. Update StatsRepository queries to return it
# =============================================================================

ride_daily_stats = Table(
    "ride_daily_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="SET NULL"), nullable=True),
    Column("stat_date", Date, nullable=False),
    Column("uptime_minutes", Integer, nullable=False, server_default="0"),
    Column("downtime_minutes", Integer, nullable=False, server_default="0"),
    Column("uptime_percentage", Numeric(5, 2), nullable=False, server_default="0.00"),
    Column("operating_hours_minutes", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("min_wait_time", Integer, nullable=True),
    Column("max_wait_time", Integer, nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("status_changes", Integer, nullable=False, server_default="0"),
    Column("longest_downtime_minutes", Integer, nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("ride_id", "stat_date", name="unique_ride_date"),
    Index("idx_rds_ride_date", "ride_id", "stat_date"),
    Index("idx_rds_stat_date", "stat_date"),
    Index("idx_rds_downtime", "downtime_minutes", "stat_date"),
)


ride_weekly_stats = Table(
    "ride_weekly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("week_number", Integer, nullable=False),  # ISO week 1-53
    Column("week_start_date", Date, nullable=False),
    Column("uptime_minutes", Integer, nullable=False, server_default="0"),
    Column("downtime_minutes", Integer, nullable=False, server_default="0"),
    Column("uptime_percentage", Numeric(5, 2), nullable=False, server_default="0.00"),
    Column("operating_hours_minutes", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("status_changes", Integer, nullable=False, server_default="0"),
    Column("trend_vs_previous_week", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("ride_id", "year", "week_number", name="unique_ride_week"),
    Index("idx_rws_ride_week", "ride_id", "year", "week_number"),
    Index("idx_rws_week", "year", "week_number"),
)


ride_monthly_stats = Table(
    "ride_monthly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("month", Integer, nullable=False),  # 1-12
    Column("uptime_minutes", Integer, nullable=False, server_default="0"),
    Column("downtime_minutes", Integer, nullable=False, server_default="0"),
    Column("uptime_percentage", Numeric(5, 2), nullable=False, server_default="0.00"),
    Column("operating_hours_minutes", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("status_changes", Integer, nullable=False, server_default="0"),
    Column("trend_vs_previous_month", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("ride_id", "year", "month", name="unique_ride_month"),
    Index("idx_rms_ride_month", "ride_id", "year", "month"),
    Index("idx_rms_month", "year", "month"),
)


ride_yearly_stats = Table(
    "ride_yearly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("uptime_minutes", Integer, nullable=False, server_default="0"),
    Column("downtime_minutes", Integer, nullable=False, server_default="0"),
    Column("uptime_percentage", Numeric(5, 2), nullable=False, server_default="0.00"),
    Column("operating_hours_minutes", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("status_changes", Integer, nullable=False, server_default="0"),
    Column("trend_vs_previous_year", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("ride_id", "year", name="unique_ride_year"),
    Index("idx_rys_ride_year", "ride_id", "year"),
    Index("idx_rys_year", "year"),
)


# =============================================================================
# PARK STATISTICS TABLES
# =============================================================================
# Per-park aggregated statistics at different time granularities.
#
# Key columns (shared):
#   - total_downtime_hours: Sum of all ride downtime (for rankings)
#   - avg_uptime_percentage: Average uptime across all rides
#   - rides_with_downtime: Count of rides that had any downtime
#   - trend_vs_previous_*: Percentage change vs previous period
#
# park_daily_stats now stores shame_score directly so AggregationService
# can persist the weighted downtime metric. This keeps downstream queries
# aligned with the API contracts that read pds.shame_score.
# =============================================================================

park_daily_stats = Table(
    "park_daily_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="SET NULL"), nullable=True),
    Column("stat_date", Date, nullable=False),
    Column("total_rides_tracked", Integer, nullable=False, server_default="0"),
    Column("avg_uptime_percentage", Numeric(5, 2), nullable=True),
    Column("shame_score", Numeric(6, 3), nullable=True),
    Column("total_downtime_hours", Numeric(8, 2), nullable=False, server_default="0.00"),
    Column("rides_with_downtime", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("operating_hours_minutes", Integer, nullable=False, server_default="0"),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("park_id", "stat_date", name="unique_park_date"),
    Index("idx_pds_park_date", "park_id", "stat_date"),
    Index("idx_pds_stat_date", "stat_date"),
    Index("idx_pds_downtime", "total_downtime_hours", "stat_date"),
)


park_weekly_stats = Table(
    "park_weekly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("week_number", Integer, nullable=False),
    Column("week_start_date", Date, nullable=False),
    Column("total_rides_tracked", Integer, nullable=False, server_default="0"),
    Column("avg_uptime_percentage", Numeric(5, 2), nullable=True),
    Column("total_downtime_hours", Numeric(8, 2), nullable=False, server_default="0.00"),
    Column("rides_with_downtime", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("trend_vs_previous_week", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("park_id", "year", "week_number", name="unique_park_week"),
    Index("idx_pws_park_week", "park_id", "year", "week_number"),
    Index("idx_pws_week", "year", "week_number"),
)


park_monthly_stats = Table(
    "park_monthly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("total_rides_tracked", Integer, nullable=False, server_default="0"),
    Column("avg_uptime_percentage", Numeric(5, 2), nullable=True),
    Column("total_downtime_hours", Numeric(8, 2), nullable=False, server_default="0.00"),
    Column("rides_with_downtime", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("trend_vs_previous_month", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("park_id", "year", "month", name="unique_park_month"),
    Index("idx_pms_park_month", "park_id", "year", "month"),
    Index("idx_pms_month", "year", "month"),
)


park_yearly_stats = Table(
    "park_yearly_stats",
    metadata,
    Column("stat_id", Integer, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="SET NULL"), nullable=True),
    Column("year", Integer, nullable=False),
    Column("total_rides_tracked", Integer, nullable=False, server_default="0"),
    Column("avg_uptime_percentage", Numeric(5, 2), nullable=True),
    Column("total_downtime_hours", Numeric(8, 2), nullable=False, server_default="0.00"),
    Column("rides_with_downtime", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("peak_wait_time", Integer, nullable=True),
    Column("trend_vs_previous_year", Numeric(6, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    # Constraints and indexes
    UniqueConstraint("park_id", "year", name="unique_park_year"),
    Index("idx_pys_park_year", "park_id", "year"),
    Index("idx_pys_year", "year"),
)
