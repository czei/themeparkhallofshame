"""
Raw Data / Snapshot Tables
==========================

Real-time data captured from external APIs (Queue-Times, ThemeParks.wiki).
These tables have 24-hour retention and are used for live queries and aggregation.

Tables:
- ride_status_snapshots: Current ride status (every 5 minutes)
- ride_status_changes: Status transition events
- park_activity_snapshots: Park-level activity summary

Database: MySQL/MariaDB
Source: migrations/002_raw_data_tables.sql, 008_themeparks_wiki.sql
"""

from sqlalchemy import (
    Table,
    Column,
    BigInteger,
    Integer,
    Boolean,
    DateTime,
    Numeric,
    Enum,
    ForeignKey,
    Index,
)
from .metadata import metadata


# =============================================================================
# RIDE_STATUS_SNAPSHOTS TABLE
# =============================================================================
# Real-time ride status captured every 5 minutes from external APIs.
#
# Key columns for queries:
#   - computed_is_open: Computed boolean (wait_time > 0 OR is_open = true)
#   - status: Rich status from ThemeParks.wiki (OPERATING/DOWN/CLOSED/REFURBISHMENT)
#   - recorded_at: UTC timestamp of data collection
#
# Status Logic (from sql_helpers.py):
#   COALESCE(status, IF(computed_is_open, 'OPERATING', 'DOWN'))
#   - If status is set (ThemeParks.wiki data), use it
#   - Otherwise map computed_is_open to OPERATING/DOWN
#
# Used by:
#   - Live rankings (today period)
#   - Status summary panel
#   - Aggregation service (calculates daily stats)
#
# Retention: 24 hours (cleanup job removes older data)
# =============================================================================

ride_status_snapshots = Table(
    "ride_status_snapshots",
    metadata,
    Column("snapshot_id", BigInteger, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="CASCADE"), nullable=False),
    Column("recorded_at", DateTime, nullable=False),
    Column("is_open", Boolean, nullable=True),  # Raw from Queue-Times API
    Column(
        "status",
        Enum("OPERATING", "DOWN", "CLOSED", "REFURBISHMENT", name="ride_status_enum"),
        nullable=True,
    ),  # Rich status from ThemeParks.wiki
    Column("wait_time", Integer, nullable=True),
    Column("computed_is_open", Boolean, nullable=False),  # Computed: wait_time > 0 OR is_open = true
    Column("last_updated_api", DateTime, nullable=True),
    # Indexes
    Index("idx_ride_recorded", "ride_id", "recorded_at"),
    Index("idx_recorded_at", "recorded_at"),
    Index("idx_computed_status", "computed_is_open", "recorded_at"),
    Index("idx_status", "status", "recorded_at"),
)


# =============================================================================
# RIDE_STATUS_CHANGES TABLE
# =============================================================================
# Records status transitions (open -> closed, closed -> open).
#
# Key columns:
#   - previous_status / new_status: Boolean (computed_is_open before/after)
#   - previous_status_enum / new_status_enum: Rich status (OPERATING/DOWN/etc.)
#   - duration_in_previous_status: Minutes spent in previous state
#
# Used by:
#   - Status change history
#   - Longest downtime calculations
#   - Aggregation service
#
# Retention: 24 hours
# =============================================================================

ride_status_changes = Table(
    "ride_status_changes",
    metadata,
    Column("change_id", BigInteger, primary_key=True, autoincrement=True),
    Column("ride_id", Integer, ForeignKey("rides.ride_id", ondelete="CASCADE"), nullable=False),
    Column("changed_at", DateTime, nullable=False),
    Column("previous_status", Boolean, nullable=False),  # Previous computed_is_open
    Column(
        "previous_status_enum",
        Enum("OPERATING", "DOWN", "CLOSED", "REFURBISHMENT", name="prev_status_enum"),
        nullable=True,
    ),
    Column("new_status", Boolean, nullable=False),  # New computed_is_open
    Column(
        "new_status_enum",
        Enum("OPERATING", "DOWN", "CLOSED", "REFURBISHMENT", name="new_status_enum"),
        nullable=True,
    ),
    Column("duration_in_previous_status", Integer, nullable=False),  # Minutes
    Column("wait_time_at_change", Integer, nullable=True),
    # Indexes
    Index("idx_ride_changed", "ride_id", "changed_at"),
    Index("idx_changed_at", "changed_at"),
    Index("idx_downtime", "ride_id", "new_status", "changed_at"),
)


# =============================================================================
# PARK_ACTIVITY_SNAPSHOTS TABLE
# =============================================================================
# Park-level summary of activity (every 5 minutes).
#
# Key columns:
#   - park_appears_open: TRUE if any ride has activity (used for park-aware status)
#   - rides_open / rides_closed: Current counts
#
# Used by:
#   - Live status summary
#   - Park-aware status calculations (a ride at a closed park shows as PARK_CLOSED, not DOWN)
#
# Retention: 24 hours
# =============================================================================

park_activity_snapshots = Table(
    "park_activity_snapshots",
    metadata,
    Column("snapshot_id", BigInteger, primary_key=True, autoincrement=True),
    Column("park_id", Integer, ForeignKey("parks.park_id", ondelete="CASCADE"), nullable=False),
    Column("recorded_at", DateTime, nullable=False),
    Column("total_rides_tracked", Integer, nullable=False, server_default="0"),
    Column("rides_open", Integer, nullable=False, server_default="0"),
    Column("rides_closed", Integer, nullable=False, server_default="0"),
    Column("avg_wait_time", Numeric(5, 2), nullable=True),
    Column("max_wait_time", Integer, nullable=True),
    Column("park_appears_open", Boolean, nullable=False),  # TRUE if any ride active
    # Indexes
    Index("idx_park_recorded", "park_id", "recorded_at"),
    Index("idx_pas_recorded_at", "recorded_at"),
    Index("idx_park_open", "park_id", "park_appears_open", "recorded_at"),
)
