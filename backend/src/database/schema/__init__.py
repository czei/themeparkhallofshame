"""
Theme Park Hall of Shame - SQLAlchemy Table Definitions
========================================================

This module provides SQLAlchemy Table objects for all database tables.
These enable type-safe, composable queries using the Expression Language.

Usage:
    from database.schema import parks, rides, ride_classifications

    stmt = select(parks.c.name).where(parks.c.is_active == True)

Tables are organized into three modules:
- core_tables: parks, rides, ride_classifications (domain entities)
- snapshot_tables: ride_status_snapshots, park_activity_snapshots, ride_status_changes (raw data)
- stats_tables: *_daily_stats, *_weekly_stats, etc. (aggregated statistics)

How to Add a New Table:
1. Add the Table definition to the appropriate module (core/snapshot/stats)
2. Import and re-export it here
3. Run schema verification: python -c "from database.schema import *; print('OK')"
"""

from .metadata import metadata
from .core_tables import parks, rides, ride_classifications, park_schedules
from .snapshot_tables import (
    ride_status_snapshots,
    ride_status_changes,
    park_activity_snapshots,
)
from .stats_tables import (
    aggregation_log,
    park_operating_sessions,
    ride_daily_stats,
    ride_weekly_stats,
    ride_monthly_stats,
    ride_yearly_stats,
    park_daily_stats,
    park_weekly_stats,
    park_monthly_stats,
    park_yearly_stats,
    park_hourly_stats,
    ride_hourly_stats,
)

__all__ = [
    # Metadata
    "metadata",
    # Core entities
    "parks",
    "rides",
    "ride_classifications",
    "park_schedules",
    # Raw data / snapshots
    "ride_status_snapshots",
    "ride_status_changes",
    "park_activity_snapshots",
    # Aggregated statistics
    "aggregation_log",
    "park_operating_sessions",
    "ride_daily_stats",
    "ride_weekly_stats",
    "ride_monthly_stats",
    "ride_yearly_stats",
    "park_daily_stats",
    "park_weekly_stats",
    "park_monthly_stats",
    "park_yearly_stats",
    "park_hourly_stats",
    "ride_hourly_stats",
]
