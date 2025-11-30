"""
Live Query Modules
==================

Query files for real-time data from snapshot tables.
Used for "today" period and status summary.

Files:
- status_summary.py: GET /api/live/status-summary
- live_park_rankings.py: GET /api/parks/downtime?period=today
- live_ride_rankings.py: GET /api/rides/downtime?period=today

Note: These query from ride_status_snapshots and park_activity_snapshots
(24-hour retention) instead of aggregated stats tables.
"""

from .status_summary import StatusSummaryQuery
from .live_park_rankings import LiveParkRankingsQuery
from .live_ride_rankings import LiveRideRankingsQuery

__all__ = [
    "StatusSummaryQuery",
    "LiveParkRankingsQuery",
    "LiveRideRankingsQuery",
]
