"""
Today Query Modules
===================

Query files for cumulative "today" data from snapshot tables.
Aggregates all data from midnight Pacific to now.

Difference from LIVE queries:
- LIVE: Shows rides CURRENTLY down (instantaneous snapshot)
- TODAY: Shows CUMULATIVE stats from midnight to now

Files:
- today_park_rankings.py: GET /api/parks/downtime?period=today
- today_ride_rankings.py: GET /api/rides/downtime?period=today
- today_park_wait_times.py: GET /api/parks/waittimes?period=today
- today_ride_wait_times.py: GET /api/rides/waittimes?period=today

Note: These query from ride_status_snapshots and park_activity_snapshots
(24-hour retention) and aggregate all snapshots since midnight Pacific.
"""

from .today_park_rankings import TodayParkRankingsQuery
from .today_ride_rankings import TodayRideRankingsQuery
from .today_park_wait_times import TodayParkWaitTimesQuery
from .today_ride_wait_times import TodayRideWaitTimesQuery

__all__ = [
    "TodayParkRankingsQuery",
    "TodayRideRankingsQuery",
    "TodayParkWaitTimesQuery",
    "TodayRideWaitTimesQuery",
]
