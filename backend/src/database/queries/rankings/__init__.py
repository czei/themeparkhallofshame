"""
Rankings Query Modules
======================

Query files for the Parks and Rides tabs ranking tables.

Files:
- park_downtime_rankings.py: GET /api/parks/downtime
- park_wait_time_rankings.py: GET /api/parks/waittimes
- ride_downtime_rankings.py: GET /api/rides/downtime
- ride_wait_time_rankings.py: GET /api/rides/waittimes
"""

from .park_downtime_rankings import ParkDowntimeRankingsQuery
from .park_wait_time_rankings import ParkWaitTimeRankingsQuery
from .ride_downtime_rankings import RideDowntimeRankingsQuery
from .ride_wait_time_rankings import RideWaitTimeRankingsQuery

__all__ = [
    "ParkDowntimeRankingsQuery",
    "ParkWaitTimeRankingsQuery",
    "RideDowntimeRankingsQuery",
    "RideWaitTimeRankingsQuery",
]
