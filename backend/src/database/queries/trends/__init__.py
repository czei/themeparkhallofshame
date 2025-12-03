"""
Trends Query Modules
====================

Query files for the Trends tab showing improving/declining parks and rides,
plus Awards section queries for longest wait times and least reliable rides.

Files:
- improving_parks.py: GET /api/trends?category=parks-improving
- declining_parks.py: GET /api/trends?category=parks-declining
- improving_rides.py: GET /api/trends?category=rides-improving
- declining_rides.py: GET /api/trends?category=rides-declining
- longest_wait_times.py: GET /api/trends/longest-wait-times
- least_reliable_rides.py: GET /api/trends/least-reliable
"""

from .improving_parks import ImprovingParksQuery
from .declining_parks import DecliningParksQuery
from .improving_rides import ImprovingRidesQuery
from .declining_rides import DecliningRidesQuery
from .longest_wait_times import LongestWaitTimesQuery
from .least_reliable_rides import LeastReliableRidesQuery

__all__ = [
    "ImprovingParksQuery",
    "DecliningParksQuery",
    "ImprovingRidesQuery",
    "DecliningRidesQuery",
    "LongestWaitTimesQuery",
    "LeastReliableRidesQuery",
]
