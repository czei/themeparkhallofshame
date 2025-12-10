"""
Charts Query Modules
====================

Query files for time-series chart data on the Trends tab and Park Details modal.

Files:
- park_shame_history.py: GET /api/trends/chart-data?type=parks
- park_waittime_history.py: GET /api/trends/chart-data?type=waittimes
- ride_downtime_history.py: GET /api/trends/chart-data?type=rides
- ride_waittime_history.py: GET /api/trends/chart-data?type=ridewaittimes
- park_rides_comparison.py: GET /api/parks/<id>/rides/charts

Output Format (Chart.js compatible):
{
    "labels": ["Nov 23", "Nov 24", ...],
    "datasets": [
        {"label": "Park Name", "data": [0.21, 0.18, ...]},
        {"label": "Another Park", "data": [0.15, 0.12, ...]}
    ]
}
"""

from .park_shame_history import ParkShameHistoryQuery
from .park_waittime_history import ParkWaitTimeHistoryQuery
from .ride_downtime_history import RideDowntimeHistoryQuery
from .ride_waittime_history import RideWaitTimeHistoryQuery
from .park_rides_comparison import ParkRidesComparisonQuery

__all__ = [
    "ParkShameHistoryQuery",
    "ParkWaitTimeHistoryQuery",
    "RideDowntimeHistoryQuery",
    "RideWaitTimeHistoryQuery",
    "ParkRidesComparisonQuery",
]
