"""
Charts Query Modules
====================

Query files for time-series chart data on the Trends tab.

Files:
- park_shame_history.py: GET /api/trends/chart-data?type=parks
- ride_downtime_history.py: GET /api/trends/chart-data?type=rides

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
from .ride_downtime_history import RideDowntimeHistoryQuery

__all__ = [
    "ParkShameHistoryQuery",
    "RideDowntimeHistoryQuery",
]
