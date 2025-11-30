"""
Trends Query Modules
====================

Query files for the Trends tab showing improving/declining parks and rides.

Files:
- improving_parks.py: GET /api/trends?category=parks-improving
- declining_parks.py: GET /api/trends?category=parks-declining
- improving_rides.py: GET /api/trends?category=rides-improving
- declining_rides.py: GET /api/trends?category=rides-declining
"""

from .improving_parks import ImprovingParksQuery
from .declining_parks import DecliningParksQuery
from .improving_rides import ImprovingRidesQuery
from .declining_rides import DecliningRidesQuery

__all__ = [
    "ImprovingParksQuery",
    "DecliningParksQuery",
    "ImprovingRidesQuery",
    "DecliningRidesQuery",
]
