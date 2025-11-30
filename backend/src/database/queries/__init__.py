"""
Theme Park Hall of Shame - Query Modules
=========================================

Feature-focused query modules organized by UI component.

Structure:
- builders/: Reusable query components (filters, expressions, CTEs)
- rankings/: Park and ride ranking queries
- trends/: Improving/declining trend queries
- charts/: Time-series chart data queries
- live/: Real-time status queries

Usage:
    from database.queries.rankings.park_downtime_rankings import ParkDowntimeRankingsQuery

    query = ParkDowntimeRankingsQuery(connection)
    results = query.get_weekly(start_date, end_date)

How to Add a New Query:
1. Create a new file in the appropriate subdirectory
2. Follow the docstring template (see park_downtime_rankings.py)
3. Add the query class with methods for each variation
4. Update this __init__.py to export the new query
"""

# Query builders (for composing queries)
from .builders import (
    Filters,
    StatusExpressions,
    ParkWeightsCTE,
    WeightedDowntimeCTE,
)

__all__ = [
    # Builders
    "Filters",
    "StatusExpressions",
    "ParkWeightsCTE",
    "WeightedDowntimeCTE",
]
