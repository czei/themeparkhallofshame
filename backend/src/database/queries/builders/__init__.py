"""
Query Builders
==============

Reusable components for building SQLAlchemy queries.

Modules:
- filters.py: Common WHERE clause conditions
- expressions.py: Status checks and calculations
- ctes.py: Common Table Expressions (park_weights, weighted_downtime)

These replace the string-based sql_helpers.py with type-safe
SQLAlchemy Expression Language equivalents.

How to Modify:
1. Add new filter/expression/CTE to the appropriate file
2. Export it from this __init__.py
3. Use it in query files: from database.queries.builders import Filters
"""

from .filters import Filters
from .expressions import StatusExpressions
from .ctes import ParkWeightsCTE, WeightedDowntimeCTE

__all__ = [
    "Filters",
    "StatusExpressions",
    "ParkWeightsCTE",
    "WeightedDowntimeCTE",
]
