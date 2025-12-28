"""
Database calculators - centralized business logic for complex calculations.

This package contains calculator classes that provide a single source of truth
for business calculations that need to be consistent across multiple queries.
"""

from database.calculators.shame_score import ShameScoreCalculator

__all__ = ["ShameScoreCalculator"]
