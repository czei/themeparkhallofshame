"""
Common Table Expressions (CTEs)
===============================

Reusable CTEs for complex queries, particularly ranking calculations.

These CTEs are used by multiple query files to calculate:
- Park weights (sum of tier weights for all rides)
- Weighted downtime (downtime * tier weight)
- Shame scores (weighted downtime / park weight)

Usage:
    from database.queries.builders import ParkWeightsCTE, WeightedDowntimeCTE

    pw = ParkWeightsCTE.build(filter_disney_universal=True)
    wd = WeightedDowntimeCTE.from_daily_stats(start_date, end_date)

    stmt = (
        select(parks.c.name, wd.c.total / pw.c.weight)
        .select_from(parks.join(pw).join(wd))
    )

How to Modify:
1. To change weight calculation: Update ParkWeightsCTE.build()
2. To add a new CTE: Create a new class following the pattern below
3. After changes: Update ranking queries that use these CTEs
"""

from datetime import date
from typing import Optional, TYPE_CHECKING

from sqlalchemy import select, func, and_, or_
from sqlalchemy.sql.selectable import CTE

from database.schema import (
    parks,
    rides,
    ride_classifications,
    ride_daily_stats,
    park_daily_stats,
)
from .filters import Filters


# =============================================================================
# PARK WEIGHTS CTE
# =============================================================================
# Calculates total tier weight per park.
# Used as denominator in shame score: shame_score = weighted_downtime / park_weight
#
# Formula:
#   park_weight = SUM(tier_weight) for all active ATTRACTION rides
#   where tier_weight comes from ride_classifications (default 2 if not classified)
#
# Tier Weights:
#   Tier 1 (flagship): weight = 3
#   Tier 2 (standard): weight = 2
#   Tier 3 (minor):    weight = 1
#   Unclassified:      weight = 2 (default)
# =============================================================================

DEFAULT_TIER_WEIGHT = 2


class ParkWeightsCTE:
    """
    CTE for calculating total tier weight per park.

    Example Output:
        park_id | total_park_weight | total_rides | tier1_count | tier2_count | tier3_count
        1       | 45                | 20          | 5           | 10          | 5
    """

    @staticmethod
    def build(filter_disney_universal: bool = False) -> CTE:
        """
        Build the park_weights CTE.

        Args:
            filter_disney_universal: If True, only include Disney/Universal parks

        Returns:
            SQLAlchemy CTE object with columns:
                - park_id
                - total_park_weight
                - total_rides
                - tier1_count
                - tier2_count
                - tier3_count

        How to Modify:
            - To change default weight: Update DEFAULT_TIER_WEIGHT constant
            - To add a column: Add to the select() and update callers
        """
        # Base conditions: active attractions at active parks
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
        ]

        # Optional Disney/Universal filter
        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                # Total weight (sum of tier weights, default 2 for unclassified)
                func.sum(
                    func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT)
                ).label("total_park_weight"),
                # Total ride count
                func.count(rides.c.ride_id).label("total_rides"),
                # Tier distribution
                func.sum(
                    func.if_(rides.c.tier == 1, 1, 0)
                ).label("tier1_count"),
                func.sum(
                    func.if_(rides.c.tier == 2, 1, 0)
                ).label("tier2_count"),
                func.sum(
                    func.if_(rides.c.tier == 3, 1, 0)
                ).label("tier3_count"),
            )
            .select_from(
                parks.join(rides, parks.c.park_id == rides.c.park_id).outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id)
        )

        return stmt.cte("park_weights")


# =============================================================================
# WEIGHTED DOWNTIME CTE
# =============================================================================
# Calculates total weighted downtime hours per park.
# Used as numerator in shame score: shame_score = weighted_downtime / park_weight
#
# Formula:
#   weighted_downtime = SUM(downtime_minutes * tier_weight) / 60
#   where downtime comes from ride_daily_stats
#
# Note: This CTE reads from AGGREGATED stats tables, not raw snapshots.
# For live (today) calculations, use WeightedDowntimeCTE.from_live_snapshots()
# =============================================================================


class WeightedDowntimeCTE:
    """
    CTE for calculating total weighted downtime hours per park.

    Example Output:
        park_id | total_weighted_downtime_hours
        1       | 12.5
    """

    @staticmethod
    def from_daily_stats(
        start_date: date,
        end_date: date,
        filter_disney_universal: bool = False,
    ) -> CTE:
        """
        Build weighted downtime CTE from aggregated daily stats.

        Use for: 7-day and 30-day periods (historical data).

        Args:
            start_date: Start of date range
            end_date: End of date range
            filter_disney_universal: If True, only include Disney/Universal parks

        Returns:
            SQLAlchemy CTE object with columns:
                - park_id
                - total_weighted_downtime_hours

        How to Modify:
            - To change weight calculation: Update the func.sum() expression
            - To add a column: Add to the select()
        """
        # Base conditions
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                # Weighted downtime: (downtime_minutes * tier_weight) / 60
                func.round(
                    func.sum(
                        ride_daily_stats.c.downtime_minutes
                        / 60.0
                        * func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT)
                    ),
                    2,
                ).label("total_weighted_downtime_hours"),
            )
            .select_from(
                parks.join(rides, parks.c.park_id == rides.c.park_id)
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
                .join(
                    ride_daily_stats,
                    rides.c.ride_id == ride_daily_stats.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id)
        )

        return stmt.cte("weighted_downtime")

    @staticmethod
    def from_weekly_stats(
        year: int,
        week_number: int,
        filter_disney_universal: bool = False,
    ) -> CTE:
        """
        Build weighted downtime CTE from weekly stats table.

        Use for: Weekly period queries.

        Note: This is an optimization - reads from pre-aggregated weekly table
        instead of summing daily stats.
        """
        from database.schema import ride_weekly_stats

        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_weekly_stats.c.year == year,
            ride_weekly_stats.c.week_number == week_number,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                func.round(
                    func.sum(
                        ride_weekly_stats.c.downtime_minutes
                        / 60.0
                        * func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT)
                    ),
                    2,
                ).label("total_weighted_downtime_hours"),
            )
            .select_from(
                parks.join(rides, parks.c.park_id == rides.c.park_id)
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
                .join(
                    ride_weekly_stats,
                    rides.c.ride_id == ride_weekly_stats.c.ride_id,
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id)
        )

        return stmt.cte("weighted_downtime")


# =============================================================================
# SHAME SCORE CALCULATION
# =============================================================================
# Shame Score = Weighted Downtime Hours / Total Park Weight
#
# Example:
#   Park has total_park_weight = 45 and total_weighted_downtime_hours = 12.5
#   Shame Score = 12.5 / 45 = 0.28
#
# Interpretation:
#   0.0       = Perfect operation
#   0.1 - 0.5 = Minor issues (well-maintained parks)
#   0.5 - 1.0 = Concerning (above-average downtime)
#   1.0+      = Significant problems
#
# To calculate: Use both CTEs and divide in the final SELECT:
#   func.round(wd.c.total_weighted_downtime_hours / pw.c.total_park_weight, 2)
# =============================================================================
