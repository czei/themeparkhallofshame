"""
Park Downtime Rankings Query (Average Shame Score)
=================================================

Endpoint: GET /api/parks/downtime?period=last_week|last_month
UI Location: Parks tab → Downtime Rankings table

Returns parks ranked by AVERAGE shame_score across the period.
Higher shame_score = more downtime relative to park's ride portfolio.

SHAME SCORE CALCULATION:
- Average of per-day shame scores across the period
- Per-day shame = (daily_weighted_downtime / total_park_weight) × 10
- This makes LAST_WEEK/LAST_MONTH comparable to LIVE/TODAY (same 0-100 scale)

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

These are fixed calendar periods for social media reporting, e.g.:
- "November's least reliable park was X"
- "Last week's worst performer was Y"

Database Tables:
- parks (park metadata)
- park_daily_stats (aggregated daily downtime data)
- rides (ride metadata for tier calculations)
- ride_classifications (tier weights)
- ride_daily_stats (per-ride downtime for weighted calculations)

How to Modify This Query:
1. To add a new column: Add to the select() in _build_rankings_query()
2. To change the ranking formula: Modify the shame_score calculation
3. To add a new filter: Add parameter and extend the where() clause

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "total_downtime_hours": 12.5,
    "shame_score": 2.45,
    "affected_rides_count": 8,
    "uptime_percentage": 94.5,
    "trend_percentage": null,
    "period_label": "Nov 24-30, 2024"
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, case, literal_column
from sqlalchemy.orm import Session, aliased

from models import Park, Ride, RideClassification, ParkDailyStats, RideDailyStats
from utils.timezone import get_last_week_date_range, get_last_month_date_range
from utils.query_helpers import QueryClassBase


# =============================================================================
# SHAME SCORE CALCULATION
# =============================================================================
# Formula: shame_score = AVG(daily_shame_score)
# Where: daily_shame_score = (daily_weighted_downtime / total_park_weight) × 10
#
# This calculates the AVERAGE daily shame score across the period, making it
# comparable to LIVE/TODAY scores (all on the same 0-100 scale).
#
# Example:
#   Day 1: Tier-1 ride down 3h → weighted = 9h, shame = 9/45 × 10 = 2.0
#   Day 2: Nothing down → weighted = 0h, shame = 0
#   Day 3: Tier-3 ride down 1h → weighted = 1h, shame = 1/45 × 10 = 0.22
#   Average shame = (2.0 + 0 + 0.22) / 3 = 0.74
#
# Higher score = worse average daily performance
# =============================================================================


class ParkDowntimeRankingsQuery(QueryClassBase):
    """
    Query handler for park downtime rankings.

    Methods:
        get_weekly(): 7-day period from daily stats
        get_monthly(): 30-day period from daily stats

    For live (today) rankings, use live/live_park_rankings.py instead.
    """

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the previous complete week (Sunday-Saturday).

        Called by: parks.get_park_downtime_rankings() when period='last_week'

        Uses calendar-based periods for social media reporting, e.g.:
        "Last week's worst performing park was Magic Kingdom"

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of parks ranked by specified column, includes period_label
        """
        start_date, end_date, period_label = get_last_week_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def get_monthly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the previous complete calendar month.

        Called by: parks.get_park_downtime_rankings() when period='last_month'

        Uses calendar-based periods for social media reporting, e.g.:
        "November's worst performing park was Magic Kingdom"

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of parks ranked by specified column, includes period_label
        """
        start_date, end_date, period_label = get_last_month_date_range()

        return self._get_rankings(
            start_date=start_date,
            end_date=end_date,
            period_label=period_label,
            filter_disney_universal=filter_disney_universal,
            limit=limit,
            sort_by=sort_by,
        )

    def _get_rankings(
        self,
        start_date: date,
        end_date: date,
        period_label: str = "",
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Internal method to build and execute rankings query.

        Uses AVERAGE daily shame scores to be comparable with LIVE/TODAY.
        For each day, calculates: (daily_weighted_downtime / total_park_weight) × 10
        Then averages these daily scores across the period.

        Args:
            start_date: Start of date range
            end_date: End of date range
            period_label: Human-readable label (e.g., "Nov 24-30, 2024")
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Column to sort by

        Returns:
            List of park ranking dictionaries with period_label included
        """
        # Build CTEs

        # CTE 1: park_weights - Total tier weight for each park
        park_weights = (
            select(
                Park.park_id,
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight'),
                func.count(func.distinct(Ride.ride_id)).label('total_rides')
            )
            .select_from(Park)
            .join(Ride, and_(
                Park.park_id == Ride.park_id,
                Ride.is_active == True,
                Ride.category == 'ATTRACTION'
            ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Park.is_active == True)
        )

        if filter_disney_universal:
            park_weights = park_weights.where(or_(Park.is_disney == True, Park.is_universal == True))

        park_weights = park_weights.group_by(Park.park_id).cte('park_weights')

        # CTE 2: daily_weighted_downtime - Weighted downtime per park per day
        daily_weighted_downtime = (
            select(
                Ride.park_id,
                RideDailyStats.stat_date,
                func.sum(
                    RideDailyStats.downtime_minutes / 60.0 * func.coalesce(RideClassification.tier_weight, 2)
                ).label('weighted_downtime_hours')
            )
            .select_from(RideDailyStats)
            .join(Ride, and_(
                RideDailyStats.ride_id == Ride.ride_id,
                Ride.is_active == True,
                Ride.category == 'ATTRACTION'
            ))
            .join(Park, and_(
                Ride.park_id == Park.park_id,
                Park.is_active == True
            ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(and_(
                RideDailyStats.stat_date >= start_date,
                RideDailyStats.stat_date <= end_date
            ))
        )

        if filter_disney_universal:
            daily_weighted_downtime = daily_weighted_downtime.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        daily_weighted_downtime = daily_weighted_downtime.group_by(
            Ride.park_id, RideDailyStats.stat_date
        ).cte('daily_weighted_downtime')

        # CTE 3: daily_shame_scores - Per-day shame score
        daily_shame_scores = (
            select(
                daily_weighted_downtime.c.park_id,
                daily_weighted_downtime.c.stat_date,
                func.coalesce(
                    (daily_weighted_downtime.c.weighted_downtime_hours /
                     func.nullif(park_weights.c.total_park_weight, 0)) * 10,
                    0
                ).label('daily_shame_score')
            )
            .select_from(daily_weighted_downtime)
            .join(park_weights, daily_weighted_downtime.c.park_id == park_weights.c.park_id)
        ).cte('daily_shame_scores')

        # Main query - Build the rankings
        base_query = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                (Park.city + ', ' + Park.state_province).label('location'),

                # Total downtime hours (sum across period)
                func.round(func.sum(ParkDailyStats.total_downtime_hours), 2).label('total_downtime_hours'),

                # AVERAGE Shame Score = average of per-day shame scores
                func.round(
                    select(func.avg(daily_shame_scores.c.daily_shame_score))
                    .where(daily_shame_scores.c.park_id == Park.park_id)
                    .scalar_subquery(),
                    1
                ).label('shame_score'),

                # Max rides affected on any day (named rides_down for frontend compatibility)
                func.max(ParkDailyStats.rides_with_downtime).label('rides_down'),

                # Average uptime percentage across days
                func.round(func.avg(ParkDailyStats.avg_uptime_percentage), 2).label('uptime_percentage'),

                # Trend not available for aggregated queries
                literal_column('NULL').label('trend_percentage')
            )
            .select_from(Park)
            .join(ParkDailyStats, Park.park_id == ParkDailyStats.park_id)
            .join(park_weights, Park.park_id == park_weights.c.park_id)
            .where(and_(
                ParkDailyStats.stat_date >= start_date,
                ParkDailyStats.stat_date <= end_date,
                Park.is_active == True,
                ParkDailyStats.operating_hours_minutes > 0
            ))
        )

        if filter_disney_universal:
            base_query = base_query.where(or_(Park.is_disney == True, Park.is_universal == True))

        base_query = base_query.group_by(Park.park_id, Park.name, Park.city, Park.state_province)
        base_query = base_query.having(func.sum(ParkDailyStats.total_downtime_hours) > 0)

        # Apply sorting
        sort_column_map = {
            "total_downtime_hours": "total_downtime_hours",
            "uptime_percentage": "uptime_percentage",
            "rides_down": "rides_down",
        }
        sort_column = sort_column_map.get(sort_by, "shame_score")

        # Sort direction - lower uptime is worse, so ASC for that column
        if sort_by == "uptime_percentage":
            base_query = base_query.order_by(literal_column(sort_column).asc())
        else:
            base_query = base_query.order_by(literal_column(sort_column).desc())

        base_query = base_query.limit(limit)

        # Execute and fetch results
        rankings = self.execute_and_fetchall(base_query)

        # Add period_label to each result
        if period_label:
            for row in rankings:
                row['period_label'] = period_label

        return rankings
