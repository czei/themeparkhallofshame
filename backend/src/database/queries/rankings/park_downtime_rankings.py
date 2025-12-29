"""
Park Downtime Rankings Query (Average Shame Score)
=================================================

Endpoint: GET /api/parks/downtime?period=last_week|last_month
UI Location: Parks tab → Downtime Rankings table

Returns parks ranked by AVERAGE shame_score across the period.
Higher shame_score = more downtime relative to park's ride portfolio.

CRITICAL FIX (2025-12-28):
==========================
This query now reads DIRECTLY from park_daily_stats.shame_score instead of
recalculating on-the-fly. This is the SINGLE SOURCE OF TRUTH pattern that
ensures Rankings and Details always show the same shame_score.

Previously, this query used a static total_park_weight (all active rides)
as the denominator, which caused discrepancies with Details API that uses
effective_park_weight (only rides that operated).

SHAME SCORE CALCULATION:
- Average of stored daily shame_scores from park_daily_stats
- Per-day shame was pre-computed by aggregate_daily.py
- This makes LAST_WEEK/LAST_MONTH comparable to LIVE/TODAY (same 0-10 scale)

CALENDAR-BASED PERIODS:
- last_week: Previous complete week (Sunday-Saturday, Pacific Time)
- last_month: Previous complete calendar month (Pacific Time)

These are fixed calendar periods for social media reporting, e.g.:
- "November's least reliable park was X"
- "Last week's worst performer was Y"

Database Tables:
- parks (park metadata)
- park_daily_stats (aggregated daily downtime data with shame_score)

Single Source of Truth:
- Daily aggregation: scripts/aggregate_daily.py
- Backfill: scripts/backfill_park_shame_scores.py

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

from sqlalchemy import select, func, and_, or_, literal_column
from sqlalchemy.orm import Session

from models import Park, ParkDailyStats
from utils.timezone import get_last_week_date_range, get_last_month_date_range
from utils.query_helpers import QueryClassBase


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

        CRITICAL: Uses pre-computed shame_score from park_daily_stats.
        This is the SINGLE SOURCE OF TRUTH - no on-the-fly calculations.

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
        # CRITICAL: Read shame_score DIRECTLY from park_daily_stats
        # NO CALCULATION HERE - this is the single source of truth
        # Average the daily shame_scores across the period
        shame_score_expr = func.round(
            func.avg(func.coalesce(ParkDailyStats.shame_score, 0)),
            1
        ).label('shame_score')

        # Total downtime hours (sum across period)
        total_downtime_expr = func.round(
            func.sum(func.coalesce(ParkDailyStats.total_downtime_hours, 0)),
            2
        ).label('total_downtime_hours')

        # Sum of weighted downtime hours across period
        weighted_downtime_expr = func.round(
            func.sum(func.coalesce(ParkDailyStats.weighted_downtime_hours, 0)),
            2
        ).label('weighted_downtime_hours')

        # Max rides affected on any day (named rides_down for frontend compatibility)
        rides_down_expr = func.max(
            func.coalesce(ParkDailyStats.rides_with_downtime, 0)
        ).label('rides_down')

        # Average uptime percentage across days
        uptime_expr = func.round(
            func.avg(func.coalesce(ParkDailyStats.avg_uptime_percentage, 0)),
            1
        ).label('uptime_percentage')

        # Main query - Build the rankings
        base_query = (
            select(
                Park.park_id,
                Park.name.label('park_name'),
                (Park.city + ', ' + Park.state_province).label('location'),
                total_downtime_expr,
                weighted_downtime_expr,
                shame_score_expr,
                rides_down_expr,
                uptime_expr,
                literal_column('NULL').label('trend_percentage')
            )
            .select_from(Park)
            .join(ParkDailyStats, Park.park_id == ParkDailyStats.park_id)
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

        # Only include parks with positive shame score
        base_query = base_query.having(func.avg(func.coalesce(ParkDailyStats.shame_score, 0)) > 0)

        # Apply sorting
        sort_column_map = {
            "total_downtime_hours": total_downtime_expr,
            "uptime_percentage": uptime_expr,
            "rides_down": rides_down_expr,
            "shame_score": shame_score_expr
        }
        sort_column = sort_column_map.get(sort_by, shame_score_expr)

        # Sort direction - lower uptime is worse, so ASC for that column
        if sort_by == "uptime_percentage":
            base_query = base_query.order_by(sort_column.asc())
        else:
            base_query = base_query.order_by(sort_column.desc())

        base_query = base_query.limit(limit)

        # Execute and fetch results
        rankings = self.execute_and_fetchall(base_query)

        # Add period_label to each result
        if period_label:
            for row in rankings:
                row['period_label'] = period_label

        return rankings
