"""
Yesterday Park Rankings Query
==============================

Endpoint: GET /api/parks/rankings?period=yesterday
UI Location: Parks tab → Yesterday Rankings

Returns parks ranked by shame score for the full previous Pacific day.

CRITICAL FIX (2025-12-28):
==========================
This query now reads DIRECTLY from park_daily_stats.shame_score instead of
computing it on-the-fly. This is the SINGLE SOURCE OF TRUTH pattern that
ensures Rankings and Details always show the same shame_score.

Previously, this query used AVG(effective_park_weight) which was mathematically
incorrect, causing discrepancies between Rankings (8.7) and Details (13.7).

Database Tables:
- park_daily_stats (pre-aggregated daily data with shame_score)
- parks (park metadata)

Single Source of Truth:
- Daily aggregation: scripts/aggregate_daily.py
- Backfill: scripts/backfill_park_shame_scores.py
"""

from typing import List, Dict, Any
from datetime import timedelta

from sqlalchemy import select, func, and_, or_, literal_column, case, desc, asc
from sqlalchemy.orm import Session

from models.orm_park import Park
from models.orm_stats import ParkDailyStats
from models.orm_snapshots import ParkActivitySnapshot
from utils.timezone import get_today_pacific
from utils.query_helpers import QueryClassBase


class YesterdayParkRankingsQuery(QueryClassBase):
    """
    Query handler for YESTERDAY park rankings using pre-aggregated daily stats.

    CRITICAL: Uses park_daily_stats.shame_score directly - NO on-the-fly calculations.
    This ensures Rankings and Details always show the exact same shame_score.

    Uses ONLY pre-aggregated tables for instant performance (<50ms).
    YESTERDAY data is immutable and highly cacheable.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings for the full previous Pacific day.

        CRITICAL FIX: Reads shame_score DIRECTLY from park_daily_stats.
        NO calculations here - single source of truth from aggregate_daily.py.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score, total_downtime_hours, uptime_percentage, rides_down)

        Returns:
            List of parks ranked by the specified sort field
        """
        # Get yesterday's date (Pacific timezone)
        today = get_today_pacific()
        yesterday = today - timedelta(days=1)

        # Build park_is_open subquery (current status - may differ from yesterday)
        # Gets the most recent park activity snapshot to determine if park is currently open
        park_is_open_subquery = (
            select(ParkActivitySnapshot.park_appears_open)
            .where(ParkActivitySnapshot.park_id == Park.park_id)
            .order_by(desc(ParkActivitySnapshot.recorded_at))
            .limit(1)
            .correlate(Park)
            .scalar_subquery()
        )

        # CRITICAL: Read shame_score DIRECTLY from park_daily_stats
        # NO CALCULATION HERE - this is the single source of truth
        shame_score_expr = func.round(
            func.coalesce(ParkDailyStats.shame_score, 0),
            1
        ).label('shame_score')

        # Total downtime hours from pre-aggregated daily stats
        total_downtime_expr = func.round(
            func.coalesce(ParkDailyStats.total_downtime_hours, 0),
            2
        ).label('total_downtime_hours')

        # Weighted downtime hours from pre-aggregated daily stats
        weighted_downtime_expr = func.round(
            func.coalesce(ParkDailyStats.weighted_downtime_hours, 0),
            2
        ).label('weighted_downtime_hours')

        # Uptime percentage from pre-aggregated daily stats
        uptime_percentage_expr = func.round(
            func.coalesce(ParkDailyStats.avg_uptime_percentage, 0),
            1
        ).label('uptime_percentage')

        # Rides with downtime from pre-aggregated daily stats
        rides_down_expr = func.coalesce(ParkDailyStats.rides_with_downtime, 0).label('rides_down')

        # Location: concatenate city and state
        location_expr = (func.concat(Park.city, literal_column("', '"), Park.state_province)).label('location')

        # Build base query - join park_daily_stats with parks
        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                location_expr,
                shame_score_expr,
                total_downtime_expr,
                weighted_downtime_expr,
                uptime_percentage_expr,
                rides_down_expr,
                park_is_open_subquery.label('park_is_open')
            )
            .select_from(ParkDailyStats)
            .join(Park, ParkDailyStats.park_id == Park.park_id)
            .where(ParkDailyStats.stat_date == yesterday)
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # HAVING clause: only parks with positive shame score
        stmt = stmt.where(ParkDailyStats.shame_score > 0)

        # Determine sort column and direction based on parameter
        sort_column_map = {
            "total_downtime_hours": total_downtime_expr,
            "uptime_percentage": uptime_percentage_expr,
            "rides_down": rides_down_expr,
            "shame_score": shame_score_expr
        }
        sort_column = sort_column_map.get(sort_by, shame_score_expr)

        # Uptime sorts ascending (higher is better), others sort descending (higher is worse)
        if sort_by == "uptime_percentage":
            stmt = stmt.order_by(asc(sort_column))
        else:
            stmt = stmt.order_by(desc(sort_column))

        # Apply limit
        stmt = stmt.limit(limit)

        # Execute and return results
        return self.execute_and_fetchall(stmt)
