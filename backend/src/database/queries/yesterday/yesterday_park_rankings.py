"""
Yesterday Park Rankings Query
==============================

Endpoint: GET /api/parks/rankings?period=yesterday
UI Location: Parks tab → Yesterday Rankings

Returns parks ranked by AVERAGE shame score from the full previous Pacific day
(midnight to midnight).

Performance Optimization (2025-12):
====================================
This query uses pre-aggregated park_hourly_stats for fast performance.
It does NOT query raw snapshot tables.

Database Tables:
- park_hourly_stats (pre-aggregated hourly data)
- parks (park metadata)

Single Source of Truth:
- Hourly aggregation: scripts/aggregate_hourly.py
"""

from typing import List, Dict, Any
from datetime import timedelta

from sqlalchemy import select, func, and_, or_, literal_column, case, desc, asc
from sqlalchemy.orm import Session

from src.models.orm_park import Park
from src.models.orm_stats import ParkHourlyStats
from src.models.orm_snapshots import ParkActivitySnapshot
from src.utils.timezone import get_today_pacific, get_pacific_day_range_utc
from src.utils.query_helpers import QueryClassBase


class YesterdayParkRankingsQuery(QueryClassBase):
    """
    Query handler for YESTERDAY park rankings using pre-aggregated hourly stats.

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
        Get park rankings for the full previous Pacific day using AVERAGE shame score.

        Uses pre-aggregated park_hourly_stats table for fast performance.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score, total_downtime_hours, uptime_percentage, rides_down)

        Returns:
            List of parks ranked by the specified sort field
        """
        # Get time range for yesterday (full previous Pacific day)
        today = get_today_pacific()
        yesterday = today - timedelta(days=1)
        start_utc, end_utc = get_pacific_day_range_utc(yesterday)

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

        # Calculate shame score using same formula as TODAY:
        # (weighted_downtime / park_weight) × 10
        # This ensures rankings table EXACTLY matches detail popup
        shame_score_expr = func.round(
            (func.sum(ParkHourlyStats.weighted_downtime_hours) /
             func.nullif(func.avg(ParkHourlyStats.effective_park_weight), 0)) * 10,
            1
        ).label('shame_score')

        # Total downtime hours: sum across yesterday
        total_downtime_expr = func.round(
            func.sum(ParkHourlyStats.total_downtime_hours),
            2
        ).label('total_downtime_hours')

        # Weighted downtime hours: sum across yesterday
        weighted_downtime_expr = func.round(
            func.sum(ParkHourlyStats.weighted_downtime_hours),
            2
        ).label('weighted_downtime_hours')

        # Uptime percentage: calculated from hourly aggregates
        uptime_percentage_expr = func.round(
            100.0 * func.sum(ParkHourlyStats.rides_operating) /
            func.nullif(
                func.sum(ParkHourlyStats.rides_operating) + func.sum(ParkHourlyStats.rides_down),
                0
            ),
            1
        ).label('uptime_percentage')

        # Rides down: max concurrent across yesterday
        rides_down_expr = func.max(ParkHourlyStats.rides_down).label('rides_down')

        # Location: concatenate city and state
        location_expr = (func.concat(Park.city, literal_column("', '"), Park.state_province)).label('location')

        # Build base query
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
            .select_from(ParkHourlyStats)
            .join(Park, ParkHourlyStats.park_id == Park.park_id)
            .where(ParkHourlyStats.hour_start_utc >= start_utc)
            .where(ParkHourlyStats.hour_start_utc < end_utc)
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Group by park
        stmt = stmt.group_by(
            Park.park_id,
            Park.queue_times_id,
            Park.name,
            Park.city,
            Park.state_province
        )

        # HAVING clause: only parks with positive shame score
        # Using the same expression as in SELECT
        having_expr = (
            (func.sum(ParkHourlyStats.weighted_downtime_hours) /
             func.nullif(func.avg(ParkHourlyStats.effective_park_weight), 0)) * 10 > 0
        )
        stmt = stmt.having(having_expr)

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
