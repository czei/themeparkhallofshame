"""
Today Park Rankings Query (Average Shame Score)
===============================================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by AVERAGE shame score from midnight Pacific to now.

SHAME SCORE CALCULATION:
- LIVE: Instantaneous shame = (sum of weights of down rides) / total_park_weight × 10
- TODAY: (SUM(weighted_downtime_hours) / AVG(effective_park_weight)) × 10

SINGLE SOURCE OF TRUTH: Uses same formula as detail popup (stats_repository.py).
This ensures shame score in rankings table EXACTLY matches detail popup.

Performance Optimization (2025-12):
====================================
This query uses ONLY pre-aggregated tables (park_hourly_stats, park_live_rankings).
It does NOT query raw snapshot tables, which eliminates the slow DATE_FORMAT joins
that were causing >2 minute query times.

Database Tables:
- park_hourly_stats (pre-aggregated hourly data with shame_score, downtime)
- park_live_rankings (current live status for rides_down, park_is_open)
- parks (park metadata)

Single Source of Truth:
- Hourly aggregation: scripts/aggregate_hourly.py
- Live aggregation: scripts/aggregate_live_rankings.py
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, literal_column
from sqlalchemy.orm import Session

from src.models.orm_park import Park
from src.models.orm_stats import ParkHourlyStats
from src.utils.query_helpers import QueryClassBase
from src.utils.timezone import get_today_pacific, get_pacific_day_range_utc


class TodayParkRankingsQuery(QueryClassBase):
    """
    Query handler for today's park rankings using AVERAGE shame score.

    Uses ONLY pre-aggregated tables for instant performance (<50ms).
    No raw snapshot queries, no DATE_FORMAT joins.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings from midnight Pacific to now using AVERAGE shame score.

        Uses pre-aggregated park_hourly_stats table for complete hours,
        combined with park_live_rankings for current status.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or total_downtime_hours)

        Returns:
            List of parks ranked by average shame_score (descending)
        """
        # Get Pacific day boundaries in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # Build base query with aggregations
        # Shame score: (SUM(weighted_downtime_hours) / AVG(effective_park_weight)) × 10
        shame_score_expr = (
            func.sum(ParkHourlyStats.weighted_downtime_hours) /
            func.nullif(func.avg(ParkHourlyStats.effective_park_weight), 0)
        ) * 10

        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                (func.concat(Park.city, ', ', Park.state_province)).label('location'),

                # Shame score: calculated from corrected downtime data (SINGLE SOURCE OF TRUTH)
                # Uses same formula as detail popup: (weighted_downtime / park_weight) × 10
                # This ensures rankings table EXACTLY matches detail popup
                func.round(shame_score_expr, 1).label('shame_score'),

                # Total downtime hours: sum across today
                func.round(func.sum(ParkHourlyStats.total_downtime_hours), 2).label('total_downtime_hours'),

                # Weighted downtime hours: sum across today
                func.round(func.sum(ParkHourlyStats.weighted_downtime_hours), 2).label('weighted_downtime_hours'),

                # Rides operating/down totals derived from today's aggregates
                func.round(func.sum(ParkHourlyStats.rides_operating), 0).label('rides_operating'),
                func.round(func.sum(ParkHourlyStats.rides_down), 0).label('rides_down'),

                # Effective park weight + snapshots for contract parity
                func.round(func.avg(ParkHourlyStats.effective_park_weight), 1).label('effective_park_weight'),
                func.sum(ParkHourlyStats.snapshot_count).label('snapshot_count'),

                # Park open flag derived from hourly aggregates
                func.max(ParkHourlyStats.park_was_open).label('park_is_open'),

                # Uptime percentage: calculated from hourly aggregates
                func.round(
                    100.0 * func.sum(ParkHourlyStats.rides_operating) /
                    func.nullif(
                        func.sum(ParkHourlyStats.rides_operating) + func.sum(ParkHourlyStats.rides_down),
                        0
                    ),
                    1
                ).label('uptime_percentage')
            )
            .select_from(ParkHourlyStats)
            .join(Park, ParkHourlyStats.park_id == Park.park_id)
            .where(ParkHourlyStats.hour_start_utc >= start_utc)
            .where(ParkHourlyStats.hour_start_utc < end_utc)
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        # Group by park
        stmt = stmt.group_by(
            Park.park_id,
            Park.queue_times_id,
            Park.name,
            Park.city,
            Park.state_province
        )

        # Having clause: only parks with shame_score > 0
        stmt = stmt.having(shame_score_expr > 0)

        # Order by selected column
        if sort_by == "shame_score":
            stmt = stmt.order_by(literal_column('shame_score').desc())
        else:
            stmt = stmt.order_by(literal_column('total_downtime_hours').desc())

        # Limit results
        stmt = stmt.limit(limit)

        # Execute and return
        return self.execute_and_fetchall(stmt)
