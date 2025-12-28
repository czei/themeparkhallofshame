"""
Fast Live Park Rankings Query
=============================

Endpoint: GET /api/parks/downtime?period=live
UI Location: Parks tab → Downtime Rankings (Live)

Returns parks ranked by INSTANTANEOUS current status from the pre-aggregated
`park_live_rankings` table. This provides true "live" data - what is down RIGHT NOW.

Performance: Uses ONLY the pre-aggregated park_live_rankings table for instant
performance (<10ms). No raw snapshot queries, no DATE_FORMAT joins.

Database Tables:
- park_live_rankings (pre-aggregated current state, updated every 5 minutes)
- parks (park metadata for queue_times_id)

Single Source of Truth:
- Live aggregation: scripts/aggregate_live_rankings.py
"""

from typing import List, Dict, Any

from sqlalchemy import Table, MetaData, Column, Integer, String, Float, Boolean, DateTime, select, func, case, and_, or_
from sqlalchemy.orm import Session

from models.orm_park import Park
from utils.query_helpers import QueryClassBase


# Define table for park_live_rankings cache
# This is a pre-aggregated cache table populated by scripts/aggregate_live_rankings.py
metadata = MetaData()
park_live_rankings = Table(
    'park_live_rankings', metadata,
    Column('park_id', Integer, primary_key=True),
    Column('park_name', String(255)),
    Column('shame_score', Float),
    Column('total_downtime_hours', Float),
    Column('weighted_downtime_hours', Float),
    Column('rides_down', Integer),
    Column('park_is_open', Boolean),
    Column('total_rides', Integer),
    extend_existing=True
)


class FastLiveParkRankingsQuery(QueryClassBase):
    """
    Query handler for TRUE live park rankings using pre-aggregated data.

    Uses ONLY the park_live_rankings cache table for instant performance (<10ms).
    This provides INSTANTANEOUS current state - what is down RIGHT NOW.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get live park rankings from pre-aggregated cache table.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or total_downtime_hours)

        Returns:
            List of parks ranked by shame_score (descending)
        """
        # Build sort column
        sort_col = (
            park_live_rankings.c.shame_score
            if sort_by == "shame_score"
            else park_live_rankings.c.total_downtime_hours
        )

        # Calculate uptime percentage
        uptime_expr = case(
            (park_live_rankings.c.total_rides > 0,
             func.round(100.0 * (park_live_rankings.c.total_rides - park_live_rankings.c.rides_down) / park_live_rankings.c.total_rides, 1)),
            else_=100.0
        ).label('uptime_percentage')

        # Build base query
        stmt = (
            select(
                park_live_rankings.c.park_id,
                Park.queue_times_id,
                park_live_rankings.c.park_name,
                (Park.city + ', ' + Park.state_province).label('location'),

                # Instantaneous shame score (current state)
                park_live_rankings.c.shame_score,

                # Total downtime hours for today so far
                func.coalesce(park_live_rankings.c.total_downtime_hours, 0).label('total_downtime_hours'),

                # Weighted downtime hours for today so far
                func.coalesce(park_live_rankings.c.weighted_downtime_hours, 0).label('weighted_downtime_hours'),

                # Rides currently down RIGHT NOW
                park_live_rankings.c.rides_down,

                # Park is open (current state)
                park_live_rankings.c.park_is_open,

                # Total rides and uptime percentage (calculated)
                park_live_rankings.c.total_rides,
                uptime_expr
            )
            .select_from(park_live_rankings)
            .join(Park, park_live_rankings.c.park_id == Park.park_id)
            .where(and_(
                Park.is_active == True,
                park_live_rankings.c.park_is_open == True,  # CRITICAL: Only show OPEN parks
                park_live_rankings.c.shame_score > 0,
                or_(park_live_rankings.c.rides_down > 0, park_live_rankings.c.total_downtime_hours > 0)  # Must have actual downtime
            ))
            .order_by(sort_col.desc())
            .limit(limit)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        return self.execute_and_fetchall(stmt)
