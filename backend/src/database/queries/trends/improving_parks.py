"""
Improving Parks Query
=====================

Endpoint: GET /api/trends?category=parks-improving
UI Location: Trends tab → Parks Improving section

Returns parks with uptime improvement >= 5% vs previous period.

Database Tables:
- parks (park metadata)
- park_weekly_stats (trend_vs_previous_week)

How to Modify:
1. To change threshold: Modify IMPROVEMENT_THRESHOLD constant
2. To add columns: Extend select()

Example Response:
{
    "park_id": 1,
    "park_name": "Magic Kingdom",
    "location": "Orlando, FL",
    "current_uptime": 96.5,
    "previous_uptime": 91.2,
    "improvement_percentage": 5.3
}
"""

from datetime import date
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, park_weekly_stats
from database.queries.builders import Filters


# =============================================================================
# IMPROVEMENT THRESHOLD
# =============================================================================
# Parks must show >= 5% improvement in uptime to appear in this list
IMPROVEMENT_THRESHOLD = 5.0


class ImprovingParksQuery:
    """
    Query for parks showing uptime improvement.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_improving(
        self,
        period: str = '7days',
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get improving parks for the specified period."""
        # Currently all periods use weekly data
        return self.get_weekly(filter_disney_universal, limit)

    def get_weekly(
        self,
        filter_disney_universal: bool = False,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Get parks with improving uptime for current week.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of improving parks sorted by improvement % (descending)
        """
        today = date.today()
        year = today.year
        week_number = today.isocalendar()[1]

        conditions = [
            parks.c.is_active == True,
            park_weekly_stats.c.year == year,
            park_weekly_stats.c.week_number == week_number,
            # Positive trend = improving (less downtime)
            park_weekly_stats.c.trend_vs_previous_week < -IMPROVEMENT_THRESHOLD,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
                func.concat(parks.c.city, ", ", parks.c.state_province).label("location"),
                park_weekly_stats.c.avg_uptime_percentage.label("current_uptime"),
                # Calculate previous uptime from trend
                func.round(
                    park_weekly_stats.c.avg_uptime_percentage
                    / (1 + park_weekly_stats.c.trend_vs_previous_week / 100),
                    2,
                ).label("previous_uptime"),
                # Improvement is negative trend (less downtime = better)
                func.abs(park_weekly_stats.c.trend_vs_previous_week).label(
                    "improvement_percentage"
                ),
            )
            .select_from(
                parks.join(
                    park_weekly_stats, parks.c.park_id == park_weekly_stats.c.park_id
                )
            )
            .where(and_(*conditions))
            .order_by(park_weekly_stats.c.trend_vs_previous_week.asc())  # Most improved first
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
