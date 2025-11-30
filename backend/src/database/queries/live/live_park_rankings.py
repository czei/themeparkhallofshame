"""
Live Park Rankings Query
========================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by current-day downtime, calculated from live snapshots.

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (live status data)
- park_activity_snapshots (park open status)

Time Window: Today's operating hours (Pacific time)

How to Modify:
1. For different time window: Modify _get_today_range()
2. For different weight calculation: Update shame score formula
"""

from datetime import date, datetime
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    ride_status_snapshots,
    park_activity_snapshots,
)
from database.queries.builders import Filters, StatusExpressions

# Each snapshot represents 5 minutes
SNAPSHOT_INTERVAL_MINUTES = 5
DEFAULT_TIER_WEIGHT = 2


class LiveParkRankingsQuery:
    """
    Query handler for live (today) park rankings.

    Uses snapshot tables instead of aggregated stats.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live park rankings for today.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        # Get today's date range in Pacific time
        start_utc, end_utc = self._get_today_range_utc()

        # Build the query with inline downtime calculation
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_status_snapshots.c.recorded_at >= start_utc,
            ride_status_snapshots.c.recorded_at <= end_utc,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        # Downtime case: count snapshots where ride is down and park is open
        downtime_case = case(
            (
                and_(
                    park_activity_snapshots.c.park_appears_open == True,
                    StatusExpressions.is_down(ride_status_snapshots),
                ),
                SNAPSHOT_INTERVAL_MINUTES,
            ),
            else_=0,
        )

        # Weighted downtime includes tier weight
        weighted_downtime_case = case(
            (
                and_(
                    park_activity_snapshots.c.park_appears_open == True,
                    StatusExpressions.is_down(ride_status_snapshots),
                ),
                SNAPSHOT_INTERVAL_MINUTES
                * func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT),
            ),
            else_=0,
        )

        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
                func.concat(parks.c.city, ", ", parks.c.state_province).label("location"),
                func.round(func.sum(downtime_case) / 60.0, 2).label(
                    "total_downtime_hours"
                ),
                func.round(
                    func.sum(weighted_downtime_case)
                    / 60.0
                    / func.nullif(
                        func.sum(
                            func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT)
                        ),
                        0,
                    ),
                    2,
                ).label("shame_score"),
                func.count(
                    func.distinct(
                        case(
                            (StatusExpressions.is_down(ride_status_snapshots), rides.c.ride_id),
                            else_=None,
                        )
                    )
                ).label("affected_rides_count"),
            )
            .select_from(
                parks.join(rides, parks.c.park_id == rides.c.park_id)
                .join(
                    ride_status_snapshots,
                    rides.c.ride_id == ride_status_snapshots.c.ride_id,
                )
                .outerjoin(
                    ride_classifications,
                    rides.c.ride_id == ride_classifications.c.ride_id,
                )
                .outerjoin(
                    park_activity_snapshots,
                    and_(
                        parks.c.park_id == park_activity_snapshots.c.park_id,
                        park_activity_snapshots.c.recorded_at
                        == ride_status_snapshots.c.recorded_at,
                    ),
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name, parks.c.city, parks.c.state_province)
            .having(func.sum(downtime_case) > 0)
            .order_by(
                func.round(
                    func.sum(weighted_downtime_case)
                    / 60.0
                    / func.nullif(
                        func.sum(
                            func.coalesce(ride_classifications.c.tier_weight, DEFAULT_TIER_WEIGHT)
                        ),
                        0,
                    ),
                    2,
                ).desc()
            )
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_today_range_utc(self):
        """
        Get today's date range in UTC (based on Pacific time).

        Returns:
            Tuple of (start_utc, end_utc) datetimes
        """
        # Import here to avoid circular dependency
        try:
            from utils.timezone import get_pacific_day_range_utc
            return get_pacific_day_range_utc()
        except ImportError:
            # Fallback: use today's date in UTC
            today = date.today()
            start = datetime.combine(today, datetime.min.time())
            end = datetime.combine(today, datetime.max.time())
            return start, end
