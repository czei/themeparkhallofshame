"""
Ride Downtime History Query
===========================

Endpoint: GET /api/trends/chart-data?type=rides&period=7days|30days|today
UI Location: Trends tab → Rides chart

Returns time-series downtime data for Chart.js visualization.

Database Tables:
- rides (ride metadata)
- parks (park metadata for labels)
- ride_daily_stats (daily aggregated data)

Output Format:
{
    "labels": ["Nov 23", "Nov 24", ...],
    "datasets": [
        {"label": "Space Mountain", "park": "Magic Kingdom", "data": [1.5, 0.5, ...]},
        {"label": "Test Track", "park": "EPCOT", "data": [2.0, 1.8, ...]}
    ]
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats
from database.queries.builders import Filters


class RideDowntimeHistoryQuery:
    """
    Query handler for ride downtime time-series.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_daily(
        self,
        days: int = 7,
        filter_disney_universal: bool = False,
        limit: int = 5,
    ) -> Dict[str, Any]:
        """
        Get daily downtime history for top rides.

        Args:
            days: Number of days (7 or 30)
            filter_disney_universal: Only Disney/Universal parks
            limit: Number of rides to include

        Returns:
            Chart.js compatible dict with labels and datasets
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

        # Generate date labels
        labels = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime("%b %d"))
            current += timedelta(days=1)

        # Get top rides by total downtime
        top_rides = self._get_top_rides(start_date, end_date, filter_disney_universal, limit)

        if not top_rides:
            return {"labels": labels, "datasets": []}

        # Get daily data for each ride
        datasets = []
        for ride in top_rides:
            ride_data = self._get_ride_daily_data(ride["ride_id"], start_date, end_date)

            # Align data to labels
            data_by_date = {
                row["stat_date"].strftime("%b %d"): row["downtime_hours"]
                for row in ride_data
            }
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": ride["ride_name"],
                "park": ride["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_top_rides(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Get top rides by total downtime for the period."""
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
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id).join(
                    ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id
                )
            )
            .where(and_(*conditions))
            .group_by(rides.c.ride_id, rides.c.name, parks.c.name)
            .having(func.sum(ride_daily_stats.c.downtime_minutes) > 0)
            .order_by(func.sum(ride_daily_stats.c.downtime_minutes).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_ride_daily_data(
        self,
        ride_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily downtime for a specific ride."""
        stmt = (
            select(
                ride_daily_stats.c.stat_date,
                func.round(ride_daily_stats.c.downtime_minutes / 60.0, 2).label(
                    "downtime_hours"
                ),
            )
            .where(
                and_(
                    ride_daily_stats.c.ride_id == ride_id,
                    ride_daily_stats.c.stat_date >= start_date,
                    ride_daily_stats.c.stat_date <= end_date,
                )
            )
            .order_by(ride_daily_stats.c.stat_date)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
