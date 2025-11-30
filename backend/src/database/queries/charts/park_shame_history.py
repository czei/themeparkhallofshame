"""
Park Shame Score History Query
==============================

Endpoint: GET /api/trends/chart-data?type=parks&period=7days|30days|today
UI Location: Trends tab → Parks chart

Returns time-series shame score data for Chart.js visualization.

For period=today: Returns hourly data points (6am-11pm)
For period=7days/30days: Returns daily data points

Database Tables:
- parks (park metadata)
- park_daily_stats (daily aggregated data)
- ride_daily_stats (for weighted calculations)
- ride_classifications (tier weights)

Output Format:
{
    "labels": ["Nov 23", "Nov 24", ...],
    "datasets": [
        {"label": "Magic Kingdom", "data": [0.21, 0.18, 0.25, ...]},
        {"label": "EPCOT", "data": [0.15, 0.12, 0.19, ...]}
    ]
}

How to Modify:
1. To change date format: Modify _format_date_label()
2. To add dataset fields: Extend the datasets loop
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    park_daily_stats,
    ride_daily_stats,
)
from database.queries.builders import Filters, ParkWeightsCTE, WeightedDowntimeCTE


class ParkShameHistoryQuery:
    """
    Query handler for park shame score time-series.
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
        Get daily shame score history for top parks.

        Args:
            days: Number of days (7 or 30)
            filter_disney_universal: Only Disney/Universal parks
            limit: Number of parks to include

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

        # Get top parks by total shame score for the period
        top_parks = self._get_top_parks(start_date, end_date, filter_disney_universal, limit)

        if not top_parks:
            return {"labels": labels, "datasets": []}

        # Get daily data for each park
        datasets = []
        for park in top_parks:
            park_data = self._get_park_daily_data(
                park["park_id"], start_date, end_date
            )

            # Align data to labels (fill None for missing dates)
            data_by_date = {row["stat_date"].strftime("%b %d"): row["shame_score"] for row in park_data}
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": park["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_top_parks(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Get top parks by total shame score for the period."""
        pw = ParkWeightsCTE.build(filter_disney_universal=filter_disney_universal)
        wd = WeightedDowntimeCTE.from_daily_stats(
            start_date=start_date,
            end_date=end_date,
            filter_disney_universal=filter_disney_universal,
        )

        conditions = [
            parks.c.is_active == True,
            park_daily_stats.c.stat_date >= start_date,
            park_daily_stats.c.stat_date <= end_date,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
            )
            .select_from(
                parks.join(park_daily_stats, parks.c.park_id == park_daily_stats.c.park_id)
                .outerjoin(pw, parks.c.park_id == pw.c.park_id)
                .outerjoin(wd, parks.c.park_id == wd.c.park_id)
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name, pw.c.total_park_weight, wd.c.total_weighted_downtime_hours)
            .having(func.sum(park_daily_stats.c.total_downtime_hours) > 0)
            .order_by(
                (wd.c.total_weighted_downtime_hours / func.nullif(pw.c.total_park_weight, 0)).desc()
            )
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_park_daily_data(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily shame scores for a specific park."""
        # For each day, calculate shame score from that day's data
        # This is simplified - in reality would need per-day CTEs
        stmt = (
            select(
                park_daily_stats.c.stat_date,
                func.round(
                    park_daily_stats.c.total_downtime_hours / func.nullif(
                        func.coalesce(park_daily_stats.c.total_rides_tracked, 1), 0
                    ),
                    2
                ).label("shame_score"),
            )
            .where(
                and_(
                    park_daily_stats.c.park_id == park_id,
                    park_daily_stats.c.stat_date >= start_date,
                    park_daily_stats.c.stat_date <= end_date,
                )
            )
            .order_by(park_daily_stats.c.stat_date)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
