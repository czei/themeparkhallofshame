"""
Park Wait Time History Query
============================

Endpoint: GET /api/trends/chart-data?type=waittimes&period=7days|30days|today
UI Location: Trends tab → Park Wait Times chart

Returns time-series average wait time data for Chart.js visualization.

For period=today: Returns hourly data points (6am-11pm)
For period=7days/30days: Returns daily data points

Database Tables:
- parks (park metadata)
- park_activity_snapshots (avg_wait_time per snapshot)
- park_daily_stats (daily aggregated data)

Output Format:
{
    "labels": ["Nov 23", "Nov 24", ...],
    "datasets": [
        {"label": "Magic Kingdom", "data": [45, 52, 38, ...]},
        {"label": "EPCOT", "data": [32, 41, 35, ...]}
    ]
}
"""

from datetime import date, timedelta
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, text
from sqlalchemy.engine import Connection

from database.schema import parks, park_daily_stats, park_activity_snapshots
from database.queries.builders import Filters
from utils.timezone import get_pacific_day_range_utc
from utils.sql_helpers import ParkStatusSQL


class ParkWaitTimeHistoryQuery:
    """
    Query handler for park average wait time time-series.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_daily(
        self,
        days: int = 7,
        filter_disney_universal: bool = False,
        limit: int = 4,
    ) -> Dict[str, Any]:
        """
        Get daily average wait time history for top parks.

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

        # Get top parks by average wait time for the period
        top_parks = self._get_top_parks_by_wait_time(
            start_date, end_date, filter_disney_universal, limit
        )

        if not top_parks:
            return {"labels": labels, "datasets": []}

        # Get daily data for each park
        datasets = []
        for park in top_parks:
            park_data = self._get_park_daily_wait_data(
                park["park_id"], start_date, end_date
            )

            # Align data to labels (fill None for missing dates)
            data_by_date = {
                row["stat_date"].strftime("%b %d"): row["avg_wait"]
                for row in park_data
            }
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": park["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def get_hourly(
        self,
        target_date: date,
        filter_disney_universal: bool = False,
        limit: int = 4,
    ) -> Dict[str, Any]:
        """
        Get hourly average wait time data for TODAY.

        Uses live snapshot data (park_activity_snapshots) to show
        wait time progression throughout the day.

        Args:
            target_date: The date to get hourly data for (usually today)
            filter_disney_universal: Only Disney/Universal parks
            limit: Number of parks to include

        Returns:
            Chart.js compatible dict with hourly labels and datasets
        """
        # Generate hourly labels (6am to 11pm = 18 hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get UTC time range for the target date in Pacific timezone
        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Build filter clause
        disney_filter = (
            "AND (p.is_disney = TRUE OR p.is_universal = TRUE)"
            if filter_disney_universal
            else ""
        )

        # Get top parks by average wait time today
        # Only include parks that appear OPEN
        top_parks_query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                AVG(pas.avg_wait_time) AS overall_avg_wait
            FROM parks p
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND pas.park_appears_open = TRUE
                AND pas.avg_wait_time IS NOT NULL
                AND pas.avg_wait_time > 0
                AND p.is_active = TRUE
                {disney_filter}
            GROUP BY p.park_id, p.name
            HAVING overall_avg_wait > 0
            ORDER BY overall_avg_wait DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_parks_query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        top_parks = [dict(row._mapping) for row in result]

        if not top_parks:
            return {"labels": labels, "datasets": []}

        # Get hourly data for each park
        datasets = []
        for park in top_parks:
            hourly_data = self._get_park_hourly_wait_data(
                park["park_id"], start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["avg_wait"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": park["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_park_hourly_wait_data(
        self,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly average wait times for a specific park from live snapshots."""
        # Use DATE_SUB with 8-hour offset for PST (UTC-8)
        query = text("""
            SELECT
                HOUR(DATE_SUB(pas.recorded_at, INTERVAL 8 HOUR)) AS hour,
                ROUND(AVG(pas.avg_wait_time), 0) AS avg_wait
            FROM park_activity_snapshots pas
            WHERE pas.park_id = :park_id
                AND pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND pas.park_appears_open = TRUE
                AND pas.avg_wait_time IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result]

    def _get_top_parks_by_wait_time(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Get top parks by average wait time for the period."""
        conditions = [
            parks.c.is_active == True,
            park_daily_stats.c.stat_date >= start_date,
            park_daily_stats.c.stat_date <= end_date,
            park_daily_stats.c.avg_wait_time.isnot(None),
            park_daily_stats.c.avg_wait_time > 0,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                parks.c.park_id,
                parks.c.name.label("park_name"),
                func.avg(park_daily_stats.c.avg_wait_time).label("overall_avg_wait"),
            )
            .select_from(
                parks.join(
                    park_daily_stats,
                    parks.c.park_id == park_daily_stats.c.park_id
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name)
            .having(func.avg(park_daily_stats.c.avg_wait_time) > 0)
            .order_by(func.avg(park_daily_stats.c.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_park_daily_wait_data(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily average wait times for a specific park."""
        stmt = (
            select(
                park_daily_stats.c.stat_date,
                func.round(park_daily_stats.c.avg_wait_time, 0).label("avg_wait"),
            )
            .where(
                and_(
                    park_daily_stats.c.park_id == park_id,
                    park_daily_stats.c.stat_date >= start_date,
                    park_daily_stats.c.stat_date <= end_date,
                    park_daily_stats.c.avg_wait_time.isnot(None),
                )
            )
            .order_by(park_daily_stats.c.stat_date)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
