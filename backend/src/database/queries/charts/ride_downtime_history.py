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

from sqlalchemy import select, func, and_, text
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats, ride_status_snapshots
from database.queries.builders import Filters
from utils.timezone import get_pacific_day_range_utc


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

    def get_hourly(
        self,
        target_date: date,
        filter_disney_universal: bool = False,
        limit: int = 5,
    ) -> Dict[str, Any]:
        """
        Get hourly downtime data for TODAY.

        Uses live snapshot data (ride_status_snapshots) to calculate
        downtime progression throughout the day.

        Args:
            target_date: The date to get hourly data for (usually today)
            filter_disney_universal: Only Disney/Universal parks
            limit: Number of rides to include

        Returns:
            Chart.js compatible dict with hourly labels and datasets
        """
        # Generate hourly labels (6am to 11pm = 18 hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get UTC time range for the target date in Pacific timezone
        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Build filter clause
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Get top rides with most downtime today
        # Only include rides from parks that appear OPEN (excludes seasonal closures)
        top_rides_query = text(f"""
            SELECT
                r.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                SUM(CASE
                    WHEN rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = 0)
                    THEN 5  -- 5-minute interval
                    ELSE 0
                END) / 60.0 AS total_downtime_hours
            FROM rides r
            INNER JOIN parks p ON r.park_id = p.park_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN (
                SELECT park_id, MAX(snapshot_id) AS max_snapshot_id
                FROM park_activity_snapshots
                WHERE recorded_at >= :start_utc AND recorded_at < :end_utc
                GROUP BY park_id
            ) latest_pas ON p.park_id = latest_pas.park_id
            INNER JOIN park_activity_snapshots pas
                ON pas.park_id = p.park_id AND pas.snapshot_id = latest_pas.max_snapshot_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND p.is_active = TRUE
                AND pas.park_appears_open = TRUE
                {disney_filter}
            GROUP BY r.ride_id, r.name, p.name
            HAVING total_downtime_hours > 0
            ORDER BY total_downtime_hours DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_rides_query, {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "limit": limit
        })
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": labels, "datasets": []}

        # Get hourly data for each ride
        datasets = []
        for ride in top_rides:
            hourly_data = self._get_ride_hourly_data(
                ride["ride_id"], start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["downtime_hours"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": ride["ride_name"],
                "park": ride["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_ride_hourly_data(
        self,
        ride_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly downtime for a specific ride from live snapshots."""
        # Use DATE_SUB with 8-hour offset for PST (UTC-8)
        # Note: This doesn't handle DST perfectly but works for most cases
        query = text("""
            SELECT
                HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour,
                ROUND(
                    SUM(CASE
                        WHEN rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = 0)
                        THEN 5  -- 5-minute interval
                        ELSE 0
                    END) / 60.0,
                    2
                ) AS downtime_hours
            FROM ride_status_snapshots rss
            WHERE rss.ride_id = :ride_id
                AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
            GROUP BY hour
            ORDER BY hour
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result]

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
