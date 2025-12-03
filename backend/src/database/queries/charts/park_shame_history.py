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

from sqlalchemy import select, func, and_, text
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_classifications,
    park_daily_stats,
    ride_daily_stats,
    ride_status_snapshots,
    park_activity_snapshots,
)
from database.queries.builders import Filters, ParkWeightsCTE, WeightedDowntimeCTE
from utils.timezone import get_pacific_day_range_utc
from utils.sql_helpers import ParkStatusSQL


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

    def get_hourly(
        self,
        target_date: date,
        filter_disney_universal: bool = False,
        limit: int = 5,
    ) -> Dict[str, Any]:
        """
        Get hourly shame score data for TODAY.

        Uses live snapshot data (ride_status_snapshots) to calculate
        shame score progression throughout the day.

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
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Get top parks with most downtime today
        # Only include parks that appear OPEN (excludes seasonal closures)
        open_parks_join = ParkStatusSQL.latest_snapshot_join_sql("p")
        top_parks_query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                SUM(CASE
                    WHEN rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = 0)
                    THEN 5  -- 5-minute interval
                    ELSE 0
                END) / 60.0 AS total_downtime_hours
            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            {open_parks_join}
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND p.is_active = TRUE
                {disney_filter}
            GROUP BY p.park_id, p.name
            HAVING total_downtime_hours > 0
            ORDER BY total_downtime_hours DESC
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
            hourly_data = self._get_park_hourly_data(
                park["park_id"], start_utc, end_utc, target_date
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["shame_score"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": park["park_name"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_park_hourly_data(
        self,
        park_id: int,
        start_utc,
        end_utc,
        target_date: date,
    ) -> List[Dict[str, Any]]:
        """Get hourly shame scores for a specific park from live snapshots.

        Logic:
        1. Only count hours where park_appears_open = TRUE
        2. Only count a ride's downtime AFTER it has operated at least once today
        3. Rides that never operated today don't contribute to shame score
        """
        # Use DATE_SUB with 8-hour offset for PST (UTC-8)
        # IMPORTANT: Use timestamp-level comparison, not hour-level, to avoid
        # counting pre-opening time within an hour as downtime
        query = text("""
            WITH ride_first_operating AS (
                -- Find the exact timestamp each ride first operated today
                SELECT
                    rss_inner.ride_id,
                    MIN(rss_inner.recorded_at) as first_op_time
                FROM ride_status_snapshots rss_inner
                INNER JOIN rides r_inner ON rss_inner.ride_id = r_inner.ride_id
                WHERE r_inner.park_id = :park_id
                    AND r_inner.is_active = TRUE
                    AND r_inner.category = 'ATTRACTION'
                    AND rss_inner.recorded_at >= :start_utc AND rss_inner.recorded_at < :end_utc
                    AND (rss_inner.status = 'OPERATING'
                         OR (rss_inner.status IS NULL AND rss_inner.computed_is_open = 1))
                GROUP BY rss_inner.ride_id
            ),
            park_hourly_open AS (
                -- Check if park was open during each hour
                -- NOTE: Must use full expression in GROUP BY for MariaDB strict mode
                SELECT
                    HOUR(DATE_SUB(pas.recorded_at, INTERVAL 8 HOUR)) as hour,
                    MAX(pas.park_appears_open) as park_open
                FROM park_activity_snapshots pas
                WHERE pas.park_id = :park_id
                    AND pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                GROUP BY HOUR(DATE_SUB(pas.recorded_at, INTERVAL 8 HOUR))
            )
            SELECT
                HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour,
                -- Only count rides that have operated at some point today
                COUNT(DISTINCT CASE WHEN rfo.ride_id IS NOT NULL THEN r.ride_id END) AS total_rides,
                -- Only count downtime when:
                -- 1. Park is open this hour
                -- 2. Ride has operated before (snapshot time >= first_op_time)
                SUM(CASE
                    WHEN pho.park_open = 1
                        AND rfo.ride_id IS NOT NULL
                        AND rss.recorded_at >= rfo.first_op_time
                        AND (rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = 0))
                    THEN 5
                    ELSE 0
                END) AS down_minutes,
                ROUND(
                    SUM(CASE
                        WHEN pho.park_open = 1
                            AND rfo.ride_id IS NOT NULL
                            AND rss.recorded_at >= rfo.first_op_time
                            AND (rss.status = 'DOWN' OR (rss.status IS NULL AND rss.computed_is_open = 0))
                        THEN 5
                        ELSE 0
                    END) / 60.0 / NULLIF(
                        COUNT(DISTINCT CASE
                            WHEN rfo.ride_id IS NOT NULL
                                AND rss.recorded_at >= rfo.first_op_time
                            THEN r.ride_id
                        END), 0) * 10,
                    1
                ) AS shame_score
            FROM rides r
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            LEFT JOIN ride_first_operating rfo ON r.ride_id = rfo.ride_id
            LEFT JOIN park_hourly_open pho
                ON HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) = pho.hour
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                AND pho.park_open = 1  -- Only include hours when park is open
            GROUP BY HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR))
            HAVING total_rides > 0  -- Only show hours with rides that have operated
            ORDER BY hour
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result]

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
