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
    park_daily_stats,
)
from database.queries.builders import Filters, ParkWeightsCTE, WeightedDowntimeCTE
# NOTE: ShameScoreCalculator removed - now reading from pas.shame_score (THE SINGLE SOURCE OF TRUTH)
from utils.timezone import get_pacific_day_range_utc, get_today_range_to_now_utc
from utils.sql_helpers import ParkStatusSQL
from utils.metrics import USE_HOURLY_TABLES


class ParkShameHistoryQuery:
    """
    Query handler for park shame score time-series.

    Supports two query paths:
    - Fast path: Pre-aggregated park_hourly_stats table
    - Slow path: GROUP BY HOUR on raw park_activity_snapshots (rollback)
    """

    def __init__(self, connection: Connection, use_hourly_tables: bool = None):
        """
        Initialize query handler.

        Args:
            connection: Database connection
            use_hourly_tables: If True, use park_hourly_stats (fast path).
                             If False, use GROUP BY HOUR on raw snapshots (rollback).
                             If None, uses global USE_HOURLY_TABLES flag (default).
        """
        self.conn = connection
        self.use_hourly_tables = use_hourly_tables if use_hourly_tables is not None else USE_HOURLY_TABLES

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
        Get hourly shame score data for any date.

        READs stored shame_score from park_activity_snapshots (THE SINGLE SOURCE OF TRUTH).
        Works for historical dates (YESTERDAY, last week) and current date (TODAY).

        Args:
            target_date: The date to get hourly data for
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

        # Get top parks with highest shame scores for the target date
        # Uses park_activity_snapshots (THE SINGLE SOURCE OF TRUTH) with stored shame_scores
        #
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        #
        # This makes charts robust against schedule data issues for historical dates.
        top_parks_query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name,
                AVG(pas.shame_score) AS avg_shame_score
            FROM parks p
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND p.is_active = TRUE
                AND (pas.park_appears_open = TRUE OR pas.rides_open > 0)
                AND pas.shame_score IS NOT NULL
                AND pas.shame_score > 0
                {disney_filter}
            GROUP BY p.park_id, p.name
            HAVING COUNT(*) > 0
            ORDER BY avg_shame_score DESC
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
        # Choose query method based on use_hourly_tables parameter
        if self.use_hourly_tables:
            query_method = self._query_hourly_tables
        else:
            query_method = self._query_raw_snapshots

        datasets = []
        for park in top_parks:
            hourly_data = query_method(
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

    def get_single_park_hourly(
        self,
        park_id: int,
        target_date: date,
        is_today: bool = False,
    ) -> Dict[str, Any]:
        """
        Get hourly shame score data for a single park.

        READs stored shame_score from park_activity_snapshots for consistency.
        THE SINGLE SOURCE OF TRUTH - calculated during data collection.

        Args:
            park_id: The park ID to get data for
            target_date: The date to get hourly data for
            is_today: If True, uses get_today_range_to_now_utc for time range

        Returns:
            Chart.js compatible dict with hourly labels and single dataset
        """
        # Get UTC time range for the target date in Pacific timezone
        if is_today:
            start_utc, end_utc = get_today_range_to_now_utc()
        else:
            start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Choose query method based on use_hourly_tables parameter
        if self.use_hourly_tables:
            hourly_data = self._query_hourly_tables(park_id, start_utc, end_utc, target_date)
        else:
            hourly_data = self._query_raw_snapshots(park_id, start_utc, end_utc, target_date)

        # Build data by hour for all metrics
        # Convert Decimal to float for JSON serialization
        shame_by_hour = {row["hour"]: row["shame_score"] for row in hourly_data}
        rides_down_by_hour = {row["hour"]: row.get("rides_down") for row in hourly_data}
        avg_wait_by_hour = {row["hour"]: row.get("avg_wait_time_minutes") for row in hourly_data}

        # Get the hours that have data, in order
        hours_with_data = sorted(shame_by_hour.keys())

        # Build labels and data arrays only for hours with data
        labels = [f"{h}:00" for h in hours_with_data]
        aligned_data = [
            float(shame_by_hour[h]) if shame_by_hour.get(h) is not None else None
            for h in hours_with_data
        ]
        rides_down_data = [
            int(rides_down_by_hour[h]) if rides_down_by_hour.get(h) is not None else None
            for h in hours_with_data
        ]
        avg_wait_data = [
            float(avg_wait_by_hour[h]) if avg_wait_by_hour.get(h) is not None else None
            for h in hours_with_data
        ]

        # Calculate average FROM the chart data points (ensures average badge matches chart)
        non_null_data = [v for v in aligned_data if v is not None]
        avg_score = round(sum(non_null_data) / len(non_null_data), 1) if non_null_data else 0.0

        return {
            "labels": labels,
            "data": aligned_data,
            "rides_down": rides_down_data,
            "avg_wait": avg_wait_data,
            "average": avg_score,
            "granularity": "hourly"
        }

    def get_live(
        self,
        filter_disney_universal: bool = False,
        limit: int = 5,
        minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Get live 5-minute granularity shame score data for multiple parks.

        Returns real-time data at 5-minute intervals for the Charts tab LIVE period,
        READing stored shame_score from park_activity_snapshots (THE SINGLE SOURCE OF TRUTH).

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Number of parks to include
            minutes: How many minutes of recent data (default 60)

        Returns:
            Chart.js compatible dict with labels and datasets at minute granularity
        """
        from datetime import datetime, timedelta, timezone

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        # Build filter clause
        disney_filter = "AND (p.is_disney = TRUE OR p.is_universal = TRUE)" if filter_disney_universal else ""

        # Get top parks by recent downtime (last 60 minutes)
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        top_parks_query = text(f"""
            SELECT
                p.park_id,
                p.name AS park_name
            FROM parks p
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND p.is_active = TRUE
                AND (pas.park_appears_open = TRUE OR pas.rides_open > 0)
                AND pas.shame_score > 0
                {disney_filter}
            GROUP BY p.park_id, p.name
            ORDER BY AVG(pas.shame_score) DESC
            LIMIT :limit
        """)

        result = self.conn.execute(top_parks_query, {
            "start_utc": start_utc,
            "end_utc": now_utc,
            "limit": limit
        })
        top_parks = [dict(row._mapping) for row in result]

        if not top_parks:
            return {"labels": [], "datasets": []}

        # Get recent snapshots for each park by READing stored shame_score
        # THE SINGLE SOURCE OF TRUTH - calculated during data collection
        datasets = []
        labels = None

        for park in top_parks:
            # READ stored shame_score from park_activity_snapshots
            # THE SINGLE SOURCE OF TRUTH - calculated during data collection
            #
            # FALLBACK HEURISTIC: Include snapshots where EITHER:
            # 1. park_appears_open = TRUE (schedule-based detection), OR
            # 2. rides_open > 0 (rides are actually operating)
            snapshot_query = text("""
                SELECT
                    DATE_FORMAT(pas.recorded_at, '%H:%i') AS label,
                    pas.shame_score
                FROM park_activity_snapshots pas
                WHERE pas.park_id = :park_id
                    AND pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                    AND (pas.park_appears_open = TRUE OR pas.rides_open > 0)
                ORDER BY pas.recorded_at
            """)

            snapshot_result = self.conn.execute(snapshot_query, {
                "park_id": park["park_id"],
                "start_utc": start_utc,
                "end_utc": now_utc
            })
            park_data = [dict(row._mapping) for row in snapshot_result]

            # Use the first park's labels as the shared labels
            if labels is None:
                labels = [row["label"] for row in park_data]

            # Convert data to strings for consistency with other chart responses
            data = [str(row["shame_score"]) if row["shame_score"] is not None else None for row in park_data]

            datasets.append({
                "label": park["park_name"],
                "data": data,
            })

        return {"labels": labels or [], "datasets": datasets}

    def _query_raw_snapshots(
        self,
        park_id: int,
        start_utc,
        end_utc,
        target_date: date,
    ) -> List[Dict[str, Any]]:
        """Get hourly shame scores from raw park_activity_snapshots.

        Slow path: Uses GROUP BY HOUR on raw snapshots (rollback path).
        READs stored shame_score from park_activity_snapshots.
        THE SINGLE SOURCE OF TRUTH - calculated during data collection.
        """
        # READ stored shame_score from park_activity_snapshots
        # THE SINGLE SOURCE OF TRUTH - calculated during data collection
        #
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        #
        # This makes charts robust against schedule data issues.
        # Include rides_closed (as rides_down) and avg_wait_time for chart display
        query = text("""
            SELECT
                HOUR(DATE_SUB(pas.recorded_at, INTERVAL 8 HOUR)) AS hour,
                ROUND(AVG(pas.shame_score), 1) AS shame_score,
                ROUND(AVG(pas.rides_closed), 0) AS rides_down,
                ROUND(AVG(pas.avg_wait_time), 1) AS avg_wait_time_minutes
            FROM park_activity_snapshots pas
            WHERE pas.park_id = :park_id
                AND pas.recorded_at >= :start_utc AND pas.recorded_at < :end_utc
                AND (pas.park_appears_open = TRUE OR pas.rides_open > 0)
                AND pas.shame_score IS NOT NULL
            GROUP BY HOUR(DATE_SUB(pas.recorded_at, INTERVAL 8 HOUR))
            HAVING COUNT(*) > 0  -- Only show hours with data
            ORDER BY hour
        """)

        result = self.conn.execute(query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result]

    def _query_hourly_tables(
        self,
        park_id: int,
        start_utc,
        end_utc,
        target_date: date,
    ) -> List[Dict[str, Any]]:
        """Get hourly shame scores from pre-aggregated park_hourly_stats table.

        Fast path: Uses pre-computed hourly aggregates instead of GROUP BY HOUR.
        Returns same format as _query_raw_snapshots for seamless switching.
        """
        # Query park_hourly_stats table for the time range
        # Include rides_down and avg_wait_time_minutes for chart display
        query = text("""
            SELECT
                HOUR(DATE_SUB(phs.hour_start_utc, INTERVAL 8 HOUR)) AS hour,
                phs.shame_score,
                phs.rides_down,
                phs.avg_wait_time_minutes
            FROM park_hourly_stats phs
            WHERE phs.park_id = :park_id
                AND phs.hour_start_utc >= :start_utc
                AND phs.hour_start_utc < :end_utc
                AND phs.park_was_open = TRUE
                AND phs.shame_score IS NOT NULL
            ORDER BY phs.hour_start_utc
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
