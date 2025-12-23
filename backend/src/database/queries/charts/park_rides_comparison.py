"""
Park Rides Comparison Charts
============================

Endpoint: GET /api/parks/<id>/rides/charts?period={today|yesterday|last_week|last_month}&type={downtime|wait_times}
UI Location: Park Details Modal → Ride Comparison Charts (toggled)

Returns time-series data for ALL rides in a specific park for Chart.js visualization.
Supports two chart types:
- downtime: Hours of downtime per ride over time
- wait_times: Average wait time per ride over time

Database Tables:
- rides (ride metadata)
- ride_daily_stats (daily aggregated data)
- ride_status_snapshots (live/today hourly data)

Output Format (Chart.js compatible):
{
    "labels": ["9:00", "10:00", ...] or ["Dec 01", "Dec 02", ...],
    "datasets": [
        {"label": "Space Mountain", "ride_id": 123, "tier": 1, "data": [1.5, 0.5, ...]},
        {"label": "Test Track", "ride_id": 456, "tier": 2, "data": [2.0, 1.8, ...]}
    ],
    "chart_type": "downtime" | "wait_times",
    "granularity": "hourly" | "daily"
}
"""

from datetime import date, timedelta
from typing import Dict, Any, List

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_pacific_day_range_utc


class ParkRidesComparisonQuery:
    """
    Query handler for park-specific ride comparison time-series.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_downtime_daily(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:
        """
        Get daily downtime for all rides in a park.

        Used for: last_week, last_month periods.

        Returns Chart.js compatible dict with one dataset per ride.
        """
        # Generate date labels
        labels = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime("%b %d"))
            current += timedelta(days=1)

        # Get all rides that operated in this park during the period
        rides_query = text("""
            SELECT DISTINCT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rds.stat_date >= :start_date
                AND rds.stat_date <= :end_date
                AND (rds.uptime_minutes > 0 OR rds.downtime_minutes > 0)
            ORDER BY tier ASC, r.name ASC
        """)

        result = self.conn.execute(rides_query, {
            "park_id": park_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rides = [dict(row._mapping) for row in result]

        if not rides:
            return {
                "labels": labels,
                "datasets": [],
                "chart_type": "downtime",
                "granularity": "daily"
            }

        # Get daily downtime for each ride
        datasets = []
        for ride in rides:
            daily_data = self._get_ride_daily_downtime(
                ride["ride_id"], start_date, end_date
            )

            # Align data to labels
            data_by_date = {
                row["stat_date"].strftime("%b %d"): row["downtime_hours"]
                for row in daily_data
            }
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": ride["ride_name"],
                "ride_id": ride["ride_id"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {
            "labels": labels,
            "datasets": datasets,
            "chart_type": "downtime",
            "granularity": "daily"
        }

    def get_wait_times_daily(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:
        """
        Get daily average wait times for all rides in a park.

        Used for: last_week, last_month periods.
        """
        # Generate date labels
        labels = []
        current = start_date
        while current <= end_date:
            labels.append(current.strftime("%b %d"))
            current += timedelta(days=1)

        # Get all rides that operated in this park during the period
        rides_query = text("""
            SELECT DISTINCT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_daily_stats rds ON r.ride_id = rds.ride_id
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rds.stat_date >= :start_date
                AND rds.stat_date <= :end_date
                AND rds.avg_wait_time IS NOT NULL
            ORDER BY tier ASC, r.name ASC
        """)

        result = self.conn.execute(rides_query, {
            "park_id": park_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rides = [dict(row._mapping) for row in result]

        if not rides:
            return {
                "labels": labels,
                "datasets": [],
                "chart_type": "wait_times",
                "granularity": "daily"
            }

        # Get daily wait times for each ride
        datasets = []
        for ride in rides:
            daily_data = self._get_ride_daily_wait_times(
                ride["ride_id"], start_date, end_date
            )

            # Align data to labels
            data_by_date = {
                row["stat_date"].strftime("%b %d"): float(row["avg_wait_time"]) if row["avg_wait_time"] else None
                for row in daily_data
            }
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": ride["ride_name"],
                "ride_id": ride["ride_id"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {
            "labels": labels,
            "datasets": datasets,
            "chart_type": "wait_times",
            "granularity": "daily"
        }

    def get_downtime_hourly(
        self,
        park_id: int,
        target_date: date,
    ) -> Dict[str, Any]:
        """
        Get hourly downtime for all rides in a park.

        Used for: today, yesterday periods.
        Uses live snapshot data.
        """
        # Generate hourly labels (6am to 11pm = 18 hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get UTC time range for the target date in Pacific timezone
        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Get all rides that operated during this period
        rides_query = text("""
            SELECT DISTINCT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rss.recorded_at >= :start_utc
                AND rss.recorded_at < :end_utc
                AND pas.park_appears_open = TRUE
                AND (rss.status = 'OPERATING' OR (rss.status IS NULL AND rss.computed_is_open = 1))
            ORDER BY tier ASC, r.name ASC
        """)

        result = self.conn.execute(rides_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        rides = [dict(row._mapping) for row in result]

        if not rides:
            return {
                "labels": labels,
                "datasets": [],
                "chart_type": "downtime",
                "granularity": "hourly"
            }

        # Get hourly downtime for each ride
        datasets = []
        for ride in rides:
            hourly_data = self._get_ride_hourly_downtime(
                ride["ride_id"], park_id, start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["downtime_hours"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": ride["ride_name"],
                "ride_id": ride["ride_id"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {
            "labels": labels,
            "datasets": datasets,
            "chart_type": "downtime",
            "granularity": "hourly"
        }

    def get_wait_times_hourly(
        self,
        park_id: int,
        target_date: date,
    ) -> Dict[str, Any]:
        """
        Get hourly wait times for all rides in a park.

        Used for: today, yesterday periods.
        Uses live snapshot data.
        """
        # Generate hourly labels (6am to 11pm = 18 hours)
        labels = [f"{h}:00" for h in range(6, 24)]

        # Get UTC time range for the target date in Pacific timezone
        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        # Get all rides that had wait time data during this period
        rides_query = text("""
            SELECT DISTINCT
                r.ride_id,
                r.name AS ride_name,
                COALESCE(rc.tier, 2) AS tier
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
                AND rss.recorded_at >= :start_utc
                AND rss.recorded_at < :end_utc
                AND pas.park_appears_open = TRUE
                AND rss.wait_time IS NOT NULL
            ORDER BY tier ASC, r.name ASC
        """)

        result = self.conn.execute(rides_query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        rides = [dict(row._mapping) for row in result]

        if not rides:
            return {
                "labels": labels,
                "datasets": [],
                "chart_type": "wait_times",
                "granularity": "hourly"
            }

        # Get hourly wait times for each ride
        datasets = []
        for ride in rides:
            hourly_data = self._get_ride_hourly_wait_times(
                ride["ride_id"], park_id, start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["avg_wait_time"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": ride["ride_name"],
                "ride_id": ride["ride_id"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {
            "labels": labels,
            "datasets": datasets,
            "chart_type": "wait_times",
            "granularity": "hourly"
        }

    # =========================================================================
    # Private helper methods
    # =========================================================================

    def _get_ride_daily_downtime(
        self,
        ride_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily downtime hours for a specific ride."""
        query = text("""
            SELECT
                stat_date,
                ROUND(downtime_minutes / 60.0, 2) AS downtime_hours
            FROM ride_daily_stats
            WHERE ride_id = :ride_id
                AND stat_date >= :start_date
                AND stat_date <= :end_date
            ORDER BY stat_date
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "start_date": start_date,
            "end_date": end_date
        })
        return [dict(row._mapping) for row in result]

    def _get_ride_daily_wait_times(
        self,
        ride_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily average wait times for a specific ride."""
        query = text("""
            SELECT
                stat_date,
                avg_wait_time
            FROM ride_daily_stats
            WHERE ride_id = :ride_id
                AND stat_date >= :start_date
                AND stat_date <= :end_date
            ORDER BY stat_date
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "start_date": start_date,
            "end_date": end_date
        })
        return [dict(row._mapping) for row in result]

    def _get_ride_hourly_downtime(
        self,
        ride_id: int,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """
        Get hourly downtime for a specific ride using ORM queries.

        SINGLE SOURCE OF TRUTH: Uses HourlyAggregationQuery (same as all other hourly metrics).

        Logic (enforced in ORM):
        1. Only count hours where park_appears_open = TRUE (no fallback heuristic)
        2. Only count downtime AFTER the ride operated anywhere during Pacific day
        """
        from src.models.base import db_session
        from src.utils.query_helpers import HourlyAggregationQuery
        from src.utils.timezone import PACIFIC_TZ, UTC_TZ

        # Debug logging
        import logging
        logger = logging.getLogger('themepark_tracker')
        logger.info(f"ORM chart query: ride_id={ride_id}, period={start_utc} to {end_utc}")

        # Get hourly metrics using ORM
        metrics = HourlyAggregationQuery.ride_hour_range_metrics(
            session=db_session,
            ride_id=ride_id,
            start_utc=start_utc,
            end_utc=end_utc,
        )

        logger.info(f"ORM returned {len(metrics)} hourly metrics")

        # Convert to chart format (Pacific hour 0-23, downtime_hours)
        result = []
        total_downtime = 0
        for m in metrics:
            # Convert UTC hour_start to Pacific hour (0-23)
            # hour_start_utc is naive, so we treat it as UTC, then convert to Pacific
            utc_dt = m.hour_start_utc.replace(tzinfo=UTC_TZ)
            pacific_dt = utc_dt.astimezone(PACIFIC_TZ)
            pacific_hour = pacific_dt.hour

            result.append({
                "hour": pacific_hour,
                "downtime_hours": float(m.downtime_hours)  # Ensure float for JSON
            })
            total_downtime += m.downtime_hours

        logger.info(f"Ride {ride_id} total downtime: {total_downtime}h, ride_operated={metrics[0].ride_operated if metrics else 'N/A'}")
        return result

    def _get_ride_hourly_wait_times(
        self,
        ride_id: int,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly average wait times for a specific ride from live snapshots."""
        query = text("""
            SELECT
                HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour,
                ROUND(AVG(rss.wait_time), 0) AS avg_wait_time
            FROM ride_status_snapshots rss
            INNER JOIN park_activity_snapshots pas ON rss.recorded_at = pas.recorded_at
                AND pas.park_id = :park_id
            WHERE rss.ride_id = :ride_id
                AND rss.recorded_at >= :start_utc
                AND rss.recorded_at < :end_utc
                AND pas.park_appears_open = TRUE
                AND rss.wait_time IS NOT NULL
            GROUP BY HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR))
            ORDER BY hour
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc
        })
        return [dict(row._mapping) for row in result]
