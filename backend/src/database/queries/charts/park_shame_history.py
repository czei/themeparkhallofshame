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

from sqlalchemy import select, func, and_, or_, case, literal_column, desc, null, text
from sqlalchemy.orm import Session

from database.schema import (
    parks,
    park_daily_stats,
)
from database.queries.builders import Filters, ParkWeightsCTE, WeightedDowntimeCTE
# NOTE: ShameScoreCalculator removed - now reading from pas.shame_score (THE SINGLE SOURCE OF TRUTH)
from utils.timezone import get_pacific_day_range_utc, get_today_range_to_now_utc
from utils.sql_helpers import ParkStatusSQL
from utils.metrics import USE_HOURLY_TABLES

# ORM models for query conversion
from models import (
    Park, Ride, RideClassification,
    ParkActivitySnapshot, ParkHourlyStats,
    RideDailyStats
)
from models.orm_stats import ParkHourlyStats as ParkHourlyStatsORM
from models.orm_schedule import ParkSchedule


class ParkShameHistoryQuery:
    """
    Query handler for park shame score time-series.

    Supports two query paths:
    - Fast path: Pre-aggregated park_hourly_stats table
    - Slow path: GROUP BY HOUR on raw park_activity_snapshots (rollback)
    """

    def __init__(self, session: Session, use_hourly_tables: bool = None):
        """
        Initialize query handler.

        Args:
            session: SQLAlchemy Session
            use_hourly_tables: If True, use park_hourly_stats (fast path).
                             If False, use GROUP BY HOUR on raw snapshots (rollback).
                             If None, uses global USE_HOURLY_TABLES flag (default).
        """
        self.session = session
        self.use_hourly_tables = use_hourly_tables if use_hourly_tables is not None else USE_HOURLY_TABLES

    def _get_schedule_for_date(self, park_id: int, target_date: date) -> Dict[str, Any]:
        """
        Get park operating schedule for a specific date.

        This is the SINGLE SOURCE OF TRUTH for determining when a park is open.
        Chart data should only include hours within this schedule.

        Args:
            park_id: The park ID
            target_date: The date to get schedule for

        Returns:
            Dict with 'opening_time' and 'closing_time' as datetime objects,
            or None if no schedule exists.
        """
        stmt = (
            select(
                ParkSchedule.opening_time,
                ParkSchedule.closing_time
            )
            .where(
                and_(
                    ParkSchedule.park_id == park_id,
                    ParkSchedule.schedule_date == target_date,
                    ParkSchedule.schedule_type == 'OPERATING',
                    ParkSchedule.opening_time.isnot(None),
                    ParkSchedule.closing_time.isnot(None)
                )
            )
            .order_by(ParkSchedule.opening_time)
            .limit(1)
        )

        result = self.session.execute(stmt)
        row = result.fetchone()

        if row:
            return {
                'opening_time': row.opening_time,
                'closing_time': row.closing_time
            }
        return None

    def _filter_by_schedule(
        self,
        hourly_data: List[Dict[str, Any]],
        schedule: Dict[str, Any],
        target_date: date
    ) -> List[Dict[str, Any]]:
        """
        Filter hourly data to only include hours within park schedule.

        Args:
            hourly_data: List of hourly data dicts with 'hour' key (Pacific hour 0-23)
            schedule: Dict with 'opening_time' and 'closing_time' (UTC datetimes)
            target_date: The date being queried

        Returns:
            Filtered list containing only hours within operating schedule.
        """
        if not schedule:
            return []

        opening_utc = schedule['opening_time']
        closing_utc = schedule['closing_time']

        # Convert UTC times to Pacific hours
        # Pacific is UTC-8 (ignoring DST for simplicity)
        opening_pacific_hour = (opening_utc.hour - 8) % 24

        # Calculate closing hour in Pacific time
        # Handle next-day closings (e.g., 6am UTC = 10pm Pacific previous day)
        closing_pacific_hour = (closing_utc.hour - 8) % 24

        # If closing is in next UTC day but still same Pacific day, adjust
        # Example: 6am UTC on Dec 26 = 10pm Pacific on Dec 25
        if closing_utc.date() > target_date:
            # The closing hour (in Pacific) is before midnight
            # e.g., 22:00 Pacific (10pm) for a 6am UTC closing
            # We need to include hours up to but not including closing_pacific_hour
            # For most parks (close before midnight), this is straightforward
            if closing_pacific_hour == 0:
                # Park closes at midnight Pacific (8am UTC next day)
                closing_pacific_hour = 24
            # Otherwise closing_pacific_hour is already correct (e.g., 22 for 10pm close)

        # Filter data to only include hours within schedule
        filtered = []
        for row in hourly_data:
            hour = row.get('hour')
            if hour is None:
                continue

            # Check if this hour is within operating hours
            if opening_pacific_hour <= hour < closing_pacific_hour:
                filtered.append(row)
            # Handle overnight parks (opening after closing time numerically, e.g., 22:00-02:00)
            elif opening_pacific_hour > closing_pacific_hour:
                if hour >= opening_pacific_hour or hour < closing_pacific_hour:
                    filtered.append(row)

        return filtered

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
                "entity_id": park["park_id"],
                "location": park["location"],
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

        # Get top parks with highest shame scores for the target date
        # Uses park_activity_snapshots (THE SINGLE SOURCE OF TRUTH) with stored shame_scores
        #
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        #
        # This makes charts robust against schedule data issues for historical dates.
        top_parks_stmt = (
            select(
                Park.park_id,
                Park.name.label("park_name"),
                func.concat(Park.city, ', ', Park.state_province).label("location"),
                func.avg(ParkActivitySnapshot.shame_score).label("avg_shame_score")
            )
            .select_from(Park)
            .join(ParkActivitySnapshot, Park.park_id == ParkActivitySnapshot.park_id)
            .where(
                and_(
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < end_utc,
                    Park.is_active == True,
                    or_(
                        ParkActivitySnapshot.park_appears_open == True,
                        ParkActivitySnapshot.rides_open > 0
                    ),
                    ParkActivitySnapshot.shame_score.isnot(None),
                    ParkActivitySnapshot.shame_score > 0
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            top_parks_stmt = top_parks_stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        top_parks_stmt = (
            top_parks_stmt
            .group_by(Park.park_id, Park.name, Park.city, Park.state_province)
            .having(func.count() > 0)
            .order_by(func.avg(ParkActivitySnapshot.shame_score).desc())
            .limit(limit)
        )

        result = self.session.execute(top_parks_stmt)
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
                "entity_id": park["park_id"],
                "location": park["location"],
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

        # Get park schedule for the target date (SINGLE SOURCE OF TRUTH)
        # This ensures we only show hours when the park was officially open
        schedule = self._get_schedule_for_date(park_id, target_date)

        # Choose query method based on use_hourly_tables parameter
        if self.use_hourly_tables:
            hourly_data = self._query_hourly_tables(park_id, start_utc, end_utc, target_date)
        else:
            hourly_data = self._query_raw_snapshots(park_id, start_utc, end_utc, target_date)

        # Filter hourly data by schedule (removes hours outside official operating times)
        # This is critical for fixing the "23 rides down at 3am" bug where park_was_open
        # or park_appears_open was incorrectly set due to fallback heuristics
        if schedule and not is_today:
            # Only filter historical data - TODAY data may not have complete schedule yet
            hourly_data = self._filter_by_schedule(hourly_data, schedule, target_date)

        # Build data by hour for all metrics
        # Convert Decimal to float for JSON serialization
        # Filter out rows with None hour values (can happen with raw data queries)
        shame_by_hour = {row["hour"]: row["shame_score"] for row in hourly_data if row["hour"] is not None}
        rides_down_by_hour = {row["hour"]: row.get("rides_down") for row in hourly_data if row["hour"] is not None}
        avg_wait_by_hour = {row["hour"]: row.get("avg_wait_time_minutes") for row in hourly_data if row["hour"] is not None}

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

        # Get top parks by recent downtime (last 60 minutes)
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        top_parks_stmt = (
            select(
                Park.park_id,
                Park.name.label("park_name")
            )
            .select_from(Park)
            .join(ParkActivitySnapshot, Park.park_id == ParkActivitySnapshot.park_id)
            .where(
                and_(
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < now_utc,
                    Park.is_active == True,
                    or_(
                        ParkActivitySnapshot.park_appears_open == True,
                        ParkActivitySnapshot.rides_open > 0
                    ),
                    ParkActivitySnapshot.shame_score > 0
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            top_parks_stmt = top_parks_stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        top_parks_stmt = (
            top_parks_stmt
            .group_by(Park.park_id, Park.name)
            .order_by(func.avg(ParkActivitySnapshot.shame_score).desc())
            .limit(limit)
        )

        result = self.session.execute(top_parks_stmt)
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
            #
            # Convert UTC timestamps to Pacific time for chart labels
            # CRITICAL: Use MySQL DATE_SUB instead of Python timedelta - SQLAlchemy cannot
            # translate Python timedelta subtraction on database columns to SQL properly
            pacific_time = func.date_sub(ParkActivitySnapshot.recorded_at, text("INTERVAL 8 HOUR"))
            snapshot_stmt = (
                select(
                    func.date_format(pacific_time, '%H:%i').label("label"),
                    ParkActivitySnapshot.shame_score
                )
                .where(
                    and_(
                        ParkActivitySnapshot.park_id == park["park_id"],
                        ParkActivitySnapshot.recorded_at >= start_utc,
                        ParkActivitySnapshot.recorded_at < now_utc,
                        or_(
                            ParkActivitySnapshot.park_appears_open == True,
                            ParkActivitySnapshot.rides_open > 0
                        )
                    )
                )
                .order_by(ParkActivitySnapshot.recorded_at)
            )

            snapshot_result = self.session.execute(snapshot_stmt)
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

        DEPRECATED: This fallback path returns SHAME SCORES (0-10 scale),
        while the fast path (_query_hourly_tables) returns DOWNTIME HOURS.
        This creates inconsistent chart behavior!

        CRITICAL: Set USE_HOURLY_TABLES=true in environment to use the fast path
        which returns actual downtime hours and matches the Problem Rides table.

        Slow path: Uses GROUP BY HOUR on raw snapshots (rollback path).
        READs stored shame_score from park_activity_snapshots.
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(
            "Using deprecated _query_raw_snapshots path for park %s. "
            "Chart will show SHAME SCORES (0-10) instead of DOWNTIME HOURS. "
            "Set USE_HOURLY_TABLES=true for consistent behavior.",
            park_id
        )

        # READ stored shame_score from park_activity_snapshots
        # WARNING: Returns shame_score (0-10 scale), NOT downtime hours!
        #
        # FALLBACK HEURISTIC: Include snapshots where EITHER:
        # 1. park_appears_open = TRUE (schedule-based detection), OR
        # 2. rides_open > 0 (rides are actually operating)
        #
        # This makes charts robust against schedule data issues.
        # Include rides_closed (as rides_down) and avg_wait_time for chart display

        # Calculate Pacific hour: UTC - 8 hours
        # CRITICAL: Use MySQL DATE_SUB instead of Python timedelta - SQLAlchemy cannot
        # translate Python timedelta subtraction on database columns to SQL properly
        pacific_time = func.date_sub(ParkActivitySnapshot.recorded_at, text("INTERVAL 8 HOUR"))
        hour_expr = func.hour(pacific_time)

        stmt = (
            select(
                hour_expr.label("hour"),
                func.round(func.avg(ParkActivitySnapshot.shame_score), 1).label("shame_score"),
                func.round(func.avg(ParkActivitySnapshot.rides_closed), 0).label("rides_down"),
                func.round(func.avg(ParkActivitySnapshot.avg_wait_time), 1).label("avg_wait_time_minutes")
            )
            .where(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < end_utc,
                    # CRITICAL: Only include data when park is officially open
                    # The rides_open > 0 fallback was including closed hours where
                    # a few rides show as "open" due to maintenance/test cycles.
                    # For Disney parks, this was causing 23 rides to show as "down"
                    # during closed hours, which is incorrect.
                    ParkActivitySnapshot.park_appears_open == True,
                    ParkActivitySnapshot.shame_score.isnot(None)
                )
            )
            .group_by(hour_expr)
            .having(func.count() > 0)  # Only show hours with data
            .order_by(literal_column("hour"))
        )

        result = self.session.execute(stmt)
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
        # CRITICAL FIX (2025-12-28): Use actual shame_score column (0-10 scale)
        # Previously used total_downtime_hours which caused chart/rankings mismatch.
        # Rankings shows 8.4 but chart was averaging ~2.3 (hours, not score!)

        # Calculate Pacific hour: UTC - 8 hours
        # CRITICAL: Use MySQL DATE_SUB instead of Python timedelta - SQLAlchemy cannot
        # translate Python timedelta subtraction on database columns to SQL properly
        pacific_time = func.date_sub(ParkHourlyStatsORM.hour_start_utc, text("INTERVAL 8 HOUR"))
        hour_expr = func.hour(pacific_time)

        stmt = (
            select(
                hour_expr.label("hour"),
                ParkHourlyStatsORM.shame_score,  # Actual shame score (0-10 scale)
                ParkHourlyStatsORM.rides_down,
                ParkHourlyStatsORM.avg_wait_time_minutes
            )
            .where(
                and_(
                    ParkHourlyStatsORM.park_id == park_id,
                    ParkHourlyStatsORM.hour_start_utc >= start_utc,
                    ParkHourlyStatsORM.hour_start_utc < end_utc,
                    ParkHourlyStatsORM.park_was_open == True,
                    ParkHourlyStatsORM.total_downtime_hours.isnot(None)
                )
            )
            .order_by(ParkHourlyStatsORM.hour_start_utc)
        )

        result = self.session.execute(stmt)
        return [dict(row._mapping) for row in result]

    def get_single_park_daily(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
    ) -> Dict[str, Any]:
        """
        Get daily shame score data for a single park over a date range.

        Used by park details modal for last_week/last_month periods to show
        a daily bar chart of shame scores.

        Args:
            park_id: The park ID to get data for
            start_date: Start date of the period
            end_date: End date of the period

        Returns:
            Chart.js compatible dict with daily labels and single dataset
        """
        # Calculate shame score per day from ride_daily_stats
        # shame_score = weighted_downtime_hours / park_weight
        # weighted_downtime = SUM(downtime_minutes * tier_weight) / 60

        # CTE 1: park_weights - total tier weight for the park
        park_weights_cte = (
            select(
                Ride.park_id,
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label("total_park_weight")
            )
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION'
                )
            )
            .group_by(Ride.park_id)
        ).cte("park_weights")

        # CTE 2: daily_weighted_downtime - daily stats with weighted downtime
        daily_weighted_cte = (
            select(
                RideDailyStats.stat_date,
                func.round(
                    func.sum(RideDailyStats.downtime_minutes / 60.0 * func.coalesce(RideClassification.tier_weight, 2)),
                    2
                ).label("weighted_downtime_hours"),
                func.round(func.sum(RideDailyStats.downtime_minutes / 60.0), 2).label("total_downtime_hours"),
                func.count(
                    func.distinct(
                        case(
                            (RideDailyStats.downtime_minutes > 0, RideDailyStats.ride_id),
                            else_=null()
                        )
                    )
                ).label("rides_with_downtime")
            )
            .select_from(RideDailyStats)
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION',
                    RideDailyStats.stat_date >= start_date,
                    RideDailyStats.stat_date <= end_date
                )
            )
            .group_by(RideDailyStats.stat_date)
        ).cte("daily_weighted_downtime")

        # Main query: join CTEs and calculate shame score
        stmt = (
            select(
                daily_weighted_cte.c.stat_date,
                func.round(
                    daily_weighted_cte.c.weighted_downtime_hours / park_weights_cte.c.total_park_weight,
                    1
                ).label("shame_score"),
                daily_weighted_cte.c.total_downtime_hours.label("downtime_hours"),
                daily_weighted_cte.c.rides_with_downtime.label("rides_down")
            )
            .select_from(daily_weighted_cte)
            .join(park_weights_cte, literal_column("1") == literal_column("1"))  # CROSS JOIN
            .order_by(daily_weighted_cte.c.stat_date)
        )

        result = self.session.execute(stmt)
        rows = [dict(row._mapping) for row in result]

        # Build labels and data arrays
        labels = [row['stat_date'].strftime("%b %d") for row in rows]
        data = [float(row['shame_score']) if row['shame_score'] is not None else 0.0 for row in rows]
        downtime_data = [float(row['downtime_hours']) if row['downtime_hours'] is not None else 0.0 for row in rows]
        rides_down_data = [int(row['rides_down']) if row['rides_down'] is not None else 0 for row in rows]

        # Calculate average FROM the chart data points
        non_null_data = [v for v in data if v is not None and v > 0]
        avg_score = round(sum(non_null_data) / len(non_null_data), 1) if non_null_data else 0.0

        return {
            "labels": labels,
            "data": data,
            "downtime_hours": downtime_data,
            "rides_down": rides_down_data,
            "average": avg_score,
            "granularity": "daily"
        }

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
                func.concat(parks.c.city, ', ', parks.c.state_province).label("location"),
            )
            .select_from(
                parks.join(park_daily_stats, parks.c.park_id == park_daily_stats.c.park_id)
                .outerjoin(pw, parks.c.park_id == pw.c.park_id)
                .outerjoin(wd, parks.c.park_id == wd.c.park_id)
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name, parks.c.city, parks.c.state_province, pw.c.total_park_weight, wd.c.total_weighted_downtime_hours)
            .having(func.sum(park_daily_stats.c.total_downtime_hours) > 0)
            .order_by(
                (wd.c.total_weighted_downtime_hours / func.nullif(pw.c.total_park_weight, 0)).desc()
            )
            .limit(limit)
        )

        result = self.session.execute(stmt)
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

        result = self.session.execute(stmt)
        return [dict(row._mapping) for row in result]
