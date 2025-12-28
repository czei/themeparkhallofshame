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

from datetime import date, timedelta, datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, literal_column
from sqlalchemy.orm import Session

from database.schema import parks, park_daily_stats
from database.queries.builders import Filters
from utils.timezone import get_pacific_day_range_utc
from utils.sql_helpers import timestamp_match_condition

# ORM models for query conversion
from models import Park, Ride, ParkActivitySnapshot, RideStatusSnapshot


class ParkWaitTimeHistoryQuery:
    """
    Query handler for park average wait time time-series.
    """

    def __init__(self, session: Session):
        self.session = session

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
                "entity_id": park["park_id"],
                "location": park["location"],
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

        # Get top parks by average wait time today
        # Only include parks that appear OPEN
        top_parks_stmt = (
            select(
                Park.park_id,
                Park.name.label("park_name"),
                func.concat(Park.city, ', ', Park.state_province).label("location"),
                func.avg(ParkActivitySnapshot.avg_wait_time).label("overall_avg_wait")
            )
            .select_from(Park)
            .join(ParkActivitySnapshot, Park.park_id == ParkActivitySnapshot.park_id)
            .where(
                and_(
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < end_utc,
                    ParkActivitySnapshot.park_appears_open == True,
                    ParkActivitySnapshot.avg_wait_time.isnot(None),
                    ParkActivitySnapshot.avg_wait_time > 0,
                    Park.is_active == True
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
            .having(func.avg(ParkActivitySnapshot.avg_wait_time) > 0)
            .order_by(func.avg(ParkActivitySnapshot.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.session.execute(top_parks_stmt)
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
                "entity_id": park["park_id"],
                "location": park["location"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def get_live(
        self,
        filter_disney_universal: bool = False,
        limit: int = 4,
        minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Get live 5-minute average wait times for parks (last N minutes).
        """
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        # Build 10-minute labels aligned to :00, :10, :20, :30, :40, :50
        # Data is collected every 10 minutes at these marks
        # Round start_utc down to nearest 10-minute boundary
        start_minute = (start_utc.minute // 10) * 10
        aligned_start = start_utc.replace(minute=start_minute, second=0, microsecond=0)

        labels = []
        current = aligned_start
        while current <= now_utc:
            labels.append(current.strftime("%H:%M"))
            current += timedelta(minutes=10)

        # ORM timestamp matching condition: match at minute precision
        ts_match_cond = (
            func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
            func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
        )

        top_parks_stmt = (
            select(
                Park.park_id,
                Park.name.label("park_name"),
                func.avg(RideStatusSnapshot.wait_time).label("avg_wait")
            )
            .select_from(Park)
            .join(Ride, and_(Park.park_id == Ride.park_id, Ride.is_active == True, Ride.category == 'ATTRACTION'))
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(Park.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at <= now_utc,
                    Park.is_active == True,
                    ParkActivitySnapshot.park_appears_open == True,
                    RideStatusSnapshot.wait_time.isnot(None)
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
            .having(func.avg(RideStatusSnapshot.wait_time) > 0)
            .order_by(func.avg(RideStatusSnapshot.wait_time).desc())
            .limit(limit)
        )

        result = self.session.execute(top_parks_stmt)
        parks_in_view = [dict(row._mapping) for row in result]

        if not parks_in_view:
            return {"labels": labels, "datasets": [], "granularity": "minutes"}

        datasets = []
        for park in parks_in_view:
            series_stmt = (
                select(
                    func.date_format(RideStatusSnapshot.recorded_at, '%H:%i').label("minute_label"),
                    func.avg(RideStatusSnapshot.wait_time).label("avg_wait")
                )
                .select_from(RideStatusSnapshot)
                .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
                .join(ParkActivitySnapshot, and_(Ride.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
                .where(
                    and_(
                        Ride.park_id == park["park_id"],
                        RideStatusSnapshot.recorded_at >= start_utc,
                        RideStatusSnapshot.recorded_at <= now_utc,
                        ParkActivitySnapshot.park_appears_open == True,
                        RideStatusSnapshot.wait_time.isnot(None)
                    )
                )
                .group_by(func.date_format(RideStatusSnapshot.recorded_at, '%H:%i'))
                .order_by(literal_column("minute_label"))
            )
            series_result = self.session.execute(series_stmt)
            points = {row.minute_label: float(row.avg_wait) for row in series_result}
            aligned = [points.get(label) for label in labels]
            datasets.append({
                "label": park["park_name"],
                "data": aligned,
            })

        return {"labels": labels, "datasets": datasets, "granularity": "minutes"}

    def _get_park_hourly_wait_data(
        self,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly average wait times for a specific park from live snapshots."""
        # Calculate Pacific hour: UTC - 8 hours
        pacific_time = ParkActivitySnapshot.recorded_at - timedelta(hours=8)
        hour_expr = func.hour(pacific_time)

        stmt = (
            select(
                hour_expr.label("hour"),
                func.round(func.avg(ParkActivitySnapshot.avg_wait_time), 0).label("avg_wait")
            )
            .where(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < end_utc,
                    ParkActivitySnapshot.park_appears_open == True,
                    ParkActivitySnapshot.avg_wait_time.isnot(None)
                )
            )
            .group_by(hour_expr)
            .order_by(literal_column("hour"))
        )

        result = self.session.execute(stmt)
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
                func.concat(parks.c.city, ', ', parks.c.state_province).label("location"),
                func.avg(park_daily_stats.c.avg_wait_time).label("overall_avg_wait"),
            )
            .select_from(
                parks.join(
                    park_daily_stats,
                    parks.c.park_id == park_daily_stats.c.park_id
                )
            )
            .where(and_(*conditions))
            .group_by(parks.c.park_id, parks.c.name, parks.c.city, parks.c.state_province)
            .having(func.avg(park_daily_stats.c.avg_wait_time) > 0)
            .order_by(func.avg(park_daily_stats.c.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.session.execute(stmt)
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

        result = self.session.execute(stmt)
        return [dict(row._mapping) for row in result]
