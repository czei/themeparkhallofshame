"""
Ride Wait Time History Query
============================

Endpoint: GET /api/trends/chart-data?type=ridewaittimes&period=7days|30days|today
UI Location: Charts tab → Ride Wait Times chart

Returns time-series average wait time data for rides for Chart.js visualization.

For period=today: Returns hourly data points (6am-11pm)
For period=7days/30days: Returns daily data points

Database Tables:
- rides (ride metadata)
- parks (park metadata for labels)
- ride_daily_stats (daily aggregated data)
- ride_status_snapshots (for hourly data)

Output Format:
{
    "labels": ["Nov 23", "Nov 24", ...],
    "datasets": [
        {"label": "Space Mountain", "park": "Magic Kingdom", "data": [45, 52, ...]},
        {"label": "Test Track", "park": "EPCOT", "data": [32, 41, ...]}
    ]
}
"""

from datetime import date, timedelta, datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, literal_column
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats, ride_classifications
from database.queries.builders import Filters
from utils.timezone import get_pacific_day_range_utc

# ORM models for query conversion
from src.models import Park, Ride, ParkActivitySnapshot, RideStatusSnapshot, RideClassification


class RideWaitTimeHistoryQuery:
    """
    Query handler for ride wait time time-series.
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
        Get daily average wait time history for top rides.

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

        # Get top rides by average wait time for the period
        top_rides = self._get_top_rides_by_wait_time(
            start_date, end_date, filter_disney_universal, limit
        )

        if not top_rides:
            return {"labels": labels, "datasets": []}

        # Get daily data for each ride
        datasets = []
        for ride in top_rides:
            ride_data = self._get_ride_daily_wait_data(
                ride["ride_id"], start_date, end_date
            )

            # Align data to labels (fill None for missing dates)
            data_by_date = {
                row["stat_date"].strftime("%b %d"): row["avg_wait"]
                for row in ride_data
            }
            aligned_data = [data_by_date.get(label) for label in labels]

            datasets.append({
                "label": ride["ride_name"],
                "entity_id": ride["ride_id"],
                "park_name": ride["park_name"],
                "tier": ride["tier"],
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
        Get hourly average wait time data for TODAY.

        Uses live snapshot data (ride_status_snapshots) to show
        wait time progression throughout the day.

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

        # Get top rides by average wait time today
        # Only include rides from parks that appear OPEN
        top_rides_stmt = (
            select(
                Ride.ride_id,
                Park.park_id,
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                RideClassification.tier,
                func.avg(RideStatusSnapshot.wait_time).label("overall_avg_wait")
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(
                Park.park_id == ParkActivitySnapshot.park_id,
                RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
            ))
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
                    ParkActivitySnapshot.park_appears_open == True,
                    RideStatusSnapshot.wait_time.isnot(None),
                    RideStatusSnapshot.wait_time > 0,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION',
                    Park.is_active == True
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            top_rides_stmt = top_rides_stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        top_rides_stmt = (
            top_rides_stmt
            .group_by(Ride.ride_id, Park.park_id, Ride.name, Park.name, RideClassification.tier)
            .having(func.avg(RideStatusSnapshot.wait_time) > 0)
            .order_by(func.avg(RideStatusSnapshot.wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(top_rides_stmt)
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": labels, "datasets": []}

        # Get hourly data for each ride
        datasets = []
        for ride in top_rides:
            hourly_data = self._get_ride_hourly_wait_data(
                ride["ride_id"], ride["park_id"], start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["avg_wait"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": ride["ride_name"],
                "entity_id": ride["ride_id"],
                "park_name": ride["park_name"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def _get_ride_hourly_wait_data(
        self,
        ride_id: int,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly average wait times for a specific ride from live snapshots."""
        # Calculate Pacific hour: UTC - 8 hours
        pacific_time = func.date_sub(
            RideStatusSnapshot.recorded_at,
            literal_column("INTERVAL 8 HOUR")
        )
        hour_expr = func.hour(pacific_time)

        stmt = (
            select(
                hour_expr.label("hour"),
                func.round(func.avg(RideStatusSnapshot.wait_time), 0).label("avg_wait")
            )
            .select_from(RideStatusSnapshot)
            .join(ParkActivitySnapshot, and_(
                ParkActivitySnapshot.park_id == park_id,
                RideStatusSnapshot.recorded_at == ParkActivitySnapshot.recorded_at
            ))
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
                    ParkActivitySnapshot.park_appears_open == True,
                    RideStatusSnapshot.wait_time.isnot(None),
                    RideStatusSnapshot.wait_time > 0
                )
            )
            .group_by(hour_expr)
            .order_by(literal_column("hour"))
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def get_live(
        self,
        filter_disney_universal: bool = False,
        limit: int = 5,
        minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Get live 5-minute wait time data for rides (last N minutes).
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

        top_rides_stmt = (
            select(
                Ride.ride_id,
                Park.park_id,
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                func.avg(RideStatusSnapshot.wait_time).label("overall_avg_wait")
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(Park.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at <= now_utc,
                    ParkActivitySnapshot.park_appears_open == True,
                    RideStatusSnapshot.wait_time.isnot(None),
                    RideStatusSnapshot.wait_time > 0,
                    Ride.is_active == True,
                    Ride.category == 'ATTRACTION',
                    Park.is_active == True
                )
            )
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            top_rides_stmt = top_rides_stmt.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        top_rides_stmt = (
            top_rides_stmt
            .group_by(Ride.ride_id, Park.park_id, Ride.name, Park.name)
            .having(func.avg(RideStatusSnapshot.wait_time) > 0)
            .order_by(func.avg(RideStatusSnapshot.wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(top_rides_stmt)
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": labels, "datasets": [], "granularity": "minutes"}

        datasets = []
        for ride in top_rides:
            series_stmt = (
                select(
                    func.date_format(RideStatusSnapshot.recorded_at, '%H:%i').label("minute_label"),
                    func.avg(RideStatusSnapshot.wait_time).label("avg_wait")
                )
                .select_from(RideStatusSnapshot)
                .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
                .join(Park, Ride.park_id == Park.park_id)
                .join(ParkActivitySnapshot, and_(Park.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
                .where(
                    and_(
                        RideStatusSnapshot.ride_id == ride["ride_id"],
                        RideStatusSnapshot.recorded_at >= start_utc,
                        RideStatusSnapshot.recorded_at <= now_utc,
                        ParkActivitySnapshot.park_appears_open == True,
                        RideStatusSnapshot.wait_time.isnot(None),
                        RideStatusSnapshot.wait_time > 0
                    )
                )
                .group_by(func.date_format(RideStatusSnapshot.recorded_at, '%H:%i'))
                .order_by(literal_column("minute_label"))
            )
            series_result = self.conn.execute(series_stmt)
            points = {row.minute_label: float(row.avg_wait) for row in series_result}
            aligned = [points.get(label) for label in labels]
            datasets.append({
                "label": ride["ride_name"],
                "park": ride["park_name"],
                "data": aligned,
            })

        return {"labels": labels, "datasets": datasets, "granularity": "minutes"}

    def _get_top_rides_by_wait_time(
        self,
        start_date: date,
        end_date: date,
        filter_disney_universal: bool,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Get top rides by average wait time for the period."""
        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
            ride_daily_stats.c.stat_date >= start_date,
            ride_daily_stats.c.stat_date <= end_date,
            ride_daily_stats.c.avg_wait_time.isnot(None),
            ride_daily_stats.c.avg_wait_time > 0,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        stmt = (
            select(
                rides.c.ride_id,
                rides.c.name.label("ride_name"),
                parks.c.name.label("park_name"),
                ride_classifications.c.tier,
                func.avg(ride_daily_stats.c.avg_wait_time).label("overall_avg_wait"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id)
                .outerjoin(ride_classifications, rides.c.ride_id == ride_classifications.c.ride_id)
            )
            .where(and_(*conditions))
            .group_by(rides.c.ride_id, rides.c.name, parks.c.name, ride_classifications.c.tier)
            .having(func.avg(ride_daily_stats.c.avg_wait_time) > 0)
            .order_by(func.avg(ride_daily_stats.c.avg_wait_time).desc())
            .limit(limit)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]

    def _get_ride_daily_wait_data(
        self,
        ride_id: int,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """Get daily average wait times for a specific ride."""
        stmt = (
            select(
                ride_daily_stats.c.stat_date,
                func.round(ride_daily_stats.c.avg_wait_time, 0).label("avg_wait"),
            )
            .where(
                and_(
                    ride_daily_stats.c.ride_id == ride_id,
                    ride_daily_stats.c.stat_date >= start_date,
                    ride_daily_stats.c.stat_date <= end_date,
                    ride_daily_stats.c.avg_wait_time.isnot(None),
                )
            )
            .order_by(ride_daily_stats.c.stat_date)
        )

        result = self.conn.execute(stmt)
        return [dict(row._mapping) for row in result]
