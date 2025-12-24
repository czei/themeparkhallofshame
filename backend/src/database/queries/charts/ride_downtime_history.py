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

from datetime import date, timedelta, datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, case, literal_column
from sqlalchemy.engine import Connection

from database.schema import parks, rides, ride_daily_stats, ride_classifications
from database.queries.builders import Filters
from utils.timezone import get_pacific_day_range_utc

# ORM models for query conversion
from src.models import Park, Ride, ParkActivitySnapshot, RideStatusSnapshot, RideClassification


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

        # ORM condition for ride being down
        is_down_cond = or_(
            RideStatusSnapshot.status == 'DOWN',
            and_(
                RideStatusSnapshot.status.is_(None),
                RideStatusSnapshot.computed_is_open == 0
            )
        )

        # Get top rides with most downtime today
        # Only include rides from parks that appear OPEN (excludes seasonal closures)
        top_rides_stmt = (
            select(
                Ride.ride_id,
                Park.park_id,
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                RideClassification.tier,
                (func.sum(
                    case((is_down_cond, 5), else_=0)
                ) / 60.0).label("total_downtime_hours")
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
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
            .having(func.sum(case((is_down_cond, 5), else_=0)) / 60.0 > 0)
            .order_by((func.sum(case((is_down_cond, 5), else_=0)) / 60.0).desc())
            .limit(limit)
        )

        result = self.conn.execute(top_rides_stmt)
        top_rides = [dict(row._mapping) for row in result]

        if not top_rides:
            return {"labels": labels, "datasets": []}

        # Get hourly data for each ride
        datasets = []
        for ride in top_rides:
            hourly_data = self._get_ride_hourly_data(
                ride["ride_id"], ride["park_id"], start_utc, end_utc
            )

            # Align data to labels (6am to 11pm)
            data_by_hour = {row["hour"]: row["downtime_hours"] for row in hourly_data}
            aligned_data = [data_by_hour.get(h) for h in range(6, 24)]

            datasets.append({
                "label": ride["ride_name"],
                "entity_id": ride["ride_id"],
                "park_name": ride["park_name"],
                "tier": ride["tier"],
                "data": aligned_data,
            })

        return {"labels": labels, "datasets": datasets}

    def get_live(
        self,
        filter_disney_universal: bool = False,
        limit: int = 5,
        minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Get live 5-minute downtime data for rides (last N minutes).
        """
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        labels = []
        current = start_utc
        while current <= now_utc:
            labels.append(current.strftime("%H:%M"))
            current += timedelta(minutes=5)

        # ORM timestamp matching condition: match at minute precision
        ts_match_cond = (
            func.date_format(ParkActivitySnapshot.recorded_at, '%Y-%m-%d %H:%i') ==
            func.date_format(RideStatusSnapshot.recorded_at, '%Y-%m-%d %H:%i')
        )

        # ORM condition for ride being down (with park type awareness for Disney/Universal)
        # For Disney/Universal: only DOWN status counts as down
        # For others: CLOSED also counts as down
        is_down_cond = or_(
            RideStatusSnapshot.status == 'DOWN',
            and_(
                RideStatusSnapshot.status.is_(None),
                RideStatusSnapshot.computed_is_open == 0
            ),
            # For non-Disney/Universal parks, CLOSED also counts
            and_(
                RideStatusSnapshot.status == 'CLOSED',
                Park.is_disney == False,
                Park.is_universal == False
            )
        )

        top_rides_stmt = (
            select(
                Ride.ride_id,
                Park.park_id,
                Ride.name.label("ride_name"),
                Park.name.label("park_name"),
                (func.sum(
                    case(
                        (and_(is_down_cond, ParkActivitySnapshot.park_appears_open == True), 5),
                        else_=0
                    )
                ) / 60.0).label("total_downtime_hours")
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(Park.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at <= now_utc,
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

        downtime_expr = func.sum(
            case(
                (and_(is_down_cond, ParkActivitySnapshot.park_appears_open == True), 5),
                else_=0
            )
        ) / 60.0

        top_rides_stmt = (
            top_rides_stmt
            .group_by(Ride.ride_id, Park.park_id, Ride.name, Park.name)
            .having(downtime_expr > 0)
            .order_by(downtime_expr.desc())
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
                    (func.sum(
                        case(
                            (and_(is_down_cond, ParkActivitySnapshot.park_appears_open == True), 5),
                            else_=0
                        )
                    ) / 60.0).label("downtime_hours")
                )
                .select_from(RideStatusSnapshot)
                .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
                .join(Park, Ride.park_id == Park.park_id)
                .join(ParkActivitySnapshot, and_(Park.park_id == ParkActivitySnapshot.park_id, ts_match_cond))
                .where(
                    and_(
                        RideStatusSnapshot.ride_id == ride["ride_id"],
                        RideStatusSnapshot.recorded_at >= start_utc,
                        RideStatusSnapshot.recorded_at <= now_utc
                    )
                )
                .group_by(func.date_format(RideStatusSnapshot.recorded_at, '%H:%i'))
                .order_by(literal_column("minute_label"))
            )
            series_result = self.conn.execute(series_stmt)
            points = {row.minute_label: float(row.downtime_hours) for row in series_result}
            aligned = [points.get(label) for label in labels]
            datasets.append({
                "label": ride["ride_name"],
                "park": ride["park_name"],
                "data": aligned,
            })

        return {"labels": labels, "datasets": datasets, "granularity": "minutes"}

    def _get_ride_hourly_data(
        self,
        ride_id: int,
        park_id: int,
        start_utc,
        end_utc,
    ) -> List[Dict[str, Any]]:
        """Get hourly downtime for a specific ride from live snapshots.

        Logic:
        1. Only count hours where park_appears_open = TRUE
        2. Only count downtime AFTER the ride first operated today
        """
        # Calculate Pacific hour: UTC - 8 hours
        pacific_hour = func.hour(func.date_sub(
            RideStatusSnapshot.recorded_at,
            literal_column("INTERVAL 8 HOUR")
        ))
        pas_pacific_hour = func.hour(func.date_sub(
            ParkActivitySnapshot.recorded_at,
            literal_column("INTERVAL 8 HOUR")
        ))

        # CTE 1: ride_first_operating - Find when ride first operated today
        ride_first_op_cte = (
            select(
                func.min(RideStatusSnapshot.recorded_at).label("first_op_time")
            )
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
                    or_(
                        RideStatusSnapshot.status == 'OPERATING',
                        and_(
                            RideStatusSnapshot.status.is_(None),
                            RideStatusSnapshot.computed_is_open == 1
                        )
                    )
                )
            )
        ).cte("ride_first_operating")

        # CTE 2: park_hourly_open - Check if park was open during each hour
        park_hourly_cte = (
            select(
                pas_pacific_hour.label("hour"),
                func.max(ParkActivitySnapshot.park_appears_open).label("park_open")
            )
            .where(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < end_utc
                )
            )
            .group_by(pas_pacific_hour)
        ).cte("park_hourly_open")

        # ORM condition for ride being down
        is_down_cond = or_(
            RideStatusSnapshot.status == 'DOWN',
            and_(
                RideStatusSnapshot.status.is_(None),
                RideStatusSnapshot.computed_is_open == 0
            )
        )

        # Main query
        stmt = (
            select(
                pacific_hour.label("hour"),
                func.round(
                    func.sum(
                        case(
                            (and_(
                                park_hourly_cte.c.park_open == 1,
                                ride_first_op_cte.c.first_op_time.isnot(None),
                                RideStatusSnapshot.recorded_at >= ride_first_op_cte.c.first_op_time,
                                is_down_cond
                            ), 5),
                            else_=0
                        )
                    ) / 60.0,
                    2
                ).label("downtime_hours")
            )
            .select_from(RideStatusSnapshot)
            .join(ride_first_op_cte, literal_column("1") == literal_column("1"))  # CROSS JOIN
            .outerjoin(park_hourly_cte, pacific_hour == park_hourly_cte.c.hour)
            .where(
                and_(
                    RideStatusSnapshot.ride_id == ride_id,
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < end_utc,
                    park_hourly_cte.c.park_open == 1,
                    ride_first_op_cte.c.first_op_time.isnot(None),
                    RideStatusSnapshot.recorded_at >= ride_first_op_cte.c.first_op_time
                )
            )
            .group_by(pacific_hour)
            .order_by(literal_column("hour"))
        )

        result = self.conn.execute(stmt)
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
                ride_classifications.c.tier,
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(ride_daily_stats, rides.c.ride_id == ride_daily_stats.c.ride_id)
                .outerjoin(ride_classifications, rides.c.ride_id == ride_classifications.c.ride_id)
            )
            .where(and_(*conditions))
            .group_by(rides.c.ride_id, rides.c.name, parks.c.name, ride_classifications.c.tier)
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
