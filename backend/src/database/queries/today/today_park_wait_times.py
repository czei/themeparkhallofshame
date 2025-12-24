"""
Today Park Wait Time Rankings Query (Cumulative)
=================================================

Endpoint: GET /api/parks/waittimes?period=today
UI Location: Parks tab → Wait Times Rankings (today - cumulative)

Returns parks ranked by CUMULATIVE wait times from midnight Pacific to now.

CRITICAL DIFFERENCE FROM 7-DAY/30-DAY:
- 7-DAY/30-DAY: Uses pre-aggregated park_daily_stats table
- TODAY: Queries ride_status_snapshots directly for real-time accuracy

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_status_snapshots (real-time wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import select, func, and_, or_, case, literal
from sqlalchemy.orm import Session

from src.models import Park, Ride, RideStatusSnapshot, ParkActivitySnapshot
from src.utils.timezone import get_today_range_to_now_utc
from src.utils.query_helpers import QueryClassBase
from utils.metrics import USE_HOURLY_TABLES
from database.repositories.stats_repository import StatsRepository


class TodayParkWaitTimesQuery(QueryClassBase):
    """
    Query handler for today's CUMULATIVE park wait time rankings.

    Unlike weekly/monthly queries which use park_daily_stats,
    this aggregates ALL wait times from ride_status_snapshots
    since midnight Pacific to now.
    """

    def __init__(self, session: Session):
        super().__init__(session)
        self.stats_repo = StatsRepository(session)

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative park wait time rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by average wait time (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # HYBRID QUERY: Use hourly tables when enabled
        if USE_HOURLY_TABLES:
            current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

            # Get hourly stats for complete hours
            hourly_stats = self.stats_repo.get_hourly_stats(
                start_hour=start_utc,
                end_hour=now_utc
            )

            # Group by park and calculate averages
            park_data = {}
            for row in hourly_stats:
                park_id = row['park_id']
                if park_id not in park_data:
                    park_data[park_id] = {'wait_times': [], 'snapshots': 0}

                if row['avg_wait_time_minutes']:
                    # Weight by snapshot count
                    for _ in range(row['snapshot_count']):
                        park_data[park_id]['wait_times'].append(float(row['avg_wait_time_minutes']))
                    park_data[park_id]['snapshots'] += row['snapshot_count']

            # Calculate final averages and get park details
            if not park_data:
                return []

            park_ids = list(park_data.keys())

            # Build query for park details
            stmt = select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                func.concat(Park.city, ', ', Park.state_province).label('location'),
                self._park_is_open_subquery().label('park_is_open')
            ).where(
                and_(
                    Park.park_id.in_(park_ids),
                    Park.is_active == True
                )
            )

            if filter_disney_universal:
                stmt = stmt.where(
                    or_(
                        Park.brand == 'Disney',
                        Park.brand == 'Universal'
                    )
                )

            result = self.execute_and_fetchall(stmt)

            rankings = []
            for row in result:
                data = park_data[row['park_id']]
                wait_times = data['wait_times']

                if wait_times:
                    avg_wait = round(sum(wait_times) / len(wait_times), 1)
                    peak_wait = round(max(wait_times), 1)

                    rankings.append({
                        'park_id': row['park_id'],
                        'queue_times_id': row['queue_times_id'],
                        'park_name': row['park_name'],
                        'location': row['location'],
                        'avg_wait_minutes': avg_wait,
                        'peak_wait_minutes': peak_wait,
                        'rides_reporting': None,  # TODO: Get from hourly stats
                        'park_is_open': row['park_is_open']
                    })

            # Sort and limit
            rankings.sort(key=lambda x: x['avg_wait_minutes'] or 0, reverse=True)
            return rankings[:limit]

        # FALLBACK: Use original query on raw snapshots

        # Build the main query using ORM
        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                func.concat(Park.city, ', ', Park.state_province).label('location'),

                # Average wait time across all rides (only when park is open and wait > 0)
                # IMPORTANT: Use avg_wait_minutes (not avg_wait_time) for frontend compatibility
                func.round(
                    func.avg(
                        case(
                            (
                                and_(
                                    ParkActivitySnapshot.park_appears_open == True,
                                    RideStatusSnapshot.wait_time > 0
                                ),
                                RideStatusSnapshot.wait_time
                            ),
                            else_=None
                        )
                    ),
                    1
                ).label('avg_wait_minutes'),

                # Peak wait time today
                # IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                func.max(
                    case(
                        (ParkActivitySnapshot.park_appears_open == True, RideStatusSnapshot.wait_time),
                        else_=None
                    )
                ).label('peak_wait_minutes'),

                # Count of rides with wait time data
                # IMPORTANT: Use rides_reporting (not rides_with_waits) for frontend compatibility
                func.count(func.distinct(
                    case(
                        (RideStatusSnapshot.wait_time > 0, Ride.ride_id),
                        else_=None
                    )
                )).label('rides_reporting'),

                # Park operating status (current)
                self._park_is_open_subquery().label('park_is_open')
            )
            .select_from(Park)
            .join(Ride, and_(
                Park.park_id == Ride.park_id,
                Ride.is_active == True,
                Ride.category == 'ATTRACTION'
            ))
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(ParkActivitySnapshot, and_(
                Park.park_id == ParkActivitySnapshot.park_id,
                ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
            ))
            .where(
                and_(
                    RideStatusSnapshot.recorded_at >= start_utc,
                    RideStatusSnapshot.recorded_at < now_utc,
                    Park.is_active == True
                )
            )
            .group_by(Park.park_id, Park.name, Park.city, Park.state_province)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(
                or_(
                    Park.brand == 'Disney',
                    Park.brand == 'Universal'
                )
            )

        # Define the avg_wait_minutes expression for reuse
        avg_wait_expr = func.round(
            func.avg(
                case(
                    (
                        and_(
                            ParkActivitySnapshot.park_appears_open == True,
                            RideStatusSnapshot.wait_time > 0
                        ),
                        RideStatusSnapshot.wait_time
                    ),
                    else_=None
                )
            ),
            1
        )

        # Add HAVING clause for non-null avg_wait_minutes
        stmt = stmt.having(avg_wait_expr.isnot(None))

        # Order by avg_wait_minutes descending and limit
        stmt = stmt.order_by(avg_wait_expr.desc()).limit(limit)

        return self.execute_and_fetchall(stmt)

    def _park_is_open_subquery(self):
        """
        Subquery to determine if a park is currently open.

        Returns:
            SQLAlchemy scalar subquery that returns TRUE/FALSE
        """
        from datetime import timedelta
        from src.utils.timezone import get_current_utc

        now_utc = get_current_utc()
        lookback_minutes = 15  # Consider park open if there was activity in last 15 minutes
        lookback_time = now_utc - timedelta(minutes=lookback_minutes)

        return (
            select(
                case(
                    (
                        func.count(ParkActivitySnapshot.snapshot_id) > 0,
                        True
                    ),
                    else_=False
                )
            )
            .select_from(ParkActivitySnapshot)
            .where(
                and_(
                    ParkActivitySnapshot.park_id == Park.park_id,
                    ParkActivitySnapshot.park_appears_open == True,
                    ParkActivitySnapshot.recorded_at >= lookback_time
                )
            )
            .scalar_subquery()
        )
