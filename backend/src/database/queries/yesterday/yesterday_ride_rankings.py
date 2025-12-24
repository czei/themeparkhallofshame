"""
Yesterday Ride Rankings Query (Cumulative)
==========================================

Endpoint: GET /api/rides/downtime?period=yesterday
UI Location: Rides tab → Downtime Rankings (yesterday)

Returns rides ranked by CUMULATIVE downtime for the full previous day.

KEY DIFFERENCES FROM TODAY:
- TODAY: midnight Pacific to NOW (partial, live-updating)
- YESTERDAY: full previous Pacific day (complete, immutable)

Because YESTERDAY is immutable, responses can be cached for 24 hours.

Database Tables:
- rides (ride metadata)
- parks (park metadata)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- ORM Helpers: utils/query_helpers.py
"""

from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import select, func, case, and_, or_, literal_column, literal
from sqlalchemy.orm import Session, aliased

from src.models.orm_ride import Ride
from src.models.orm_park import Park
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.utils.timezone import get_yesterday_range_utc
from src.utils.metrics import SNAPSHOT_INTERVAL_MINUTES
from src.utils.query_helpers import QueryClassBase, TimeIntervalHelper


class YesterdayRideRankingsQuery(QueryClassBase):
    """
    Query handler for yesterday's CUMULATIVE ride rankings.

    Aggregates ALL downtime for the full previous Pacific day.
    Unlike TODAY, this data is immutable and highly cacheable.

    Uses SNAPSHOT_INTERVAL_MINUTES from utils.metrics (10 minutes).
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative ride rankings for the full previous Pacific day.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of rides ranked by cumulative downtime hours (descending)
        """
        # Get time range for yesterday (full previous day)
        start_utc, end_utc, label = get_yesterday_range_utc()

        # Aliases for clarity
        RSS = aliased(RideStatusSnapshot)
        PAS = aliased(ParkActivitySnapshot)

        # === CTE: Rides that operated yesterday ===
        # A ride "operated" if it had status='OPERATING' or computed_is_open=TRUE
        # while park was open (park_appears_open=TRUE)
        rides_that_operated = (
            select(Ride.ride_id, Ride.park_id)
            .join(RSS, Ride.ride_id == RSS.ride_id)
            .join(
                PAS,
                and_(
                    Ride.park_id == PAS.park_id,
                    # Minute-level timestamp matching (1-2 second drift between tables)
                    func.date_format(PAS.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(RSS.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .join(Park, Ride.park_id == Park.park_id)
            .where(RSS.recorded_at >= start_utc)
            .where(RSS.recorded_at < end_utc)
            .where(
                or_(
                    RSS.status == 'OPERATING',
                    and_(RSS.status.is_(None), RSS.computed_is_open == True)
                )
            )
            .where(PAS.park_appears_open == True)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            rides_that_operated = rides_that_operated.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        rides_that_operated = rides_that_operated.distinct().cte('rides_that_operated')

        # === CTE: Operating snapshots (total snapshots while park was open) ===
        operating_snapshots = (
            select(
                RSS.ride_id,
                func.count().label('total_operating_snapshots')
            )
            .join(PAS, and_(
                func.date_format(PAS.recorded_at, '%Y-%m-%d %H:%i') ==
                func.date_format(RSS.recorded_at, '%Y-%m-%d %H:%i')
            ))
            .where(RSS.recorded_at >= start_utc)
            .where(RSS.recorded_at < end_utc)
            .where(PAS.park_appears_open == True)
            .group_by(RSS.ride_id)
        ).cte('operating_snapshots')

        # === Park-type aware is_down condition ===
        # Disney/Universal/Dollywood: Only count status='DOWN' (not CLOSED)
        # Other parks: Count DOWN, CLOSED, or computed_is_open=FALSE
        parks_with_down_status = or_(
            Park.is_disney == True,
            Park.is_universal == True,
            Park.name == 'Dollywood'
        )

        is_down_condition = case(
            (
                parks_with_down_status,
                RSS.status == 'DOWN'
            ),
            else_=or_(
                RSS.status.in_(['DOWN', 'CLOSED']),
                and_(RSS.status.is_(None), RSS.computed_is_open == False)
            )
        )

        # === Current status subqueries (for display - may differ from yesterday) ===
        # Subquery for current ride status
        rss_current = aliased(RideStatusSnapshot)
        current_status_subquery = (
            select(
                func.coalesce(
                    rss_current.status,
                    case(
                        (rss_current.computed_is_open == True, literal('OPERATING')),
                        else_=literal('DOWN')
                    )
                )
            )
            .where(rss_current.ride_id == Ride.ride_id)
            .where(rss_current.recorded_at >= TimeIntervalHelper.hours_ago(2))
            .order_by(rss_current.recorded_at.desc())
            .limit(1)
            .correlate(Ride)
            .scalar_subquery()
            .label('current_status')
        )

        # Subquery for current ride is_open boolean
        current_is_open_subquery = (
            select(
                case(
                    (
                        func.coalesce(
                            rss_current.status,
                            case(
                                (rss_current.computed_is_open == True, literal('OPERATING')),
                                else_=literal('DOWN')
                            )
                        ) == 'OPERATING',
                        literal(True)
                    ),
                    else_=literal(False)
                )
            )
            .where(rss_current.ride_id == Ride.ride_id)
            .where(rss_current.recorded_at >= TimeIntervalHelper.hours_ago(2))
            .order_by(rss_current.recorded_at.desc())
            .limit(1)
            .correlate(Ride)
            .scalar_subquery()
            .label('current_is_open')
        )

        # Subquery for current park is_open boolean
        pas_current = aliased(ParkActivitySnapshot)
        park_is_open_subquery = (
            select(pas_current.park_appears_open)
            .where(pas_current.park_id == Park.park_id)
            .where(pas_current.recorded_at >= TimeIntervalHelper.hours_ago(2))
            .order_by(pas_current.recorded_at.desc())
            .limit(1)
            .correlate(Park)
            .scalar_subquery()
            .label('park_is_open')
        )

        # === Main query ===
        query = (
            select(
                Ride.ride_id,
                Ride.queue_times_id,
                Park.queue_times_id.label('park_queue_times_id'),
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                Park.park_id,
                (Park.city + ', ' + Park.state_province).label('location'),
                Ride.tier,

                # CUMULATIVE downtime hours (all downtime yesterday)
                func.round(
                    func.sum(
                        case(
                            (
                                and_(
                                    is_down_condition,
                                    PAS.park_appears_open == True,
                                    rides_that_operated.c.ride_id.isnot(None)
                                ),
                                literal(SNAPSHOT_INTERVAL_MINUTES / 60.0)
                            ),
                            else_=literal(0)
                        )
                    ),
                    2
                ).label('downtime_hours'),

                # Uptime percentage for yesterday
                func.round(
                    100 - (
                        func.sum(
                            case(
                                (
                                    and_(
                                        is_down_condition,
                                        PAS.park_appears_open == True,
                                        rides_that_operated.c.ride_id.isnot(None)
                                    ),
                                    literal(1)
                                ),
                                else_=literal(0)
                            )
                        ) * 100.0 / func.nullif(operating_snapshots.c.total_operating_snapshots, 0)
                    ),
                    1
                ).label('uptime_percentage'),

                # Current status (for display - may differ from yesterday's status)
                current_status_subquery,
                current_is_open_subquery,
                park_is_open_subquery,

                # Wait time stats for yesterday
                func.max(RSS.wait_time).label('peak_wait_time'),
                func.round(
                    func.avg(
                        case(
                            (RSS.wait_time > 0, RSS.wait_time),
                            else_=None
                        )
                    ),
                    0
                ).label('avg_wait_time')
            )
            .select_from(Ride)
            .join(Park, Ride.park_id == Park.park_id)
            .join(RSS, Ride.ride_id == RSS.ride_id)
            .join(
                PAS,
                and_(
                    Park.park_id == PAS.park_id,
                    func.date_format(PAS.recorded_at, '%Y-%m-%d %H:%i') ==
                    func.date_format(RSS.recorded_at, '%Y-%m-%d %H:%i')
                )
            )
            .outerjoin(
                rides_that_operated,
                Ride.ride_id == rides_that_operated.c.ride_id
            )
            .outerjoin(
                operating_snapshots,
                Ride.ride_id == operating_snapshots.c.ride_id
            )
            .where(RSS.recorded_at >= start_utc)
            .where(RSS.recorded_at < end_utc)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            query = query.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        # Group and filter
        query = (
            query
            .group_by(
                Ride.ride_id,
                Ride.name,
                Park.name,
                Park.park_id,
                Park.city,
                Park.state_province,
                Ride.tier,
                operating_snapshots.c.total_operating_snapshots
            )
            .having(literal_column('downtime_hours') > 0)
            .order_by(literal_column('downtime_hours').desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(query)
