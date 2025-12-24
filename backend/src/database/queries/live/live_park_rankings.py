"""
Live Park Rankings Query
========================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by current-day downtime from real-time snapshots.

NOTE: This class is currently bypassed for performance. The routes use
StatsRepository.get_park_live_downtime_rankings() instead, which uses
the same centralized SQL helpers but with optimized CTEs.

CRITICAL: Shame score only counts rides that are CURRENTLY down.
Rides that were down earlier but are now operating do NOT contribute
to the shame score. "Rides Down" shows count of currently down rides.

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py (now converted to ORM)
- Python calculations: utils/metrics.py

Performance: Uses SQLAlchemy ORM with CTEs for consistency.
"""

from typing import List, Dict, Any

from sqlalchemy import select, func, and_, or_, case, literal_column
from sqlalchemy.orm import Session, aliased

from src.models import Park, Ride, RideClassification, RideStatusSnapshot, ParkActivitySnapshot
from src.utils.query_helpers import QueryClassBase, TimeIntervalHelper
from utils.timezone import get_today_pacific, get_pacific_day_range_utc


class LiveParkRankingsQuery(QueryClassBase):
    """
    Query handler for live (today) park rankings.

    Uses SQLAlchemy ORM to ensure consistent calculations across all queries.

    CRITICAL: Shame score only counts rides CURRENTLY down (latest snapshot),
    not cumulative downtime throughout the day.

    NOTE: For production use, prefer StatsRepository.get_park_live_downtime_rankings()
    which has additional optimizations.
    """

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get live park rankings for today from real-time snapshots.

        Uses SQLAlchemy ORM for consistent status logic.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by shame_score (descending)
        """
        # Get Pacific day bounds in UTC
        today = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today)

        # CTE 1: latest_snapshot - Find the most recent snapshot timestamp for each ride today
        latest_snapshot = (
            select(
                RideStatusSnapshot.ride_id,
                func.max(RideStatusSnapshot.recorded_at).label('latest_recorded_at')
            )
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .group_by(RideStatusSnapshot.ride_id)
        ).cte('latest_snapshot')

        # CTE 2: latest_park_snapshot - Find the most recent park_activity_snapshot for each park
        # Contains the pre-calculated shame_score (THE single source of truth)
        latest_park_snapshot = (
            select(
                ParkActivitySnapshot.park_id,
                func.max(ParkActivitySnapshot.recorded_at).label('latest_recorded_at')
            )
            .where(ParkActivitySnapshot.recorded_at >= start_utc)
            .where(ParkActivitySnapshot.recorded_at < end_utc)
            .group_by(ParkActivitySnapshot.park_id)
        ).cte('latest_park_snapshot')

        # CTE 3: rides_currently_down - Identify rides that are DOWN in their latest snapshot
        # This is used to count currently down rides (not for shame score)
        # Create aliases for the joins
        rss_latest = aliased(RideStatusSnapshot)
        pas_latest = aliased(ParkActivitySnapshot)
        r_inner = aliased(Ride)
        p_for_down = aliased(Park)

        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        # For Disney/Universal parks: only status='DOWN'
        # For other parks: status IN ('DOWN', 'CLOSED') OR (status IS NULL AND computed_is_open=FALSE)
        is_down_latest = case(
            (
                or_(p_for_down.is_disney == True, p_for_down.is_universal == True),
                rss_latest.status == 'DOWN'
            ),
            else_=or_(
                rss_latest.status.in_(['DOWN', 'CLOSED']),
                and_(rss_latest.status.is_(None), rss_latest.computed_is_open == False)
            )
        )

        rides_currently_down = (
            select(
                r_inner.ride_id.label('ride_id'),
                r_inner.park_id.label('park_id')
            )
            .select_from(r_inner)
            .join(p_for_down, r_inner.park_id == p_for_down.park_id)
            .join(rss_latest, r_inner.ride_id == rss_latest.ride_id)
            .join(
                latest_snapshot,
                and_(
                    rss_latest.ride_id == latest_snapshot.c.ride_id,
                    rss_latest.recorded_at == latest_snapshot.c.latest_recorded_at
                )
            )
            .join(
                pas_latest,
                and_(
                    r_inner.park_id == pas_latest.park_id,
                    pas_latest.recorded_at == rss_latest.recorded_at
                )
            )
            .where(r_inner.is_active == True)
            .where(r_inner.category == 'ATTRACTION')
            .where(is_down_latest)
            .where(pas_latest.park_appears_open == True)
            .distinct()
        ).cte('rides_currently_down')

        # CTE 4: park_weights - Total tier weight for each park (for reference only)
        park_weights = (
            select(
                Park.park_id,
                func.sum(func.coalesce(RideClassification.tier_weight, 2)).label('total_park_weight')
            )
            .select_from(Park)
            .join(Ride, Park.park_id == Ride.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .where(Ride.last_operated_at >= TimeIntervalHelper.days_ago(7))
            .where(Park.is_active == True)
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            park_weights = park_weights.where(
                or_(Park.is_disney == True, Park.is_universal == True)
            )

        park_weights = park_weights.group_by(Park.park_id).cte('park_weights')

        # Subquery for shame_score: READ from stored value in park_activity_snapshots
        # THE SINGLE SOURCE OF TRUTH - calculated during data collection
        pas_stored = aliased(ParkActivitySnapshot)
        shame_score_subquery = (
            select(pas_stored.shame_score)
            .select_from(pas_stored)
            .join(
                latest_park_snapshot,
                and_(
                    pas_stored.park_id == latest_park_snapshot.c.park_id,
                    pas_stored.recorded_at == latest_park_snapshot.c.latest_recorded_at
                )
            )
            .where(pas_stored.park_id == Park.park_id)
            .correlate(Park)
            .scalar_subquery()
        )

        # Subquery for park_is_open: Check if park is currently operating
        park_is_open_subquery = (
            select(ParkActivitySnapshot.park_appears_open)
            .where(ParkActivitySnapshot.park_id == Park.park_id)
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .limit(1)
            .correlate(Park)
            .scalar_subquery()
        )

        # PARK-TYPE AWARE downtime logic for main query
        # Disney/Universal: only status='DOWN'
        # Other parks: status IN ('DOWN', 'CLOSED') OR (status IS NULL AND computed_is_open=FALSE)
        is_down_main = case(
            (
                or_(Park.is_disney == True, Park.is_universal == True),
                RideStatusSnapshot.status == 'DOWN'
            ),
            else_=or_(
                RideStatusSnapshot.status.in_(['DOWN', 'CLOSED']),
                and_(RideStatusSnapshot.status.is_(None), RideStatusSnapshot.computed_is_open == False)
            )
        )

        # Downtime hours calculation (using centralized helper logic)
        # Only count when park is open AND ride is down
        downtime_minutes = case(
            (
                and_(ParkActivitySnapshot.park_appears_open == True, is_down_main),
                10  # SNAPSHOT_INTERVAL_MINUTES from utils.metrics
            ),
            else_=0
        )

        # Weighted downtime hours calculation
        weighted_downtime_minutes = case(
            (
                and_(ParkActivitySnapshot.park_appears_open == True, is_down_main),
                10 * func.coalesce(RideClassification.tier_weight, 2)
            ),
            else_=0
        )

        # Main SELECT query
        stmt = (
            select(
                Park.park_id,
                Park.queue_times_id,
                Park.name.label('park_name'),
                func.concat(Park.city, ', ', Park.state_province).label('location'),

                # Total downtime hours (sum of downtime minutes / 60, rounded to 1 decimal)
                func.round(func.sum(downtime_minutes) / 60.0, 1).label('total_downtime_hours'),

                # Weighted downtime hours (sum of weighted downtime minutes / 60, rounded to 1 decimal)
                func.round(func.sum(weighted_downtime_minutes) / 60.0, 1).label('weighted_downtime_hours'),

                # Shame Score: READ from stored value in park_activity_snapshots
                shame_score_subquery.label('shame_score'),

                # Count of rides CURRENTLY down (not cumulative)
                func.count(func.distinct(rides_currently_down.c.ride_id)).label('rides_down'),

                # Park operating status
                park_is_open_subquery.label('park_is_open')
            )
            .select_from(Park)
            .join(Ride, Park.park_id == Ride.park_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
            .join(
                ParkActivitySnapshot,
                and_(
                    Park.park_id == ParkActivitySnapshot.park_id,
                    ParkActivitySnapshot.recorded_at == RideStatusSnapshot.recorded_at
                )
            )
            .join(park_weights, Park.park_id == park_weights.c.park_id)
            .outerjoin(rides_currently_down, Ride.ride_id == rides_currently_down.c.ride_id)
            .where(RideStatusSnapshot.recorded_at >= start_utc)
            .where(RideStatusSnapshot.recorded_at < end_utc)
            .where(Park.is_active == True)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
        )

        # Apply Disney/Universal filter if requested
        if filter_disney_universal:
            stmt = stmt.where(or_(Park.is_disney == True, Park.is_universal == True))

        # Group by and order
        stmt = (
            stmt
            .group_by(Park.park_id, Park.name, Park.city, Park.state_province, park_weights.c.total_park_weight)
            .having(func.round(func.sum(downtime_minutes) / 60.0, 1) > 0)
            .order_by(shame_score_subquery.desc())
            .limit(limit)
        )

        return self.execute_and_fetchall(stmt)
