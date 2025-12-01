"""
Live Status Summary Query
=========================

Endpoint: GET /api/live/status-summary
UI Location: Dashboard status panel

Returns current counts of rides by status (OPERATING, DOWN, CLOSED, REFURBISHMENT).

Database Tables:
- rides (ride metadata)
- parks (park metadata for filtering)
- ride_status_snapshots (current status)
- park_activity_snapshots (park open status)

Time Window: Last 2 hours (LIVE_WINDOW_HOURS)

Example Response:
{
    "OPERATING": 245,
    "DOWN": 12,
    "CLOSED": 8,
    "REFURBISHMENT": 3,
    "PARK_CLOSED": 15,
    "total": 283
}
"""

from typing import Dict, Any, Optional

from sqlalchemy import select, func, and_, or_, case, exists
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_status_snapshots,
    park_activity_snapshots,
)
from database.queries.builders import Filters, StatusExpressions
from database.queries.builders.filters import LIVE_WINDOW_HOURS
from utils.timezone import get_today_pacific, get_pacific_day_range_utc


class StatusSummaryQuery:
    """
    Query handler for live status summary counts.
    """

    def __init__(self, connection: Connection):
        self.conn = connection

    def get_summary(
        self,
        filter_disney_universal: bool = False,
        park_id: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Get current status counts across all rides.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            park_id: Optional specific park filter

        Returns:
            Dict with counts per status category
        """
        # Get latest snapshot for each ride within live window
        latest_snapshot = self._get_latest_snapshots_subquery()

        # Get latest park_activity_snapshot per park to avoid count inflation
        # (joining all snapshots in window causes ~24x multiplication)
        latest_park_snapshot = self._get_latest_park_snapshots_subquery()

        # Get subquery for rides that have operated today
        # CRITICAL: Rides that have NEVER operated today are seasonal closures, not breakdowns
        has_operated_today = self._get_has_operated_today_subquery()

        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        if park_id:
            conditions.append(parks.c.park_id == park_id)

        # Check if ride has operated today (for distinguishing DOWN vs seasonal CLOSED)
        ride_has_operated = exists(
            select(1).where(
                has_operated_today.c.ride_id == rides.c.ride_id
            )
        )

        # Status expression with park awareness AND seasonal closure detection
        # CRITICAL: DOWN status only counts as DOWN if the ride has operated today.
        # Rides that have NEVER operated today are seasonal closures (count as CLOSED).
        status_expr = case(
            # If park appears closed, show PARK_CLOSED
            (park_activity_snapshots.c.park_appears_open == False, "PARK_CLOSED"),
            # Explicit OPERATING status
            (ride_status_snapshots.c.status == "OPERATING", "OPERATING"),
            # DOWN status - only if ride has operated today (actual breakdown)
            # If ride has NEVER operated today, it's a seasonal closure → CLOSED
            (
                and_(
                    ride_status_snapshots.c.status == "DOWN",
                    ride_has_operated,
                ),
                "DOWN",
            ),
            (
                and_(
                    ride_status_snapshots.c.status == "DOWN",
                    ~ride_has_operated,
                ),
                "CLOSED",  # Seasonal closure, not a breakdown
            ),
            (ride_status_snapshots.c.status == "CLOSED", "CLOSED"),
            (ride_status_snapshots.c.status == "REFURBISHMENT", "REFURBISHMENT"),
            # Map NULL status based on computed_is_open
            (
                and_(
                    ride_status_snapshots.c.status.is_(None),
                    ride_status_snapshots.c.computed_is_open == True,
                ),
                "OPERATING",
            ),
            # NULL status with computed_is_open=False - check if operated today
            (
                and_(
                    ride_status_snapshots.c.status.is_(None),
                    ride_status_snapshots.c.computed_is_open == False,
                    ride_has_operated,
                ),
                "DOWN",
            ),
            else_="CLOSED",  # Never operated = seasonal closure
        )

        stmt = (
            select(
                func.sum(case((status_expr == "OPERATING", 1), else_=0)).label(
                    "operating"
                ),
                func.sum(case((status_expr == "DOWN", 1), else_=0)).label("down"),
                func.sum(case((status_expr == "CLOSED", 1), else_=0)).label("closed"),
                func.sum(case((status_expr == "REFURBISHMENT", 1), else_=0)).label(
                    "refurbishment"
                ),
                func.sum(case((status_expr == "PARK_CLOSED", 1), else_=0)).label(
                    "park_closed"
                ),
                func.count().label("total"),
            )
            .select_from(
                rides.join(parks, rides.c.park_id == parks.c.park_id)
                .join(
                    latest_snapshot,  # Join the subquery first
                    rides.c.ride_id == latest_snapshot.c.ride_id,
                )
                .join(
                    ride_status_snapshots,
                    and_(
                        rides.c.ride_id == ride_status_snapshots.c.ride_id,
                        ride_status_snapshots.c.snapshot_id == latest_snapshot.c.max_snapshot_id,
                    ),
                )
                .outerjoin(
                    latest_park_snapshot,
                    parks.c.park_id == latest_park_snapshot.c.park_id,
                )
                .outerjoin(
                    park_activity_snapshots,
                    and_(
                        parks.c.park_id == park_activity_snapshots.c.park_id,
                        park_activity_snapshots.c.snapshot_id == latest_park_snapshot.c.max_pas_snapshot_id,
                    ),
                )
            )
            .where(and_(*conditions))
        )

        result = self.conn.execute(stmt).fetchone()

        if result:
            return {
                "OPERATING": result.operating or 0,
                "DOWN": result.down or 0,
                "CLOSED": result.closed or 0,
                "REFURBISHMENT": result.refurbishment or 0,
                "PARK_CLOSED": result.park_closed or 0,
                "total": result.total or 0,
            }

        return {
            "OPERATING": 0,
            "DOWN": 0,
            "CLOSED": 0,
            "REFURBISHMENT": 0,
            "PARK_CLOSED": 0,
            "total": 0,
        }

    def _get_latest_snapshots_subquery(self):
        """Get subquery for latest snapshot per ride within live window."""
        return (
            select(
                ride_status_snapshots.c.ride_id,
                func.max(ride_status_snapshots.c.snapshot_id).label("max_snapshot_id"),
            )
            .where(Filters.within_live_window(ride_status_snapshots.c.recorded_at))
            .group_by(ride_status_snapshots.c.ride_id)
            .subquery()
        )

    def _get_latest_park_snapshots_subquery(self):
        """Get subquery for latest park_activity_snapshot per park within live window.

        CRITICAL: Without this, joining directly to park_activity_snapshots
        causes each ride to be counted once per snapshot in the window
        (~24x inflation in a 2-hour window with 5-minute snapshots).
        """
        return (
            select(
                park_activity_snapshots.c.park_id,
                func.max(park_activity_snapshots.c.snapshot_id).label("max_pas_snapshot_id"),
            )
            .where(Filters.within_live_window(park_activity_snapshots.c.recorded_at))
            .group_by(park_activity_snapshots.c.park_id)
            .subquery()
        )

    def _get_has_operated_today_subquery(self):
        """Get subquery for rides that have operated at least once today.

        CRITICAL: This distinguishes actual breakdowns from seasonal closures.
        A ride showing status='DOWN' that has NEVER operated today is likely
        a seasonal closure (e.g., water ride in winter), NOT a breakdown.

        Uses the Pacific day range for consistency with other "today" queries.

        Returns:
            Subquery with ride_id for rides that had at least one OPERATING
            snapshot today.
        """
        today_pacific = get_today_pacific()
        start_utc, end_utc = get_pacific_day_range_utc(today_pacific)

        # Create an alias for the subquery to avoid conflicts with main query
        rss_operated = ride_status_snapshots.alias("rss_operated")

        return (
            select(rss_operated.c.ride_id)
            .where(
                and_(
                    rss_operated.c.recorded_at >= start_utc,
                    rss_operated.c.recorded_at < end_utc,
                    or_(
                        rss_operated.c.status == "OPERATING",
                        and_(
                            rss_operated.c.status.is_(None),
                            rss_operated.c.computed_is_open == True,
                        ),
                    ),
                )
            )
            .distinct()
            .subquery()
        )
