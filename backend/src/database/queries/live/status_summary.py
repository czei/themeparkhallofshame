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
    "operating": 245,
    "down": 12,
    "closed": 8,
    "refurbishment": 3,
    "park_closed": 15,
    "total": 283
}
"""

from typing import Dict, Any, Optional

from sqlalchemy import select, func, and_, or_, case
from sqlalchemy.engine import Connection

from database.schema import (
    parks,
    rides,
    ride_status_snapshots,
    park_activity_snapshots,
)
from database.queries.builders import Filters, StatusExpressions
from database.queries.builders.filters import LIVE_WINDOW_HOURS


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

        conditions = [
            rides.c.is_active == True,
            rides.c.category == "ATTRACTION",
            parks.c.is_active == True,
        ]

        if filter_disney_universal:
            conditions.append(Filters.disney_universal(parks))

        if park_id:
            conditions.append(parks.c.park_id == park_id)

        # Status expression with park awareness
        status_expr = case(
            # If park appears closed, show PARK_CLOSED
            (park_activity_snapshots.c.park_appears_open == False, "PARK_CLOSED"),
            # Otherwise use ride status
            (ride_status_snapshots.c.status == "OPERATING", "OPERATING"),
            (ride_status_snapshots.c.status == "DOWN", "DOWN"),
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
            else_="DOWN",
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
                    park_activity_snapshots,
                    and_(
                        parks.c.park_id == park_activity_snapshots.c.park_id,
                        Filters.within_live_window(park_activity_snapshots.c.recorded_at),
                    ),
                )
            )
            .where(and_(*conditions))
        )

        result = self.conn.execute(stmt).fetchone()

        if result:
            return {
                "operating": result.operating or 0,
                "down": result.down or 0,
                "closed": result.closed or 0,
                "refurbishment": result.refurbishment or 0,
                "park_closed": result.park_closed or 0,
                "total": result.total or 0,
            }

        return {
            "operating": 0,
            "down": 0,
            "closed": 0,
            "refurbishment": 0,
            "park_closed": 0,
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
