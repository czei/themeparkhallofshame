"""
Theme Park Downtime Tracker - Status Change Detector
Detects ride status transitions (open ↔ closed) and calculates downtime duration.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import select, insert, func
from sqlalchemy.orm import Session

from models import Park, Ride, RideStatusSnapshot, RideStatusChange
from utils.logger import logger


class StatusChangeDetector:
    """
    Detects ride status changes from snapshots.

    Identifies transitions from open → closed (downtime start)
    and closed → open (downtime end), then calculates duration.
    """

    def __init__(self, session: Session):
        """
        Initialize detector with database session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def detect_status_changes(
        self,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Detect status changes for a ride within a time range.

        Args:
            ride_id: Ride ID
            start_time: Start of analysis period (UTC)
            end_time: End of analysis period (UTC)

        Returns:
            List of status change dictionaries
        """
        # Get all snapshots ordered by time
        stmt = (
            select(
                RideStatusSnapshot.snapshot_id,
                RideStatusSnapshot.ride_id,
                RideStatusSnapshot.recorded_at,
                RideStatusSnapshot.computed_is_open
            )
            .where(
                RideStatusSnapshot.ride_id == ride_id,
                RideStatusSnapshot.recorded_at >= start_time,
                RideStatusSnapshot.recorded_at <= end_time
            )
            .order_by(RideStatusSnapshot.recorded_at.asc())
        )

        result = self.session.execute(stmt)
        snapshots = [dict(row._mapping) for row in result]

        if len(snapshots) < 2:
            return []

        # Detect transitions
        changes = []
        previous_status = snapshots[0]['computed_is_open']

        for i in range(1, len(snapshots)):
            current_status = snapshots[i]['computed_is_open']
            current_time = snapshots[i]['recorded_at']
            previous_time = snapshots[i-1]['recorded_at']

            # Convert string datetime to datetime object if needed (SQLite compatibility)
            if isinstance(current_time, str):
                current_time = datetime.fromisoformat(current_time.replace(' ', 'T'))
            if isinstance(previous_time, str):
                previous_time = datetime.fromisoformat(previous_time.replace(' ', 'T'))

            if current_status != previous_status:
                # Status transition detected
                change = {
                    "ride_id": ride_id,
                    "previous_status": previous_status,
                    "new_status": current_status,
                    "change_detected_at": current_time,
                    "downtime_duration_minutes": None
                }

                # If transition is closed → open, calculate downtime duration
                if not previous_status and current_status:
                    # Find when it went down (last False status before this True)
                    downtime_start = previous_time
                    downtime_end = current_time
                    duration = (downtime_end - downtime_start).total_seconds() / 60.0
                    change["downtime_duration_minutes"] = int(duration)

                changes.append(change)
                logger.debug(f"Ride {ride_id} status change: {previous_status} → {current_status} at {current_time}")

            previous_status = current_status

        return changes

    def save_status_change(self, change_data: Dict[str, Any]) -> int:
        """
        Save status change to database.

        Args:
            change_data: Dictionary with change details

        Returns:
            Inserted change_id
        """
        # Map internal field names to database schema
        stmt = insert(RideStatusChange).values(
            ride_id=change_data['ride_id'],
            previous_status=change_data['previous_status'],
            new_status=change_data['new_status'],
            changed_at=change_data['change_detected_at'],
            duration_in_previous_status=change_data.get('downtime_duration_minutes', 0) or 0,
            wait_time_at_change=change_data.get('wait_time_at_change')
        )

        result = self.session.execute(stmt)
        change_id = result.lastrowid

        logger.debug(f"Saved status change {change_id} for ride {change_data['ride_id']}")
        return change_id

    def detect_all_rides_for_period(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Detect status changes for all active rides within a period.

        Args:
            start_time: Start of analysis period (UTC)
            end_time: End of analysis period (UTC)

        Returns:
            Dictionary mapping ride_id to list of changes
        """
        # Get all active rides
        stmt = (
            select(Ride.ride_id)
            .where(Ride.is_active == True)
            .order_by(Ride.ride_id)
        )

        result = self.session.execute(stmt)
        ride_ids = [row.ride_id for row in result]

        all_changes = {}
        total_changes = 0

        for ride_id in ride_ids:
            changes = self.detect_status_changes(ride_id, start_time, end_time)
            if changes:
                all_changes[ride_id] = changes
                total_changes += len(changes)

        logger.info(f"Detected {total_changes} status changes across {len(all_changes)} rides")
        return all_changes

    def calculate_downtime_summary(
        self,
        ride_id: int,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Calculate downtime summary statistics for a ride.

        Args:
            ride_id: Ride ID
            start_time: Start of analysis period (UTC)
            end_time: End of analysis period (UTC)

        Returns:
            Dictionary with downtime summary metrics
        """
        changes = self.detect_status_changes(ride_id, start_time, end_time)

        # Count downtime events (transitions to closed)
        downtime_events = [c for c in changes if not c['new_status']]

        # Sum total downtime minutes
        total_downtime = sum(
            c.get('downtime_duration_minutes', 0) or 0
            for c in changes
            if c.get('downtime_duration_minutes')
        )

        # Calculate uptime percentage
        period_minutes = (end_time - start_time).total_seconds() / 60.0
        uptime_minutes = period_minutes - total_downtime
        uptime_percentage = (uptime_minutes / period_minutes * 100.0) if period_minutes > 0 else 0.0

        return {
            "ride_id": ride_id,
            "downtime_event_count": len(downtime_events),
            "total_downtime_minutes": total_downtime,
            "uptime_percentage": round(uptime_percentage, 2),
            "period_start": start_time,
            "period_end": end_time
        }

    def get_longest_downtime_events(
        self,
        park_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get longest downtime events within a period.

        Args:
            park_id: Optional park ID to filter
            start_time: Optional start time (UTC)
            end_time: Optional end time (UTC)
            limit: Maximum number of results

        Returns:
            List of longest downtime events
        """
        # Build query with ORM models
        stmt = (
            select(
                RideStatusChange.change_id,
                RideStatusChange.ride_id,
                Ride.name.label('ride_name'),
                Park.name.label('park_name'),
                RideStatusChange.changed_at.label('change_detected_at'),
                RideStatusChange.duration_in_previous_status.label('downtime_duration_minutes'),
                func.round(RideStatusChange.duration_in_previous_status / 60.0, 2).label('downtime_hours')
            )
            .select_from(RideStatusChange)
            .join(Ride, RideStatusChange.ride_id == Ride.ride_id)
            .join(Park, Ride.park_id == Park.park_id)
            .where(
                RideStatusChange.duration_in_previous_status.isnot(None),
                RideStatusChange.new_status == True
            )
        )

        # Add optional filters
        if park_id:
            stmt = stmt.where(Ride.park_id == park_id)
        if start_time:
            stmt = stmt.where(RideStatusChange.changed_at >= start_time)
        if end_time:
            stmt = stmt.where(RideStatusChange.changed_at <= end_time)

        stmt = stmt.order_by(RideStatusChange.duration_in_previous_status.desc()).limit(limit)

        result = self.session.execute(stmt)
        return [dict(row._mapping) for row in result]
