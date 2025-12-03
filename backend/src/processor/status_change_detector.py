"""
Theme Park Downtime Tracker - Status Change Detector
Detects ride status transitions (open ↔ closed) and calculates downtime duration.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.logger import logger


class StatusChangeDetector:
    """
    Detects ride status changes from snapshots.

    Identifies transitions from open → closed (downtime start)
    and closed → open (downtime end), then calculates duration.
    """

    def __init__(self, connection: Connection):
        """
        Initialize detector with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

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
        query = text("""
            SELECT
                snapshot_id,
                ride_id,
                recorded_at,
                computed_is_open
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
                AND recorded_at >= :start_time
                AND recorded_at <= :end_time
            ORDER BY recorded_at ASC
        """)

        result = self.conn.execute(query, {
            "ride_id": ride_id,
            "start_time": start_time,
            "end_time": end_time
        })

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
        query = text("""
            INSERT INTO ride_status_changes (
                ride_id, previous_status, new_status,
                change_detected_at, downtime_duration_minutes
            )
            VALUES (
                :ride_id, :previous_status, :new_status,
                :change_detected_at, :downtime_duration_minutes
            )
        """)

        result = self.conn.execute(query, change_data)
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
        rides_query = text("""
            SELECT ride_id
            FROM rides
            WHERE is_active = TRUE
            ORDER BY ride_id
        """)

        result = self.conn.execute(rides_query)
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
        park_filter = "AND r.park_id = :park_id" if park_id else ""
        time_filter = ""

        if start_time:
            time_filter += "AND rsc.change_detected_at >= :start_time "
        if end_time:
            time_filter += "AND rsc.change_detected_at <= :end_time "

        query = text(f"""
            SELECT
                rsc.change_id,
                rsc.ride_id,
                r.name AS ride_name,
                p.name AS park_name,
                rsc.change_detected_at,
                rsc.downtime_duration_minutes,
                ROUND(rsc.downtime_duration_minutes / 60.0, 2) AS downtime_hours
            FROM ride_status_changes rsc
            INNER JOIN rides r ON rsc.ride_id = r.ride_id
            INNER JOIN parks p ON r.park_id = p.park_id
            WHERE rsc.downtime_duration_minutes IS NOT NULL
                AND rsc.new_status = TRUE
                {park_filter}
                {time_filter}
            ORDER BY rsc.downtime_duration_minutes DESC
            LIMIT :limit
        """)

        params = {"limit": limit}
        if park_id:
            params["park_id"] = park_id
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result]
