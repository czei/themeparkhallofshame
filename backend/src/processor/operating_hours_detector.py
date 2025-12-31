"""
Theme Park Downtime Tracker - Operating Hours Detector
Detects park operating hours from ride activity in local timezone.
"""

from datetime import datetime, date, time, timedelta
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo
from sqlalchemy import select, func, case
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert

from utils.logger import logger
from models import Park, Ride, RideStatusSnapshot, ParkOperatingSession


class OperatingHoursDetector:
    """
    Detects park operating hours from ride activity snapshots.

    Uses first/last ride activity to infer park open/close times.
    All times are handled in the park's local timezone (parks.timezone field).
    """

    def __init__(self, session: Session):
        """
        Initialize detector with database session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def detect_operating_session(
        self,
        park_id: int,
        operating_date: date,
        park_timezone: str
    ) -> Optional[Dict[str, Any]]:
        """
        Detect operating hours for a park on a specific date.

        Args:
            park_id: Park ID
            operating_date: Date to analyze (YYYY-MM-DD)
            park_timezone: Park's IANA timezone (e.g., 'America/New_York')

        Returns:
            Dictionary with operating session data or None if no activity
        """
        # Convert operating date to UTC boundaries for the park's timezone
        tz = ZoneInfo(park_timezone)
        local_start = datetime.combine(operating_date, time(0, 0), tzinfo=tz)
        local_end = datetime.combine(operating_date, time(23, 59, 59), tzinfo=tz)

        utc_start = local_start.astimezone(ZoneInfo('UTC'))
        utc_end = local_end.astimezone(ZoneInfo('UTC'))

        # Find first and last ride activity
        stmt = (
            select(
                func.min(RideStatusSnapshot.recorded_at).label('first_activity'),
                func.max(RideStatusSnapshot.recorded_at).label('last_activity'),
                func.count(func.distinct(RideStatusSnapshot.ride_id)).label('active_rides_count'),
                func.sum(
                    case((RideStatusSnapshot.computed_is_open == True, 1), else_=0)
                ).label('open_ride_snapshots')
            )
            .select_from(RideStatusSnapshot)
            .join(Ride, RideStatusSnapshot.ride_id == Ride.ride_id)
            .where(
                Ride.park_id == park_id,
                RideStatusSnapshot.recorded_at >= utc_start,
                RideStatusSnapshot.recorded_at <= utc_end,
                Ride.is_active == True
            )
        )

        result = self.session.execute(stmt)
        row = result.fetchone()

        if not row or not row.first_activity:
            logger.debug(f"No activity detected for park {park_id} on {operating_date}")
            return None

        # Convert UTC times back to park's local timezone for display
        first_activity_utc = row.first_activity
        last_activity_utc = row.last_activity

        # Convert string datetime to datetime object if needed (SQLite compatibility)
        if isinstance(first_activity_utc, str):
            first_activity_utc = datetime.fromisoformat(first_activity_utc.replace(' ', 'T'))
        if isinstance(last_activity_utc, str):
            last_activity_utc = datetime.fromisoformat(last_activity_utc.replace(' ', 'T'))

        first_activity_local = first_activity_utc.replace(tzinfo=ZoneInfo('UTC')).astimezone(tz)
        last_activity_local = last_activity_utc.replace(tzinfo=ZoneInfo('UTC')).astimezone(tz)

        # Calculate total operating hours
        duration = last_activity_utc - first_activity_utc
        total_hours = duration.total_seconds() / 3600.0

        logger.info(f"Detected operating session for park {park_id} on {operating_date}: "
                   f"{first_activity_local.strftime('%H:%M')} - {last_activity_local.strftime('%H:%M')} "
                   f"({total_hours:.2f} hours)")

        # Calculate operating minutes
        operating_minutes = int(duration.total_seconds() / 60)

        return {
            "park_id": park_id,
            "session_date": operating_date,
            "session_start_utc": first_activity_utc,
            "session_end_utc": last_activity_utc,
            "operating_minutes": operating_minutes,
            "active_rides_count": row.active_rides_count,
            "open_ride_snapshots": row.open_ride_snapshots
        }

    def save_operating_session(self, session_data: Dict[str, Any]) -> int:
        """
        Save operating session to database.

        Args:
            session_data: Dictionary from detect_operating_session()

        Returns:
            Inserted session_id
        """
        # Use MySQL ON DUPLICATE KEY UPDATE for upsert
        stmt = mysql_insert(ParkOperatingSession).values(
            park_id=session_data['park_id'],
            session_date=session_data['session_date'],
            session_start_utc=session_data['session_start_utc'],
            session_end_utc=session_data['session_end_utc'],
            operating_minutes=session_data['operating_minutes']
        )

        # On duplicate (park_id, session_date), update the values
        stmt = stmt.on_duplicate_key_update(
            session_start_utc=stmt.inserted.session_start_utc,
            session_end_utc=stmt.inserted.session_end_utc,
            operating_minutes=stmt.inserted.operating_minutes
        )

        result = self.session.execute(stmt)
        session_id = result.lastrowid

        logger.info(f"Saved operating session {session_id} for park {session_data['park_id']}")
        return session_id

    def detect_all_parks_for_date(
        self,
        operating_date: date
    ) -> List[Dict[str, Any]]:
        """
        Detect operating sessions for all active parks on a specific date.

        Args:
            operating_date: Date to analyze

        Returns:
            List of operating session dictionaries
        """
        # Get all active parks with their timezones
        stmt = (
            select(Park.park_id, Park.name, Park.timezone)
            .where(Park.is_active == True)
            .order_by(Park.park_id)
        )

        result = self.session.execute(stmt)
        parks = [dict(row._mapping) for row in result]

        sessions = []
        for park in parks:
            session = self.detect_operating_session(
                park_id=park['park_id'],
                operating_date=operating_date,
                park_timezone=park['timezone']
            )

            if session:
                sessions.append(session)

        logger.info(f"Detected {len(sessions)} operating sessions for {operating_date}")
        return sessions

    def backfill_operating_sessions(
        self,
        park_id: int,
        start_date: date,
        end_date: date,
        park_timezone: str
    ) -> int:
        """
        Backfill operating sessions for a date range.

        Args:
            park_id: Park ID
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            park_timezone: Park's IANA timezone

        Returns:
            Number of sessions created
        """
        sessions_created = 0
        current_date = start_date

        while current_date <= end_date:
            session = self.detect_operating_session(
                park_id=park_id,
                operating_date=current_date,
                park_timezone=park_timezone
            )

            if session:
                self.save_operating_session(session)
                sessions_created += 1

            current_date += timedelta(days=1)

        logger.info(f"Backfilled {sessions_created} operating sessions for park {park_id}")
        return sessions_created
