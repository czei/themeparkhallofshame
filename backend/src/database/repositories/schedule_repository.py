"""
Theme Park Downtime Tracker - Schedule Repository
=================================================

Provides data access layer for park_schedules table.
Handles fetching schedules from ThemeParks.wiki API and checking if parks are open.

This replaces the hacky "park_appears_open" heuristic that inferred park status
from ride counts. Now we use actual schedule data from the API.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from dateutil import parser as date_parser
import pytz

from sqlalchemy.orm import Session
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.models import ParkSchedule, Park
from src.collector.themeparks_wiki_client import get_themeparks_wiki_client
from src.utils.logger import logger, log_database_error


class ScheduleRepository:
    """
    Repository for park schedule operations.

    Primary functions:
    - Fetch and store schedules from ThemeParks.wiki API
    - Check if a park is currently open based on schedule
    - Get schedule for a specific date
    """

    def __init__(self, session: Session):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy Session object
        """
        self.session = session

    def fetch_and_store_schedule(self, park_id: int, themeparks_wiki_id: str) -> int:
        """
        Fetch schedule from ThemeParks.wiki API and store in database.

        The API returns next 30 days of schedule data. We upsert each day's
        schedule to handle updates.

        Args:
            park_id: Internal park ID
            themeparks_wiki_id: ThemeParks.wiki entity UUID

        Returns:
            Number of schedule entries stored

        Raises:
            Exception: If API call or database operation fails
        """
        client = get_themeparks_wiki_client()

        try:
            schedule_data = client.get_entity_schedule(themeparks_wiki_id)
        except Exception as e:
            logger.error(f"Failed to fetch schedule for park {park_id}: {e}")
            raise

        schedule_entries = schedule_data.get("schedule", [])
        timezone_str = schedule_data.get("timezone", "America/New_York")

        if not schedule_entries:
            logger.warning(f"No schedule entries returned for park {park_id}")
            return 0

        count = 0
        for entry in schedule_entries:
            try:
                stored = self._store_schedule_entry(park_id, entry, timezone_str)
                if stored:
                    count += 1
            except Exception as e:
                logger.warning(f"Failed to store schedule entry for park {park_id}: {e}")
                continue

        logger.info(f"Stored {count} schedule entries for park {park_id}")
        return count

    def _store_schedule_entry(
        self,
        park_id: int,
        entry: Dict[str, Any],
        timezone_str: str
    ) -> bool:
        """
        Store a single schedule entry using upsert logic.

        Args:
            park_id: Internal park ID
            entry: Schedule entry from API
            timezone_str: Park's timezone

        Returns:
            True if entry was stored successfully
        """
        schedule_date_str = entry.get("date")
        if not schedule_date_str:
            return False

        # Parse schedule date
        schedule_date = datetime.strptime(schedule_date_str, "%Y-%m-%d").date()

        # Parse opening/closing times (may be None for closed days)
        opening_time_str = entry.get("openingTime")
        closing_time_str = entry.get("closingTime")
        schedule_type = entry.get("type", "OPERATING")

        # Convert to UTC for storage
        opening_time_utc = None
        closing_time_utc = None

        if opening_time_str:
            opening_time_utc = self._parse_to_utc(opening_time_str, timezone_str)
        if closing_time_str:
            closing_time_utc = self._parse_to_utc(closing_time_str, timezone_str)

        # Validate schedule type
        valid_types = ("OPERATING", "TICKETED_EVENT", "PRIVATE_EVENT", "EXTRA_HOURS", "INFO")
        if schedule_type not in valid_types:
            schedule_type = "OPERATING"

        # Upsert using MySQL dialect - update if exists, insert if not
        stmt = mysql_insert(ParkSchedule).values(
            park_id=park_id,
            schedule_date=schedule_date,
            opening_time=opening_time_utc,
            closing_time=closing_time_utc,
            schedule_type=schedule_type,
            fetched_at=func.now()
        )
        stmt = stmt.on_duplicate_key_update(
            opening_time=stmt.inserted.opening_time,
            closing_time=stmt.inserted.closing_time,
            schedule_type=stmt.inserted.schedule_type,
            fetched_at=func.now(),
            updated_at=func.now()
        )

        try:
            self.session.execute(stmt)
            return True
        except Exception as e:
            log_database_error(e, f"Failed to upsert schedule for park {park_id}")
            return False

    def _parse_to_utc(self, iso_string: str, fallback_tz: str) -> Optional[datetime]:
        """
        Parse ISO datetime string to UTC datetime.

        Args:
            iso_string: ISO format datetime string (may include timezone)
            fallback_tz: Timezone to use if not in string

        Returns:
            UTC datetime or None if parsing fails
        """
        try:
            dt = date_parser.isoparse(iso_string)

            # If no timezone info, assume park's local timezone
            if dt.tzinfo is None:
                local_tz = pytz.timezone(fallback_tz)
                dt = local_tz.localize(dt)

            # Convert to UTC
            return dt.astimezone(pytz.UTC).replace(tzinfo=None)
        except Exception as e:
            logger.warning(f"Failed to parse datetime '{iso_string}': {e}")
            return None

    def is_park_open_now(self, park_id: int, now_utc: Optional[datetime] = None) -> bool:
        """
        Check if a park is currently open based on its schedule.

        This is the SINGLE SOURCE OF TRUTH for park open status.

        Args:
            park_id: Internal park ID
            now_utc: Current time in UTC (defaults to now)

        Returns:
            True if park is within operating hours, False otherwise
        """
        if now_utc is None:
            now_utc = datetime.utcnow()

        # Get today's date for the schedule lookup
        # Note: We need to check if current UTC time falls within any schedule entry
        today = now_utc.date()

        stmt = (
            select(ParkSchedule)
            .where(ParkSchedule.park_id == park_id)
            .where(ParkSchedule.schedule_date == today)
            .where(ParkSchedule.schedule_type == 'OPERATING')
            .where(ParkSchedule.opening_time.is_not(None))
            .where(ParkSchedule.closing_time.is_not(None))
            .order_by(ParkSchedule.opening_time)
        )

        result = self.session.execute(stmt)
        schedules = result.scalars().all()

        for schedule in schedules:
            opening = schedule.opening_time
            closing = schedule.closing_time

            # Check if current time is within operating hours
            if opening <= now_utc <= closing:
                return True

        # Also check yesterday's schedule (for parks open past midnight)
        yesterday = today - timedelta(days=1)
        stmt = (
            select(ParkSchedule)
            .where(ParkSchedule.park_id == park_id)
            .where(ParkSchedule.schedule_date == yesterday)
            .where(ParkSchedule.schedule_type == 'OPERATING')
            .where(ParkSchedule.opening_time.is_not(None))
            .where(ParkSchedule.closing_time.is_not(None))
            .order_by(ParkSchedule.opening_time)
        )

        result = self.session.execute(stmt)
        schedules = result.scalars().all()

        for schedule in schedules:
            opening = schedule.opening_time
            closing = schedule.closing_time

            # Parks open past midnight will have closing_time > opening_time by ~24+ hours
            if opening <= now_utc <= closing:
                return True

        return False

    def get_schedule_for_date(
        self,
        park_id: int,
        schedule_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Get schedule entry for a specific park and date.

        Args:
            park_id: Internal park ID
            schedule_date: Date to look up

        Returns:
            Dictionary with schedule data or None if not found
        """
        stmt = (
            select(ParkSchedule)
            .where(ParkSchedule.park_id == park_id)
            .where(ParkSchedule.schedule_date == schedule_date)
            .where(ParkSchedule.schedule_type == 'OPERATING')
            .order_by(ParkSchedule.opening_time)
            .limit(1)
        )

        result = self.session.execute(stmt)
        schedule = result.scalar_one_or_none()

        if schedule is None:
            return None

        return {
            'schedule_id': schedule.schedule_id,
            'park_id': schedule.park_id,
            'schedule_date': schedule.schedule_date,
            'opening_time': schedule.opening_time,
            'closing_time': schedule.closing_time,
            'schedule_type': schedule.schedule_type,
            'fetched_at': schedule.fetched_at
        }

    def has_recent_schedule(self, park_id: int, max_age_hours: int = 24) -> bool:
        """
        Check if we have recently fetched schedule data for a park.

        Args:
            park_id: Internal park ID
            max_age_hours: Maximum age of schedule data in hours

        Returns:
            True if we have schedule data fetched within the time limit
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

        stmt = (
            select(func.count())
            .select_from(ParkSchedule)
            .where(ParkSchedule.park_id == park_id)
            .where(ParkSchedule.fetched_at >= cutoff_time)
        )

        result = self.session.execute(stmt)
        count = result.scalar()

        return count > 0 if count else False

    def get_parks_needing_schedule_refresh(
        self,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get list of active parks that need schedule refresh.

        Args:
            max_age_hours: Maximum age before refresh is needed

        Returns:
            List of parks with park_id and themeparks_wiki_id
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

        # Build subquery for max fetched_at per park
        subq = (
            select(
                Park.park_id,
                Park.themeparks_wiki_id,
                Park.name,
                func.max(ParkSchedule.fetched_at).label('last_fetched')
            )
            .outerjoin(ParkSchedule, Park.park_id == ParkSchedule.park_id)
            .where(Park.is_active == True)
            .where(Park.themeparks_wiki_id.is_not(None))
            .group_by(Park.park_id, Park.themeparks_wiki_id, Park.name)
            .subquery()
        )

        # Filter for parks with no schedule or stale schedule
        stmt = (
            select(subq)
            .where(
                (subq.c.last_fetched.is_(None)) |
                (subq.c.last_fetched < cutoff_time)
            )
            .order_by(
                subq.c.last_fetched.is_(None).desc(),  # NULL values first
                subq.c.last_fetched.asc()
            )
        )

        result = self.session.execute(stmt)
        return [dict(row._mapping) for row in result]

    def cleanup_old_schedules(self, days_to_keep: int = 7) -> int:
        """
        Remove schedule entries older than specified days.

        Args:
            days_to_keep: Number of days of past schedules to retain

        Returns:
            Number of entries deleted
        """
        cutoff_date = date.today() - timedelta(days=days_to_keep)

        stmt = delete(ParkSchedule).where(ParkSchedule.schedule_date < cutoff_date)

        result = self.session.execute(stmt)
        deleted = result.rowcount

        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old schedule entries")

        return deleted
