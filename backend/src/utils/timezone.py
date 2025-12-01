"""
Theme Park Downtime Tracker - Timezone Utilities
Provides Pacific Time date handling for US parks.

All US parks use Pacific Time as the day boundary because:
- When midnight Pacific hits, all US parks (Eastern to Pacific) are closed
- Orlando parks (9 AM - 11 PM ET) fall entirely within a single Pacific day
- Prevents evening data from being cut off mid-operation
"""

from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

# Pacific Time - used as the day boundary for all US parks
PACIFIC_TZ = ZoneInfo('America/Los_Angeles')
UTC_TZ = ZoneInfo('UTC')


def get_today_pacific() -> date:
    """
    Get current date in Pacific Time.

    This is the authoritative "today" for US theme parks.
    At 11 PM Eastern, this still returns "today" (8 PM Pacific).
    At 3 AM Eastern, this returns the new day (midnight Pacific).

    Returns:
        date: Current date in Pacific Time
    """
    return datetime.now(PACIFIC_TZ).date()


def get_now_pacific() -> datetime:
    """
    Get current datetime in Pacific Time.

    Returns:
        datetime: Current datetime with Pacific timezone
    """
    return datetime.now(PACIFIC_TZ)


def get_pacific_day_range_utc(target_date: date) -> tuple[datetime, datetime]:
    """
    Get UTC datetime range for a Pacific Time calendar day.

    Useful for querying UTC-timestamped data within a Pacific day.

    Args:
        target_date: The Pacific date to get range for

    Returns:
        tuple: (start_utc, end_utc) where:
            - start_utc = midnight Pacific converted to UTC
            - end_utc = next midnight Pacific converted to UTC

    Example:
        For 2025-11-27 Pacific:
        - In winter (PST = UTC-8):
          start = 2025-11-27 08:00:00 UTC
          end = 2025-11-28 08:00:00 UTC
        - In summer (PDT = UTC-7):
          start = 2025-11-27 07:00:00 UTC
          end = 2025-11-28 07:00:00 UTC
    """
    # Create midnight Pacific for the target date
    start_pacific = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=PACIFIC_TZ
    )

    # Next midnight Pacific
    end_pacific = start_pacific + timedelta(days=1)

    # Convert to UTC for database queries
    return (
        start_pacific.astimezone(UTC_TZ),
        end_pacific.astimezone(UTC_TZ)
    )


def get_today_range_to_now_utc() -> tuple[datetime, datetime]:
    """
    Get UTC datetime range from midnight Pacific to now.

    Used for TODAY cumulative queries - aggregates data from
    the start of the Pacific day to the current moment.

    Returns:
        tuple: (start_utc, now_utc) where:
            - start_utc = midnight Pacific today, converted to UTC
            - now_utc = current time in UTC

    Example:
        At 3 PM Pacific on 2025-11-27:
        - start_utc = 2025-11-27 08:00:00 UTC (midnight Pacific)
        - now_utc = 2025-11-27 23:00:00 UTC (3 PM Pacific)
    """
    today = get_today_pacific()
    start_utc, _ = get_pacific_day_range_utc(today)
    now_utc = datetime.now(UTC_TZ)
    return (start_utc, now_utc)


def date_to_pacific(utc_datetime: datetime) -> date:
    """
    Convert a UTC datetime to a Pacific date.

    Useful for determining which Pacific "day" a UTC timestamp belongs to.

    Args:
        utc_datetime: A datetime in UTC (or timezone-aware)

    Returns:
        date: The Pacific date this timestamp falls within
    """
    if utc_datetime.tzinfo is None:
        # Assume naive datetime is UTC
        utc_datetime = utc_datetime.replace(tzinfo=UTC_TZ)

    pacific_dt = utc_datetime.astimezone(PACIFIC_TZ)
    return pacific_dt.date()
