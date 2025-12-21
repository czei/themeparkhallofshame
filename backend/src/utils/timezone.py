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

# Period aliases - maps user-friendly names to internal period names
# Used by API routes for consistent period normalization
PERIOD_ALIASES = {
    '7days': 'last_week',
    '30days': 'last_month',
}


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


def get_yesterday_range_utc() -> tuple[datetime, datetime, str]:
    """
    Get UTC datetime range for yesterday (previous complete Pacific day).

    YESTERDAY is the complete 24-hour period before today in Pacific Time.
    Unlike TODAY which is partial, YESTERDAY is immutable and cacheable.

    Returns:
        tuple: (start_utc, end_utc, label) where:
            - start_utc = midnight Pacific yesterday, in UTC
            - end_utc = midnight Pacific today (end of yesterday), in UTC
            - label = Human-readable date (e.g., "Dec 1, 2025")

    Example:
        On Dec 2, 2025 (Pacific):
        - Returns Dec 1, 2025 00:00:00 to Dec 2, 2025 00:00:00 Pacific
        - label = "Dec 1, 2025"
    """
    today = get_today_pacific()
    yesterday = today - timedelta(days=1)

    # Get UTC ranges for yesterday
    start_utc, end_utc = get_pacific_day_range_utc(yesterday)

    # Format label: "Dec 1, 2025"
    label = yesterday.strftime("%b %-d, %Y")

    return (start_utc, end_utc, label)


def get_yesterday_date_range() -> tuple[date, date, str]:
    """
    Get Pacific date for yesterday (previous complete day).

    Returns:
        tuple: (yesterday_date, yesterday_date, label) where:
            - yesterday_date = Yesterday's date in Pacific Time
            - end_date = Same as yesterday_date (single day)
            - label = Human-readable date (e.g., "Dec 1, 2025")

    Note: Both start and end are the same since YESTERDAY is a single day.
    This matches the signature of get_last_week_date_range() for consistency.
    """
    today = get_today_pacific()
    yesterday = today - timedelta(days=1)

    # Format label: "Dec 1, 2025"
    label = yesterday.strftime("%b %-d, %Y")

    return (yesterday, yesterday, label)


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


# =============================================================================
# Calendar Period Functions (for reporting/social media)
# =============================================================================

def get_last_week_range_utc() -> tuple[datetime, datetime, str]:
    """
    Get UTC datetime range for the previous complete week (Sunday-Saturday).

    Week starts on Sunday (US convention for theme parks).

    Returns:
        tuple: (start_utc, end_utc, label) where:
            - start_utc = Previous Sunday midnight Pacific, in UTC
            - end_utc = Previous Saturday 11:59:59 PM Pacific, in UTC
            - label = Human-readable date range (e.g., "Nov 24-30, 2024")

    Example:
        On Monday Dec 2, 2024 (Pacific):
        - Returns Sunday Nov 24 through Saturday Nov 30
        - label = "Nov 24-30, 2024"
    """
    today = get_today_pacific()

    # Find the most recent Sunday (start of current week)
    # weekday(): Monday=0, Sunday=6
    days_since_sunday = (today.weekday() + 1) % 7

    # Current week's Sunday
    current_week_sunday = today - timedelta(days=days_since_sunday)

    # Previous week's Sunday (7 days before current week's Sunday)
    last_week_sunday = current_week_sunday - timedelta(days=7)

    # Previous week's Saturday (6 days after previous Sunday)
    last_week_saturday = last_week_sunday + timedelta(days=6)

    # Get UTC ranges
    start_utc, _ = get_pacific_day_range_utc(last_week_sunday)
    _, end_utc = get_pacific_day_range_utc(last_week_saturday)

    # Format label
    if last_week_sunday.month == last_week_saturday.month:
        # Same month: "Nov 24-30, 2024"
        label = f"{last_week_sunday.strftime('%b')} {last_week_sunday.day}-{last_week_saturday.day}, {last_week_saturday.year}"
    else:
        # Different months: "Nov 24 - Dec 1, 2024"
        label = f"{last_week_sunday.strftime('%b')} {last_week_sunday.day} - {last_week_saturday.strftime('%b')} {last_week_saturday.day}, {last_week_saturday.year}"

    return (start_utc, end_utc, label)


def get_last_month_range_utc() -> tuple[datetime, datetime, str]:
    """
    Get UTC datetime range for the previous complete calendar month.

    Returns:
        tuple: (start_utc, end_utc, label) where:
            - start_utc = 1st of previous month, midnight Pacific, in UTC
            - end_utc = Last day of previous month, 11:59:59 PM Pacific, in UTC
            - label = Human-readable month name (e.g., "November 2024")

    Example:
        On Dec 2, 2024 (Pacific):
        - Returns Nov 1 through Nov 30
        - label = "November 2024"
    """
    today = get_today_pacific()

    # Get first day of current month
    first_of_current_month = date(today.year, today.month, 1)

    # Last day of previous month = day before first of current month
    last_day_prev_month = first_of_current_month - timedelta(days=1)

    # First day of previous month
    first_day_prev_month = date(last_day_prev_month.year, last_day_prev_month.month, 1)

    # Get UTC ranges
    start_utc, _ = get_pacific_day_range_utc(first_day_prev_month)
    _, end_utc = get_pacific_day_range_utc(last_day_prev_month)

    # Format label: "November 2024"
    label = first_day_prev_month.strftime("%B %Y")

    return (start_utc, end_utc, label)


def get_last_week_date_range() -> tuple[date, date, str]:
    """
    Get Pacific date range for the previous complete week (Sunday-Saturday).

    Returns:
        tuple: (start_date, end_date, label) where:
            - start_date = Previous Sunday (Pacific)
            - end_date = Previous Saturday (Pacific)
            - label = Human-readable date range (e.g., "Nov 24-30, 2024")
    """
    today = get_today_pacific()

    # Find the most recent Sunday (start of current week)
    days_since_sunday = (today.weekday() + 1) % 7

    # Current week's Sunday
    current_week_sunday = today - timedelta(days=days_since_sunday)

    # Previous week's Sunday and Saturday
    last_week_sunday = current_week_sunday - timedelta(days=7)
    last_week_saturday = last_week_sunday + timedelta(days=6)

    # Format label
    if last_week_sunday.month == last_week_saturday.month:
        label = f"{last_week_sunday.strftime('%b')} {last_week_sunday.day}-{last_week_saturday.day}, {last_week_saturday.year}"
    else:
        label = f"{last_week_sunday.strftime('%b')} {last_week_sunday.day} - {last_week_saturday.strftime('%b')} {last_week_saturday.day}, {last_week_saturday.year}"

    return (last_week_sunday, last_week_saturday, label)


def get_last_month_date_range() -> tuple[date, date, str]:
    """
    Get Pacific date range for the previous complete calendar month.

    Returns:
        tuple: (start_date, end_date, label) where:
            - start_date = 1st of previous month (Pacific)
            - end_date = Last day of previous month (Pacific)
            - label = Human-readable month name (e.g., "November 2024")
    """
    today = get_today_pacific()

    # Get first day of current month
    first_of_current_month = date(today.year, today.month, 1)

    # Last day of previous month
    last_day_prev_month = first_of_current_month - timedelta(days=1)

    # First day of previous month
    first_day_prev_month = date(last_day_prev_month.year, last_day_prev_month.month, 1)

    # Format label
    label = first_day_prev_month.strftime("%B %Y")

    return (first_day_prev_month, last_day_prev_month, label)


def get_calendar_period_info(period: str) -> dict:
    """
    Get information about a calendar period for API responses.

    Args:
        period: 'yesterday', 'last_week', or 'last_month'

    Returns:
        dict with:
            - start_date: ISO date string (YYYY-MM-DD)
            - end_date: ISO date string (YYYY-MM-DD)
            - label: Human-readable label for display
            - period_type: 'day', 'week', or 'month'

    Raises:
        ValueError: If period is not recognized
    """
    if period == 'yesterday':
        yesterday_date, _, label = get_yesterday_date_range()
        return {
            'start_date': yesterday_date.isoformat(),
            'end_date': yesterday_date.isoformat(),
            'label': label,
            'period_type': 'day'
        }
    elif period == 'last_week':
        start_utc, end_utc, label = get_last_week_range_utc()
        # Convert back to Pacific dates for the response
        start_date = date_to_pacific(start_utc)
        end_date = date_to_pacific(end_utc - timedelta(seconds=1))  # End is exclusive, so subtract 1s
        return {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'label': label,
            'period_type': 'week'
        }
    elif period == 'last_month':
        start_utc, end_utc, label = get_last_month_range_utc()
        start_date = date_to_pacific(start_utc)
        end_date = date_to_pacific(end_utc - timedelta(seconds=1))
        return {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'label': label,
            'period_type': 'month'
        }
    else:
        raise ValueError(f"Unknown period: {period}. Must be 'yesterday', 'last_week', or 'last_month'")
