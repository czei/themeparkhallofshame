"""
Theme Park Downtime Tracker - Status Calculator
Implements computed_is_open logic from data-model.md.
"""

from typing import Optional


def computed_is_open(wait_time: Optional[int], is_open: Optional[bool]) -> bool:
    """
    Calculate computed ride status based on wait time and API is_open flag.

    Logic from data-model.md:
    - wait_time > 0 → OPEN (overrides is_open flag)
    - wait_time = 0 AND is_open = true → OPEN
    - wait_time = 0 AND is_open = false → CLOSED
    - wait_time = NULL AND is_open = true → OPEN
    - wait_time = NULL AND is_open = false → CLOSED

    Args:
        wait_time: Wait time in minutes from API (can be None)
        is_open: is_open flag from API (can be None)

    Returns:
        True if ride is computed to be open, False otherwise

    Examples:
        >>> computed_is_open(45, False)  # Wait time overrides
        True
        >>> computed_is_open(0, True)  # Open with no wait
        True
        >>> computed_is_open(0, False)  # Closed
        False
        >>> computed_is_open(None, True)  # Rely on is_open flag
        True
        >>> computed_is_open(None, None)  # No data, assume closed
        False
    """
    # If wait_time > 0, ride is definitely open (people are waiting)
    if wait_time is not None and wait_time > 0:
        return True

    # If wait_time = 0 or NULL, check is_open flag
    if is_open is True:
        return True

    # Default to closed if no clear signal
    return False


def validate_wait_time(wait_time: Optional[int]) -> Optional[int]:
    """
    Validate and sanitize wait time value.

    Args:
        wait_time: Raw wait time from API

    Returns:
        Validated wait time or None if invalid

    Examples:
        >>> validate_wait_time(45)
        45
        >>> validate_wait_time(-1)  # Negative values invalid
        None
        >>> validate_wait_time(999)  # Unrealistic values capped
        999
        >>> validate_wait_time(None)
        None
    """
    if wait_time is None:
        return None

    # Negative wait times are invalid
    if wait_time < 0:
        return None

    # Very large wait times (>500 min = 8+ hours) are suspicious but possible
    # Don't cap, let data analysis decide if it's an anomaly
    return wait_time
