"""
Theme Park Hall of Shame - Centralized Metric Calculations

SINGLE SOURCE OF TRUTH for all business metrics.
All reports, charts, and API endpoints MUST use these functions.

IMPORTANT: If you need to change how any metric is calculated, change it HERE
and it will apply everywhere consistently.
"""
from typing import Optional


# =============================================================================
# CONSTANTS - Used by both Python calculations and SQL helpers
# =============================================================================

# Each snapshot represents approximately 5 minutes of data
SNAPSHOT_INTERVAL_MINUTES = 5

# Precision for different metric types
SHAME_SCORE_PRECISION = 2
PERCENTAGE_PRECISION = 1
HOURS_PRECISION = 2

# Default tier weight when ride classification is missing
DEFAULT_TIER_WEIGHT = 2


# =============================================================================
# CORE METRIC CALCULATIONS
# =============================================================================

def calculate_downtime_hours(downtime_snapshots: int) -> float:
    """
    Convert downtime snapshots to hours.

    Args:
        downtime_snapshots: Number of 5-minute snapshots where ride was down

    Returns:
        Total downtime in hours
    """
    if downtime_snapshots is None or downtime_snapshots < 0:
        return 0.0
    return round((downtime_snapshots * SNAPSHOT_INTERVAL_MINUTES) / 60.0, HOURS_PRECISION)


def calculate_uptime_percentage(
    operating_snapshots: int,
    total_snapshots: int
) -> Optional[float]:
    """
    Calculate uptime percentage (0-100).

    Only counts snapshots when park was open.

    Args:
        operating_snapshots: Number of snapshots where ride was operating
        total_snapshots: Total number of snapshots while park was open

    Returns:
        Uptime percentage (0-100), or None if no data
    """
    if total_snapshots is None or total_snapshots <= 0:
        return None
    if operating_snapshots is None:
        operating_snapshots = 0
    return round(100.0 * operating_snapshots / total_snapshots, PERCENTAGE_PRECISION)


def calculate_downtime_percentage(uptime_percentage: Optional[float]) -> Optional[float]:
    """
    Calculate downtime percentage (inverse of uptime).

    Args:
        uptime_percentage: The uptime percentage (0-100)

    Returns:
        Downtime percentage (0-100), or None if no uptime data
    """
    if uptime_percentage is None:
        return None
    return round(100.0 - uptime_percentage, PERCENTAGE_PRECISION)


def calculate_weighted_downtime_hours(
    downtime_hours: float,
    tier_weight: int = DEFAULT_TIER_WEIGHT
) -> float:
    """
    Calculate tier-weighted downtime hours.

    Tier 1 (flagship) rides have weight 3, Tier 2 = 2, Tier 3 = 1.
    Higher weight = more impact on shame score.

    Args:
        downtime_hours: Raw downtime in hours
        tier_weight: Weight based on ride tier (1-3)

    Returns:
        Weighted downtime hours
    """
    if downtime_hours is None:
        return 0.0
    weight = tier_weight if tier_weight else DEFAULT_TIER_WEIGHT
    return round(downtime_hours * weight, HOURS_PRECISION)


def calculate_shame_score(
    total_weighted_downtime_hours: float,
    total_park_weight: float
) -> Optional[float]:
    """
    Calculate park shame score.

    Returns HOURS of weighted downtime per unit weight.
    NOT a percentage!

    Example: A shame score of 2.5 means on average, each "weight unit" of
    rides experienced 2.5 hours of downtime.

    Args:
        total_weighted_downtime_hours: Sum of (downtime_hours * tier_weight) for all rides
        total_park_weight: Sum of all ride tier weights in the park

    Returns:
        Shame score in hours, or None if no weight data
    """
    if total_park_weight is None or total_park_weight <= 0:
        return None
    if total_weighted_downtime_hours is None:
        return 0.0
    return round(total_weighted_downtime_hours / total_park_weight, SHAME_SCORE_PRECISION)


# =============================================================================
# HELPER FUNCTIONS FOR CHART DATA
# =============================================================================

def calculate_hourly_shame_score(
    weighted_downtime_minutes: float,
    total_weight: float
) -> Optional[float]:
    """
    Calculate shame score for a single hour.

    Converts weighted downtime minutes to hours before calculating.

    Args:
        weighted_downtime_minutes: Sum of (downtime_minutes * tier_weight) for hour
        total_weight: Sum of all ride tier weights

    Returns:
        Shame score in hours for that hour
    """
    if total_weight is None or total_weight <= 0:
        return None
    if weighted_downtime_minutes is None:
        return 0.0
    # Convert minutes to hours, then divide by weight
    weighted_downtime_hours = weighted_downtime_minutes / 60.0
    return round(weighted_downtime_hours / total_weight, SHAME_SCORE_PRECISION)


def calculate_hourly_downtime_percentage(
    downtime_minutes: int,
    total_minutes: int = 60
) -> Optional[float]:
    """
    Calculate downtime percentage for a single hour.

    Args:
        downtime_minutes: Minutes of downtime in the hour
        total_minutes: Total minutes in the period (usually 60)

    Returns:
        Downtime percentage (0-100)
    """
    if total_minutes is None or total_minutes <= 0:
        return None
    if downtime_minutes is None:
        return 0.0
    return round(100.0 * downtime_minutes / total_minutes, PERCENTAGE_PRECISION)
