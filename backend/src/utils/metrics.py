"""
Theme Park Hall of Shame - Centralized Metric Calculations
==========================================================

SINGLE SOURCE OF TRUTH for all business metrics.
All reports, charts, and API endpoints MUST use these functions.

IMPORTANT: If you need to change how any metric is calculated, change it HERE
and it will apply everywhere consistently.

Architecture Overview
---------------------
This file contains the Python implementations and constants for core calculations.
The same logic is implemented in SQL via helper classes:

    utils/metrics.py          <-- YOU ARE HERE (constants, Python functions)
         |
         v
    utils/sql_helpers.py      <-- SQL fragment generators (imports from here)
         |
         +---> database/repositories/stats_repository.py (production queries)
         +---> database/queries/live/*.py (query classes)
         +---> database/queries/rankings/*.py (query classes)

    database/queries/builders/expressions.py  <-- SQLAlchemy expressions
    database/queries/builders/ctes.py         <-- Common Table Expressions

When modifying calculations, update:
1. This file (metrics.py) - the constants and Python functions
2. sql_helpers.py - if the SQL logic changes
3. expressions.py/ctes.py - if SQLAlchemy implementations are affected

Key Concepts
------------
- Snapshot: A 10-minute data point for each ride's status
- Tier Weight: Importance multiplier (Tier 1=3x, Tier 2=2x, Tier 3=1x)
- Shame Score: Weighted downtime hours normalized by park's total weight
- Park appears open: Determined by checking if any rides show as operating

Query Files Using These Calculations
------------------------------------
Production (fast raw SQL via sql_helpers.py):
- database/repositories/stats_repository.py (get_park_live_downtime_rankings, etc.)

Query Classes (use sql_helpers.py):
- database/queries/live/live_park_rankings.py
- database/queries/live/live_ride_rankings.py
- database/queries/rankings/park_downtime_rankings.py
- database/queries/rankings/ride_downtime_rankings.py
- database/queries/trends/*.py
- database/queries/charts/*.py
"""
from typing import Optional


# =============================================================================
# CONSTANTS - Used by both Python calculations and SQL helpers
# =============================================================================

# Each snapshot represents approximately 10 minutes of data
# The collector runs every 10 minutes, so each snapshot = 10 minutes of status
SNAPSHOT_INTERVAL_MINUTES = 10

# Precision for different metric types (decimal places)
SHAME_SCORE_PRECISION = 1   # e.g., 3.4 (on 0-10 scale)
PERCENTAGE_PRECISION = 1    # e.g., 95.5%
HOURS_PRECISION = 2         # e.g., 3.25 hours

# Shame score is multiplied by this to get a 0-10 scale (instead of 0-1)
# This makes scores more intuitive: "3.4" is easier to understand than "0.34"
SHAME_SCORE_MULTIPLIER = 10

# Default tier weight when ride classification is missing
# Tier 1 (flagship) = 3, Tier 2 (major) = 2, Tier 3 (minor) = 1
# Default of 2 assumes unclassified rides are "major" attractions
DEFAULT_TIER_WEIGHT = 2

# Time window for "live" data - only consider snapshots from last 2 hours
# Used by live rankings and real-time status queries
LIVE_WINDOW_HOURS = 2

# Feature flag for hourly aggregation tables
# When True: Use pre-computed hourly tables (park_hourly_stats, ride_hourly_stats)
# When False: Use original GROUP BY HOUR queries on raw snapshots (rollback path)
# Default: False for safe rollback during initial deployment
# Set via environment variable: USE_HOURLY_TABLES=true
import os
USE_HOURLY_TABLES = os.getenv('USE_HOURLY_TABLES', 'false').lower() in ('true', '1', 'yes')


# =============================================================================
# CORE METRIC CALCULATIONS
# =============================================================================

def calculate_downtime_hours(downtime_snapshots: int) -> float:
    """
    Convert downtime snapshots to hours.

    Formula
    -------
    downtime_hours = (downtime_snapshots × SNAPSHOT_INTERVAL_MINUTES) ÷ 60

    Worked Example
    --------------
    If a ride was down for 24 snapshots:
    - Each snapshot = 5 minutes
    - Total downtime = 24 × 5 = 120 minutes
    - In hours = 120 ÷ 60 = 2.0 hours

    Used By
    -------
    - database/queries/rankings/ride_downtime_rankings.py
    - database/queries/charts/ride_downtime_history.py

    How to Modify
    -------------
    - To change snapshot interval: Update SNAPSHOT_INTERVAL_MINUTES constant
    - To change precision: Update HOURS_PRECISION constant
    - SQL equivalent: See queries/builders/expressions.py downtime calculations

    Args:
        downtime_snapshots: Number of 5-minute snapshots where ride was down

    Returns:
        Total downtime in hours (rounded to HOURS_PRECISION decimals)
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

    Only counts snapshots when park was open. This ensures we don't penalize
    rides for being "closed" when the entire park is closed.

    Formula
    -------
    uptime_percentage = (operating_snapshots ÷ total_snapshots) × 100

    Worked Example
    --------------
    A ride during a 12-hour operating day (144 snapshots):
    - Operating snapshots: 136
    - Down snapshots: 8 (40 minutes)
    - Uptime = (136 ÷ 144) × 100 = 94.4%

    Used By
    -------
    - database/queries/rankings/ride_downtime_rankings.py (uptime_percentage column)
    - API response formatting

    How to Modify
    -------------
    - To change precision: Update PERCENTAGE_PRECISION constant
    - To include park-closed time: Remove the park_appears_open filter in SQL
    - SQL equivalent: See queries/rankings/ride_downtime_rankings.py uptime_case

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

    Formula
    -------
    downtime_percentage = 100 - uptime_percentage

    Worked Example
    --------------
    If uptime is 94.4%, then downtime = 100 - 94.4 = 5.6%

    Used By
    -------
    - Trends calculations (percent change in downtime)
    - UI displays that show "X% down" instead of "Y% up"

    How to Modify
    -------------
    - This is a simple inverse; typically you'd modify calculate_uptime_percentage instead

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

    This weights downtime by ride importance. A flagship ride (Tier 1) going
    down for 1 hour counts as 3 weighted hours, while a minor ride (Tier 3)
    going down for 1 hour counts as just 1 weighted hour.

    Tier Weights
    ------------
    - Tier 1 (Flagship): weight = 3 (e.g., Space Mountain, Hagrid's)
    - Tier 2 (Major): weight = 2 (e.g., Pirates, Test Track)
    - Tier 3 (Minor): weight = 1 (e.g., Carousel, playground)
    - Unclassified: weight = 2 (assumes major attraction)

    Formula
    -------
    weighted_downtime = downtime_hours × tier_weight

    Worked Example
    --------------
    Space Mountain (Tier 1) down for 2 hours:
    - Raw downtime: 2.0 hours
    - Tier weight: 3
    - Weighted downtime: 2.0 × 3 = 6.0 weighted hours

    Carousel (Tier 3) down for 2 hours:
    - Raw downtime: 2.0 hours
    - Tier weight: 1
    - Weighted downtime: 2.0 × 1 = 2.0 weighted hours

    Used By
    -------
    - database/queries/builders/ctes.py (WeightedDowntimeCTE)
    - database/queries/rankings/park_downtime_rankings.py

    How to Modify
    -------------
    - To change tier weights: Update ride_classifications table in database
    - To change default weight: Update DEFAULT_TIER_WEIGHT constant
    - To add a new tier: Add to ride_classifications, no code changes needed
    - SQL equivalent: See queries/builders/ctes.py WeightedDowntimeCTE

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
    Calculate park shame score on a 0-10 scale.

    THE CORE METRIC of Theme Park Hall of Shame.

    This is NOT a percentage! It represents the average weighted downtime
    per unit of park capacity, scaled by 10 for readability. Think of it
    as a "reliability rating" where higher is worse.

    Formula
    -------
    shame_score = (total_weighted_downtime_hours ÷ total_park_weight) × 10

    Where:
    - total_weighted_downtime_hours = Σ(ride_downtime_hours × ride_tier_weight)
    - total_park_weight = Σ(ride_tier_weight) for all active rides

    Worked Example
    --------------
    A park with 3 rides:
    - Space Mountain (Tier 1, weight=3): 2 hours down → 6 weighted hours
    - Pirates (Tier 2, weight=2): 1 hour down → 2 weighted hours
    - Carousel (Tier 3, weight=1): 0 hours down → 0 weighted hours

    Calculations:
    - total_weighted_downtime = 6 + 2 + 0 = 8 weighted hours
    - total_park_weight = 3 + 2 + 1 = 6
    - raw_score = 8 ÷ 6 = 1.33
    - shame_score = 1.33 × 10 = 13.3

    Interpretation: A score of 13.3 means significant downtime issues.
    Typical scores range from 0 (perfect) to 10+ (severe problems).

    Why Normalize by Weight?
    ------------------------
    Without normalization, a park with 50 rides would always have more
    total downtime than a park with 10 rides. Dividing by total weight
    makes parks of different sizes comparable.

    Used By
    -------
    - database/queries/rankings/park_downtime_rankings.py (main ranking metric)
    - database/queries/charts/park_shame_history.py (time series)
    - database/queries/live/live_park_rankings.py (today's rankings)

    How to Modify
    -------------
    - To change the formula: Update this function AND the SQL in:
      - queries/builders/ctes.py (WeightedDowntimeCTE, ParkWeightsCTE)
      - queries/rankings/park_downtime_rankings.py (shame_score calculation)
    - To weight by ride popularity instead of tier: Replace tier_weight
      with a popularity metric in ride_classifications table
    - To add time-of-day weighting: Multiply by hour weight factor

    Args:
        total_weighted_downtime_hours: Sum of (downtime_hours × tier_weight) for all rides
        total_park_weight: Sum of all ride tier weights in the park

    Returns:
        Shame score in hours, or None if no weight data
    """
    if total_park_weight is None or total_park_weight <= 0:
        return None
    if total_weighted_downtime_hours is None:
        return 0.0
    raw_score = total_weighted_downtime_hours / total_park_weight
    return round(raw_score * SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION)


def calculate_instantaneous_shame_score(
    total_weighted_down: float,
    total_park_weight: float
) -> Optional[float]:
    """
    Calculate LIVE/INSTANTANEOUS park shame score on a 0-10 scale.

    This is the REAL-TIME shame score measuring what proportion of the
    park's ride capacity is currently down. No time component involved.

    Formula
    -------
    shame_score = (sum of tier_weights for rides currently down / total_park_weight) × 10

    This gives a 0-10 scale where:
    - 0 = All rides operating
    - 10 = 100% of park weight is down (all rides down)

    Worked Example
    --------------
    A park with 3 rides and currently 1 down:
    - Space Mountain (Tier 1, weight=3): DOWN → contributes 3 weight
    - Pirates (Tier 2, weight=2): OPERATING → contributes 0
    - Carousel (Tier 3, weight=1): OPERATING → contributes 0

    Calculations:
    - total_weighted_down = 3
    - total_park_weight = 3 + 2 + 1 = 6
    - shame_score = (3 / 6) × 10 = 5.0

    When to Use This vs calculate_shame_score
    ------------------------------------------
    - calculate_instantaneous_shame_score: For LIVE "right now" displays
    - calculate_shame_score: For historical/cumulative data (period rankings)

    SQL Equivalent
    --------------
    Use ShameScoreSQL.instantaneous_shame_score() from utils/sql_helpers.py

    Args:
        total_weighted_down: Sum of tier_weights for rides currently down
        total_park_weight: Sum of all ride tier weights in the park

    Returns:
        Shame score on 0-10 scale, or None if no weight data
    """
    if total_park_weight is None or total_park_weight <= 0:
        return None
    if total_weighted_down is None:
        return 0.0
    raw_score = total_weighted_down / total_park_weight
    return round(raw_score * SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION)


# =============================================================================
# HELPER FUNCTIONS FOR CHART DATA
# =============================================================================

def calculate_hourly_shame_score(
    weighted_downtime_minutes: float,
    total_weight: float
) -> Optional[float]:
    """
    Calculate shame score for a single hour (used in hourly charts).

    Same formula as calculate_shame_score, but input is in minutes
    and represents a single hour's data point.

    Formula
    -------
    hourly_shame = (weighted_downtime_minutes ÷ 60) ÷ total_weight

    Worked Example
    --------------
    During the 2pm hour:
    - Space Mountain (Tier 1, weight=3): 30 min down → 90 weighted minutes
    - Pirates (Tier 2, weight=2): 0 min down → 0 weighted minutes
    - Total weight: 5

    Calculation:
    - weighted_downtime_minutes = 90
    - weighted_hours = 90 ÷ 60 = 1.5
    - hourly_shame = 1.5 ÷ 5 = 0.30

    Used By
    -------
    - database/queries/charts/park_shame_history.py (when period=today)
    - Hourly granularity chart data

    How to Modify
    -------------
    - For different time granularity: Adjust the 60 divisor
    - SQL equivalent: Same CTE logic but with hourly grouping

    Args:
        weighted_downtime_minutes: Sum of (downtime_minutes × tier_weight) for hour
        total_weight: Sum of all ride tier weights

    Returns:
        Shame score in hours for that hour
    """
    if total_weight is None or total_weight <= 0:
        return None
    if weighted_downtime_minutes is None:
        return 0.0
    # Convert minutes to hours, then divide by weight, then apply multiplier
    weighted_downtime_hours = weighted_downtime_minutes / 60.0
    raw_score = weighted_downtime_hours / total_weight
    return round(raw_score * SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION)


def calculate_hourly_downtime_percentage(
    downtime_minutes: int,
    total_minutes: int = 60
) -> Optional[float]:
    """
    Calculate downtime percentage for a single hour.

    Formula
    -------
    hourly_downtime_pct = (downtime_minutes ÷ total_minutes) × 100

    Worked Example
    --------------
    A ride down for 15 minutes during an hour:
    - downtime_minutes = 15
    - total_minutes = 60
    - percentage = (15 ÷ 60) × 100 = 25.0%

    Used By
    -------
    - database/queries/charts/ride_downtime_history.py (hourly granularity)

    How to Modify
    -------------
    - For partial hours (first/last of day): Pass actual minutes as total_minutes
    - SQL equivalent: Direct calculation in chart queries

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


# =============================================================================
# TREND CALCULATIONS
# =============================================================================

def calculate_percent_change(
    current_value: Optional[float],
    previous_value: Optional[float]
) -> Optional[float]:
    """
    Calculate percent change between two periods.

    Used for "improving" and "declining" trends.

    Formula
    -------
    percent_change = ((current - previous) ÷ previous) × 100

    Interpretation
    --------------
    - Positive value: Metric increased (for downtime, this is BAD)
    - Negative value: Metric decreased (for downtime, this is GOOD)
    - None: Cannot calculate (previous was zero or missing)

    Worked Example
    --------------
    Last week's shame score: 2.0
    This week's shame score: 1.5
    Change = ((1.5 - 2.0) ÷ 2.0) × 100 = -25%
    → Park improved by 25%!

    Used By
    -------
    - database/queries/trends/improving_parks.py
    - database/queries/trends/declining_parks.py
    - database/queries/trends/improving_rides.py
    - database/queries/trends/declining_rides.py

    How to Modify
    -------------
    - To cap extreme values: Add min/max bounds
    - To handle zero→nonzero: Return a special value like float('inf')
    - SQL equivalent: Direct calculation in trends queries

    Args:
        current_value: This period's value
        previous_value: Last period's value

    Returns:
        Percent change, or None if cannot calculate
    """
    if previous_value is None or previous_value == 0:
        return None
    if current_value is None:
        current_value = 0
    return round(((current_value - previous_value) / previous_value) * 100, PERCENTAGE_PRECISION)
