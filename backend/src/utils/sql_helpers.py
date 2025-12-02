"""
Theme Park Hall of Shame - Centralized SQL Helpers

This module provides a SINGLE SOURCE OF TRUTH for all ride status and park
operating logic used across queries. Every report and chart should use these
helpers to ensure consistent calculations.

IMPORTANT: If you need to change how ride status or park operating hours are
determined, change it HERE and it will apply everywhere.

NOTE: All core metric constants are defined in metrics.py - SQL helpers
import from there to ensure consistency with Python calculations.
"""

from utils.metrics import (
    SNAPSHOT_INTERVAL_MINUTES,
    SHAME_SCORE_MULTIPLIER,
    SHAME_SCORE_PRECISION,
)


class RideStatusSQL:
    """
    Centralized SQL fragments for ride status calculations.

    All queries should use these methods instead of writing inline SQL
    for status determination. This ensures:
    - Consistent status logic across all reports
    - Single place to update rules
    - Matching counts between panels and tables
    """

    # Core status expression - maps NULL status to OPERATING/DOWN based on computed_is_open
    STATUS_EXPR = "COALESCE(status, IF(computed_is_open, 'OPERATING', 'DOWN'))"

    # Time window for "live" data - only consider snapshots from last 2 hours
    LIVE_WINDOW_HOURS = 2

    @staticmethod
    def status_expression(table_alias: str = "rss") -> str:
        """
        Get the SQL expression for computing a ride's status.

        Maps NULL status (Queue-Times data) to OPERATING/DOWN based on computed_is_open.
        ThemeParks.wiki data has explicit status values.

        Args:
            table_alias: The alias used for ride_status_snapshots table

        Returns:
            SQL expression that evaluates to OPERATING/DOWN/CLOSED/REFURBISHMENT
        """
        return f"COALESCE({table_alias}.status, IF({table_alias}.computed_is_open, 'OPERATING', 'DOWN'))"

    @staticmethod
    def is_operating(table_alias: str = "rss") -> str:
        """
        Get SQL condition for checking if a ride is operating.

        Args:
            table_alias: The alias used for ride_status_snapshots table

        Returns:
            SQL condition that is TRUE when ride is operating
        """
        return f"({table_alias}.status = 'OPERATING' OR ({table_alias}.status IS NULL AND {table_alias}.computed_is_open = TRUE))"

    @staticmethod
    def is_down(table_alias: str = "rss", parks_alias: str = None) -> str:
        """
        Get SQL condition for checking if a ride is down (not operating).

        PARK-TYPE AWARE LOGIC:
        - Disney/Universal parks: Only count DOWN status (not CLOSED)
          because they properly distinguish between breakdowns (DOWN) and
          scheduled closures (CLOSED)
        - Other parks: Include both DOWN and CLOSED because many parks
          (Dollywood, Busch Gardens, etc.) only report CLOSED for all
          non-operating rides. The has_operated_subquery filter ensures
          we only count rides that were operating at some point.

        Note: REFURBISHMENT is excluded as it indicates long-term scheduled maintenance.

        Args:
            table_alias: The alias used for ride_status_snapshots table
            parks_alias: Optional alias for parks table. If provided, uses
                        park-type-aware logic. If None, uses legacy behavior.

        Returns:
            SQL condition that is TRUE when ride is down
        """
        if parks_alias:
            # Park-type aware logic
            return f"""(
                CASE
                    WHEN ({parks_alias}.is_disney = TRUE OR {parks_alias}.is_universal = TRUE) THEN
                        -- Disney/Universal: Only count explicit DOWN status
                        {table_alias}.status = 'DOWN'
                    ELSE
                        -- Other parks: Count DOWN, CLOSED, or computed_is_open=FALSE
                        ({table_alias}.status IN ('DOWN', 'CLOSED') OR ({table_alias}.status IS NULL AND {table_alias}.computed_is_open = FALSE))
                END
            )"""
        else:
            # Legacy behavior (backwards compatibility)
            return f"({table_alias}.status IN ('DOWN', 'CLOSED') OR ({table_alias}.status IS NULL AND {table_alias}.computed_is_open = FALSE))"

    @staticmethod
    def is_down_disney_universal(table_alias: str = "rss") -> str:
        """
        Get SQL condition for checking if a ride is down at a Disney/Universal park.

        Disney and Universal parks properly distinguish between:
        - DOWN: Unexpected breakdown (counts as downtime)
        - CLOSED: Scheduled closure (does NOT count as downtime)

        Args:
            table_alias: The alias used for ride_status_snapshots table

        Returns:
            SQL condition that is TRUE when ride is DOWN (broken)
        """
        return f"{table_alias}.status = 'DOWN'"

    @staticmethod
    def is_down_other_parks(table_alias: str = "rss") -> str:
        """
        Get SQL condition for checking if a ride is down at non-Disney/Universal parks.

        Most other parks (Dollywood, Busch Gardens, SeaWorld, etc.) only report
        CLOSED status for all non-operating rides - they don't distinguish between
        breakdowns and scheduled closures. We include both DOWN and CLOSED.

        Use with a stricter has_operated filter to avoid counting seasonal closures.

        Args:
            table_alias: The alias used for ride_status_snapshots table

        Returns:
            SQL condition that is TRUE when ride is not operating
        """
        return f"({table_alias}.status IN ('DOWN', 'CLOSED') OR ({table_alias}.status IS NULL AND {table_alias}.computed_is_open = FALSE))"

    # Minimum operating snapshots required for non-Disney/Universal parks
    # to count their CLOSED status as downtime (6 snapshots = 30 minutes)
    MIN_OPERATING_SNAPSHOTS_OTHER_PARKS = 6

    @staticmethod
    def has_operated_subquery(ride_id_expr: str, start_param: str = ":start_utc", end_param: str = ":end_utc") -> str:
        """
        Get SQL EXISTS subquery to check if a ride has operated during a period.

        SINGLE SOURCE OF TRUTH: This determines if a ride is a "breakdown" vs "seasonal closure".
        A ride that has NEVER operated during the period is a seasonal closure, not a breakdown.

        SQLAlchemy equivalent: StatusExpressions.has_operated_today_subquery()
        in database/queries/builders/expressions.py
        BOTH MUST STAY IN SYNC - if you change this, update that too!

        This is used to:
        1. Exclude seasonal closures from "Rides DOWN" count in status summary panel
        2. Exclude seasonal closures from downtime rankings table
        3. Ensure panel count matches table count for DOWN rides

        A ride is considered to have operated if it had at least one snapshot
        with status='OPERATING' or computed_is_open=TRUE.

        Args:
            ride_id_expr: Expression for the ride_id to check (e.g., "r.ride_id")
            start_param: SQL parameter name for start time
            end_param: SQL parameter name for end time

        Returns:
            SQL EXISTS clause that is TRUE if ride has operated during period

        Example:
            # Only count downtime for rides that have operated today
            has_operated = RideStatusSQL.has_operated_subquery("r.ride_id")
            query = f"... AND {has_operated} ..."
        """
        return f"""EXISTS (
            SELECT 1 FROM ride_status_snapshots rss_op
            WHERE rss_op.ride_id = {ride_id_expr}
            AND rss_op.recorded_at >= {start_param}
            AND rss_op.recorded_at < {end_param}
            AND (rss_op.status = 'OPERATING' OR (rss_op.status IS NULL AND rss_op.computed_is_open = TRUE))
        )"""

    @staticmethod
    def has_operated_minimum_subquery(
        ride_id_expr: str,
        start_param: str = ":start_utc",
        end_param: str = ":end_utc",
        min_snapshots: int = None
    ) -> str:
        """
        Get SQL subquery to check if a ride has operated for a minimum duration.

        STRICTER VERSION for non-Disney/Universal parks:
        Requires at least MIN_OPERATING_SNAPSHOTS_OTHER_PARKS (6) operating snapshots
        before counting CLOSED as downtime. This filters out rides that only
        operated briefly (e.g., for testing) before going CLOSED.

        Args:
            ride_id_expr: Expression for the ride_id to check (e.g., "r.ride_id")
            start_param: SQL parameter name for start time
            end_param: SQL parameter name for end time
            min_snapshots: Minimum number of operating snapshots required
                          (defaults to MIN_OPERATING_SNAPSHOTS_OTHER_PARKS)

        Returns:
            SQL subquery that returns TRUE if ride has operated enough

        Example:
            # Only count downtime for rides that have operated at least 30 min
            has_min = RideStatusSQL.has_operated_minimum_subquery("r.ride_id")
            query = f"... AND {has_min} ..."
        """
        if min_snapshots is None:
            min_snapshots = RideStatusSQL.MIN_OPERATING_SNAPSHOTS_OTHER_PARKS

        return f"""(
            SELECT COUNT(*) >= {min_snapshots}
            FROM ride_status_snapshots rss_op
            WHERE rss_op.ride_id = {ride_id_expr}
            AND rss_op.recorded_at >= {start_param}
            AND rss_op.recorded_at < {end_param}
            AND (rss_op.status = 'OPERATING' OR (rss_op.status IS NULL AND rss_op.computed_is_open = TRUE))
        )"""

    @staticmethod
    def has_operated_for_park_type(
        ride_id_expr: str,
        parks_alias: str,
        start_param: str = ":start_utc",
        end_param: str = ":end_utc"
    ) -> str:
        """
        Get park-type-aware "has operated" check.

        - Disney/Universal: Just needs 1 operating snapshot (they have proper DOWN status)
        - Other parks: Need at least MIN_OPERATING_SNAPSHOTS_OTHER_PARKS (6) snapshots
          to count CLOSED as downtime (filters out seasonal/weather closures)

        Args:
            ride_id_expr: Expression for the ride_id to check
            parks_alias: Alias for the parks table
            start_param: SQL parameter name for start time
            end_param: SQL parameter name for end time

        Returns:
            SQL condition that returns TRUE if ride has operated enough for its park type
        """
        min_snaps = RideStatusSQL.MIN_OPERATING_SNAPSHOTS_OTHER_PARKS

        return f"""(
            CASE
                WHEN ({parks_alias}.is_disney = TRUE OR {parks_alias}.is_universal = TRUE) THEN
                    -- Disney/Universal: Just need 1 operating snapshot
                    EXISTS (
                        SELECT 1 FROM ride_status_snapshots rss_op
                        WHERE rss_op.ride_id = {ride_id_expr}
                        AND rss_op.recorded_at >= {start_param}
                        AND rss_op.recorded_at < {end_param}
                        AND (rss_op.status = 'OPERATING' OR (rss_op.status IS NULL AND rss_op.computed_is_open = TRUE))
                    )
                ELSE
                    -- Other parks: Need at least {min_snaps} operating snapshots (30 min)
                    (
                        SELECT COUNT(*) >= {min_snaps}
                        FROM ride_status_snapshots rss_op
                        WHERE rss_op.ride_id = {ride_id_expr}
                        AND rss_op.recorded_at >= {start_param}
                        AND rss_op.recorded_at < {end_param}
                        AND (rss_op.status = 'OPERATING' OR (rss_op.status IS NULL AND rss_op.computed_is_open = TRUE))
                    )
            END
        )"""

    @staticmethod
    def current_status_subquery(
        ride_id_expr: str = "r.ride_id",
        include_time_window: bool = True,
        alias: str = "current_status",
        park_id_expr: str = None
    ) -> str:
        """
        Get SQL subquery for a ride's current status from latest snapshot.

        IMPORTANT: When include_time_window=True (default), only considers snapshots
        from the last 2 hours. This ensures consistency with the status summary panel.

        IMPORTANT: When park_id_expr is provided, returns 'PARK_CLOSED' if the park
        is not currently open. This ensures rides at closed parks don't show as 'DOWN'.

        Args:
            ride_id_expr: Expression for the ride_id to look up
            include_time_window: If True, only looks at last 2 hours (matches panel)
            alias: Column alias for the result
            park_id_expr: Expression for the park_id (e.g., "r.park_id" or "p.park_id")
                         If provided, will return PARK_CLOSED when park is not open

        Returns:
            SQL subquery that returns the current status enum
        """
        time_filter = ""
        if include_time_window:
            time_filter = f"AND rss_inner.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)"

        # If park_id_expr is provided, wrap the status with park-open check
        if park_id_expr:
            # Check if park is currently open
            park_open_check = f"""
                SELECT pas_inner.park_appears_open
                FROM park_activity_snapshots pas_inner
                WHERE pas_inner.park_id = {park_id_expr}
                    AND pas_inner.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)
                ORDER BY pas_inner.recorded_at DESC
                LIMIT 1
            """
            return f"""(
                SELECT CASE
                    WHEN ({park_open_check}) = FALSE THEN 'PARK_CLOSED'
                    ELSE COALESCE(rss_inner.status, IF(rss_inner.computed_is_open, 'OPERATING', 'DOWN'))
                END
                FROM ride_status_snapshots rss_inner
                WHERE rss_inner.ride_id = {ride_id_expr}
                    {time_filter}
                ORDER BY rss_inner.recorded_at DESC
                LIMIT 1
            ) AS {alias}"""
        else:
            # Original behavior without park awareness (for backwards compatibility)
            return f"""(
                SELECT COALESCE(rss_inner.status, IF(rss_inner.computed_is_open, 'OPERATING', 'DOWN'))
                FROM ride_status_snapshots rss_inner
                WHERE rss_inner.ride_id = {ride_id_expr}
                    {time_filter}
                ORDER BY rss_inner.recorded_at DESC
                LIMIT 1
            ) AS {alias}"""

    @staticmethod
    def current_is_open_subquery(
        ride_id_expr: str = "r.ride_id",
        include_time_window: bool = True,
        alias: str = "current_is_open",
        park_id_expr: str = None
    ) -> str:
        """
        Get SQL subquery for whether a ride is currently operating (boolean).

        This is the boolean version of current_status_subquery for frontend compatibility.

        IMPORTANT: When park_id_expr is provided, returns FALSE if the park is not
        currently open (since a ride can't be "operating" if the park is closed).

        Args:
            ride_id_expr: Expression for the ride_id to look up
            include_time_window: If True, only looks at last 2 hours (matches panel)
            alias: Column alias for the result
            park_id_expr: Expression for the park_id (e.g., "r.park_id" or "p.park_id")
                         If provided, will return FALSE when park is not open

        Returns:
            SQL subquery that returns TRUE if ride is operating, FALSE otherwise
        """
        time_filter = ""
        if include_time_window:
            time_filter = f"AND rss_inner.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)"

        # If park_id_expr is provided, wrap the check with park-open check
        if park_id_expr:
            # Check if park is currently open
            park_open_check = f"""
                SELECT pas_inner.park_appears_open
                FROM park_activity_snapshots pas_inner
                WHERE pas_inner.park_id = {park_id_expr}
                    AND pas_inner.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)
                ORDER BY pas_inner.recorded_at DESC
                LIMIT 1
            """
            return f"""(
                SELECT CASE
                    WHEN ({park_open_check}) = FALSE THEN FALSE
                    ELSE COALESCE(rss_inner.status, IF(rss_inner.computed_is_open, 'OPERATING', 'DOWN')) = 'OPERATING'
                END
                FROM ride_status_snapshots rss_inner
                WHERE rss_inner.ride_id = {ride_id_expr}
                    {time_filter}
                ORDER BY rss_inner.recorded_at DESC
                LIMIT 1
            ) AS {alias}"""
        else:
            # Original behavior without park awareness (for backwards compatibility)
            return f"""(
                SELECT COALESCE(rss_inner.status, IF(rss_inner.computed_is_open, 'OPERATING', 'DOWN')) = 'OPERATING'
                FROM ride_status_snapshots rss_inner
                WHERE rss_inner.ride_id = {ride_id_expr}
                    {time_filter}
                ORDER BY rss_inner.recorded_at DESC
                LIMIT 1
            ) AS {alias}"""


class ParkStatusSQL:
    """
    Centralized SQL fragments for park operating status calculations.

    SCHEDULE-BASED LOGIC (preferred when available):
    Uses park_schedules table from ThemeParks.wiki API for accurate operating hours.
    Falls back to heuristic (park_appears_open) when schedule data is unavailable.

    Schedule data is considered available if:
    - park_schedules has an entry for today with schedule_type = 'OPERATING'
    - Both opening_time and closing_time are not NULL
    """

    @staticmethod
    def park_is_open_subquery(
        park_id_expr: str = "p.park_id",
        alias: str = "park_is_open"
    ) -> str:
        """
        Get SQL subquery for whether a park is currently open.

        Priority:
        1. Use park_schedules if available (actual operating hours from API)
        2. Fall back to park_appears_open heuristic (inferred from ride activity)

        IMPORTANT: Uses time-range check (NOW() between opening/closing) rather than
        date matching to handle timezone differences correctly. The database is UTC
        but schedules are stored by local date, so we check if NOW() falls within
        any schedule's actual opening/closing timestamps.

        Args:
            park_id_expr: Expression for the park_id to check
            alias: Column alias for the result

        Returns:
            SQL subquery that returns 1 if park is open, 0 otherwise
        """
        return f"""(
            SELECT CASE
                -- Check if NOW() falls within any OPERATING schedule's time window
                -- Uses time-range check instead of date match to handle UTC/local timezone correctly
                WHEN EXISTS (
                    SELECT 1 FROM park_schedules ps
                    WHERE ps.park_id = {park_id_expr}
                        AND ps.schedule_type = 'OPERATING'
                        AND ps.opening_time IS NOT NULL
                        AND ps.closing_time IS NOT NULL
                        AND NOW() >= ps.opening_time
                        AND NOW() <= ps.closing_time
                ) THEN 1
                -- Fall back to heuristic if no schedule matches
                ELSE (
                    SELECT CASE WHEN pas2.park_appears_open = TRUE THEN 1 ELSE 0 END
                    FROM park_activity_snapshots pas2
                    WHERE pas2.park_id = {park_id_expr}
                        AND pas2.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)
                    ORDER BY pas2.recorded_at DESC
                    LIMIT 1
                )
            END
        ) AS {alias}"""

    @staticmethod
    def park_appears_open_filter(table_alias: str = "pas") -> str:
        """
        Get SQL condition for filtering to only open parks.

        Uses park_activity_snapshots.park_appears_open field as the base check.
        Note: For schedule-based filtering in historical queries, use
        park_is_open_at_time_filter() instead.

        Args:
            table_alias: The alias used for park_activity_snapshots table

        Returns:
            SQL condition that is TRUE when park is open
        """
        return f"{table_alias}.park_appears_open = TRUE"

    @staticmethod
    def park_is_open_at_time_filter(
        park_id_expr: str = "p.park_id",
        timestamp_expr: str = "rss.recorded_at",
        pas_alias: str = "pas"
    ) -> str:
        """
        Get SQL condition for checking if park was open at a specific timestamp.

        Priority:
        1. Use park_schedules if available for the date
        2. Fall back to park_appears_open from park_activity_snapshots

        This is for historical queries where we check each snapshot timestamp.

        Args:
            park_id_expr: Expression for the park_id to check
            timestamp_expr: Expression for the timestamp to check
            pas_alias: Alias for park_activity_snapshots table (fallback)

        Returns:
            SQL condition that is TRUE when park was open at that time
        """
        return f"""(
            -- Check if park has schedule for the date
            CASE WHEN EXISTS (
                SELECT 1 FROM park_schedules ps
                WHERE ps.park_id = {park_id_expr}
                    AND ps.schedule_date = DATE({timestamp_expr})
                    AND ps.schedule_type = 'OPERATING'
                    AND ps.opening_time IS NOT NULL
                    AND ps.closing_time IS NOT NULL
            ) THEN (
                -- Use schedule: check if timestamp is within operating hours
                SELECT {timestamp_expr} >= ps2.opening_time AND {timestamp_expr} <= ps2.closing_time
                FROM park_schedules ps2
                WHERE ps2.park_id = {park_id_expr}
                    AND ps2.schedule_date = DATE({timestamp_expr})
                    AND ps2.schedule_type = 'OPERATING'
                    AND ps2.opening_time IS NOT NULL
                    AND ps2.closing_time IS NOT NULL
                ORDER BY ps2.opening_time
                LIMIT 1
            )
            -- Fall back to heuristic
            ELSE {pas_alias}.park_appears_open = TRUE
            END
        )"""

    @staticmethod
    def latest_snapshot_join_sql(
        park_alias: str = "p",
        start_param: str = ":start_utc",
        end_param: str = ":end_utc",
    ) -> str:
        """
        Get SQL JOIN clauses for filtering to parks that appear open.

        Use this in time-range queries (charts, historical data) to exclude
        closed/seasonal parks. Joins to latest park_activity_snapshot in the
        time range and filters where park_appears_open = TRUE.

        Args:
            park_alias: Alias for parks table (default "p")
            start_param: Parameter name for start time (default ":start_utc")
            end_param: Parameter name for end time (default ":end_utc")

        Returns:
            SQL string containing INNER JOIN clauses and filter condition

        Usage:
            query = f'''
                SELECT ...
                FROM parks p
                {ParkStatusSQL.latest_snapshot_join_sql("p")}
                WHERE ...
            '''
        """
        return f"""
            INNER JOIN (
                SELECT park_id, MAX(snapshot_id) AS max_snapshot_id
                FROM park_activity_snapshots
                WHERE recorded_at >= {start_param} AND recorded_at < {end_param}
                GROUP BY park_id
            ) latest_pas ON {park_alias}.park_id = latest_pas.park_id
            INNER JOIN park_activity_snapshots pas
                ON pas.park_id = {park_alias}.park_id
                AND pas.snapshot_id = latest_pas.max_snapshot_id
                AND pas.park_appears_open = TRUE"""


class DowntimeSQL:
    """
    Centralized SQL fragments for downtime calculations.
    """

    # Import from metrics.py for single source of truth
    # (class attribute referencing module-level import)

    @staticmethod
    def downtime_minutes_sum(
        rss_alias: str = "rss",
        pas_alias: str = "pas"
    ) -> str:
        """
        Get SQL expression for summing downtime minutes from snapshots.

        Only counts downtime when park is open. Each snapshot = 5 minutes.

        Args:
            rss_alias: Alias for ride_status_snapshots table
            pas_alias: Alias for park_activity_snapshots table

        Returns:
            SQL expression that sums to total downtime minutes
        """
        is_down = RideStatusSQL.is_down(rss_alias)
        park_open = ParkStatusSQL.park_appears_open_filter(pas_alias)

        return f"""SUM(CASE
            WHEN {park_open} AND {is_down}
            THEN {SNAPSHOT_INTERVAL_MINUTES}
            ELSE 0
        END)"""

    @staticmethod
    def downtime_hours_rounded(
        rss_alias: str = "rss",
        pas_alias: str = "pas",
        decimal_places: int = 2
    ) -> str:
        """
        Get SQL expression for downtime hours (rounded).

        Args:
            rss_alias: Alias for ride_status_snapshots table
            pas_alias: Alias for park_activity_snapshots table
            decimal_places: Number of decimal places to round to

        Returns:
            SQL expression for downtime hours
        """
        minutes = DowntimeSQL.downtime_minutes_sum(rss_alias, pas_alias)
        return f"ROUND({minutes} / 60.0, {decimal_places})"

    @staticmethod
    def weighted_downtime_hours(
        rss_alias: str = "rss",
        pas_alias: str = "pas",
        tier_weight_expr: str = "COALESCE(rc.tier_weight, 2)",
        decimal_places: int = 2
    ) -> str:
        """
        Get SQL expression for tier-weighted downtime hours.

        Tier 1 rides (flagship) have weight 3, Tier 2 = 2, Tier 3 = 1.

        Args:
            rss_alias: Alias for ride_status_snapshots table
            pas_alias: Alias for park_activity_snapshots table
            tier_weight_expr: Expression for getting the tier weight
            decimal_places: Number of decimal places to round to

        Returns:
            SQL expression for weighted downtime hours
        """
        is_down = RideStatusSQL.is_down(rss_alias)
        park_open = ParkStatusSQL.park_appears_open_filter(pas_alias)

        return f"""ROUND(
            SUM(CASE
                WHEN {park_open} AND {is_down}
                THEN {SNAPSHOT_INTERVAL_MINUTES} * {tier_weight_expr}
                ELSE 0
            END) / 60.0,
            {decimal_places}
        )"""


class UptimeSQL:
    """
    Centralized SQL fragments for uptime calculations.
    """

    @staticmethod
    def uptime_percentage(
        rss_alias: str = "rss",
        pas_alias: str = "pas",
        decimal_places: int = 2
    ) -> str:
        """
        Get SQL expression for uptime percentage during park operating hours.

        Only considers time when park is open. Returns percentage (0-100).

        Args:
            rss_alias: Alias for ride_status_snapshots table
            pas_alias: Alias for park_activity_snapshots table
            decimal_places: Number of decimal places to round to

        Returns:
            SQL expression for uptime percentage
        """
        is_operating = RideStatusSQL.is_operating(rss_alias)
        park_open = ParkStatusSQL.park_appears_open_filter(pas_alias)

        return f"""ROUND(
            100.0 * SUM(CASE
                WHEN {park_open} AND {is_operating}
                THEN 1
                ELSE 0
            END) /
            NULLIF(SUM(CASE WHEN {park_open} THEN 1 ELSE 0 END), 0),
            {decimal_places}
        )"""


class RideFilterSQL:
    """
    Centralized SQL fragments for ride filtering.
    """

    @staticmethod
    def active_attractions_filter(
        rides_alias: str = "r",
        parks_alias: str = "p"
    ) -> str:
        """
        Get SQL condition for filtering to active attraction rides at active parks.

        Args:
            rides_alias: Alias for rides table
            parks_alias: Alias for parks table

        Returns:
            SQL condition for active attractions
        """
        return f"{rides_alias}.is_active = TRUE AND {rides_alias}.category = 'ATTRACTION' AND {parks_alias}.is_active = TRUE"

    @staticmethod
    def disney_universal_filter(parks_alias: str = "p") -> str:
        """
        Get SQL condition for filtering to Disney and Universal parks only.

        Args:
            parks_alias: Alias for parks table

        Returns:
            SQL condition for Disney/Universal parks
        """
        return f"({parks_alias}.is_disney = TRUE OR {parks_alias}.is_universal = TRUE)"

    @staticmethod
    def live_time_window_filter(
        recorded_at_expr: str = "rss.recorded_at"
    ) -> str:
        """
        Get SQL condition for filtering to snapshots within the live time window.

        Args:
            recorded_at_expr: Expression for the recorded_at timestamp

        Returns:
            SQL condition for live time window
        """
        return f"{recorded_at_expr} >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)"


class ShameScoreSQL:
    """
    Centralized SQL for shame score calculations.

    IMPORTANT: Shame Score is LIVE/INSTANTANEOUS - it measures the proportion
    of park weight that is currently down, NOT cumulative downtime over time.

    Formula: (sum of tier_weights for rides currently down / total_park_weight) × 10

    This gives a 0-10 scale where:
    - 0 = All rides operating
    - 10 = 100% of park weight is down

    Constants imported from metrics.py (single source of truth):
    - SHAME_SCORE_MULTIPLIER = 10
    - SHAME_SCORE_PRECISION = 1
    """

    @staticmethod
    def instantaneous_shame_score(
        tier_weight_expr: str = "COALESCE(rc.tier_weight, 2)",
        total_weight_expr: str = "pw.total_park_weight",
        currently_down_condition: str = "rcd.ride_id IS NOT NULL",
    ) -> str:
        """
        Get SQL expression for instantaneous shame score.

        This calculates shame based on rides CURRENTLY down, not cumulative
        downtime over time. It's a snapshot of "how bad is it RIGHT NOW".

        Args:
            tier_weight_expr: Expression for individual ride tier weight
            total_weight_expr: Expression for total park weight
            currently_down_condition: Condition that is TRUE when ride is currently down

        Returns:
            SQL expression for shame score (0-10 scale, 1 decimal place)

        Usage in query:
            Must have:
            - rides_currently_down CTE (rcd) with ride_id for currently down rides
            - park_weights CTE (pw) with total_park_weight
            - LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        """
        # Use a subquery to get DISTINCT sum of tier weights for currently down rides
        # This avoids counting each ride multiple times across snapshots
        return f"""(
            SELECT ROUND(
                COALESCE(SUM(COALESCE(rc_inner.tier_weight, 2)), 0) / NULLIF({total_weight_expr}, 0) * {SHAME_SCORE_MULTIPLIER},
                {SHAME_SCORE_PRECISION}
            )
            FROM rides r_inner
            LEFT JOIN ride_classifications rc_inner ON r_inner.ride_id = rc_inner.ride_id
            WHERE r_inner.ride_id IN (SELECT ride_id FROM rides_currently_down WHERE park_id = p.park_id)
        )"""

    @staticmethod
    def rides_currently_down_cte(
        start_param: str = ":start_utc",
        end_param: str = ":end_utc",
        filter_clause: str = ""
    ) -> str:
        """
        Get SQL CTE for identifying rides that are currently down.

        "Currently down" means:
        1. The ride's LATEST snapshot shows it as DOWN
        2. The park appears open at that snapshot time

        Args:
            start_param: Parameter for period start (usually today's start)
            end_param: Parameter for period end (usually today's end)
            filter_clause: Optional additional filter (e.g., Disney/Universal only)

        Returns:
            SQL CTE definition (without the WITH keyword)
        """
        is_down = RideStatusSQL.is_down("rss_latest")
        park_open = ParkStatusSQL.park_appears_open_filter("pas_latest")

        return f"""latest_snapshot AS (
            -- Find the most recent snapshot timestamp for each ride
            SELECT ride_id, MAX(recorded_at) as latest_recorded_at
            FROM ride_status_snapshots
            WHERE recorded_at >= {start_param} AND recorded_at < {end_param}
            GROUP BY ride_id
        ),
        rides_currently_down AS (
            -- Identify rides that are DOWN in their latest snapshot
            SELECT DISTINCT r_inner.ride_id, r_inner.park_id
            FROM rides r_inner
            INNER JOIN ride_status_snapshots rss_latest ON r_inner.ride_id = rss_latest.ride_id
            INNER JOIN latest_snapshot ls ON rss_latest.ride_id = ls.ride_id
                AND rss_latest.recorded_at = ls.latest_recorded_at
            INNER JOIN park_activity_snapshots pas_latest ON r_inner.park_id = pas_latest.park_id
                AND pas_latest.recorded_at = rss_latest.recorded_at
            WHERE r_inner.is_active = TRUE
                AND r_inner.category = 'ATTRACTION'
                AND {is_down}
                AND {park_open}
                {filter_clause}
        )"""

    @staticmethod
    def park_weights_cte(
        has_operated_condition: str = "",
        filter_clause: str = ""
    ) -> str:
        """
        Get SQL CTE for calculating total park weight.

        Args:
            has_operated_condition: Optional condition to only count rides that have operated
            filter_clause: Optional additional filter (e.g., Disney/Universal only)

        Returns:
            SQL CTE definition (without the WITH keyword or comma)
        """
        return f"""park_weights AS (
            SELECT
                p.park_id,
                SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id AND r.is_active = TRUE
                AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE p.is_active = TRUE
                {has_operated_condition}
                {filter_clause}
            GROUP BY p.park_id
        )"""


class AffectedRidesSQL:
    """
    Centralized SQL for counting affected rides.
    """

    @staticmethod
    def count_distinct_down_rides(
        ride_id_expr: str = "r.ride_id",
        rss_alias: str = "rss",
        pas_alias: str = "pas"
    ) -> str:
        """
        Get SQL expression for counting distinct rides that experienced downtime.

        Args:
            ride_id_expr: Expression for the ride_id
            rss_alias: Alias for ride_status_snapshots table
            pas_alias: Alias for park_activity_snapshots table

        Returns:
            SQL expression for count of affected rides
        """
        is_down = RideStatusSQL.is_down(rss_alias)
        park_open = ParkStatusSQL.park_appears_open_filter(pas_alias)

        return f"""COUNT(DISTINCT CASE
            WHEN {park_open} AND {is_down}
            THEN {ride_id_expr}
        END)"""
