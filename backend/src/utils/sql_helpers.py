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
    def is_down(table_alias: str = "rss") -> str:
        """
        Get SQL condition for checking if a ride is down (unscheduled breakdown).

        Note: This is specifically for DOWN status, not CLOSED or REFURBISHMENT.

        Args:
            table_alias: The alias used for ride_status_snapshots table

        Returns:
            SQL condition that is TRUE when ride is down
        """
        return f"({table_alias}.status = 'DOWN' OR ({table_alias}.status IS NULL AND {table_alias}.computed_is_open = FALSE))"

    @staticmethod
    def has_operated_subquery(ride_id_expr: str, start_param: str = ":start_utc", end_param: str = ":end_utc") -> str:
        """
        Get SQL EXISTS subquery to check if a ride has operated during a period.

        This is used to filter out rides that have NEVER operated during the
        measurement period - such rides should not count as having "downtime"
        since they may be seasonally closed or simply not operating.

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
    """

    @staticmethod
    def park_is_open_subquery(
        park_id_expr: str = "p.park_id",
        alias: str = "park_is_open"
    ) -> str:
        """
        Get SQL subquery for whether a park is currently open.

        Uses park_appears_open from park_activity_snapshots for consistency
        with the status summary panel. This ensures the same parks are
        considered "open" in both the panel and the table.

        Args:
            park_id_expr: Expression for the park_id to check
            alias: Column alias for the result

        Returns:
            SQL subquery that returns 1 if park is open, 0 otherwise
        """
        return f"""(
            SELECT CASE WHEN pas2.park_appears_open = TRUE THEN 1 ELSE 0 END
            FROM park_activity_snapshots pas2
            WHERE pas2.park_id = {park_id_expr}
                AND pas2.recorded_at >= DATE_SUB(NOW(), INTERVAL {RideStatusSQL.LIVE_WINDOW_HOURS} HOUR)
            ORDER BY pas2.recorded_at DESC
            LIMIT 1
        ) AS {alias}"""

    @staticmethod
    def park_appears_open_filter(table_alias: str = "pas") -> str:
        """
        Get SQL condition for filtering to only open parks.

        Uses park_activity_snapshots.park_appears_open field.

        Args:
            table_alias: The alias used for park_activity_snapshots table

        Returns:
            SQL condition that is TRUE when park is open
        """
        return f"{table_alias}.park_appears_open = TRUE"

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
        return f"""ROUND(
            SUM(CASE
                WHEN {currently_down_condition}
                THEN {tier_weight_expr}
                ELSE 0
            END) / NULLIF({total_weight_expr}, 0) * {SHAME_SCORE_MULTIPLIER},
            {SHAME_SCORE_PRECISION}
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
