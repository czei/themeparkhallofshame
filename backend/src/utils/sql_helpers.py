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

from utils.metrics import SNAPSHOT_INTERVAL_MINUTES


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
