"""
ShameScoreCalculator - Single Source of Truth for shame score calculations.

This calculator ensures consistency across all UI components:
- Rankings table (TODAY period)
- Breakdown panel (park details modal)
- Chart average display

Key Formula:
    shame_score = AVG(per-snapshot instantaneous shame scores)

    Where instantaneous shame at timestamp T =
        (sum of tier_weights for down rides at T) / total_park_weight * 10

Architecture:
    This calculator generates SQL for all shame score calculations,
    ensuring consistent filtering and formulas across all queries.

    The calculator accepts a db_session via dependency injection,
    enabling unit testing with mock sessions.
"""
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.metrics import SHAME_SCORE_PRECISION, SHAME_SCORE_MULTIPLIER

# Feature flag for 7-day hybrid denominator (allows instant rollback)
import os
USE_7_DAY_HYBRID_DENOMINATOR = os.getenv('USE_7_DAY_HYBRID_DENOMINATOR', 'true').lower() == 'true'


class ShameScoreCalculator:
    """
    Single source of truth for shame score SQL generation.

    Usage:
        calc = ShameScoreCalculator(db_connection)
        score = calc.get_average(park_id=1, start=start_dt, end=end_dt)
    """

    def __init__(self, db: Connection):
        """
        Initialize the calculator with a database connection.

        Args:
            db: SQLAlchemy Connection or Session for executing queries
        """
        self.db = db

    def get_effective_park_weight(self, park_id: int, as_of: datetime = None) -> float:
        """
        Get total weight of rides that operated in the last 7 days.
        This is the denominator for shame score calculations.

        Uses the 7-day hybrid denominator approach:
        - Full roster MINUS rides that haven't operated in 7 days
        - Provides stability (no morning volatility)
        - Provides accountability (closed rides don't pad denominator)

        Args:
            park_id: The park to calculate for
            as_of: Reference time (default: now UTC)

        Returns:
            Total tier weight of eligible rides, or 0.0 if none
        """
        if not USE_7_DAY_HYBRID_DENOMINATOR:
            return self.get_park_weight(park_id)  # Rollback path

        # Note: Query uses UTC_TIMESTAMP() for timezone consistency (Zen review fix)
        # The as_of parameter is for testing; production uses UTC_TIMESTAMP()
        query = text("""
            SELECT SUM(COALESCE(rc.tier_weight, 2)) AS effective_weight
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
              AND r.last_operated_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
        """)

        result = self.db.execute(query, {"park_id": park_id})
        weight = result.scalar()

        # Return 0.0 for NULL (no eligible rides) - CRITICAL for division by zero protection
        return float(weight) if weight is not None else 0.0

    def get_park_weight(self, park_id: int) -> float:
        """
        Get full roster weight for a park (all active attractions).
        This is the original denominator before 7-day filtering.

        Used for:
        - Rollback path when USE_7_DAY_HYBRID_DENOMINATOR is False
        - Comparison/validation (effective weight <= full roster weight)

        Args:
            park_id: The park to calculate for

        Returns:
            Total tier weight of all active attractions
        """
        query = text("""
            SELECT SUM(COALESCE(rc.tier_weight, 2)) AS total_weight
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
        """)

        result = self.db.execute(query, {"park_id": park_id})
        weight = result.scalar()

        return float(weight) if weight is not None else 0.0

    def calculate_shame_score(self, down_weight: float, effective_park_weight: float) -> float:
        """
        Calculate shame score with zero-denominator protection.

        Formula: (down_weight / effective_park_weight) * 10

        Returns 0.0 if effective_park_weight is 0 (e.g., seasonal closure,
        no rides operated in 7 days). This is CRITICAL for preventing
        ZeroDivisionError.

        Args:
            down_weight: Sum of tier weights for down rides
            effective_park_weight: Sum of tier weights for eligible rides

        Returns:
            Shame score on 0-10 scale, or 0.0 if no eligible rides
        """
        if not effective_park_weight:
            return 0.0  # No eligible rides = no shame (CRITICAL: division by zero protection)

        return round((down_weight / effective_park_weight) * SHAME_SCORE_MULTIPLIER, SHAME_SCORE_PRECISION)

    def get_instantaneous(
        self,
        park_id: int,
        timestamp: datetime
    ) -> Optional[float]:
        """
        DEPRECATED: Use stored shame_score from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. All queries should READ that value.

        This method is kept for reference/testing only.

        Args:
            park_id: The park to calculate for
            timestamp: The exact moment to check

        Returns:
            Shame score (0-10 scale) or None if no data
        """
        query = text("""
            WITH rides_with_status AS (
                -- Get ride statuses at this timestamp
                SELECT
                    r.ride_id,
                    COALESCE(rc.tier_weight, 2) AS tier_weight,
                    rss.status,
                    rss.computed_is_open,
                    p.is_disney,
                    p.is_universal
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                LEFT JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                    AND rss.recorded_at = :timestamp
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
            ),
            park_weight AS (
                SELECT SUM(tier_weight) AS total_weight
                FROM rides_with_status
            ),
            down_weight AS (
                SELECT SUM(tier_weight) AS total_down_weight
                FROM rides_with_status
                WHERE (
                    -- Disney/Universal: only DOWN counts
                    ((is_disney = TRUE OR is_universal = TRUE) AND status = 'DOWN')
                    OR
                    -- Other parks: DOWN or CLOSED counts
                    ((is_disney = FALSE AND is_universal = FALSE) AND
                        (status = 'DOWN' OR status = 'CLOSED' OR
                         (status IS NULL AND computed_is_open = 0)))
                )
            )
            SELECT
                pw.total_weight,
                COALESCE(dw.total_down_weight, 0) AS total_down_weight,
                CASE
                    WHEN pw.total_weight IS NULL OR pw.total_weight = 0 THEN NULL
                    ELSE ROUND(
                        (COALESCE(dw.total_down_weight, 0) / pw.total_weight) * :multiplier,
                        :precision
                    )
                END AS shame_score
            FROM park_weight pw, down_weight dw
        """)

        result = self.db.execute(query, {
            "park_id": park_id,
            "timestamp": timestamp,
            "multiplier": SHAME_SCORE_MULTIPLIER,
            "precision": SHAME_SCORE_PRECISION
        })
        row = result.fetchone()

        if row is None:
            return None

        return row.shame_score

    def get_average(
        self,
        park_id: int,
        start: datetime,
        end: datetime
    ) -> Optional[float]:
        """
        DEPRECATED: Use AVG(pas.shame_score) from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For averages, use:
            SELECT AVG(shame_score) FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start AND recorded_at < :end

        This method is kept for reference/testing only.

        Args:
            park_id: The park to calculate for
            start: Start of the time range (inclusive, UTC)
            end: End of the time range (exclusive, UTC)

        Returns:
            Average shame score (0-10 scale) or None if no data
        """
        query = text("""
            WITH rides_that_operated AS (
                -- Only include rides that had at least one OPERATING snapshot
                SELECT DISTINCT r.ride_id
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start AND rss.recorded_at < :end
                    AND pas.park_appears_open = TRUE
                    AND (rss.status = 'OPERATING'
                         OR (rss.status IS NULL AND rss.computed_is_open = 1))
            ),
            park_weights AS (
                -- Calculate total weight using only rides that operated
                SELECT
                    r.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM rides r
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY r.park_id
            ),
            per_snapshot_shame AS (
                -- Calculate instantaneous shame for each snapshot
                SELECT
                    rss.recorded_at,
                    SUM(CASE
                        WHEN (
                            -- Disney/Universal: only DOWN counts
                            ((p.is_disney = TRUE OR p.is_universal = TRUE)
                                AND rss.status = 'DOWN')
                            OR
                            -- Other parks: DOWN or CLOSED counts
                            ((p.is_disney = FALSE AND p.is_universal = FALSE)
                                AND (rss.status = 'DOWN' OR rss.status = 'CLOSED'
                                     OR (rss.status IS NULL AND rss.computed_is_open = 0)))
                        )
                        THEN COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) AS down_weight
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start AND rss.recorded_at < :end
                    AND pas.park_appears_open = TRUE
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                GROUP BY rss.recorded_at
            )
            SELECT
                pw.total_park_weight,
                COUNT(pss.recorded_at) AS total_snapshots,
                CASE
                    WHEN pw.total_park_weight IS NULL OR pw.total_park_weight = 0 THEN NULL
                    WHEN COUNT(pss.recorded_at) = 0 THEN NULL
                    ELSE ROUND(
                        AVG(pss.down_weight / pw.total_park_weight) * :multiplier,
                        :precision
                    )
                END AS avg_shame_score
            FROM park_weights pw
            CROSS JOIN per_snapshot_shame pss
            GROUP BY pw.total_park_weight
        """)

        result = self.db.execute(query, {
            "park_id": park_id,
            "start": start,
            "end": end,
            "multiplier": SHAME_SCORE_MULTIPLIER,
            "precision": SHAME_SCORE_PRECISION
        })
        row = result.fetchone()

        if row is None:
            return None

        # Handle both attribute and dict-like access
        try:
            return row.avg_shame_score
        except AttributeError:
            return row.get('avg_shame_score')

    def get_hourly_breakdown(
        self,
        park_id: int,
        target_date: date
    ) -> List[Dict[str, Any]]:
        """
        DEPRECATED: Use grouped AVG(pas.shame_score) from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For hourly breakdown, use:
            SELECT HOUR(recorded_at) AS hour, AVG(shame_score) AS shame_score
            FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start AND recorded_at < :end
            GROUP BY HOUR(recorded_at)

        This method is kept for reference/testing only.

        Args:
            park_id: The park to get data for
            target_date: The date to get hourly data for

        Returns:
            List of dicts with hourly breakdown data
        """
        # Import here to avoid circular dependency
        from utils.timezone import get_pacific_day_range_utc

        start_utc, end_utc = get_pacific_day_range_utc(target_date)

        query = text("""
            WITH rides_that_operated AS (
                -- Rides that operated at any point today
                SELECT DISTINCT r.ride_id
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND pas.park_appears_open = TRUE
                    AND (rss.status = 'OPERATING'
                         OR (rss.status IS NULL AND rss.computed_is_open = 1))
            ),
            park_weight AS (
                -- Total park weight (using only rides that operated)
                SELECT SUM(COALESCE(rc.tier_weight, 2)) AS total_weight
                FROM rides r
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
            ),
            hourly_data AS (
                SELECT
                    HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR)) AS hour,
                    COUNT(DISTINCT CASE
                        WHEN r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                        THEN r.ride_id
                    END) AS total_rides,
                    SUM(CASE
                        WHEN r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                            AND (
                                ((p.is_disney = TRUE OR p.is_universal = TRUE)
                                    AND rss.status = 'DOWN')
                                OR
                                ((p.is_disney = FALSE AND p.is_universal = FALSE)
                                    AND (rss.status = 'DOWN' OR rss.status = 'CLOSED'
                                         OR (rss.status IS NULL AND rss.computed_is_open = 0)))
                            )
                        THEN 5  -- 5-minute intervals
                        ELSE 0
                    END) AS down_minutes,
                    SUM(CASE
                        WHEN r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                            AND (
                                ((p.is_disney = TRUE OR p.is_universal = TRUE)
                                    AND rss.status = 'DOWN')
                                OR
                                ((p.is_disney = FALSE AND p.is_universal = FALSE)
                                    AND (rss.status = 'DOWN' OR rss.status = 'CLOSED'
                                         OR (rss.status IS NULL AND rss.computed_is_open = 0)))
                            )
                        THEN COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) AS down_weight_sum
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND pas.park_appears_open = TRUE
                GROUP BY HOUR(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR))
                HAVING total_rides > 0
            )
            SELECT
                hd.hour,
                hd.total_rides,
                hd.down_minutes,
                CASE
                    WHEN pw.total_weight IS NULL OR pw.total_weight = 0 THEN NULL
                    ELSE ROUND(
                        (hd.down_weight_sum / (hd.total_rides * 12)) / pw.total_weight * :multiplier,
                        :precision
                    )
                END AS shame_score
            FROM hourly_data hd
            CROSS JOIN park_weight pw
            ORDER BY hd.hour
        """)

        result = self.db.execute(query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": end_utc,
            "multiplier": SHAME_SCORE_MULTIPLIER,
            "precision": SHAME_SCORE_PRECISION
        })

        return [dict(row._mapping) for row in result]

    def get_recent_snapshots(
        self,
        park_id: int,
        minutes: int = 60
    ) -> Dict[str, Any]:
        """
        DEPRECATED: Use pas.shame_score from park_activity_snapshots instead.

        Shame score is now calculated ONCE during data collection and stored in
        park_activity_snapshots.shame_score. For recent snapshots, use:
            SELECT DATE_FORMAT(recorded_at, '%H:%i') AS label, shame_score
            FROM park_activity_snapshots
            WHERE park_id = :park_id AND recorded_at >= :start
            ORDER BY recorded_at

        This method is kept for reference/testing only.

        Args:
            park_id: The park to get data for
            minutes: How many minutes of recent data (default 60)

        Returns:
            Dict with:
                - labels: List of time strings in "HH:MM" format
                - data: List of instantaneous shame scores at each interval
                - granularity: "minutes" to distinguish from hourly charts
        """
        # Calculate time range: now back to (now - minutes)
        # Note: datetime, timedelta, timezone are imported at module level
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        query = text("""
            WITH rides_that_operated AS (
                -- Rides that operated at any point in this window
                SELECT DISTINCT r.ride_id
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND pas.park_appears_open = TRUE
                    AND (rss.status = 'OPERATING'
                         OR (rss.status IS NULL AND rss.computed_is_open = 1))
            ),
            park_weight AS (
                -- Total park weight (using only rides that operated)
                SELECT SUM(COALESCE(rc.tier_weight, 2)) AS total_weight
                FROM rides r
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
            ),
            snapshot_data AS (
                -- Get instantaneous shame for each 5-minute snapshot
                SELECT
                    rss.recorded_at,
                    DATE_FORMAT(DATE_SUB(rss.recorded_at, INTERVAL 8 HOUR), '%H:%i') AS time_label,
                    SUM(CASE
                        WHEN r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                            AND (
                                ((p.is_disney = TRUE OR p.is_universal = TRUE)
                                    AND rss.status = 'DOWN')
                                OR
                                ((p.is_disney = FALSE AND p.is_universal = FALSE)
                                    AND (rss.status = 'DOWN' OR rss.status = 'CLOSED'
                                         OR (rss.status IS NULL AND rss.computed_is_open = 0)))
                            )
                        THEN COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END) AS down_weight
                FROM rides r
                INNER JOIN parks p ON r.park_id = p.park_id
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND pas.recorded_at = rss.recorded_at
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                    AND r.is_active = TRUE
                    AND r.category = 'ATTRACTION'
                    AND rss.recorded_at >= :start_utc AND rss.recorded_at < :end_utc
                    AND pas.park_appears_open = TRUE
                GROUP BY rss.recorded_at
                ORDER BY rss.recorded_at
            )
            SELECT
                sd.recorded_at,
                sd.time_label,
                CASE
                    WHEN pw.total_weight IS NULL OR pw.total_weight = 0 THEN NULL
                    ELSE ROUND(
                        (sd.down_weight / pw.total_weight) * :multiplier,
                        :precision
                    )
                END AS shame_score
            FROM snapshot_data sd
            CROSS JOIN park_weight pw
            ORDER BY sd.recorded_at
        """)

        result = self.db.execute(query, {
            "park_id": park_id,
            "start_utc": start_utc,
            "end_utc": now_utc,
            "multiplier": SHAME_SCORE_MULTIPLIER,
            "precision": SHAME_SCORE_PRECISION
        })

        rows = [dict(row._mapping) for row in result]

        # Build labels and data arrays
        labels = []
        data = []
        for row in rows:
            labels.append(row['time_label'])
            # Convert Decimal to float for JSON serialization
            score = row['shame_score']
            data.append(float(score) if score is not None else None)

        # If we have fewer points than expected, that's OK - it means
        # the park wasn't open for the full duration or data is sparse
        return {
            "labels": labels,
            "data": data,
            "granularity": "minutes"
        }
