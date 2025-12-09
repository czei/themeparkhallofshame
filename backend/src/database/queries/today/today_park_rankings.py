"""
Today Park Rankings Query (Average Shame Score)
===============================================

Endpoint: GET /api/parks/downtime?period=today
UI Location: Parks tab → Downtime Rankings (today)

Returns parks ranked by AVERAGE shame score from midnight Pacific to now.

SHAME SCORE CALCULATION:
- LIVE: Instantaneous shame = (sum of weights of down rides) / total_park_weight × 10
- TODAY: Average of instantaneous shame scores across all snapshots

This makes TODAY comparable to LIVE - both on the same 0-100 scale representing
"percentage of weighted capacity that was down".

Example: If a park had shame scores of [0, 0, 2, 2, 0, 0] across 6 snapshots:
- Average shame score = (0+0+2+2+0+0) / 6 = 0.67

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_classifications (tier weights)
- ride_status_snapshots (real-time status)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc
from utils.sql_helpers import (
    RideStatusSQL,
    ParkStatusSQL,
    RideFilterSQL,
)
from utils.metrics import USE_HOURLY_TABLES
from database.repositories.stats_repository import StatsRepository


class TodayParkRankingsQuery:
    """
    Query handler for today's park rankings using AVERAGE shame score.

    Shame score for TODAY = average of instantaneous shame scores across all
    snapshots from midnight Pacific to now.

    This makes the score comparable to LIVE (both on the same 0-100 scale).
    """

    # Snapshot interval in minutes (for converting snapshot counts to time)
    SNAPSHOT_INTERVAL_MINUTES = 5

    def __init__(self, connection: Connection):
        self.conn = connection
        self.stats_repo = StatsRepository(connection)

    def _query_hourly_tables(
        self,
        start_hour: datetime,
        end_hour: datetime,
        filter_disney_universal: bool = False
    ) -> Dict[int, Dict[str, Any]]:
        """
        Query complete hours from park_hourly_stats table.

        Args:
            start_hour: Start hour (inclusive)
            end_hour: End hour (exclusive)
            filter_disney_universal: Only Disney/Universal parks

        Returns:
            Dict mapping park_id to aggregated stats
        """
        # Get hourly stats from repository
        hourly_stats = self.stats_repo.get_hourly_stats(
            start_hour=start_hour,
            end_hour=end_hour
        )

        # Group by park and aggregate
        park_data = {}
        for row in hourly_stats:
            park_id = row['park_id']

            if park_id not in park_data:
                park_data[park_id] = {
                    'shame_scores': [],
                    'snapshot_count': 0,
                    'total_downtime_hours': 0,
                    'weighted_downtime_hours': 0,
                    'max_rides_down': 0,
                    'rides_operating_sum': 0,
                    'hours_count': 0,
                }

            # Accumulate data from this hour
            if row['shame_score'] is not None:
                park_data[park_id]['shame_scores'].append(float(row['shame_score']))
            park_data[park_id]['snapshot_count'] += row['snapshot_count']
            park_data[park_id]['total_downtime_hours'] += float(row['total_downtime_hours'] or 0)
            park_data[park_id]['weighted_downtime_hours'] += float(row['weighted_downtime_hours'] or 0)
            park_data[park_id]['max_rides_down'] = max(park_data[park_id]['max_rides_down'], int(row.get('rides_down', 0) or 0))
            park_data[park_id]['rides_operating_sum'] += int(row.get('rides_operating', 0) or 0)
            park_data[park_id]['hours_count'] += 1

        return park_data

    def _query_raw_snapshots(
        self,
        start_time: datetime,
        end_time: datetime,
        filter_disney_universal: bool = False
    ) -> Dict[int, Dict[str, Any]]:
        """
        Query current incomplete hour from raw snapshots.

        Args:
            start_time: Start time (inclusive)
            end_time: End time (exclusive)
            filter_disney_universal: Only Disney/Universal parks

        Returns:
            Dict mapping park_id to aggregated stats for current hour
        """
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        query = text(f"""
            WITH
            rides_operated AS (
                -- Find rides that operated in the current hour
                SELECT DISTINCT r.ride_id
                FROM rides r
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON r.park_id = pas.park_id
                    AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
                INNER JOIN parks p ON r.park_id = p.park_id
                WHERE rss.recorded_at >= :start_time AND rss.recorded_at < :end_time
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                    AND p.is_active = TRUE
                    AND {park_open}
                    AND (rss.status = 'OPERATING' OR rss.computed_is_open = TRUE)
                    {filter_clause}
            )
            SELECT
                pas.park_id,
                pas.recorded_at,
                pas.shame_score
            FROM park_activity_snapshots pas
            WHERE pas.recorded_at >= :start_time AND pas.recorded_at < :end_time
                AND pas.park_appears_open = TRUE
                AND pas.shame_score IS NOT NULL
        """)

        result = self.conn.execute(query, {
            "start_time": start_time,
            "end_time": end_time
        })

        # Group by park
        park_data = {}
        for row in result:
            park_id = row.park_id
            if park_id not in park_data:
                park_data[park_id] = {
                    'shame_scores': [],
                    'snapshot_count': 0
                }

            park_data[park_id]['shame_scores'].append(float(row.shame_score))
            park_data[park_id]['snapshot_count'] += 1

        return park_data

    def _combine_hourly_and_raw(
        self,
        hourly_data: Dict[int, Dict[str, Any]],
        raw_data: Dict[int, Dict[str, Any]]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Combine hourly table data with raw snapshot data using weighted averaging.

        Args:
            hourly_data: Data from park_hourly_stats (complete hours)
            raw_data: Data from raw snapshots (current hour)

        Returns:
            Combined data with weighted average shame scores and complete metrics
        """
        combined = {}

        # Get all unique park IDs
        all_park_ids = set(hourly_data.keys()) | set(raw_data.keys())

        for park_id in all_park_ids:
            hourly = hourly_data.get(park_id, {})
            raw = raw_data.get(park_id, {})

            # Combine shame scores from both sources
            all_shame_scores = hourly.get('shame_scores', []) + raw.get('shame_scores', [])

            # Calculate average shame score
            avg_shame_score = None
            if all_shame_scores:
                avg_shame_score = round(sum(all_shame_scores) / len(all_shame_scores), 1)

            # Calculate uptime percentage from snapshot counts
            # For hourly data: use rides_operating_sum / (hours_count * 12 avg snapshots/hour) as approximation
            # For raw data: we don't have operating counts, so estimate from current snapshot
            total_snapshots = hourly.get('snapshot_count', 0) + raw.get('snapshot_count', 0)
            # Uptime approximation: assume total rides operating over period
            # This is a simplified calculation - full accuracy requires tracking per-ride snapshots
            hours_count = hourly.get('hours_count', 0)
            avg_rides_operating = (hourly.get('rides_operating_sum', 0) / hours_count) if hours_count > 0 else 0

            # Conservative uptime estimate (can be refined with more detailed tracking)
            uptime_percentage = None  # Will be queried separately for accuracy

            # Rides down: Use maximum concurrent rides down across all periods
            rides_down = hourly.get('max_rides_down', 0)

            combined[park_id] = {
                'shame_score': avg_shame_score,
                'total_downtime_hours': hourly.get('total_downtime_hours', 0),
                'weighted_downtime_hours': hourly.get('weighted_downtime_hours', 0),
                'snapshot_count': total_snapshots,
                'rides_down': rides_down,
                'uptime_percentage': uptime_percentage  # Will be calculated in build step
            }

        return combined

    def _build_rankings_from_combined_data(
        self,
        combined_data: Dict[int, Dict[str, Any]],
        start_utc: datetime,
        now_utc: datetime,
        filter_disney_universal: bool,
        limit: int,
        sort_by: str
    ) -> List[Dict[str, Any]]:
        """
        Build rankings response from combined hourly + raw data.

        Args:
            combined_data: Combined data from _combine_hourly_and_raw()
            start_utc: Query start time
            now_utc: Query end time
            filter_disney_universal: Filter flag
            limit: Maximum results
            sort_by: Sort field

        Returns:
            List of ranked parks with full details
        """
        if not combined_data:
            return []

        # Get park details for all parks in combined_data, including uptime and current status
        park_ids = list(combined_data.keys())
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")

        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,
                (
                    SELECT MAX(pas.recorded_at)
                    FROM park_activity_snapshots pas
                    WHERE pas.park_id = p.park_id
                      AND pas.recorded_at >= :start_utc AND pas.recorded_at < :now_utc
                      AND pas.park_appears_open = TRUE
                ) AS last_updated,
                -- Calculate uptime percentage from today's ride snapshots
                ROUND(
                    100.0 * SUM(CASE
                        WHEN {park_open} AND NOT ({is_down})
                        THEN 1
                        ELSE 0
                    END) / NULLIF(
                        SUM(CASE WHEN {park_open} THEN 1 ELSE 0 END),
                        0
                    ),
                    1
                ) AS uptime_percentage,
                -- Park operating status (current)
                {park_is_open_sq}
            FROM parks p
            LEFT JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            LEFT JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                AND rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
            LEFT JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
            WHERE p.park_id IN :park_ids
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
        """)

        result = self.conn.execute(query, {
            "park_ids": tuple(park_ids),
            "start_utc": start_utc,
            "now_utc": now_utc
        })

        # Build rankings list
        rankings = []
        for row in result:
            park_id = row.park_id
            data = combined_data[park_id]

            # Skip parks with no shame score data
            if data['shame_score'] is None:
                continue

            rankings.append({
                'park_id': park_id,
                'queue_times_id': row.queue_times_id,
                'park_name': row.park_name,
                'location': row.location,
                'shame_score': data['shame_score'],
                'total_downtime_hours': round(data['total_downtime_hours'], 2),
                'weighted_downtime_hours': round(data['weighted_downtime_hours'], 2),
                'uptime_percentage': row.uptime_percentage,
                'rides_down': data['rides_down'],
                'park_is_open': row.park_is_open
            })

        # Sort by specified column
        sort_column = 'shame_score' if sort_by == 'shame_score' else 'total_downtime_hours'
        rankings.sort(key=lambda x: x[sort_column] or 0, reverse=True)

        return rankings[:limit]

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
        sort_by: str = "shame_score",
    ) -> List[Dict[str, Any]]:
        """
        Get park rankings from midnight Pacific to now using AVERAGE shame score.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results
            sort_by: Sort field (shame_score or downtime_hours)

        Returns:
            List of parks ranked by average shame_score (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # HYBRID QUERY: Use hourly tables + raw snapshots when feature flag enabled
        if USE_HOURLY_TABLES:
            # Calculate current hour boundary (truncate to hour)
            current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

            # Query complete hours from hourly tables (midnight to current hour)
            hourly_data = {}
            if current_hour_start > start_utc:
                hourly_data = self._query_hourly_tables(
                    start_hour=start_utc,
                    end_hour=current_hour_start,
                    filter_disney_universal=filter_disney_universal
                )

            # Query current incomplete hour from raw snapshots
            raw_data = self._query_raw_snapshots(
                start_time=current_hour_start,
                end_time=now_utc,
                filter_disney_universal=filter_disney_universal
            )

            # Combine hourly and raw data with weighted averaging
            combined_data = self._combine_hourly_and_raw(hourly_data, raw_data)

            # Build rankings from combined data
            return self._build_rankings_from_combined_data(
                combined_data,
                start_utc,
                now_utc,
                filter_disney_universal,
                limit,
                sort_by
            )

        # FALLBACK: Use original query on raw snapshots

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        filter_clause_inner = f"AND {RideFilterSQL.disney_universal_filter('p_inner')}" if filter_disney_universal else ""
        # PARK-TYPE AWARE: Disney/Universal only counts DOWN (not CLOSED)
        is_down = RideStatusSQL.is_down("rss", parks_alias="p")
        is_down_inner = RideStatusSQL.is_down("rss_inner", parks_alias="p_inner")
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_open_inner = ParkStatusSQL.park_appears_open_filter("pas_inner")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        # Use centralized CTE for rides that operated (includes park-open check)
        rides_operated_cte = RideStatusSQL.rides_that_operated_cte(
            start_param=":start_utc",
            end_param=":now_utc",
            filter_clause=filter_clause
        )

        # Determine sort column
        sort_column = "shame_score" if sort_by == "shame_score" else "total_downtime_hours"

        query = text(f"""
            WITH
            {rides_operated_cte},
            park_weights AS (
                -- Total tier weight for each park (for shame score normalization)
                -- Only count rides that have operated
                SELECT
                    p.park_id,
                    SUM(COALESCE(rc.tier_weight, 2)) AS total_park_weight
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE p.is_active = TRUE
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                    {filter_clause}
                GROUP BY p.park_id
            ),
            park_operating_snapshots AS (
                -- Count total snapshots when park was open (for averaging)
                SELECT
                    p.park_id,
                    COUNT(DISTINCT rss.recorded_at) AS total_snapshots
                FROM parks p
                INNER JOIN rides r ON p.park_id = r.park_id
                    AND r.is_active = TRUE AND r.category = 'ATTRACTION'
                INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                    AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
                WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                    AND p.is_active = TRUE
                    AND {park_open}
                    AND r.ride_id IN (SELECT ride_id FROM rides_that_operated)
                    {filter_clause}
                GROUP BY p.park_id
            ),
            stored_shame_scores AS (
                -- READ stored shame_score from park_activity_snapshots
                -- THE SINGLE SOURCE OF TRUTH - calculated during data collection
                SELECT
                    pas_inner.park_id,
                    pas_inner.recorded_at,
                    pas_inner.shame_score AS snapshot_shame_score
                FROM park_activity_snapshots pas_inner
                WHERE pas_inner.recorded_at >= :start_utc AND pas_inner.recorded_at < :now_utc
                    AND pas_inner.park_appears_open = TRUE
                    AND pas_inner.shame_score IS NOT NULL
            )
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Total downtime hours (for reference)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN {self.SNAPSHOT_INTERVAL_MINUTES} / 60.0
                        ELSE 0
                    END),
                    2
                ) AS total_downtime_hours,

                -- Weighted downtime hours (for reference)
                ROUND(
                    SUM(CASE
                        WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                        THEN ({self.SNAPSHOT_INTERVAL_MINUTES} / 60.0) * COALESCE(rc.tier_weight, 2)
                        ELSE 0
                    END),
                    2
                ) AS weighted_downtime_hours,

                -- AVERAGE Shame Score: READ stored values from park_activity_snapshots
                -- THE SINGLE SOURCE OF TRUTH - calculated during data collection
                ROUND(
                    (SELECT AVG(sss.snapshot_shame_score) FROM stored_shame_scores sss WHERE sss.park_id = p.park_id),
                    1
                ) AS shame_score,

                -- Uptime percentage = (operating snapshots) / (total ride-snapshots) * 100
                -- Must divide by total (rides × snapshots), not just snapshots
                ROUND(
                    100.0 * SUM(CASE
                        WHEN {park_open} AND rto.ride_id IS NOT NULL AND NOT ({is_down})
                        THEN 1
                        ELSE 0
                    END) / NULLIF(
                        SUM(CASE WHEN {park_open} AND rto.ride_id IS NOT NULL THEN 1 ELSE 0 END),
                        0
                    ),
                    1
                ) AS uptime_percentage,

                -- Count of DISTINCT rides that were down at some point today
                COUNT(DISTINCT CASE
                    WHEN {is_down} AND {park_open} AND rto.ride_id IS NOT NULL
                    THEN r.ride_id
                END) AS rides_down,

                -- Park operating status (current)
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND DATE_FORMAT(pas.recorded_at, '%Y-%m-%d %H:%i') = DATE_FORMAT(rss.recorded_at, '%Y-%m-%d %H:%i')
            INNER JOIN park_weights pw ON p.park_id = pw.park_id
            LEFT JOIN park_operating_snapshots pos ON p.park_id = pos.park_id
            LEFT JOIN rides_that_operated rto ON r.ride_id = rto.ride_id
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province, pw.total_park_weight, pos.total_snapshots
            HAVING shame_score IS NOT NULL AND shame_score > 0
            ORDER BY {sort_column} DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
