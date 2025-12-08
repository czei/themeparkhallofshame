"""
Today Park Wait Time Rankings Query (Cumulative)
=================================================

Endpoint: GET /api/parks/waittimes?period=today
UI Location: Parks tab → Wait Times Rankings (today - cumulative)

Returns parks ranked by CUMULATIVE wait times from midnight Pacific to now.

CRITICAL DIFFERENCE FROM 7-DAY/30-DAY:
- 7-DAY/30-DAY: Uses pre-aggregated park_daily_stats table
- TODAY: Queries ride_status_snapshots directly for real-time accuracy

Database Tables:
- parks (park metadata)
- rides (ride metadata)
- ride_status_snapshots (real-time wait time data)
- park_activity_snapshots (park open status)

Single Source of Truth:
- Formulas: utils/metrics.py
- SQL Helpers: utils/sql_helpers.py
"""

from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.timezone import get_today_range_to_now_utc
from utils.sql_helpers import ParkStatusSQL, RideFilterSQL
from utils.metrics import USE_HOURLY_TABLES
from database.repositories.stats_repository import StatsRepository


class TodayParkWaitTimesQuery:
    """
    Query handler for today's CUMULATIVE park wait time rankings.

    Unlike weekly/monthly queries which use park_daily_stats,
    this aggregates ALL wait times from ride_status_snapshots
    since midnight Pacific to now.
    """

    def __init__(self, connection: Connection):
        self.conn = connection
        self.stats_repo = StatsRepository(connection)

    def get_rankings(
        self,
        filter_disney_universal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get cumulative park wait time rankings from midnight Pacific to now.

        Args:
            filter_disney_universal: Only Disney/Universal parks
            limit: Maximum results

        Returns:
            List of parks ranked by average wait time (descending)
        """
        # Get time range from midnight Pacific to now
        start_utc, now_utc = get_today_range_to_now_utc()

        # HYBRID QUERY: Use hourly tables when enabled
        if USE_HOURLY_TABLES:
            current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

            # Get hourly stats for complete hours
            hourly_stats = self.stats_repo.get_hourly_stats(
                start_hour=start_utc,
                end_hour=now_utc
            )

            # Group by park and calculate averages
            park_data = {}
            for row in hourly_stats:
                park_id = row['park_id']
                if park_id not in park_data:
                    park_data[park_id] = {'wait_times': [], 'snapshots': 0}

                if row['avg_wait_time_minutes']:
                    # Weight by snapshot count
                    for _ in range(row['snapshot_count']):
                        park_data[park_id]['wait_times'].append(float(row['avg_wait_time_minutes']))
                    park_data[park_id]['snapshots'] += row['snapshot_count']

            # Calculate final averages and get park details
            if not park_data:
                return []

            park_ids = list(park_data.keys())
            filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
            park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

            query = text(f"""
                SELECT
                    p.park_id,
                    p.queue_times_id,
                    p.name AS park_name,
                    CONCAT(p.city, ', ', p.state_province) AS location,
                    {park_is_open_sq}
                FROM parks p
                WHERE p.park_id IN :park_ids
                    AND p.is_active = TRUE
                    {filter_clause}
            """)

            result = self.conn.execute(query, {"park_ids": tuple(park_ids)})

            rankings = []
            for row in result:
                data = park_data[row.park_id]
                wait_times = data['wait_times']

                if wait_times:
                    avg_wait = round(sum(wait_times) / len(wait_times), 1)
                    peak_wait = round(max(wait_times), 1)

                    rankings.append({
                        'park_id': row.park_id,
                        'queue_times_id': row.queue_times_id,
                        'park_name': row.park_name,
                        'location': row.location,
                        'avg_wait_minutes': avg_wait,
                        'peak_wait_minutes': peak_wait,
                        'rides_reporting': None,  # TODO: Get from hourly stats
                        'park_is_open': row.park_is_open
                    })

            # Sort and limit
            rankings.sort(key=lambda x: x['avg_wait_minutes'] or 0, reverse=True)
            return rankings[:limit]

        # FALLBACK: Use original query on raw snapshots

        # Use centralized SQL helpers for consistent logic
        filter_clause = f"AND {RideFilterSQL.disney_universal_filter('p')}" if filter_disney_universal else ""
        park_open = ParkStatusSQL.park_appears_open_filter("pas")
        park_is_open_sq = ParkStatusSQL.park_is_open_subquery("p.park_id")

        query = text(f"""
            SELECT
                p.park_id,
                p.queue_times_id,
                p.name AS park_name,
                CONCAT(p.city, ', ', p.state_province) AS location,

                -- Average wait time across all rides (only when park is open and wait > 0)
                -- IMPORTANT: Use avg_wait_minutes (not avg_wait_time) for frontend compatibility
                ROUND(
                    AVG(CASE
                        WHEN {park_open} AND rss.wait_time > 0
                        THEN rss.wait_time
                    END),
                    1
                ) AS avg_wait_minutes,

                -- Peak wait time today
                -- IMPORTANT: Use peak_wait_minutes (not peak_wait_time) for frontend compatibility
                MAX(CASE
                    WHEN {park_open}
                    THEN rss.wait_time
                END) AS peak_wait_minutes,

                -- Count of rides with wait time data
                -- IMPORTANT: Use rides_reporting (not rides_with_waits) for frontend compatibility
                COUNT(DISTINCT CASE
                    WHEN rss.wait_time > 0
                    THEN r.ride_id
                END) AS rides_reporting,

                -- Park operating status (current)
                {park_is_open_sq}

            FROM parks p
            INNER JOIN rides r ON p.park_id = r.park_id
                AND r.is_active = TRUE AND r.category = 'ATTRACTION'
            INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            INNER JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE rss.recorded_at >= :start_utc AND rss.recorded_at < :now_utc
                AND p.is_active = TRUE
                {filter_clause}
            GROUP BY p.park_id, p.name, p.city, p.state_province
            HAVING avg_wait_minutes IS NOT NULL
            ORDER BY avg_wait_minutes DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, {
            "start_utc": start_utc,
            "now_utc": now_utc,
            "limit": limit
        })
        return [dict(row._mapping) for row in result]
