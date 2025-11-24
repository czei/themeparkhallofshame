"""
Theme Park Downtime Tracker - Aggregation Service
Calculates daily/weekly/monthly/yearly statistics from raw snapshots with timezone awareness.
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sqlalchemy.engine import Connection

from utils.logger import logger
from processor.operating_hours_detector import OperatingHoursDetector
from processor.status_change_detector import StatusChangeDetector


class AggregationService:
    """
    Aggregates raw snapshot data into permanent statistics tables.

    Features:
    - Timezone-aware aggregation (iterates through distinct park timezones)
    - Daily/weekly/monthly/yearly aggregation
    - Aggregation log tracking for safe cleanup
    - Retry logic support (3 attempts)
    - Operating hours detection
    - Status change calculation
    """

    def __init__(self, connection: Connection):
        """
        Initialize aggregation service.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection
        self.hours_detector = OperatingHoursDetector(connection)
        self.change_detector = StatusChangeDetector(connection)

    def aggregate_daily(
        self,
        aggregation_date: date,
        park_timezone: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run daily aggregation for a specific date.

        Args:
            aggregation_date: Date to aggregate (in park's local timezone)
            park_timezone: Optional specific timezone (or None for all timezones)

        Returns:
            Dictionary with aggregation results
        """
        logger.info(f"Starting daily aggregation for {aggregation_date} (timezone: {park_timezone or 'all'})")

        # Start aggregation log
        log_id = self._create_aggregation_log(aggregation_date, 'daily')

        try:
            # Get distinct timezones to aggregate
            if park_timezone:
                timezones = [park_timezone]
            else:
                timezones = self._get_distinct_timezones()

            parks_processed = 0
            rides_processed = 0

            # Aggregate each timezone separately
            for tz in timezones:
                tz_results = self._aggregate_daily_for_timezone(aggregation_date, tz)
                parks_processed += tz_results['parks_count']
                rides_processed += tz_results['rides_count']

            # Calculate aggregated_until_ts (end of the day in UTC for the last timezone)
            last_tz = ZoneInfo(timezones[-1])
            local_end = datetime.combine(aggregation_date, datetime.max.time(), tzinfo=last_tz)
            aggregated_until_ts = local_end.astimezone(ZoneInfo('UTC'))

            # Update aggregation log with success
            self._complete_aggregation_log(
                log_id=log_id,
                status='success',
                aggregated_until_ts=aggregated_until_ts,
                parks_processed=parks_processed,
                rides_processed=rides_processed
            )

            logger.info(f"Daily aggregation complete: {parks_processed} parks, {rides_processed} rides")

            return {
                "log_id": log_id,
                "status": "success",
                "parks_processed": parks_processed,
                "rides_processed": rides_processed,
                "aggregated_until_ts": aggregated_until_ts
            }

        except Exception as e:
            # Update aggregation log with failure
            self._complete_aggregation_log(
                log_id=log_id,
                status='failed',
                error_message=str(e)
            )

            logger.error(f"Daily aggregation failed: {e}", exc_info=True)
            raise

    def _aggregate_daily_for_timezone(
        self,
        aggregation_date: date,
        timezone: str
    ) -> Dict[str, int]:
        """
        Aggregate daily stats for all parks in a specific timezone.

        Args:
            aggregation_date: Local date to aggregate
            timezone: IANA timezone string

        Returns:
            Dictionary with counts
        """
        logger.info(f"Aggregating {aggregation_date} for timezone {timezone}")

        # Get parks in this timezone
        parks_query = text("""
            SELECT park_id, name
            FROM parks
            WHERE timezone = :timezone
                AND is_active = TRUE
            ORDER BY park_id
        """)

        result = self.conn.execute(parks_query, {"timezone": timezone})
        parks = [dict(row._mapping) for row in result]

        parks_count = 0
        rides_count = 0

        for park in parks:
            # Detect operating session
            session = self.hours_detector.detect_operating_session(
                park_id=park['park_id'],
                operating_date=aggregation_date,
                park_timezone=timezone
            )

            if session:
                self.hours_detector.save_operating_session(session)

                # Aggregate park stats
                self._aggregate_park_daily_stats(park['park_id'], aggregation_date, timezone, session)
                parks_count += 1

                # Aggregate ride stats for this park
                ride_count = self._aggregate_rides_daily_stats(
                    park['park_id'],
                    aggregation_date,
                    timezone,
                    session
                )
                rides_count += ride_count

        return {
            "parks_count": parks_count,
            "rides_count": rides_count
        }

    def _aggregate_park_daily_stats(
        self,
        park_id: int,
        stat_date: date,
        timezone: str,
        operating_session: Dict[str, Any]
    ):
        """
        Calculate and save daily park statistics.

        Args:
            park_id: Park ID
            stat_date: Local date
            timezone: Park timezone
            operating_session: Operating session data
        """
        # Get time boundaries in UTC
        tz = ZoneInfo(timezone)
        local_start = datetime.combine(stat_date, datetime.min.time(), tzinfo=tz)
        local_end = datetime.combine(stat_date, datetime.max.time(), tzinfo=tz)
        utc_start = local_start.astimezone(ZoneInfo('UTC'))
        utc_end = local_end.astimezone(ZoneInfo('UTC'))

        # Calculate park-wide statistics
        stats_query = text("""
            SELECT
                COUNT(DISTINCT r.ride_id) AS total_rides_tracked,
                COUNT(DISTINCT CASE WHEN rss.computed_is_open = FALSE THEN r.ride_id END) AS rides_with_downtime,
                SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS total_uptime_snapshots,
                SUM(CASE WHEN rss.computed_is_open = FALSE THEN 1 ELSE 0 END) AS total_downtime_snapshots,
                COUNT(rss.snapshot_id) AS total_snapshots
            FROM rides r
            LEFT JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
                AND rss.recorded_at >= :utc_start
                AND rss.recorded_at <= :utc_end
            WHERE r.park_id = :park_id
                AND r.is_active = TRUE
        """)

        result = self.conn.execute(stats_query, {
            "park_id": park_id,
            "utc_start": utc_start,
            "utc_end": utc_end
        })

        row = result.fetchone()

        # Calculate downtime hours and uptime percentage
        total_snapshots = row.total_snapshots or 0
        downtime_snapshots = row.total_downtime_snapshots or 0

        if total_snapshots > 0:
            downtime_ratio = float(downtime_snapshots) / float(total_snapshots)
            operating_minutes = float(operating_session['operating_minutes'])
            operating_hours = operating_minutes / 60.0
            total_downtime_hours = operating_hours * downtime_ratio
            avg_uptime_percentage = ((float(total_snapshots) - float(downtime_snapshots)) / float(total_snapshots)) * 100.0
        else:
            total_downtime_hours = 0.0
            avg_uptime_percentage = 0.0
            operating_hours = float(operating_session['operating_minutes']) / 60.0

        # Insert/update park daily stats
        upsert_query = text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_downtime_hours, avg_uptime_percentage,
                rides_with_downtime, total_rides_tracked, operating_hours_minutes
            )
            VALUES (
                :park_id, :stat_date, :total_downtime_hours, :avg_uptime_percentage,
                :rides_with_downtime, :total_rides_tracked, :operating_hours_minutes
            )
            ON DUPLICATE KEY UPDATE
                total_downtime_hours = VALUES(total_downtime_hours),
                avg_uptime_percentage = VALUES(avg_uptime_percentage),
                rides_with_downtime = VALUES(rides_with_downtime),
                total_rides_tracked = VALUES(total_rides_tracked),
                operating_hours_minutes = VALUES(operating_hours_minutes)
        """)

        self.conn.execute(upsert_query, {
            "park_id": park_id,
            "stat_date": stat_date,
            "total_downtime_hours": round(total_downtime_hours, 2),
            "avg_uptime_percentage": round(avg_uptime_percentage, 2),
            "rides_with_downtime": row.rides_with_downtime or 0,
            "total_rides_tracked": row.total_rides_tracked or 0,
            "operating_hours_minutes": int(operating_minutes)
        })

        logger.debug(f"Aggregated park {park_id} daily stats for {stat_date}")

    def _aggregate_rides_daily_stats(
        self,
        park_id: int,
        stat_date: date,
        timezone: str,
        operating_session: Dict[str, Any]
    ) -> int:
        """
        Calculate and save daily ride statistics for all rides in a park.

        Args:
            park_id: Park ID
            stat_date: Local date
            timezone: Park timezone
            operating_session: Operating session data

        Returns:
            Number of rides processed
        """
        # Get time boundaries in UTC
        tz = ZoneInfo(timezone)
        local_start = datetime.combine(stat_date, datetime.min.time(), tzinfo=tz)
        local_end = datetime.combine(stat_date, datetime.max.time(), tzinfo=tz)
        utc_start = local_start.astimezone(ZoneInfo('UTC'))
        utc_end = local_end.astimezone(ZoneInfo('UTC'))

        # Get all active rides for this park
        rides_query = text("""
            SELECT ride_id
            FROM rides
            WHERE park_id = :park_id
                AND is_active = TRUE
        """)

        result = self.conn.execute(rides_query, {"park_id": park_id})
        ride_ids = [row.ride_id for row in result]

        operating_minutes = operating_session['operating_minutes']

        for ride_id in ride_ids:
            # Calculate ride statistics
            self._aggregate_single_ride_daily_stats(
                ride_id=ride_id,
                stat_date=stat_date,
                utc_start=utc_start,
                utc_end=utc_end,
                operating_minutes=operating_minutes
            )

        return len(ride_ids)

    def _aggregate_single_ride_daily_stats(
        self,
        ride_id: int,
        stat_date: date,
        utc_start: datetime,
        utc_end: datetime,
        operating_minutes: float
    ):
        """Calculate and save daily statistics for a single ride."""
        # Get ride snapshots for the day
        snapshots_query = text("""
            SELECT
                COUNT(*) AS total_snapshots,
                SUM(CASE WHEN computed_is_open = TRUE THEN 1 ELSE 0 END) AS uptime_snapshots,
                SUM(CASE WHEN computed_is_open = FALSE THEN 1 ELSE 0 END) AS downtime_snapshots,
                AVG(CASE WHEN wait_time > 0 THEN wait_time ELSE NULL END) AS avg_wait_time,
                MIN(CASE WHEN wait_time > 0 THEN wait_time ELSE NULL END) AS min_wait_time,
                MAX(wait_time) AS max_wait_time
            FROM ride_status_snapshots
            WHERE ride_id = :ride_id
                AND recorded_at >= :utc_start
                AND recorded_at <= :utc_end
        """)

        result = self.conn.execute(snapshots_query, {
            "ride_id": ride_id,
            "utc_start": utc_start,
            "utc_end": utc_end
        })

        row = result.fetchone()
        total_snapshots = row.total_snapshots or 0

        # Always create a record, even with zero snapshots (data consistency)
        if total_snapshots == 0:
            # No snapshots for this ride - create record with zeros
            zero_stats_query = text("""
                INSERT INTO ride_daily_stats (
                    ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                    operating_hours_minutes, avg_wait_time, min_wait_time, max_wait_time, peak_wait_time,
                    status_changes, longest_downtime_minutes
                )
                VALUES (
                    :ride_id, :stat_date, 0, 0, 0.0, 0, NULL, NULL, NULL, NULL, 0, NULL
                )
                ON DUPLICATE KEY UPDATE
                    uptime_minutes = 0,
                    downtime_minutes = 0,
                    uptime_percentage = 0.0,
                    operating_hours_minutes = 0,
                    avg_wait_time = NULL,
                    min_wait_time = NULL,
                    max_wait_time = NULL,
                    peak_wait_time = NULL,
                    status_changes = 0,
                    longest_downtime_minutes = NULL
            """)
            self.conn.execute(zero_stats_query, {
                "ride_id": ride_id,
                "stat_date": stat_date
            })
            logger.debug(f"Created zero-snapshot record for ride {ride_id} on {stat_date}")
            return

        uptime_snapshots = row.uptime_snapshots or 0
        downtime_snapshots = row.downtime_snapshots or 0

        # Calculate uptime/downtime minutes
        uptime_ratio = float(uptime_snapshots) / float(total_snapshots) if total_snapshots > 0 else 0.0
        uptime_minutes = float(operating_minutes) * uptime_ratio
        downtime_minutes = float(operating_minutes) - uptime_minutes
        uptime_percentage = uptime_ratio * 100.0

        # Get status changes count
        changes = self.change_detector.detect_status_changes(ride_id, utc_start, utc_end)
        status_changes = len([c for c in changes if c['new_status'] is False])

        # Find longest downtime
        longest_downtime = max(
            (c.get('downtime_duration_minutes', 0) for c in changes if c.get('downtime_duration_minutes')),
            default=None
        )

        # Insert/update ride daily stats
        upsert_query = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, min_wait_time, max_wait_time, peak_wait_time,
                status_changes, longest_downtime_minutes
            )
            VALUES (
                :ride_id, :stat_date, :uptime_minutes, :downtime_minutes, :uptime_percentage,
                :operating_hours_minutes, :avg_wait_time, :min_wait_time, :max_wait_time, :peak_wait_time,
                :status_changes, :longest_downtime_minutes
            )
            ON DUPLICATE KEY UPDATE
                uptime_minutes = VALUES(uptime_minutes),
                downtime_minutes = VALUES(downtime_minutes),
                uptime_percentage = VALUES(uptime_percentage),
                operating_hours_minutes = VALUES(operating_hours_minutes),
                avg_wait_time = VALUES(avg_wait_time),
                min_wait_time = VALUES(min_wait_time),
                max_wait_time = VALUES(max_wait_time),
                peak_wait_time = VALUES(peak_wait_time),
                status_changes = VALUES(status_changes),
                longest_downtime_minutes = VALUES(longest_downtime_minutes)
        """)

        # Handle NULL wait times safely
        avg_wait = None
        if row.avg_wait_time is not None:
            avg_wait = round(float(row.avg_wait_time), 2)

        self.conn.execute(upsert_query, {
            "ride_id": ride_id,
            "stat_date": stat_date,
            "uptime_minutes": int(uptime_minutes),
            "downtime_minutes": int(downtime_minutes),
            "uptime_percentage": round(uptime_percentage, 2),
            "operating_hours_minutes": int(operating_minutes),
            "avg_wait_time": avg_wait,
            "min_wait_time": row.min_wait_time,
            "max_wait_time": row.max_wait_time,
            "peak_wait_time": row.max_wait_time,
            "status_changes": status_changes,
            "longest_downtime_minutes": longest_downtime
        })

    def _get_distinct_timezones(self) -> List[str]:
        """Get list of distinct timezones from active parks."""
        query = text("""
            SELECT DISTINCT timezone
            FROM parks
            WHERE is_active = TRUE
            ORDER BY timezone
        """)

        result = self.conn.execute(query)
        return [row.timezone for row in result]

    def _create_aggregation_log(
        self,
        aggregation_date: date,
        aggregation_type: str
    ) -> int:
        """
        Create or restart aggregation log entry.

        Returns:
            log_id
        """
        query = text("""
            INSERT INTO aggregation_log (
                aggregation_date, aggregation_type, status, started_at
            )
            VALUES (
                :aggregation_date, :aggregation_type, 'running', NOW()
            )
            ON DUPLICATE KEY UPDATE
                status = 'running',
                started_at = NOW(),
                completed_at = NULL,
                error_message = NULL,
                parks_processed = 0,
                rides_processed = 0
        """)

        result = self.conn.execute(query, {
            "aggregation_date": aggregation_date,
            "aggregation_type": aggregation_type
        })

        # For ON DUPLICATE KEY UPDATE, lastrowid may not be the actual ID
        # We need to query for it
        if result.lastrowid == 0:
            # Update case - query for the log_id
            select_query = text("""
                SELECT log_id FROM aggregation_log
                WHERE aggregation_date = :aggregation_date
                AND aggregation_type = :aggregation_type
            """)
            log_id = self.conn.execute(select_query, {
                "aggregation_date": aggregation_date,
                "aggregation_type": aggregation_type
            }).scalar()
            return log_id
        else:
            return result.lastrowid

    def _complete_aggregation_log(
        self,
        log_id: int,
        status: str,
        aggregated_until_ts: Optional[datetime] = None,
        parks_processed: int = 0,
        rides_processed: int = 0,
        error_message: Optional[str] = None
    ):
        """Update aggregation log with completion status."""
        query = text("""
            UPDATE aggregation_log
            SET status = :status,
                completed_at = NOW(),
                aggregated_until_ts = :aggregated_until_ts,
                parks_processed = :parks_processed,
                rides_processed = :rides_processed,
                error_message = :error_message
            WHERE log_id = :log_id
        """)

        self.conn.execute(query, {
            "log_id": log_id,
            "status": status,
            "aggregated_until_ts": aggregated_until_ts,
            "parks_processed": parks_processed,
            "rides_processed": rides_processed,
            "error_message": error_message
        })

    def get_last_successful_aggregation(
        self,
        aggregation_type: str = 'daily'
    ) -> Optional[Dict[str, Any]]:
        """
        Get most recent successful aggregation.

        Args:
            aggregation_type: 'daily', 'weekly', 'monthly', or 'yearly'

        Returns:
            Dictionary with aggregation log data or None
        """
        query = text("""
            SELECT *
            FROM aggregation_log
            WHERE aggregation_type = :aggregation_type
                AND status = 'success'
            ORDER BY aggregated_until_ts DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {"aggregation_type": aggregation_type})
        row = result.fetchone()

        return dict(row._mapping) if row else None
