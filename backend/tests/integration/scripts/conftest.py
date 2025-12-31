import pytest
from datetime import datetime, timedelta
from typing import List, Tuple
import pytz

from sqlalchemy import text
from sqlalchemy.engine import Connection

# Add src to path to import aggregator
import sys
from pathlib import Path
backend_src = Path(__file__).parent.parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))
from scripts.aggregate_hourly import HourlyAggregator # noqa
from utils.sql_helpers import RideStatusSQL, ParkStatusSQL # noqa


@pytest.fixture
def get_pacific_day_range_utc():
    """Provides a helper function to calculate the UTC range for a Pacific day."""
    def _get_pacific_day_range_utc(utc_dt: datetime) -> Tuple[datetime, datetime]:
        """
        For a given UTC datetime, find the start and end of the corresponding
        Pacific calendar day, returned in UTC.
        """
        pacific_tz = pytz.timezone('America/Los_Angeles')
        # Convert the target UTC time to Pacific to find out what "day" it is
        target_pacific = utc_dt.astimezone(pacific_tz)
        # Get the start of that day in Pacific time (midnight)
        day_start_pacific = target_pacific.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Get the start of the next day
        day_end_pacific = day_start_pacific + timedelta(days=1)
        # Convert the day boundaries back to UTC
        day_start_utc = day_start_pacific.astimezone(pytz.utc)
        day_end_utc = day_end_pacific.astimezone(pytz.utc)
        return day_start_utc, day_end_utc

    return _get_pacific_day_range_utc


@pytest.fixture
def patched_hourly_aggregator(monkeypatch, get_pacific_day_range_utc):
    """
    Provides a patched version of the HourlyAggregator class where the
    _aggregate_ride method uses the "operated today" CTE logic.
    """

    def _aggregate_ride_fixed(aggregator_instance, conn: Connection, ride, operated_today_ride_ids: set):
        """
        This patched version of _aggregate_ride uses the pre-calculated set
        of operated_today_ride_ids to avoid N+1 query problem.
        """
        ride_id = ride.ride_id
        park_id = ride.park_id
        target_hour_utc = aggregator_instance.target_hour

        # Pre-check: Skip if no snapshots exist for this ride in this hour
        check = conn.execute(text("""
            SELECT 1 FROM ride_status_snapshots
            WHERE ride_id = :ride_id AND recorded_at >= :hour_start AND recorded_at < :hour_end
            LIMIT 1
        """), {
            'ride_id': ride_id,
            'hour_start': target_hour_utc,
            'hour_end': aggregator_instance.hour_end
        })
        if check.fetchone() is None:
            return

        is_down_sql = RideStatusSQL.is_down("rss", parks_alias="p")
        park_open_sql = ParkStatusSQL.park_appears_open_filter("pas")

        query = text(f"""
            INSERT INTO ride_hourly_stats (
                ride_id, park_id, hour_start_utc, avg_wait_time_minutes,
                operating_snapshots, down_snapshots, downtime_hours, uptime_percentage,
                snapshot_count, ride_operated, created_at
            )
            SELECT
                :ride_id,
                :park_id,
                :hour_start,
                ROUND(AVG(CASE WHEN rss.computed_is_open AND rss.wait_time IS NOT NULL THEN rss.wait_time END), 2),
                SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END),
                SUM(CASE WHEN {park_open_sql} AND ({is_down_sql}) THEN 1 ELSE 0 END),
                ROUND(SUM(CASE WHEN {park_open_sql} AND ({is_down_sql}) THEN 5.0 / 60.0 ELSE 0 END), 2),
                CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * SUM(CASE WHEN rss.computed_is_open THEN 1 ELSE 0 END) / COUNT(*), 2) ELSE 0 END,
                COUNT(*),
                :ride_operated,
                NOW()
            FROM ride_status_snapshots rss
            JOIN rides r ON rss.ride_id = r.ride_id
            JOIN parks p ON r.park_id = p.park_id
            JOIN park_activity_snapshots pas ON r.park_id = pas.park_id AND pas.recorded_at = rss.recorded_at
            WHERE rss.ride_id = :ride_id
              AND rss.recorded_at >= :hour_start
              AND rss.recorded_at < :hour_end
            ON DUPLICATE KEY UPDATE
                avg_wait_time_minutes = VALUES(avg_wait_time_minutes),
                operating_snapshots = VALUES(operating_snapshots),
                down_snapshots = VALUES(down_snapshots),
                downtime_hours = VALUES(downtime_hours),
                uptime_percentage = VALUES(uptime_percentage),
                snapshot_count = VALUES(snapshot_count),
                ride_operated = VALUES(ride_operated),
                updated_at = NOW()
        """)
        conn.execute(query, {
            'ride_id': ride_id,
            'park_id': park_id,
            'hour_start': target_hour_utc,
            'hour_end': aggregator_instance.hour_end,
            'ride_operated': 1 if ride_id in operated_today_ride_ids else 0
        })

    monkeypatch.setattr(HourlyAggregator, '_aggregate_ride', _aggregate_ride_fixed)
    return HourlyAggregator


@pytest.fixture
def snapshot_creator(mysql_session):
    """Provides a helper function to create snapshots for tests."""
    def _create_snapshots(
        park_id: int,
        ride_id: int,
        snapshots: List[Tuple[datetime, str, bool]]
    ):
        """
        Inserts a series of ride and park snapshots.

        Args:
            park_id: The park ID.
            ride_id: The ride ID.
            snapshots: A list of tuples, each containing:
                       (timestamp_utc, ride_status, park_appears_open)
                       ride_status can be 'OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT'
        """
        for ts, status, park_open in snapshots:
            is_open = status == 'OPERATING'
            wait_time = 10 if is_open else 0

            # Insert park activity snapshot
            mysql_session.execute(text("""
                INSERT INTO park_activity_snapshots (park_id, recorded_at, park_appears_open, rides_open, rides_closed)
                VALUES (:park_id, :ts, :park_open, 0, 0)
            """), {'park_id': park_id, 'ts': ts, 'park_open': park_open})

            # Insert ride status snapshot
            mysql_session.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, status, computed_is_open, wait_time)
                VALUES (:ride_id, :ts, :status, :is_open, :wait_time)
            """), {'ride_id': ride_id, 'ts': ts, 'status': status, 'is_open': is_open, 'wait_time': wait_time})
    return _create_snapshots
