import pytest
from datetime import datetime, timedelta
import pytz

from sqlalchemy import text


class TestHourlyMultiHourOutages:
    """
    Tests the core logic of the multi-hour outage fix.
    A ride that operates once during a Pacific day should have its downtime
    counted in all subsequent hours of that day if it remains down.
    """

    @pytest.fixture(scope="class")
    def park_and_ride(self, mysql_connection):
        """Creates a single park and ride for all tests in this class."""
        # Using a high ID to avoid conflicts
        park_id = 9501
        ride_id = 95001

        mysql_connection.execute(text("""
            INSERT INTO parks (park_id, name, timezone, is_disney, is_universal, is_active)
            VALUES (:id, 'Test Park East', 'America/New_York', FALSE, FALSE, TRUE)
        """), {'id': park_id})

        mysql_connection.execute(text("""
            INSERT INTO rides (ride_id, park_id, name, is_active, category, last_operated_at)
            VALUES (:id, :park_id, 'Outage Coaster', TRUE, 'ATTRACTION', NOW())
        """), {'id': ride_id, 'park_id': park_id})

        return park_id, ride_id

    def test_ride_down_for_multiple_hours_is_counted(
        self, mysql_connection, patched_hourly_aggregator, snapshot_creator, park_and_ride
    ):
        """
        Scenario: A ride operates at 10:05, goes down at 10:30, and stays down.
        Hypothesis: The aggregator for the 11:00 and 12:00 hours should both record
                    a full hour of downtime because the ride operated earlier "today".
        """
        # Arrange
        park_id, ride_id = park_and_ride
        pacific_tz = pytz.timezone('America/Los_Angeles')
        day_start = pacific_tz.localize(datetime(2025, 12, 5, 0, 0))

        # Create snapshots: OPERATING at 10:05, then DOWN from 10:10 onwards
        snapshots = [
            (day_start.astimezone(pytz.utc) + timedelta(hours=10, minutes=5), 'OPERATING', True)
        ]
        # Add DOWN snapshots every 5 minutes from 10:10 to 12:55
        current_time = day_start + timedelta(hours=10, minutes=10)
        while current_time < day_start + timedelta(hours=13):
            snapshots.append((current_time.astimezone(pytz.utc), 'DOWN', True))
            current_time += timedelta(minutes=5)

        snapshot_creator(park_id, ride_id, snapshots)

        # Act & Assert for 11:00 hour
        aggregator_11 = patched_hourly_aggregator(
            target_hour=day_start.astimezone(pytz.utc) + timedelta(hours=11)
        )
        aggregator_11.run()

        result_11 = mysql_connection.execute(text("""
            SELECT ride_operated, downtime_hours, operating_snapshots FROM ride_hourly_stats
            WHERE ride_id = :ride_id AND hour_start_utc = :hour
        """), {'ride_id': ride_id, 'hour': aggregator_11.target_hour}).fetchone()

        assert result_11 is not None, "No stats row created for 11:00 hour"
        assert result_11._mapping['ride_operated'] == 1, "Ride should be considered 'operated' for 11:00 hour"
        assert result_11._mapping['operating_snapshots'] == 0, "Should have no operating snapshots in 11:00 hour"
        assert pytest.approx(result_11._mapping['downtime_hours']) == 1.0, "Should have 1 full hour of downtime for 11:00 hour"

        # Act & Assert for 12:00 hour
        aggregator_12 = patched_hourly_aggregator(
            target_hour=day_start.astimezone(pytz.utc) + timedelta(hours=12)
        )
        aggregator_12.run()

        result_12 = mysql_connection.execute(text("""
            SELECT ride_operated, downtime_hours FROM ride_hourly_stats
            WHERE ride_id = :ride_id AND hour_start_utc = :hour
        """), {'ride_id': ride_id, 'hour': aggregator_12.target_hour}).fetchone()

        assert result_12 is not None, "No stats row created for 12:00 hour"
        assert result_12._mapping['ride_operated'] == 1, "Ride should still be 'operated' for 12:00 hour"
        assert pytest.approx(result_12._mapping['downtime_hours']) == 1.0, "Should have 1 full hour of downtime for 12:00 hour"

    def test_ride_recovers_and_stops_counting_downtime(
        self, mysql_connection, patched_hourly_aggregator, snapshot_creator, park_and_ride
    ):
        """
        Scenario: Ride operates at 10:05, is down until 11:55, then recovers at 12:05.
        Hypothesis: The 11:00 hour shows full downtime. The 12:00 hour shows mostly
                    operating snapshots and minimal downtime.
        """
        # Arrange
        park_id, ride_id = park_and_ride
        pacific_tz = pytz.timezone('America/Los_Angeles')
        day_start = pacific_tz.localize(datetime(2025, 12, 6, 0, 0))

        snapshots = [
            (day_start.astimezone(pytz.utc) + timedelta(hours=10, minutes=5), 'OPERATING', True)
        ]
        # DOWN from 10:10 to 11:55
        current_time = day_start + timedelta(hours=10, minutes=10)
        while current_time < day_start + timedelta(hours=12):
            snapshots.append((current_time.astimezone(pytz.utc), 'DOWN', True))
            current_time += timedelta(minutes=5)
        # OPERATING from 12:00 onwards
        while current_time < day_start + timedelta(hours=13):
            snapshots.append((current_time.astimezone(pytz.utc), 'OPERATING', True))
            current_time += timedelta(minutes=5)

        snapshot_creator(park_id, ride_id, snapshots)

        # Act for 11:00 hour
        aggregator_11 = patched_hourly_aggregator(
            target_hour=day_start.astimezone(pytz.utc) + timedelta(hours=11)
        )
        aggregator_11.run()

        # Assert for 11:00 hour
        result_11 = mysql_connection.execute(text("""
            SELECT ride_operated, downtime_hours FROM ride_hourly_stats
            WHERE ride_id = :ride_id AND hour_start_utc = :hour
        """), {'ride_id': ride_id, 'hour': aggregator_11.target_hour}).fetchone()

        assert result_11 is not None
        assert result_11._mapping['ride_operated'] == 1
        assert pytest.approx(result_11._mapping['downtime_hours']) == 1.0

        # Act for 12:00 hour
        aggregator_12 = patched_hourly_aggregator(
            target_hour=day_start.astimezone(pytz.utc) + timedelta(hours=12)
        )
        aggregator_12.run()

        # Assert for 12:00 hour
        result_12 = mysql_connection.execute(text("""
            SELECT ride_operated, downtime_hours, operating_snapshots FROM ride_hourly_stats
            WHERE ride_id = :ride_id AND hour_start_utc = :hour
        """), {'ride_id': ride_id, 'hour': aggregator_12.target_hour}).fetchone()

        assert result_12 is not None
        assert result_12._mapping['ride_operated'] == 1
        assert result_12._mapping['operating_snapshots'] > 0
        assert result_12._mapping['downtime_hours'] == 0.0 # It was operating at 12:00, so 0 downtime for this hour.

    def test_outage_crossing_pacific_day_boundary_is_not_counted_on_next_day(
        self, mysql_connection, patched_hourly_aggregator, snapshot_creator, park_and_ride
    ):
        """
        Scenario: Ride operates at 11:55 PM on Day 1, then goes down. It remains
                  down into Day 2 but never operates on Day 2.
        Hypothesis: When aggregating for Day 2, 'ride_operated' should be 0, and
                    no downtime should be recorded for this ride.
        """
        # Arrange
        park_id, ride_id = park_and_ride
        pacific_tz = pytz.timezone('America/Los_Angeles')
        day1_start = pacific_tz.localize(datetime(2025, 12, 7, 0, 0))
        day2_start = pacific_tz.localize(datetime(2025, 12, 8, 0, 0))

        snapshots = [
            # Day 1: Operate and go down just before midnight
            (day1_start.astimezone(pytz.utc) + timedelta(hours=23, minutes=55), 'OPERATING', True),
            (day1_start.astimezone(pytz.utc) + timedelta(hours=23, minutes=59), 'DOWN', True),
            # Day 2: Stay down for the first hour
            (day2_start.astimezone(pytz.utc) + timedelta(minutes=5), 'DOWN', True),
            (day2_start.astimezone(pytz.utc) + timedelta(minutes=10), 'DOWN', True),
        ]
        snapshot_creator(park_id, ride_id, snapshots)

        # Act: Aggregate the first hour of Day 2 (00:00 Pacific)
        # The UTC hour for 00:00 Pacific is 08:00 UTC during standard time
        aggregator_day2 = patched_hourly_aggregator(
            target_hour=day2_start.astimezone(pytz.utc)
        )
        aggregator_day2.run()

        # Assert
        result_day2 = mysql_connection.execute(text("""
            SELECT ride_operated, downtime_hours FROM ride_hourly_stats
            WHERE ride_id = :ride_id AND hour_start_utc = :hour
        """), {'ride_id': ride_id, 'hour': aggregator_day2.target_hour}).fetchone()

        assert result_day2 is not None, "Stats row should exist as there are snapshots"
        assert result_day2._mapping['ride_operated'] == 0, "Ride should NOT be 'operated' on Day 2"
        assert result_day2._mapping['downtime_hours'] == 0.0, "Downtime should not be counted if ride did not operate today"
