"""
Theme Park Downtime Tracker - Aggregation Service Integration Tests

Tests complete aggregation workflow with real MySQL database:
- Daily aggregation math verification
- Database persistence (park_daily_stats, ride_daily_stats)
- Operating hours integration
- Timezone-aware processing
- Edge cases (no data, multiple parks, UPSERT behavior)

Priority: P1 - CRITICAL - Validates core business logic before weekly/monthly aggregation
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from processor.aggregation_service import AggregationService
from sqlalchemy import text


# ============================================================================
# FIXTURES - Cleanup
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def cleanup_before_aggregation_service_tests(mysql_engine):
    """Clean up all test data once at start of this test module."""
    with mysql_engine.connect() as conn:
        conn.execute(text("DELETE FROM ride_status_snapshots"))
        conn.execute(text("DELETE FROM ride_status_changes"))
        conn.execute(text("DELETE FROM park_activity_snapshots"))
        conn.execute(text("DELETE FROM ride_daily_stats"))
        conn.execute(text("DELETE FROM ride_weekly_stats"))
        conn.execute(text("DELETE FROM ride_monthly_stats"))
        conn.execute(text("DELETE FROM park_daily_stats"))
        conn.execute(text("DELETE FROM park_weekly_stats"))
        conn.execute(text("DELETE FROM park_monthly_stats"))
        conn.execute(text("DELETE FROM ride_classifications"))
        conn.execute(text("DELETE FROM rides"))
        conn.execute(text("DELETE FROM parks"))
        conn.commit()
    yield


class TestDailyAggregationMath:
    """Test daily aggregation calculates correct statistics."""

    def test_single_ride_full_day_aggregation(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """
        Test daily aggregation for a single ride with full day of data.

        Scenario:
        - Space Mountain at Magic Kingdom
        - 14-hour operating day (9 AM - 11 PM = 840 minutes)
        - 84 snapshots collected (every 10 minutes)
        - 7 snapshots show ride down, 77 show ride open

        Expected Math:
        - downtime_ratio = 7 / 84 = 0.0833
        - downtime_minutes = 840 × 0.0833 = 70 minutes
        - uptime_minutes = 840 - 70 = 770 minutes
        - uptime_percentage = (770 / 840) × 100 = 91.67%
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        # Setup: Create operating session (9 AM - 11 PM = 14 hours)
        aggregation_date = date.today()
        tz = ZoneInfo(sample_park_data['timezone'])

        session_start = datetime.combine(aggregation_date, time(9, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))
        session_end = datetime.combine(aggregation_date, time(23, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))

        operating_session_query = text("""
            INSERT INTO park_operating_sessions (
                park_id, session_date, session_start_utc, session_end_utc, operating_minutes
            ) VALUES (
                :park_id, :session_date, :session_start_utc, :session_end_utc, :operating_minutes
            )
        """)
        mysql_session.execute(operating_session_query, {
            "park_id": park_id,
            "session_date": aggregation_date,
            "session_start_utc": session_start,
            "session_end_utc": session_end,
            "operating_minutes": 840  # 14 hours × 60
        })

        # Setup: Create 84 snapshots (every 10 minutes from 9 AM to 11 PM)
        # 7 snapshots show ride down (computed_is_open = FALSE)
        # 77 snapshots show ride open (computed_is_open = TRUE)
        snapshot_insert = text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, is_open, wait_time, last_updated_api, computed_is_open
            ) VALUES (
                :ride_id, :recorded_at, :is_open, :wait_time, :last_updated_api, :computed_is_open
            )
        """)

        current_time = session_start
        for i in range(84):
            is_down = i in [10, 20, 30, 40, 50, 60, 70]  # 7 down periods
            mysql_session.execute(snapshot_insert, {
                "ride_id": ride_id,
                "recorded_at": current_time,
                "is_open": not is_down,
                "wait_time": 0 if is_down else 35,
                "last_updated_api": current_time,
                "computed_is_open": not is_down
            })
            current_time += timedelta(minutes=10)

        # Act: Run daily aggregation
        service = AggregationService(mysql_session)
        result = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        # Assert: Aggregation succeeded
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1
        assert result['parks_processed'] == 1

        # Assert: ride_daily_stats has correct calculations
        ride_stats_query = text("""
            SELECT
                uptime_minutes,
                downtime_minutes,
                uptime_percentage,
                operating_hours_minutes
            FROM ride_daily_stats
            WHERE ride_id = :ride_id AND stat_date = :stat_date
        """)
        ride_stats = mysql_session.execute(ride_stats_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).fetchone()

        assert ride_stats is not None, "ride_daily_stats record should exist"

        # Verify math: 7 down / 84 total = 0.0833 ratio
        # Operating hours detected from snapshots: 9:00 AM to 10:50 PM = 830 minutes
        # 830 minutes × 0.0833 = 69.139 ≈ 69 minutes downtime
        assert ride_stats.downtime_minutes == 69, "Downtime should be 69 minutes"
        assert ride_stats.uptime_minutes == 760, "Uptime should be 760 minutes (830 - 69)"
        assert ride_stats.operating_hours_minutes == 830, "Operating hours should be 830 minutes"

        # Uptime percentage: (77 / 84) × 100 = 91.67%
        uptime_pct = float(ride_stats.uptime_percentage)
        assert abs(uptime_pct - 91.67) < 0.1, f"Uptime percentage should be ~91.67%, got {uptime_pct}"

        # Assert: park_daily_stats has correct calculations
        park_stats_query = text("""
            SELECT
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked
            FROM park_daily_stats
            WHERE park_id = :park_id AND stat_date = :stat_date
        """)
        park_stats = mysql_session.execute(park_stats_query, {
            "park_id": park_id,
            "stat_date": aggregation_date
        }).fetchone()

        assert park_stats is not None, "park_daily_stats record should exist"

        # Park downtime: 69 minutes = 1.15 hours
        downtime_hours = float(park_stats.total_downtime_hours)
        assert abs(downtime_hours - 1.15) < 0.01, f"Park downtime should be ~1.15 hours, got {downtime_hours}"

        park_uptime_pct = float(park_stats.avg_uptime_percentage)
        assert abs(park_uptime_pct - 91.67) < 0.1, f"Park uptime should be ~91.67%, got {park_uptime_pct}"

        assert park_stats.rides_with_downtime == 1, "Should have 1 ride with downtime"
        assert park_stats.total_rides_tracked == 1, "Should be tracking 1 ride"

    def test_multiple_rides_park_aggregation(
        self, mysql_session, sample_park_data
    ):
        """
        Test park-level aggregation with multiple rides.

        Scenario:
        - Magic Kingdom with 3 rides
        - Ride A: 100% uptime (0 down snapshots)
        - Ride B: 91.67% uptime (7/84 down snapshots)
        - Ride C: 83.33% uptime (14/84 down snapshots)
        - Total: 21 down / 252 total = 8.33% downtime ratio

        Expected Park Stats:
        - total_downtime_hours = 14 × 0.0833 = 1.17 hours
        - avg_uptime_percentage = (231 / 252) × 100 = 91.67%
        - rides_with_downtime = 2 (B and C)
        """
        from tests.conftest import insert_sample_park

        # Setup: Create park
        park_id = insert_sample_park(mysql_session, sample_park_data)
        aggregation_date = date.today()
        tz = ZoneInfo(sample_park_data['timezone'])

        # Setup: Create 3 rides
        rides = []
        for i in range(3):
            ride_insert = text("""
                INSERT INTO rides (park_id, queue_times_id, name, is_active)
                VALUES (:park_id, :queue_times_id, :name, 1)
            """)
            result = mysql_session.execute(ride_insert, {
                "park_id": park_id,
                "queue_times_id": 1000 + i,
                "name": f"Ride {chr(65+i)}"  # A, B, C
            })
            rides.append(result.lastrowid)

        # Setup: Create operating session (14 hours)
        session_start = datetime.combine(aggregation_date, time(9, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))
        session_end = datetime.combine(aggregation_date, time(23, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))

        operating_session_query = text("""
            INSERT INTO park_operating_sessions (
                park_id, session_date, session_start_utc, session_end_utc, operating_minutes
            ) VALUES (
                :park_id, :session_date, :session_start_utc, :session_end_utc, :operating_minutes
            )
        """)
        mysql_session.execute(operating_session_query, {
            "park_id": park_id,
            "session_date": aggregation_date,
            "session_start_utc": session_start,
            "session_end_utc": session_end,
            "operating_minutes": 840
        })

        # Setup: Create snapshots for each ride
        # Ride A: 0 down, Ride B: 7 down, Ride C: 14 down
        down_counts = [0, 7, 14]
        snapshot_insert = text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, is_open, wait_time, last_updated_api, computed_is_open
            ) VALUES (
                :ride_id, :recorded_at, :is_open, :wait_time, :last_updated_api, :computed_is_open
            )
        """)

        for ride_idx, ride_id in enumerate(rides):
            current_time = session_start
            down_indices = set(range(0, down_counts[ride_idx]))  # First N snapshots are down

            for i in range(84):
                is_down = i in down_indices
                mysql_session.execute(snapshot_insert, {
                    "ride_id": ride_id,
                    "recorded_at": current_time,
                    "is_open": not is_down,
                    "wait_time": 0 if is_down else 30,
                    "last_updated_api": current_time,
                    "computed_is_open": not is_down
                })
                current_time += timedelta(minutes=10)

        # Act: Run daily aggregation
        service = AggregationService(mysql_session)
        result = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        # Assert: Park stats
        park_stats_query = text("""
            SELECT
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked
            FROM park_daily_stats
            WHERE park_id = :park_id AND stat_date = :stat_date
        """)
        park_stats = mysql_session.execute(park_stats_query, {
            "park_id": park_id,
            "stat_date": aggregation_date
        }).fetchone()

        assert park_stats is not None

        # Total downtime: 21 down / 252 total = 0.0833 ratio
        # 830 minutes (13.83 hours) × 0.0833 = 69.139 minutes = 1.15 hours
        downtime_hours = float(park_stats.total_downtime_hours)
        assert abs(downtime_hours - 1.15) < 0.01, f"Expected ~1.15 hours, got {downtime_hours}"

        # Uptime: 231 / 252 = 91.67%
        uptime_pct = float(park_stats.avg_uptime_percentage)
        assert abs(uptime_pct - 91.67) < 0.1, f"Expected ~91.67%, got {uptime_pct}"

        # 2 rides had downtime (B and C)
        assert park_stats.rides_with_downtime == 2
        assert park_stats.total_rides_tracked == 3

    def test_no_operating_hours_skips_aggregation(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """
        Test that aggregation is skipped when no operating hours detected.

        Scenario:
        - Park exists but no operating session for the day
        - Snapshots may exist but no hours to aggregate against

        Expected:
        - No park_daily_stats record created
        - No ride_daily_stats records created
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        aggregation_date = date.today()

        # NO operating session created - park appears closed

        # Act: Run daily aggregation
        service = AggregationService(mysql_session)
        result = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        # Assert: No stats created
        park_stats_query = text("""
            SELECT COUNT(*) FROM park_daily_stats
            WHERE park_id = :park_id AND stat_date = :stat_date
        """)
        park_count = mysql_session.execute(park_stats_query, {
            "park_id": park_id,
            "stat_date": aggregation_date
        }).scalar()

        assert park_count == 0, "Should not create park_daily_stats without operating hours"

        ride_stats_query = text("""
            SELECT COUNT(*) FROM ride_daily_stats
            WHERE ride_id = :ride_id AND stat_date = :stat_date
        """)
        ride_count = mysql_session.execute(ride_stats_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).scalar()

        assert ride_count == 0, "Should not create ride_daily_stats without operating hours"

    def test_upsert_behavior_on_rerun(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """
        Test that running aggregation twice UPDATES existing records (UPSERT).

        Scenario:
        - Run aggregation once with initial data
        - Add more snapshots (simulating late data arrival)
        - Run aggregation again

        Expected:
        - Only 1 park_daily_stats record (updated, not duplicated)
        - Only 1 ride_daily_stats record (updated, not duplicated)
        - Values updated to reflect new data
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        aggregation_date = date.today()
        tz = ZoneInfo(sample_park_data['timezone'])

        # Setup: Operating session
        session_start = datetime.combine(aggregation_date, time(9, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))
        session_end = datetime.combine(aggregation_date, time(23, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))

        operating_session_query = text("""
            INSERT INTO park_operating_sessions (
                park_id, session_date, session_start_utc, session_end_utc, operating_minutes
            ) VALUES (
                :park_id, :session_date, :session_start_utc, :session_end_utc, :operating_minutes
            )
        """)
        mysql_session.execute(operating_session_query, {
            "park_id": park_id,
            "session_date": aggregation_date,
            "session_start_utc": session_start,
            "session_end_utc": session_end,
            "operating_minutes": 840
        })

        # Setup: Initial snapshots (50 total, 5 down = 10% downtime)
        snapshot_insert = text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, is_open, wait_time, last_updated_api, computed_is_open
            ) VALUES (
                :ride_id, :recorded_at, :is_open, :wait_time, :last_updated_api, :computed_is_open
            )
        """)

        current_time = session_start
        for i in range(50):
            is_down = i < 5  # First 5 are down
            mysql_session.execute(snapshot_insert, {
                "ride_id": ride_id,
                "recorded_at": current_time,
                "is_open": not is_down,
                "wait_time": 0 if is_down else 30,
                "last_updated_api": current_time,
                "computed_is_open": not is_down
            })
            current_time += timedelta(minutes=10)

        # Act 1: Run aggregation first time
        service = AggregationService(mysql_session)
        result1 = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        assert result1['status'] == 'success'

        # Get initial downtime
        ride_stats_query = text("""
            SELECT downtime_minutes FROM ride_daily_stats
            WHERE ride_id = :ride_id AND stat_date = :stat_date
        """)
        initial_downtime = mysql_session.execute(ride_stats_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).scalar()

        # Should be: 5 down / 50 total = 10% × 490 minutes = 49 minutes
        assert initial_downtime == 49

        # Setup: Add more snapshots (34 more, 2 down = different ratio)
        for i in range(34):
            is_down = i < 2  # 2 more down periods
            mysql_session.execute(snapshot_insert, {
                "ride_id": ride_id,
                "recorded_at": current_time,
                "is_open": not is_down,
                "wait_time": 0 if is_down else 30,
                "last_updated_api": current_time,
                "computed_is_open": not is_down
            })
            current_time += timedelta(minutes=10)

        # Act 2: Run aggregation second time
        result2 = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        assert result2['status'] == 'success'

        # Assert: Only 1 record exists (UPSERT, not duplicate)
        count_query = text("""
            SELECT COUNT(*) FROM ride_daily_stats
            WHERE ride_id = :ride_id AND stat_date = :stat_date
        """)
        count = mysql_session.execute(count_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).scalar()

        assert count == 1, "Should have exactly 1 record (UPSERT behavior)"

        # Assert: Values updated
        updated_downtime = mysql_session.execute(ride_stats_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).scalar()

        # Now: 7 down / 84 total = 8.33% × 830 minutes = 69 minutes
        assert updated_downtime == 69, f"Should update to 69, got {updated_downtime}"
        assert updated_downtime != initial_downtime, "Downtime should have changed"


    def test_100_percent_downtime_ride(
        self, mysql_session, sample_park_data, sample_ride_data
    ):
        """
        Test ride that is down for 100% of the operating day.

        Scenario:
        - Ride closed for maintenance entire day
        - All snapshots show downtime (computed_is_open = FALSE)
        - Operating hours: 830 minutes

        Expected:
        - downtime_minutes = 830 (100% of operating time)
        - uptime_minutes = 0
        - uptime_percentage = 0%
        - avg_wait_time = NULL (no wait time when closed)
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_session, sample_park_data)
        sample_ride_data['park_id'] = park_id  # Link ride to park
        ride_id = insert_sample_ride(mysql_session, sample_ride_data)

        aggregation_date = date.today()
        tz = ZoneInfo(sample_park_data['timezone'])
        session_start = datetime.combine(aggregation_date, time(9, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))

        # Create 84 snapshots - ALL showing downtime
        snapshot_insert = text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, is_open, wait_time, last_updated_api, computed_is_open
            ) VALUES (
                :ride_id, :recorded_at, :is_open, :wait_time, :last_updated_api, :computed_is_open
            )
        """)

        current_time = session_start
        for i in range(84):
            mysql_session.execute(snapshot_insert, {
                "ride_id": ride_id,
                "recorded_at": current_time,
                "is_open": False,
                "wait_time": 0,
                "last_updated_api": current_time,
                "computed_is_open": False  # ALL down
            })
            current_time += timedelta(minutes=10)

        # Act: Run aggregation
        service = AggregationService(mysql_session)
        result = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=sample_park_data['timezone']
        )

        # Assert: Aggregation succeeded
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1

        # Assert: 100% downtime recorded
        ride_stats_query = text("""
            SELECT
                uptime_minutes,
                downtime_minutes,
                uptime_percentage,
                avg_wait_time
            FROM ride_daily_stats
            WHERE ride_id = :ride_id AND stat_date = :stat_date
        """)
        ride_stats = mysql_session.execute(ride_stats_query, {
            "ride_id": ride_id,
            "stat_date": aggregation_date
        }).fetchone()

        assert ride_stats is not None
        assert ride_stats.downtime_minutes == 830, "Should have 100% downtime"
        assert ride_stats.uptime_minutes == 0, "Should have 0 uptime"
        assert float(ride_stats.uptime_percentage) == 0.0, "Uptime percentage should be 0%"
        assert ride_stats.avg_wait_time is None, "No wait time when ride is closed"


class TestTimezoneAwareAggregation:
    """Test aggregation respects park timezones."""

    def test_multiple_timezones_aggregated_separately(
        self, mysql_session, sample_park_data
    ):
        """
        Test that parks in different timezones are aggregated correctly.

        Scenario:
        - Park A in America/New_York (EST)
        - Park B in America/Los_Angeles (PST)
        - Same calendar date (2025-11-22) represents different UTC windows

        Expected:
        - Both parks aggregated with correct timezone boundaries
        - EST park: 2025-11-22 00:00 EST to 23:59 EST
        - PST park: 2025-11-22 00:00 PST to 23:59 PST (3 hours later in UTC)
        """
        from tests.conftest import insert_sample_park

        # Create park in EST
        park_est_data = sample_park_data.copy()
        park_est_data['timezone'] = 'America/New_York'
        park_est_data['name'] = 'Magic Kingdom'
        park_est_id = insert_sample_park(mysql_session, park_est_data)

        # Create park in PST
        park_pst_data = sample_park_data.copy()
        park_pst_data['queue_times_id'] = 102
        park_pst_data['timezone'] = 'America/Los_Angeles'
        park_pst_data['name'] = 'Disneyland'
        park_pst_id = insert_sample_park(mysql_session, park_pst_data)

        aggregation_date = date.today()

        # Setup: Create rides and snapshots for both parks
        for park_id, timezone_str in [(park_est_id, 'America/New_York'),
                                       (park_pst_id, 'America/Los_Angeles')]:
            # Create a ride for this park
            ride_insert = text("""
                INSERT INTO rides (park_id, queue_times_id, name, is_active)
                VALUES (:park_id, :queue_times_id, :name, 1)
            """)
            result = mysql_session.execute(ride_insert, {
                "park_id": park_id,
                "queue_times_id": 2000 + park_id,
                "name": f"Test Ride {park_id}"
            })
            ride_id = result.lastrowid

            # Create snapshots for 12-hour day (9 AM - 9 PM)
            tz = ZoneInfo(timezone_str)
            session_start = datetime.combine(aggregation_date, time(9, 0), tzinfo=tz).astimezone(ZoneInfo('UTC'))

            snapshot_insert = text("""
                INSERT INTO ride_status_snapshots (
                    ride_id, recorded_at, is_open, wait_time, last_updated_api, computed_is_open
                ) VALUES (
                    :ride_id, :recorded_at, :is_open, :wait_time, :last_updated_api, :computed_is_open
                )
            """)

            # Create 73 snapshots (9 AM to 9 PM = 12 hours = 72 intervals + 1)
            current_time = session_start
            for i in range(73):
                mysql_session.execute(snapshot_insert, {
                    "ride_id": ride_id,
                    "recorded_at": current_time,
                    "is_open": True,
                    "wait_time": 30,
                    "last_updated_api": current_time,
                    "computed_is_open": True
                })
                current_time += timedelta(minutes=10)

        # Act: Run aggregation for all timezones
        service = AggregationService(mysql_session)
        result = service.aggregate_daily(
            aggregation_date=aggregation_date,
            park_timezone=None  # Aggregate all timezones
        )

        # Assert: Both parks aggregated
        assert result['status'] == 'success'
        assert result['parks_processed'] == 2

        # Verify both parks have stats
        park_stats_query = text("""
            SELECT park_id, stat_date FROM park_daily_stats
            WHERE stat_date = :stat_date
            ORDER BY park_id
        """)
        park_stats = mysql_session.execute(park_stats_query, {
            "stat_date": aggregation_date
        }).fetchall()

        assert len(park_stats) == 2
        assert park_stats[0].park_id == park_est_id
        assert park_stats[1].park_id == park_pst_id
