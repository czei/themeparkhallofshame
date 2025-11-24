"""
Integration tests for weekly aggregation.

Test-Driven Development: These tests are written BEFORE implementing
aggregate_weekly() to ensure correct behavior from the start.

Weekly Aggregation Logic:
- Aggregates FROM daily stats (NOT from raw snapshots)
- Uses ISO week numbers (1-53)
- week_start_date = Monday of the ISO week
- Sums: uptime_minutes, downtime_minutes, operating_hours_minutes, status_changes
- Calculates: uptime_percentage from summed totals
- Averages: avg_wait_time (weighted by operating_hours_minutes)
- Max: peak_wait_time across the week
- Trends: Compares to previous week's downtime
"""

import pytest
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text

from processor.aggregation_service import AggregationService


class TestWeeklyAggregationMath:
    """Test mathematical correctness of weekly aggregation calculations."""

    def test_single_ride_full_week_aggregation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test weekly aggregation for a single ride across 7 days.

        Scenario:
        - Week 48 of 2025 (Nov 24-30, Monday-Sunday)
        - 7 days of daily stats already aggregated
        - Each day: 830 operating minutes, 69 downtime minutes, 91.67% uptime
        - Total week: 5,810 operating minutes, 483 downtime minutes

        Expected Weekly Stats:
        - uptime_minutes = 5,327 (sum of daily uptime)
        - downtime_minutes = 483 (sum of daily downtime)
        - operating_hours_minutes = 5,810 (sum of daily operating minutes)
        - uptime_percentage = (5,327 / 5,810) × 100 = 91.67%
        - avg_wait_time = weighted average across 7 days
        - peak_wait_time = max across 7 days
        - status_changes = sum of daily status changes
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Setup: Insert 7 days of daily stats for week 48 (Nov 24-30, 2025)
        week_start = date(2025, 11, 24)  # Monday
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, min_wait_time, max_wait_time,
                peak_wait_time, status_changes, longest_downtime_minutes
            ) VALUES (
                :ride_id, :stat_date, :uptime_minutes, :downtime_minutes, :uptime_percentage,
                :operating_hours_minutes, :avg_wait_time, :min_wait_time, :max_wait_time,
                :peak_wait_time, :status_changes, :longest_downtime_minutes
            )
        """)

        for day_offset in range(7):
            stat_date = week_start + timedelta(days=day_offset)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date,
                "uptime_minutes": 760,
                "downtime_minutes": 69,
                "uptime_percentage": 91.67,
                "operating_hours_minutes": 830,
                "avg_wait_time": 35.0 + day_offset,  # Varies by day
                "min_wait_time": 20,
                "max_wait_time": 50 + day_offset,  # Varies: 50-56
                "peak_wait_time": 50 + day_offset,
                "status_changes": 3,
                "longest_downtime_minutes": 20
            })

        # Act: Run weekly aggregation for week 48 of 2025
        service = AggregationService(mysql_connection)
        result = service.aggregate_weekly(
            year=2025,
            week_number=48
        )

        # Assert: Aggregation succeeded
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1
        assert result['parks_processed'] == 1

        # Assert: ride_weekly_stats has correct calculations
        ride_stats_query = text("""
            SELECT
                uptime_minutes,
                downtime_minutes,
                uptime_percentage,
                operating_hours_minutes,
                avg_wait_time,
                peak_wait_time,
                status_changes,
                week_start_date
            FROM ride_weekly_stats
            WHERE ride_id = :ride_id AND year = :year AND week_number = :week_number
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id,
            "year": 2025,
            "week_number": 48
        }).fetchone()

        assert ride_stats is not None, "ride_weekly_stats record should exist"

        # Verify sums
        assert ride_stats.uptime_minutes == 5320, "Sum of 7 days: 760 × 7"
        assert ride_stats.downtime_minutes == 483, "Sum of 7 days: 69 × 7"
        assert ride_stats.operating_hours_minutes == 5810, "Sum of 7 days: 830 × 7"
        assert ride_stats.status_changes == 21, "Sum of 7 days: 3 × 7"

        # Verify calculated values
        uptime_pct = float(ride_stats.uptime_percentage)
        assert abs(uptime_pct - 91.67) < 0.1, f"Uptime percentage should be ~91.67%, got {uptime_pct}"

        # Weighted average wait time: (35 + 36 + 37 + 38 + 39 + 40 + 41) / 7 = 38.0
        assert abs(float(ride_stats.avg_wait_time) - 38.0) < 0.1

        # Peak should be max across week: 56
        assert ride_stats.peak_wait_time == 56

        # Week start should be Monday Nov 24
        assert ride_stats.week_start_date == week_start

    def test_weekly_aggregation_with_missing_days(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test weekly aggregation when some days have no data.

        Scenario:
        - Week 48 has data for only 3 days (Mon, Wed, Fri)
        - Other days have no daily_stats records

        Expected:
        - Weekly stats should sum only the 3 days with data
        - Should not assume zero for missing days
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Insert data for only 3 days
        week_start = date(2025, 11, 24)
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 760, 69, 91.67, 830, 35.0, 50, 3
            )
        """)

        # Only Mon (0), Wed (2), Fri (4)
        for day_offset in [0, 2, 4]:
            stat_date = week_start + timedelta(days=day_offset)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_weekly(year=2025, week_number=48)

        # Assert: Should sum only 3 days
        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes, operating_hours_minutes
            FROM ride_weekly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND week_number = 48
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        assert ride_stats.uptime_minutes == 2280, "3 days × 760"
        assert ride_stats.downtime_minutes == 207, "3 days × 69"
        assert ride_stats.operating_hours_minutes == 2490, "3 days × 830"

    def test_weekly_trend_calculation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test trend_vs_previous_week calculation.

        Scenario:
        - Week 47: 400 downtime minutes
        - Week 48: 483 downtime minutes
        - Trend: ((483 - 400) / 400) × 100 = +20.75%

        Expected:
        - trend_vs_previous_week = 20.75
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Insert daily stats for week 47 (Nov 17-23)
        week_47_start = date(2025, 11, 17)
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, :uptime, :downtime, 93.0, 700, 30.0, 45, 2
            )
        """)

        # Week 47: Total 400 downtime minutes across 7 days
        for day_offset in range(7):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": week_47_start + timedelta(days=day_offset),
                "uptime": 650,
                "downtime": 57  # 57 × 7 ≈ 400
            })

        # Insert daily stats for week 48 (Nov 24-30)
        week_48_start = date(2025, 11, 24)
        for day_offset in range(7):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": week_48_start + timedelta(days=day_offset),
                "uptime": 760,
                "downtime": 69  # 69 × 7 = 483
            })

        # Act: Aggregate week 47 first
        service = AggregationService(mysql_connection)
        service.aggregate_weekly(year=2025, week_number=47)

        # Then aggregate week 48 (should calculate trend)
        result = service.aggregate_weekly(year=2025, week_number=48)

        # Assert: Trend calculated correctly
        ride_stats_query = text("""
            SELECT downtime_minutes, trend_vs_previous_week
            FROM ride_weekly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND week_number = 48
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        assert ride_stats.downtime_minutes == 483
        trend = float(ride_stats.trend_vs_previous_week)
        expected_trend = ((483 - 399) / 399.0) * 100  # 399 = 57 × 7
        assert abs(trend - expected_trend) < 0.5, f"Expected trend ~{expected_trend}%, got {trend}%"


class TestParkWeeklyAggregation:
    """Test park-level weekly aggregation."""

    def test_park_weekly_aggregation_multiple_rides(
        self, mysql_connection, sample_park_data
    ):
        """
        Test park weekly stats aggregate correctly across multiple rides.

        Scenario:
        - Park with 3 rides
        - Each has different daily patterns across week 48

        Expected:
        - Park stats sum total_downtime_hours across all rides
        - avg_uptime_percentage is calculated from total uptime/downtime
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create 3 rides
        ride_ids = []
        for i in range(3):
            ride_insert = text("""
                INSERT INTO rides (park_id, queue_times_id, name, is_active)
                VALUES (:park_id, :queue_times_id, :name, 1)
            """)
            result = mysql_connection.execute(ride_insert, {
                "park_id": park_id,
                "queue_times_id": 2000 + i,
                "name": f"Ride {chr(65 + i)}"
            })
            ride_ids.append(result.lastrowid)

        # Insert daily stats for all 3 rides for week 48
        week_start = date(2025, 11, 24)
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, :uptime, :downtime, :uptime_pct,
                :operating, :avg_wait, :peak_wait, :changes
            )
        """)

        # Each ride has different patterns
        ride_patterns = [
            {"uptime": 800, "downtime": 30, "uptime_pct": 96.4, "operating": 830, "avg_wait": 40, "peak_wait": 60, "changes": 2},
            {"uptime": 760, "downtime": 69, "uptime_pct": 91.7, "operating": 830, "avg_wait": 35, "peak_wait": 50, "changes": 3},
            {"uptime": 720, "downtime": 110, "uptime_pct": 86.7, "operating": 830, "avg_wait": 30, "peak_wait": 45, "changes": 5}
        ]

        for ride_idx, ride_id in enumerate(ride_ids):
            pattern = ride_patterns[ride_idx]
            for day_offset in range(7):
                mysql_connection.execute(daily_stats_insert, {
                    "ride_id": ride_id,
                    "stat_date": week_start + timedelta(days=day_offset),
                    **pattern
                })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_weekly(year=2025, week_number=48)

        assert result['status'] == 'success'
        assert result['parks_processed'] == 1
        assert result['rides_processed'] == 3

        # Assert: Park weekly stats correct
        park_stats_query = text("""
            SELECT
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked
            FROM park_weekly_stats
            WHERE park_id = :park_id AND year = 2025 AND week_number = 48
        """)
        park_stats = mysql_connection.execute(park_stats_query, {
            "park_id": park_id
        }).fetchone()

        # Total downtime across 3 rides, 7 days: (30 + 69 + 110) × 7 = 1,463 min = 24.38 hours
        downtime_hours = float(park_stats.total_downtime_hours)
        assert abs(downtime_hours - 24.38) < 0.1

        # Average uptime: (800 + 760 + 720) / (830 + 830 + 830) × 100 = 89.16%
        # But this needs to be across 7 days: (2280 × 3) / (2490 × 3) × 100 = 91.57%
        # Actually: Total uptime = (800 + 760 + 720) × 7 = 15,680
        #           Total operating = 830 × 3 × 7 = 17,430
        #           Percentage = 15,680 / 17,430 × 100 = 89.96%
        uptime_pct = float(park_stats.avg_uptime_percentage)
        assert abs(uptime_pct - 89.96) < 0.5

        assert park_stats.rides_with_downtime == 3
        assert park_stats.total_rides_tracked == 3


class TestWeeklyAggregationEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_no_daily_stats_for_week(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test weekly aggregation when no daily stats exist for the week.

        Expected:
        - No weekly stats records created
        - Aggregation completes successfully with 0 rides/parks processed
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Act: Try to aggregate week 48 with no daily data
        service = AggregationService(mysql_connection)
        result = service.aggregate_weekly(year=2025, week_number=48)

        # Assert: Completes but processes nothing
        assert result['status'] == 'success'
        assert result['rides_processed'] == 0
        assert result['parks_processed'] == 0

        # No records created
        ride_count_query = text("""
            SELECT COUNT(*) FROM ride_weekly_stats
            WHERE year = 2025 AND week_number = 48
        """)
        count = mysql_connection.execute(ride_count_query).scalar()
        assert count == 0

    def test_weekly_upsert_behavior(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test that running weekly aggregation twice updates existing records.

        Scenario:
        - Run aggregation with 5 days of data
        - Add 2 more days of data
        - Run aggregation again

        Expected:
        - Only 1 record exists (UPSERT)
        - Values updated to reflect all 7 days
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        week_start = date(2025, 11, 24)
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 760, 69, 91.67, 830, 35.0, 50, 3
            )
        """)

        # Insert 5 days
        for day_offset in range(5):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": week_start + timedelta(days=day_offset)
            })

        # First aggregation
        service = AggregationService(mysql_connection)
        service.aggregate_weekly(year=2025, week_number=48)

        # Get initial values
        ride_stats_query = text("""
            SELECT downtime_minutes FROM ride_weekly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND week_number = 48
        """)
        initial_downtime = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).scalar()

        assert initial_downtime == 345, "5 days × 69"

        # Add 2 more days
        for day_offset in range(5, 7):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": week_start + timedelta(days=day_offset)
            })

        # Second aggregation
        service.aggregate_weekly(year=2025, week_number=48)

        # Assert: Only 1 record, values updated
        count_query = text("""
            SELECT COUNT(*) FROM ride_weekly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND week_number = 48
        """)
        count = mysql_connection.execute(count_query, {"ride_id": ride_id}).scalar()
        assert count == 1, "Should have exactly 1 record (UPSERT)"

        updated_downtime = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).scalar()
        assert updated_downtime == 483, "7 days × 69"
