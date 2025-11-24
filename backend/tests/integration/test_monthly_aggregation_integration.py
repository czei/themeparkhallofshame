"""
Integration tests for monthly aggregation service.

Tests monthly aggregation FROM daily stats with:
- Mathematical correctness (sums, averages, percentages)
- Edge cases (partial months, leap years, missing data)
- Trends: Compares to previous month's downtime
- Month boundary handling (Dec -> Jan year transition)
"""

import pytest
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import text

from processor.aggregation_service import AggregationService


# ============================================================================
# FIXTURES - Cleanup
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def cleanup_before_monthly_aggregation_tests(mysql_connection):
    """Clean up all test data once at start of this test module."""
    mysql_connection.execute(text("DELETE FROM ride_status_snapshots"))
    mysql_connection.execute(text("DELETE FROM ride_status_changes"))
    mysql_connection.execute(text("DELETE FROM park_activity_snapshots"))
    mysql_connection.execute(text("DELETE FROM ride_daily_stats"))
    mysql_connection.execute(text("DELETE FROM ride_weekly_stats"))
    mysql_connection.execute(text("DELETE FROM ride_monthly_stats"))
    mysql_connection.execute(text("DELETE FROM park_daily_stats"))
    mysql_connection.execute(text("DELETE FROM park_weekly_stats"))
    mysql_connection.execute(text("DELETE FROM park_monthly_stats"))
    mysql_connection.execute(text("DELETE FROM ride_classifications"))
    mysql_connection.execute(text("DELETE FROM rides"))
    mysql_connection.execute(text("DELETE FROM parks"))
    mysql_connection.commit()
    yield


class TestMonthlyAggregationMath:
    """Test mathematical correctness of monthly aggregation calculations."""

    def test_full_month_aggregation_30_days(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test monthly aggregation for a 30-day month (November).

        Scenario:
        - November 2025 (30 days)
        - Daily stats already aggregated for all 30 days
        - Each day: 840 operating minutes, 60 downtime minutes, 92.86% uptime
        - Total month: 25,200 operating minutes, 1,800 downtime minutes

        Expected Monthly Stats:
        - uptime_minutes = 23,400 (780 × 30)
        - downtime_minutes = 1,800 (60 × 30)
        - operating_hours_minutes = 25,200 (840 × 30)
        - uptime_percentage = (23,400 / 25,200) × 100 = 92.86%
        - avg_wait_time = weighted average across 30 days
        - peak_wait_time = max across 30 days
        - status_changes = sum of daily status changes
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Setup: Create park and ride
        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Setup: Insert 30 days of daily stats for November 2025
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

        for day_num in range(1, 31):  # November has 30 days
            stat_date = date(2025, 11, day_num)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date,
                "uptime_minutes": 780,
                "downtime_minutes": 60,
                "uptime_percentage": 92.86,
                "operating_hours_minutes": 840,
                "avg_wait_time": 30.0 + (day_num % 10),  # Varies: 30-40
                "min_wait_time": 15,
                "max_wait_time": 50 + (day_num % 5),  # Varies: 50-54
                "peak_wait_time": 50 + (day_num % 5),
                "status_changes": 4,
                "longest_downtime_minutes": 15
            })

        # Act: Run monthly aggregation for November 2025
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert: Aggregation succeeded
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1
        assert result['parks_processed'] == 1

        # Assert: ride_monthly_stats has correct calculations
        ride_stats_query = text("""
            SELECT
                uptime_minutes,
                downtime_minutes,
                uptime_percentage,
                operating_hours_minutes,
                avg_wait_time,
                peak_wait_time,
                status_changes
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = :year AND month = :month
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id,
            "year": 2025,
            "month": 11
        }).fetchone()

        assert ride_stats is not None, "ride_monthly_stats record should exist"

        # Verify sums
        assert ride_stats.uptime_minutes == 23400, "Sum of 30 days: 780 × 30"
        assert ride_stats.downtime_minutes == 1800, "Sum of 30 days: 60 × 30"
        assert ride_stats.operating_hours_minutes == 25200, "Sum of 30 days: 840 × 30"
        assert ride_stats.status_changes == 120, "Sum of 30 days: 4 × 30"

        # Verify calculated values
        uptime_pct = float(ride_stats.uptime_percentage)
        assert abs(uptime_pct - 92.86) < 0.1, f"Uptime percentage should be ~92.86%, got {uptime_pct}"

        # Weighted average wait time calculation
        # Days 1-30: wait_time = 30.0 + (day_num % 10)
        # Results in: 31,32,33,34,35,36,37,38,39,30 (repeats 3 times)
        # Average: 34.5
        assert abs(float(ride_stats.avg_wait_time) - 34.5) < 1.0

        # Peak should be max across month: 54
        assert ride_stats.peak_wait_time == 54

    def test_full_month_aggregation_31_days(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test monthly aggregation for a 31-day month (January).

        Tests different month length and validates calculations.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, :uptime, :downtime, :uptime_pct,
                :operating, :avg_wait, :peak_wait, :changes
            )
        """)

        for day_num in range(1, 32):  # January has 31 days
            stat_date = date(2025, 1, day_num)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date,
                "uptime": 800,
                "downtime": 30,
                "uptime_pct": 96.39,
                "operating": 830,
                "avg_wait": 25.0,
                "peak_wait": 45,
                "changes": 2
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=1)

        # Assert
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes, operating_hours_minutes
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 1
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        # Verify 31-day calculations
        assert ride_stats.uptime_minutes == 24800, "Sum of 31 days: 800 × 31"
        assert ride_stats.downtime_minutes == 930, "Sum of 31 days: 30 × 31"
        assert ride_stats.operating_hours_minutes == 25730, "Sum of 31 days: 830 × 31"

    def test_leap_year_february_aggregation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test February aggregation in leap year (29 days).

        Edge case: Leap year February has 29 days.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 750, 90, 89.29, 840, 35.0, 55, 5
            )
        """)

        # 2024 is a leap year
        for day_num in range(1, 30):  # February 2024 has 29 days
            stat_date = date(2024, 2, day_num)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2024, month=2)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2024 AND month = 2
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        # Verify 29-day calculations
        assert ride_stats.uptime_minutes == 21750, "Sum of 29 days: 750 × 29"
        assert ride_stats.downtime_minutes == 2610, "Sum of 29 days: 90 × 29"

    def test_non_leap_year_february_aggregation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test February aggregation in non-leap year (28 days).

        Edge case: Non-leap year February has 28 days.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes, uptime_percentage,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 750, 90, 89.29, 840, 35.0, 55, 5
            )
        """)

        # 2025 is NOT a leap year
        for day_num in range(1, 29):  # February 2025 has 28 days
            stat_date = date(2025, 2, day_num)
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": stat_date
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=2)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 2
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        # Verify 28-day calculations
        assert ride_stats.uptime_minutes == 21000, "Sum of 28 days: 750 × 28"
        assert ride_stats.downtime_minutes == 2520, "Sum of 28 days: 90 × 28"

    def test_monthly_trend_calculation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test month-over-month trend calculation.

        Scenario:
        - October 2025: 1,200 downtime minutes
        - November 2025: 1,800 downtime minutes
        - Expected trend: ((1,800 - 1,200) / 1,200) × 100 = +50.0%
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, :uptime, :downtime, :operating, 30.0, 50, 3
            )
        """)

        # October 2025: 31 days with lower downtime (40 min/day = 1,240 total)
        for day_num in range(1, 32):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 10, day_num),
                "uptime": 800,
                "downtime": 40,
                "operating": 840
            })

        # November 2025: 30 days with higher downtime (60 min/day = 1,800 total)
        for day_num in range(1, 31):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 11, day_num),
                "uptime": 780,
                "downtime": 60,
                "operating": 840
            })

        # Act: Aggregate October first (needed for trend calculation)
        service = AggregationService(mysql_connection)
        service.aggregate_monthly(year=2025, month=10)

        # Then aggregate November
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT downtime_minutes, trend_vs_previous_month
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 11
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        assert ride_stats.downtime_minutes == 1800, "November downtime: 60 × 30"

        # Trend: ((1800 - 1240) / 1240) × 100 = +45.16%
        trend = float(ride_stats.trend_vs_previous_month)
        expected_trend = ((1800 - 1240) / 1240) * 100
        assert abs(trend - expected_trend) < 0.5, f"Expected trend ~{expected_trend:.2f}%, got {trend}%"

    def test_monthly_trend_year_boundary(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test trend calculation across year boundary (Dec -> Jan).

        Scenario:
        - December 2024: 1,500 downtime minutes
        - January 2025: 1,200 downtime minutes
        - Expected trend: ((1,200 - 1,500) / 1,500) × 100 = -20.0%
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, :uptime, :downtime, 840, 30.0, 50, 3
            )
        """)

        # December 2024: 31 days with higher downtime
        for day_num in range(1, 32):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2024, 12, day_num),
                "uptime": 800,
                "downtime": 50,  # 50 × 31 = 1,550
                "operating": 850
            })

        # January 2025: 31 days with lower downtime
        for day_num in range(1, 32):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 1, day_num),
                "uptime": 810,
                "downtime": 40,  # 40 × 31 = 1,240
                "operating": 850
            })

        # Act
        service = AggregationService(mysql_connection)
        service.aggregate_monthly(year=2024, month=12)
        result = service.aggregate_monthly(year=2025, month=1)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT downtime_minutes, trend_vs_previous_month
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 1
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        # Trend: ((1240 - 1550) / 1550) × 100 = -20.0%
        trend = float(ride_stats.trend_vs_previous_month)
        expected_trend = ((1240 - 1550) / 1550) * 100
        assert abs(trend - expected_trend) < 0.5, f"Expected trend ~{expected_trend:.2f}%, got {trend}%"

    def test_partial_month_aggregation(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test aggregation with partial month data (only 15 days).

        Edge case: Month has incomplete data.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 760, 80, 840, 35.0, 50, 4
            )
        """)

        # Only first 15 days of November 2025
        for day_num in range(1, 16):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 11, day_num)
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert: Should aggregate partial data correctly
        assert result['status'] == 'success'
        assert result['rides_processed'] == 1

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 11
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        # Should sum only the 15 days present
        assert ride_stats.uptime_minutes == 11400, "Sum of 15 days: 760 × 15"
        assert ride_stats.downtime_minutes == 1200, "Sum of 15 days: 80 × 15"


class TestParkMonthlyAggregation:
    """Test park-level monthly aggregation."""

    def test_park_monthly_aggregation_multiple_rides(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test park-level monthly aggregation across multiple rides.

        Scenario:
        - November 2025 (30 days)
        - 3 rides with different patterns
        - Verify park stats aggregate correctly
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create 3 rides with unique queue_times_id
        ride_ids = []
        for i in range(3):
            ride_data = sample_ride_data.copy()
            ride_data['park_id'] = park_id
            ride_data['queue_times_id'] = sample_ride_data['queue_times_id'] + i
            ride_data['name'] = f"Test Ride {i+1}"
            ride_data['thrill_level'] = ['family', 'moderate', 'extreme'][i]
            ride_id = insert_sample_ride(mysql_connection, ride_data)
            ride_ids.append(ride_id)

        # Insert daily stats with different patterns
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
            {"uptime": 820, "downtime": 20, "uptime_pct": 97.6, "operating": 840, "avg_wait": 45, "peak_wait": 70, "changes": 1},
            {"uptime": 780, "downtime": 60, "uptime_pct": 92.9, "operating": 840, "avg_wait": 35, "peak_wait": 55, "changes": 3},
            {"uptime": 700, "downtime": 140, "uptime_pct": 83.3, "operating": 840, "avg_wait": 25, "peak_wait": 40, "changes": 7}
        ]

        for ride_idx, ride_id in enumerate(ride_ids):
            pattern = ride_patterns[ride_idx]
            for day_num in range(1, 31):  # 30 days
                mysql_connection.execute(daily_stats_insert, {
                    "ride_id": ride_id,
                    "stat_date": date(2025, 11, day_num),
                    **pattern
                })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        assert result['status'] == 'success'
        assert result['parks_processed'] == 1
        assert result['rides_processed'] == 3

        # Assert: Park monthly stats correct
        park_stats_query = text("""
            SELECT
                total_downtime_hours,
                avg_uptime_percentage,
                rides_with_downtime,
                total_rides_tracked,
                avg_wait_time,
                peak_wait_time
            FROM park_monthly_stats
            WHERE park_id = :park_id AND year = 2025 AND month = 11
        """)
        park_stats = mysql_connection.execute(park_stats_query, {
            "park_id": park_id
        }).fetchone()

        # Total downtime across 3 rides, 30 days: (20 + 60 + 140) × 30 = 6,600 min = 110 hours
        downtime_hours = float(park_stats.total_downtime_hours)
        assert abs(downtime_hours - 110.0) < 0.5

        # Average uptime percentage (average of ride percentages)
        # Ride 1: 97.6%, Ride 2: 92.9%, Ride 3: 83.3%
        # Average: (97.6 + 92.9 + 83.3) / 3 = 91.27%
        uptime_pct = float(park_stats.avg_uptime_percentage)
        assert abs(uptime_pct - 91.27) < 1.0

        assert park_stats.rides_with_downtime == 3
        assert park_stats.total_rides_tracked == 3

        # Peak wait time should be max across all rides: 70
        assert park_stats.peak_wait_time == 70


class TestMonthlyAggregationEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_no_daily_stats_for_month(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test monthly aggregation when no daily stats exist.

        Edge case: Should return gracefully with zero counts.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Act: No daily stats inserted
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert: Should succeed with zero rides processed
        assert result['status'] == 'success'
        assert result['rides_processed'] == 0
        assert result['parks_processed'] == 0

    def test_monthly_upsert_behavior(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test UPSERT behavior when running monthly aggregation twice.

        Edge case: Should update existing record, not create duplicate.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        # Insert daily stats
        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 800, 40, 840, 30.0, 50, 3
            )
        """)

        for day_num in range(1, 31):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 11, day_num)
            })

        # Act: Run aggregation twice
        service = AggregationService(mysql_connection)
        result1 = service.aggregate_monthly(year=2025, month=11)
        result2 = service.aggregate_monthly(year=2025, month=11)

        # Assert: Both succeed
        assert result1['status'] == 'success'
        assert result2['status'] == 'success'

        # Should only have ONE record (UPSERT behavior)
        count_query = text("""
            SELECT COUNT(*) as count
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 11
        """)
        count_result = mysql_connection.execute(count_query, {
            "ride_id": ride_id
        }).fetchone()

        assert count_result.count == 1, "Should have exactly 1 record (UPSERT, not duplicate)"

    def test_zero_downtime_month(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test month with zero downtime (perfect uptime).

        Edge case: All days have 0 downtime.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 840, 0, 840, 30.0, 50, 0
            )
        """)

        for day_num in range(1, 31):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 11, day_num)
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes, uptime_percentage
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 11
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        assert ride_stats.uptime_minutes == 25200, "840 × 30 = 25,200"
        assert ride_stats.downtime_minutes == 0, "Zero downtime"
        assert float(ride_stats.uptime_percentage) == 100.0, "Perfect uptime"

    def test_100_percent_downtime_month(
        self, mysql_connection, sample_park_data, sample_ride_data
    ):
        """
        Test month with 100% downtime (ride closed all month).

        Edge case: Ride closed for maintenance entire month.
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id
        ride_id = insert_sample_ride(mysql_connection, sample_ride_data)

        daily_stats_insert = text("""
            INSERT INTO ride_daily_stats (
                ride_id, stat_date, uptime_minutes, downtime_minutes,
                operating_hours_minutes, avg_wait_time, peak_wait_time, status_changes
            ) VALUES (
                :ride_id, :stat_date, 0, 840, 840, NULL, NULL, 0
            )
        """)

        for day_num in range(1, 31):
            mysql_connection.execute(daily_stats_insert, {
                "ride_id": ride_id,
                "stat_date": date(2025, 11, day_num)
            })

        # Act
        service = AggregationService(mysql_connection)
        result = service.aggregate_monthly(year=2025, month=11)

        # Assert
        assert result['status'] == 'success'

        ride_stats_query = text("""
            SELECT uptime_minutes, downtime_minutes, uptime_percentage, avg_wait_time
            FROM ride_monthly_stats
            WHERE ride_id = :ride_id AND year = 2025 AND month = 11
        """)
        ride_stats = mysql_connection.execute(ride_stats_query, {
            "ride_id": ride_id
        }).fetchone()

        assert ride_stats.uptime_minutes == 0, "Zero uptime"
        assert ride_stats.downtime_minutes == 25200, "840 × 30 = 25,200"
        assert float(ride_stats.uptime_percentage) == 0.0, "0% uptime"
        assert ride_stats.avg_wait_time is None, "No wait time when closed"
