"""
Theme Park Downtime Tracker - Statistics Models Unit Tests

Tests all statistics dataclasses:
- RideDailyStats (dataclass fields, downtime_hours property, to_dict method)
- ParkDailyStats (dataclass fields, to_dict method)
- RideWeeklyStats (dataclass fields, downtime_hours property, to_dict method)
- ParkWeeklyStats (dataclass fields, to_dict method)

Priority: P1 - Quick win for coverage increase
"""

from datetime import date, datetime
from models.statistics import (
    RideDailyStats,
    ParkDailyStats,
    RideWeeklyStats,
    ParkWeeklyStats
)


class TestRideDailyStats:
    """Test RideDailyStats dataclass."""

    def test_create_ride_daily_stats(self):
        """RideDailyStats should create instance with all fields."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=420,
            downtime_minutes=60,
            uptime_percentage=87.5,
            operating_hours_minutes=480,
            avg_wait_time=35.5,
            min_wait_time=10,
            max_wait_time=90,
            peak_wait_time=90,
            status_changes=3,
            longest_downtime_minutes=30,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.stat_id == 1
        assert stats.ride_id == 101
        assert stats.stat_date == date(2024, 1, 15)
        assert stats.uptime_minutes == 420
        assert stats.downtime_minutes == 60
        assert stats.uptime_percentage == 87.5
        assert stats.operating_hours_minutes == 480
        assert stats.avg_wait_time == 35.5
        assert stats.min_wait_time == 10
        assert stats.max_wait_time == 90
        assert stats.peak_wait_time == 90
        assert stats.status_changes == 3
        assert stats.longest_downtime_minutes == 30
        assert stats.created_at == datetime(2024, 1, 16, 2, 0, 0)

    def test_downtime_hours_property(self):
        """downtime_hours should convert minutes to hours (rounded to 2 decimals)."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=360,
            downtime_minutes=120,  # 2 hours
            uptime_percentage=75.0,
            operating_hours_minutes=480,
            avg_wait_time=None,
            min_wait_time=None,
            max_wait_time=None,
            peak_wait_time=None,
            status_changes=2,
            longest_downtime_minutes=60,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.downtime_hours == 2.0

    def test_downtime_hours_rounds_to_two_decimals(self):
        """downtime_hours should round to 2 decimal places."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=360,
            downtime_minutes=75,  # 1.25 hours
            uptime_percentage=82.76,
            operating_hours_minutes=435,
            avg_wait_time=None,
            min_wait_time=None,
            max_wait_time=None,
            peak_wait_time=None,
            status_changes=1,
            longest_downtime_minutes=75,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.downtime_hours == 1.25

    def test_to_dict_all_fields_populated(self):
        """to_dict() should return dictionary with all fields."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=420,
            downtime_minutes=60,
            uptime_percentage=87.5,
            operating_hours_minutes=480,
            avg_wait_time=35.5,
            min_wait_time=10,
            max_wait_time=90,
            peak_wait_time=90,
            status_changes=3,
            longest_downtime_minutes=30,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['stat_id'] == 1
        assert result['ride_id'] == 101
        assert result['stat_date'] == '2024-01-15'
        assert result['uptime_minutes'] == 420
        assert result['downtime_minutes'] == 60
        assert result['downtime_hours'] == 1.0
        assert result['uptime_percentage'] == 87.5
        assert result['operating_hours_minutes'] == 480
        assert result['avg_wait_time'] == 35.5
        assert result['peak_wait_time'] == 90
        assert result['status_changes'] == 3
        assert result['longest_downtime_minutes'] == 30

    def test_to_dict_optional_fields_none(self):
        """to_dict() should handle None values for optional fields."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=480,
            downtime_minutes=0,
            uptime_percentage=100.0,
            operating_hours_minutes=480,
            avg_wait_time=None,  # No wait time data
            min_wait_time=None,
            max_wait_time=None,
            peak_wait_time=None,
            status_changes=0,
            longest_downtime_minutes=None,  # No downtime
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['avg_wait_time'] is None
        assert result['peak_wait_time'] is None
        assert result['longest_downtime_minutes'] is None

    def test_to_dict_excludes_created_at(self):
        """to_dict() should not include created_at timestamp."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=420,
            downtime_minutes=60,
            uptime_percentage=87.5,
            operating_hours_minutes=480,
            avg_wait_time=35.5,
            min_wait_time=10,
            max_wait_time=90,
            peak_wait_time=90,
            status_changes=3,
            longest_downtime_minutes=30,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert 'created_at' not in result


class TestParkDailyStats:
    """Test ParkDailyStats dataclass."""

    def test_create_park_daily_stats(self):
        """ParkDailyStats should create instance with all fields."""
        stats = ParkDailyStats(
            stat_id=1,
            park_id=201,
            stat_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=12.5,
            rides_with_downtime=8,
            avg_wait_time=28.7,
            peak_wait_time=120,
            operating_hours_minutes=720,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.stat_id == 1
        assert stats.park_id == 201
        assert stats.stat_date == date(2024, 1, 15)
        assert stats.total_rides_tracked == 45
        assert stats.avg_uptime_percentage == 92.3
        assert stats.total_downtime_hours == 12.5
        assert stats.rides_with_downtime == 8
        assert stats.avg_wait_time == 28.7
        assert stats.peak_wait_time == 120
        assert stats.operating_hours_minutes == 720
        assert stats.created_at == datetime(2024, 1, 16, 2, 0, 0)

    def test_to_dict_all_fields_populated(self):
        """to_dict() should return dictionary with all fields."""
        stats = ParkDailyStats(
            stat_id=1,
            park_id=201,
            stat_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=12.5,
            rides_with_downtime=8,
            avg_wait_time=28.7,
            peak_wait_time=120,
            operating_hours_minutes=720,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['stat_id'] == 1
        assert result['park_id'] == 201
        assert result['stat_date'] == '2024-01-15'
        assert result['total_rides_tracked'] == 45
        assert result['avg_uptime_percentage'] == 92.3
        assert result['total_downtime_hours'] == 12.5
        assert result['rides_with_downtime'] == 8
        assert result['avg_wait_time'] == 28.7
        assert result['peak_wait_time'] == 120
        assert result['operating_hours_minutes'] == 720

    def test_to_dict_optional_fields_none(self):
        """to_dict() should handle None values for optional fields."""
        stats = ParkDailyStats(
            stat_id=1,
            park_id=201,
            stat_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=None,  # No rides with data
            total_downtime_hours=0.0,
            rides_with_downtime=0,
            avg_wait_time=None,  # No wait time data
            peak_wait_time=None,
            operating_hours_minutes=720,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['avg_uptime_percentage'] is None
        assert result['avg_wait_time'] is None
        assert result['peak_wait_time'] is None

    def test_to_dict_excludes_created_at(self):
        """to_dict() should not include created_at timestamp."""
        stats = ParkDailyStats(
            stat_id=1,
            park_id=201,
            stat_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=12.5,
            rides_with_downtime=8,
            avg_wait_time=28.7,
            peak_wait_time=120,
            operating_hours_minutes=720,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        result = stats.to_dict()

        assert 'created_at' not in result


class TestRideWeeklyStats:
    """Test RideWeeklyStats dataclass."""

    def test_create_ride_weekly_stats(self):
        """RideWeeklyStats should create instance with all fields."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=2940,  # 7 days
            downtime_minutes=420,
            uptime_percentage=87.5,
            operating_hours_minutes=3360,
            avg_wait_time=35.5,
            peak_wait_time=120,
            status_changes=12,
            trend_vs_previous_week=-2.3,  # Downtime increased
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        assert stats.stat_id == 1
        assert stats.ride_id == 101
        assert stats.year == 2024
        assert stats.week_number == 3
        assert stats.week_start_date == date(2024, 1, 15)
        assert stats.uptime_minutes == 2940
        assert stats.downtime_minutes == 420
        assert stats.uptime_percentage == 87.5
        assert stats.operating_hours_minutes == 3360
        assert stats.avg_wait_time == 35.5
        assert stats.peak_wait_time == 120
        assert stats.status_changes == 12
        assert stats.trend_vs_previous_week == -2.3
        assert stats.created_at == datetime(2024, 1, 22, 2, 0, 0)

    def test_downtime_hours_property(self):
        """downtime_hours should convert minutes to hours (rounded to 2 decimals)."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=2520,
            downtime_minutes=840,  # 14 hours
            uptime_percentage=75.0,
            operating_hours_minutes=3360,
            avg_wait_time=None,
            peak_wait_time=None,
            status_changes=10,
            trend_vs_previous_week=None,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        assert stats.downtime_hours == 14.0

    def test_downtime_hours_rounds_to_two_decimals(self):
        """downtime_hours should round to 2 decimal places."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=2520,
            downtime_minutes=525,  # 8.75 hours
            uptime_percentage=82.76,
            operating_hours_minutes=3045,
            avg_wait_time=None,
            peak_wait_time=None,
            status_changes=7,
            trend_vs_previous_week=None,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        assert stats.downtime_hours == 8.75

    def test_to_dict_all_fields_populated(self):
        """to_dict() should return dictionary with all fields."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=2940,
            downtime_minutes=420,
            uptime_percentage=87.5,
            operating_hours_minutes=3360,
            avg_wait_time=35.5,
            peak_wait_time=120,
            status_changes=12,
            trend_vs_previous_week=-2.3,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['stat_id'] == 1
        assert result['ride_id'] == 101
        assert result['year'] == 2024
        assert result['week_number'] == 3
        assert result['week_start_date'] == '2024-01-15'
        assert result['uptime_minutes'] == 2940
        assert result['downtime_minutes'] == 420
        assert result['downtime_hours'] == 7.0
        assert result['uptime_percentage'] == 87.5
        assert result['operating_hours_minutes'] == 3360
        assert result['avg_wait_time'] == 35.5
        assert result['peak_wait_time'] == 120
        assert result['status_changes'] == 12
        assert result['trend_vs_previous_week'] == -2.3

    def test_to_dict_optional_fields_none(self):
        """to_dict() should handle None values for optional fields."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=3360,
            downtime_minutes=0,
            uptime_percentage=100.0,
            operating_hours_minutes=3360,
            avg_wait_time=None,  # No wait time data
            peak_wait_time=None,
            status_changes=0,
            trend_vs_previous_week=None,  # First week
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['avg_wait_time'] is None
        assert result['peak_wait_time'] is None
        assert result['trend_vs_previous_week'] is None

    def test_to_dict_excludes_created_at(self):
        """to_dict() should not include created_at timestamp."""
        stats = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            uptime_minutes=2940,
            downtime_minutes=420,
            uptime_percentage=87.5,
            operating_hours_minutes=3360,
            avg_wait_time=35.5,
            peak_wait_time=120,
            status_changes=12,
            trend_vs_previous_week=-2.3,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert 'created_at' not in result


class TestParkWeeklyStats:
    """Test ParkWeeklyStats dataclass."""

    def test_create_park_weekly_stats(self):
        """ParkWeeklyStats should create instance with all fields."""
        stats = ParkWeeklyStats(
            stat_id=1,
            park_id=201,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=87.5,
            rides_with_downtime=15,
            avg_wait_time=28.7,
            peak_wait_time=120,
            trend_vs_previous_week=1.2,  # Improvement
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        assert stats.stat_id == 1
        assert stats.park_id == 201
        assert stats.year == 2024
        assert stats.week_number == 3
        assert stats.week_start_date == date(2024, 1, 15)
        assert stats.total_rides_tracked == 45
        assert stats.avg_uptime_percentage == 92.3
        assert stats.total_downtime_hours == 87.5
        assert stats.rides_with_downtime == 15
        assert stats.avg_wait_time == 28.7
        assert stats.peak_wait_time == 120
        assert stats.trend_vs_previous_week == 1.2
        assert stats.created_at == datetime(2024, 1, 22, 2, 0, 0)

    def test_to_dict_all_fields_populated(self):
        """to_dict() should return dictionary with all fields."""
        stats = ParkWeeklyStats(
            stat_id=1,
            park_id=201,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=87.5,
            rides_with_downtime=15,
            avg_wait_time=28.7,
            peak_wait_time=120,
            trend_vs_previous_week=1.2,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['stat_id'] == 1
        assert result['park_id'] == 201
        assert result['year'] == 2024
        assert result['week_number'] == 3
        assert result['week_start_date'] == '2024-01-15'
        assert result['total_rides_tracked'] == 45
        assert result['avg_uptime_percentage'] == 92.3
        assert result['total_downtime_hours'] == 87.5
        assert result['rides_with_downtime'] == 15
        assert result['avg_wait_time'] == 28.7
        assert result['peak_wait_time'] == 120
        assert result['trend_vs_previous_week'] == 1.2

    def test_to_dict_optional_fields_none(self):
        """to_dict() should handle None values for optional fields."""
        stats = ParkWeeklyStats(
            stat_id=1,
            park_id=201,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=None,  # No rides with data
            total_downtime_hours=0.0,
            rides_with_downtime=0,
            avg_wait_time=None,  # No wait time data
            peak_wait_time=None,
            trend_vs_previous_week=None,  # First week
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert result['avg_uptime_percentage'] is None
        assert result['avg_wait_time'] is None
        assert result['peak_wait_time'] is None
        assert result['trend_vs_previous_week'] is None

    def test_to_dict_excludes_created_at(self):
        """to_dict() should not include created_at timestamp."""
        stats = ParkWeeklyStats(
            stat_id=1,
            park_id=201,
            year=2024,
            week_number=3,
            week_start_date=date(2024, 1, 15),
            total_rides_tracked=45,
            avg_uptime_percentage=92.3,
            total_downtime_hours=87.5,
            rides_with_downtime=15,
            avg_wait_time=28.7,
            peak_wait_time=120,
            trend_vs_previous_week=1.2,
            created_at=datetime(2024, 1, 22, 2, 0, 0)
        )

        result = stats.to_dict()

        assert 'created_at' not in result


class TestEdgeCases:
    """Test edge cases for all statistics models."""

    def test_zero_downtime_ride_daily_stats(self):
        """RideDailyStats with zero downtime should have 100% uptime."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=480,
            downtime_minutes=0,
            uptime_percentage=100.0,
            operating_hours_minutes=480,
            avg_wait_time=25.0,
            min_wait_time=5,
            max_wait_time=60,
            peak_wait_time=60,
            status_changes=0,
            longest_downtime_minutes=0,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.downtime_hours == 0.0
        assert stats.uptime_percentage == 100.0

    def test_full_downtime_ride_daily_stats(self):
        """RideDailyStats with full downtime should have 0% uptime."""
        stats = RideDailyStats(
            stat_id=1,
            ride_id=101,
            stat_date=date(2024, 1, 15),
            uptime_minutes=0,
            downtime_minutes=480,
            uptime_percentage=0.0,
            operating_hours_minutes=480,
            avg_wait_time=None,  # No wait time when down
            min_wait_time=None,
            max_wait_time=None,
            peak_wait_time=None,
            status_changes=1,
            longest_downtime_minutes=480,
            created_at=datetime(2024, 1, 16, 2, 0, 0)
        )

        assert stats.downtime_hours == 8.0
        assert stats.uptime_percentage == 0.0

    def test_week_number_boundaries(self):
        """RideWeeklyStats should handle week number boundaries (1-53)."""
        # Week 1
        stats_week1 = RideWeeklyStats(
            stat_id=1,
            ride_id=101,
            year=2024,
            week_number=1,
            week_start_date=date(2024, 1, 1),
            uptime_minutes=2940,
            downtime_minutes=420,
            uptime_percentage=87.5,
            operating_hours_minutes=3360,
            avg_wait_time=None,
            peak_wait_time=None,
            status_changes=0,
            trend_vs_previous_week=None,  # First week
            created_at=datetime(2024, 1, 8, 2, 0, 0)
        )

        # Week 53
        stats_week53 = RideWeeklyStats(
            stat_id=2,
            ride_id=101,
            year=2024,
            week_number=53,
            week_start_date=date(2024, 12, 30),
            uptime_minutes=2940,
            downtime_minutes=420,
            uptime_percentage=87.5,
            operating_hours_minutes=3360,
            avg_wait_time=None,
            peak_wait_time=None,
            status_changes=0,
            trend_vs_previous_week=0.0,
            created_at=datetime(2025, 1, 6, 2, 0, 0)
        )

        assert stats_week1.week_number == 1
        assert stats_week53.week_number == 53
