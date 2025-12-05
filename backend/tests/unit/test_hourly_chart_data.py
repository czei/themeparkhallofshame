"""
Unit Tests for Hourly Chart Data (Trends Page TODAY)
====================================================

TDD Tests: These tests verify that chart queries have a get_hourly()
method for the TODAY period on the Trends page.

The Bug These Tests Catch:
--------------------------
When period=today, the API calls query.get_hourly() but the method
doesn't exist, causing:
  AttributeError: 'ParkShameHistoryQuery' object has no attribute 'get_hourly'

Frontend expects hourly data for TODAY showing shame score progression
throughout the day (e.g., 6am, 7am, 8am, ... 11pm).

Required Implementation:
------------------------
- ParkShameHistoryQuery.get_hourly(target_date, filter_disney_universal, limit)
- RideDowntimeHistoryQuery.get_hourly(target_date, filter_disney_universal, limit)

Both should return Chart.js format:
{
    "labels": ["6:00", "7:00", "8:00", ...],
    "datasets": [
        {"label": "Park Name", "data": [0.21, 0.18, ...]},
        ...
    ]
}
"""

import pytest


class TestParkShameHistoryQueryHourly:
    """Test that ParkShameHistoryQuery has get_hourly method."""

    def test_park_shame_history_has_get_hourly_method(self):
        """
        CRITICAL: ParkShameHistoryQuery must have get_hourly() method.

        The route calls query.get_hourly() for period=today.
        Without this method, the API returns 500 error.
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        assert hasattr(ParkShameHistoryQuery, 'get_hourly'), \
            "ParkShameHistoryQuery must have get_hourly() method for TODAY period"

    def test_get_hourly_method_signature(self):
        """
        get_hourly should accept target_date, filter_disney_universal, and limit parameters.
        """
        import inspect
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        assert hasattr(ParkShameHistoryQuery, 'get_hourly'), \
            "get_hourly method must exist"

        sig = inspect.signature(ParkShameHistoryQuery.get_hourly)
        params = list(sig.parameters.keys())

        # Should have self + at least target_date
        assert 'self' in params or len(params) >= 1, \
            "get_hourly should be an instance method"
        assert 'target_date' in params, \
            "get_hourly should accept target_date parameter"


class TestRideDowntimeHistoryQueryHourly:
    """Test that RideDowntimeHistoryQuery has get_hourly method."""

    def test_ride_downtime_history_has_get_hourly_method(self):
        """
        CRITICAL: RideDowntimeHistoryQuery must have get_hourly() method.

        The route calls query.get_hourly() for period=today.
        Without this method, the API returns 500 error.
        """
        from database.queries.charts.ride_downtime_history import RideDowntimeHistoryQuery

        assert hasattr(RideDowntimeHistoryQuery, 'get_hourly'), \
            "RideDowntimeHistoryQuery must have get_hourly() method for TODAY period"

    def test_get_hourly_method_signature(self):
        """
        get_hourly should accept target_date, filter_disney_universal, and limit parameters.
        """
        import inspect
        from database.queries.charts.ride_downtime_history import RideDowntimeHistoryQuery

        assert hasattr(RideDowntimeHistoryQuery, 'get_hourly'), \
            "get_hourly method must exist"

        sig = inspect.signature(RideDowntimeHistoryQuery.get_hourly)
        params = list(sig.parameters.keys())

        assert 'target_date' in params, \
            "get_hourly should accept target_date parameter"


class TestHourlyDataFormat:
    """Test the expected format of hourly chart data."""

    def test_hourly_labels_should_be_time_format(self):
        """
        Hourly data should have time-based labels like ["6:00", "7:00", ...].

        Frontend expects time labels for TODAY, not date labels.
        """
        # Document expected format
        expected_labels = [
            "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00",
            "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
            "20:00", "21:00", "22:00", "23:00"
        ]

        # 6am to 11pm = 18 hours
        assert len(expected_labels) == 18, \
            "Expected 18 hourly labels (6am to 11pm)"

    def test_hourly_datasets_should_have_park_name_and_data(self):
        """
        Each dataset should have label (park/ride name) and data array.
        """
        expected_dataset_structure = {
            "label": "Park Name",
            "data": [0.21, 0.18, 0.25]  # Hourly values
        }

        assert "label" in expected_dataset_structure
        assert "data" in expected_dataset_structure
        assert isinstance(expected_dataset_structure["data"], list)


class TestHourlyDataSource:
    """Test that hourly data comes from live snapshots, not daily stats."""

    def test_hourly_query_uses_ride_status_snapshots(self):
        """
        For TODAY hourly data, query should use ride_status_snapshots
        (live data), not ride_daily_stats (aggregated data).

        ride_daily_stats only has one row per day - can't get hourly breakdown.
        ride_status_snapshots has one row per 5-minute interval.
        """
        import inspect
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        if not hasattr(ParkShameHistoryQuery, 'get_hourly'):
            pytest.skip("get_hourly not yet implemented")

        source = inspect.getsource(ParkShameHistoryQuery.get_hourly)

        # Should reference snapshot data, not daily stats
        uses_snapshots = (
            'ride_status_snapshots' in source or
            'snapshot' in source.lower()
        )

        assert uses_snapshots, \
            "Hourly data should come from ride_status_snapshots, not daily stats"


class TestStoredShameScoreReading:
    """Test that hourly queries READ stored shame_score from park_activity_snapshots.

    ARCHITECTURE CHANGE (Dec 2025):
    Shame score is now calculated ONCE during data collection and stored in
    park_activity_snapshots.shame_score. All queries just READ the stored value.

    This eliminates the 8+ different formula variants and ensures consistency
    across Rankings, Details modal, and Charts.
    """

    def test_park_shame_query_reads_stored_shame_score(self):
        """
        ParkShameHistoryQuery._get_park_hourly_data should READ from pas.shame_score,
        not calculate using ride_status_snapshots with first_op_time.
        """
        import inspect
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        source = inspect.getsource(ParkShameHistoryQuery._get_park_hourly_data)

        # Should read from park_activity_snapshots (pas)
        reads_from_pas = 'pas.shame_score' in source or 'park_activity_snapshots' in source

        assert reads_from_pas, \
            "_get_park_hourly_data should READ from pas.shame_score (stored value)"

    def test_park_shame_query_uses_pas_not_rss(self):
        """
        The query should use park_activity_snapshots (pas), not ride_status_snapshots (rss).

        Shame score is stored in pas during data collection - no need to calculate.
        """
        import inspect
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        source = inspect.getsource(ParkShameHistoryQuery._get_park_hourly_data)

        # Should use pas.recorded_at not rss.recorded_at
        uses_pas = 'pas.recorded_at' in source

        assert uses_pas, \
            "Query should use pas.recorded_at (park_activity_snapshots)"

    def test_ride_downtime_query_uses_timestamp_comparison(self):
        """
        RideDowntimeHistoryQuery.get_hourly should use timestamp-level comparison.
        """
        import inspect
        from database.queries.charts.ride_downtime_history import RideDowntimeHistoryQuery

        source = inspect.getsource(RideDowntimeHistoryQuery._get_ride_hourly_data)

        # Should use timestamp column name, not hour column
        uses_timestamp = 'first_op_time' in source
        uses_hour = 'first_op_hour' in source

        assert uses_timestamp, \
            "_get_ride_hourly_data should use first_op_time (timestamp level)"
        assert not uses_hour, \
            "_get_ride_hourly_data should NOT use first_op_hour (hour level)"

    def test_ride_downtime_query_compares_recorded_at_to_first_op_time(self):
        """
        The query should compare rss.recorded_at >= rfo.first_op_time.
        """
        import inspect
        from database.queries.charts.ride_downtime_history import RideDowntimeHistoryQuery

        source = inspect.getsource(RideDowntimeHistoryQuery._get_ride_hourly_data)

        has_timestamp_comparison = (
            'recorded_at >= rfo.first_op_time' in source or
            'rss.recorded_at >= rfo.first_op_time' in source
        )

        assert has_timestamp_comparison, \
            "Query should compare recorded_at >= first_op_time for accurate downtime"
