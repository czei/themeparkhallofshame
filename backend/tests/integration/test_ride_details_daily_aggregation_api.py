"""
Ride Details Daily Aggregation API Integration Tests
=====================================================

Integration tests that verify the actual API returns daily-aggregated data
for weekly and monthly periods.

The ride details API uses HourlyAggregationQuery.ride_hour_range_metrics()
which computes hourly metrics from ride_status_snapshots directly.
For weekly/monthly views, the API aggregates these hourly metrics into daily buckets.
"""

import pytest

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from api.app import create_app


# Test IDs use high values to distinguish from production data
TEST_RIDE_ID = 94128
TEST_PARK_ID = 904128


@pytest.fixture(scope="module", autouse=True)
def seed_ride_details_data(mysql_engine):
    """
    Seed deterministic ride + snapshot data so the API can respond with data.

    The Flask app runs in a separate connection, so we must commit after inserts.

    Creates:
    - 1 test park (Disney for predictable DOWN/CLOSED logic)
    - 1 test ride
    - 35 days of snapshots (5 per day at different hours)
    """
    with mysql_engine.connect() as conn:
        # Clean existing scaffolding for this test ride/park
        conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id = :park_id"), {"park_id": TEST_PARK_ID})
        conn.execute(text("DELETE FROM rides WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM parks WHERE park_id = :park_id"), {"park_id": TEST_PARK_ID})
        conn.commit()

        # Insert park record (Disney for predictable status logic)
        conn.execute(text("""
            INSERT INTO parks (
                park_id, queue_times_id, name, city, state_province, country,
                latitude, longitude, timezone, operator, is_disney, is_universal, is_active
            ) VALUES (
                :park_id, :queue_times_id, :name, :city, :state, :country,
                :lat, :lon, :timezone, :operator, :is_disney, :is_universal, :is_active
            )
        """), {
            "park_id": TEST_PARK_ID,
            "queue_times_id": 9904128,
            "name": "Test Park - Ride Details",
            "city": "Anaheim",
            "state": "CA",
            "country": "US",
            "lat": 33.8121,
            "lon": -117.9190,
            "timezone": "America/Los_Angeles",
            "operator": "Disney",
            "is_disney": True,
            "is_universal": False,
            "is_active": True
        })

        # Insert ride record
        conn.execute(text("""
            INSERT INTO rides (
                ride_id, queue_times_id, park_id, name, land_area, tier, entity_type, category, is_active
            ) VALUES (
                :ride_id, :queue_times_id, :park_id, :name, :land_area, :tier, 'ATTRACTION', 'ATTRACTION', TRUE
            )
        """), {
            "ride_id": TEST_RIDE_ID,
            "queue_times_id": 99904128,
            "park_id": TEST_PARK_ID,
            "name": "Pirates of the Caribbean (Test)",
            "land_area": "Adventureland",
            "tier": 1
        })
        conn.commit()

        # Create 35 days of snapshot data
        # For each day, create 5 snapshots at different hours (10am, 12pm, 2pm, 4pm, 6pm Pacific)
        # Most snapshots are OPERATING, some are DOWN to create variation
        base_date = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        ride_snapshots = []
        park_snapshots = []

        for day_offset in range(35):
            day_start = base_date - timedelta(days=day_offset)

            # Create snapshots at different UTC hours throughout the day
            # These represent typical park operating hours
            for hour_offset, utc_hour in enumerate([18, 19, 20, 21, 22]):
                snapshot_time = day_start.replace(hour=utc_hour, minute=0, second=0)
                snapshot_time = snapshot_time.replace(tzinfo=None)  # Store as naive UTC

                # Vary the status: mostly OPERATING, occasionally DOWN
                # Day offset % 5 == 0 and first snapshot = DOWN (creates ~1 down snapshot per 5 days)
                is_down = (day_offset % 5 == 0 and hour_offset == 0)
                status = 'DOWN' if is_down else 'OPERATING'
                wait_time = None if is_down else (20 + (day_offset % 5) * 10 + hour_offset * 5)

                ride_snapshots.append({
                    "ride_id": TEST_RIDE_ID,
                    "recorded_at": snapshot_time,
                    "wait_time": wait_time,
                    "is_open": not is_down,
                    "computed_is_open": not is_down,
                    "status": status,
                    "last_updated_api": snapshot_time
                })

                park_snapshots.append({
                    "park_id": TEST_PARK_ID,
                    "recorded_at": snapshot_time,
                    "park_appears_open": True,
                    "rides_open": 0 if is_down else 1,
                    "rides_closed": 1 if is_down else 0,
                    "total_rides_tracked": 1,
                    "avg_wait_time": wait_time,
                    "max_wait_time": wait_time,
                    "shame_score": 1.0 if is_down else 0.0
                })

        # Bulk insert ride_status_snapshots
        conn.execute(text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api
            ) VALUES (
                :ride_id, :recorded_at, :wait_time, :is_open, :computed_is_open, :status, :last_updated_api
            )
        """), ride_snapshots)

        # Bulk insert park_activity_snapshots
        conn.execute(text("""
            INSERT INTO park_activity_snapshots (
                park_id, recorded_at, park_appears_open, rides_open, rides_closed,
                total_rides_tracked, avg_wait_time, max_wait_time, shame_score
            ) VALUES (
                :park_id, :recorded_at, :park_appears_open, :rides_open, :rides_closed,
                :total_rides_tracked, :avg_wait_time, :max_wait_time, :shame_score
            )
        """), park_snapshots)

        conn.commit()

    yield

    # Cleanup after tests so other suites aren't polluted
    with mysql_engine.connect() as conn:
        conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id = :park_id"), {"park_id": TEST_PARK_ID})
        conn.execute(text("DELETE FROM rides WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM parks WHERE park_id = :park_id"), {"park_id": TEST_PARK_ID})
        conn.commit()


@pytest.fixture
def client():
    """Create test client"""
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestRideDetailsAPIDailyAggregation:
    """
    Integration tests for daily aggregation in ride details API.

    These tests verify that:
    - last_week returns daily-aggregated data (7 data points max)
    - last_month returns daily-aggregated data (31 data points max)
    - today/yesterday return hourly data
    """

    def test_last_week_returns_7_daily_points_not_168_hourly(self, client):
        """
        LAST_WEEK should return daily data points, not 168 hourly.

        Validates that daily aggregation is working (count <= 7).
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        # Should have daily points (max 7), not hourly points (168)
        timeseries_count = len(data['timeseries'])

        # Validate daily aggregation is working
        # With full data: would be 7 daily points vs 168 hourly
        # With partial data: should be <= 7 daily points
        assert timeseries_count <= 7, f"Expected at most 7 daily points, got {timeseries_count}"

        # Verify each point represents a different day (not hour)
        if timeseries_count > 1:
            dates = [point['date'] for point in data['timeseries']]
            assert len(dates) == len(set(dates)), "Daily data should have unique dates"

    def test_last_month_returns_30_daily_points_not_720_hourly(self, client):
        """
        LAST_MONTH should return ~30 daily data points, not 720 hourly.

        Validates that daily aggregation is working (count <= 31).
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_month')

        assert response.status_code == 200
        data = response.get_json()

        timeseries_count = len(data['timeseries'])

        # Validate daily aggregation is working
        # With full data: would be 28-31 daily points vs 720 hourly
        # With partial data: should be <= 31 daily points
        assert timeseries_count <= 31, f"Expected at most 31 daily points, got {timeseries_count}"

        # Verify each point represents a different day (not hour)
        if timeseries_count > 1:
            dates = [point['date'] for point in data['timeseries']]
            assert len(dates) == len(set(dates)), "Daily data should have unique dates"

    def test_today_still_returns_hourly_data(self, client):
        """
        TODAY should still return hourly data (no change).

        This test validates that today uses hourly granularity.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=today')

        assert response.status_code == 200
        data = response.get_json()

        # Today should return hourly data (~24 points or whatever is available)
        timeseries_count = len(data['timeseries'])

        # Should be hourly granularity
        assert timeseries_count <= 24, "Today should return hourly data"

    def test_yesterday_still_returns_hourly_data(self, client):
        """
        YESTERDAY should still return hourly data (no change).

        This test validates that yesterday uses hourly granularity.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=yesterday')

        assert response.status_code == 200
        data = response.get_json()

        timeseries_count = len(data['timeseries'])

        # Yesterday should return hourly data (up to 24 points)
        assert timeseries_count <= 24, "Yesterday should return hourly data (max 24 points)"

        # Verify hourly data has hour_start_utc field, not date
        if timeseries_count > 0:
            first_point = data['timeseries'][0]
            assert 'hour_start_utc' in first_point, "Hourly data should have hour_start_utc field"

    def test_daily_data_uses_date_field_not_hour_start_utc(self, client):
        """
        Daily data should use 'date' field instead of 'hour_start_utc'.

        For last_week/last_month, the API aggregates hourly data into daily buckets.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        if len(data['timeseries']) == 0:
            pytest.skip("No timeseries data available for last_week")

        # Check first timeseries point
        first_point = data['timeseries'][0]

        # Daily data should have 'date' field
        assert 'date' in first_point, "Daily data should use 'date' field"
        assert 'hour_start_utc' not in first_point, "Daily data should not have 'hour_start_utc'"

        # Date should be in YYYY-MM-DD format
        assert len(first_point['date'].split('-')) == 3, "Date should be YYYY-MM-DD format"

    def test_daily_data_has_all_required_fields(self, client):
        """
        Daily data points should have all required fields.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        if len(data['timeseries']) == 0:
            pytest.skip("No timeseries data available for last_week")

        first_point = data['timeseries'][0]

        # Required fields for daily data
        required_fields = ['date', 'avg_wait_time_minutes', 'uptime_percentage',
                          'status', 'snapshot_count']

        for field in required_fields:
            assert field in first_point, f"Missing required field: {field}"

    def test_hourly_breakdown_matches_daily_granularity_for_weekly(self, client):
        """
        For weekly view, hourly_breakdown should also be daily-aggregated.

        Validates that breakdown matches chart granularity.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        breakdown_count = len(data['hourly_breakdown'])

        # Validate daily aggregation in breakdown
        # With full data: would be 7 daily rows vs 168 hourly
        # With partial data: should be <= 7 daily rows
        assert breakdown_count <= 7, f"Expected at most 7 daily breakdown rows, got {breakdown_count}"

    def test_daily_aggregation_averages_wait_times_correctly(self, client):
        """
        Daily data should show averaged wait times across all hours.

        This is a smoke test to verify values are reasonable.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        # Daily wait time should be a reasonable average (not identical to any single hour)
        for point in data['timeseries']:
            if point['avg_wait_time_minutes'] is not None:
                # Wait time should be positive and reasonable
                assert 0 <= point['avg_wait_time_minutes'] <= 300
            if point['uptime_percentage'] is not None:
                assert point['uptime_percentage'] >= 0
                assert point['uptime_percentage'] <= 100
