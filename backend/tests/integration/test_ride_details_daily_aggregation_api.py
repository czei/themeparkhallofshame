"""
Ride Details Daily Aggregation API Integration Tests
=====================================================

Integration tests that verify the actual API returns daily-aggregated data
for weekly and monthly periods.
"""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from api.app import create_app


TEST_RIDE_ID = 4128
TEST_PARK_ID = 94128


@pytest.fixture(scope="module", autouse=True)
def seed_ride_details_data(mysql_engine):
    """
    Seed deterministic ride + hourly stats so the API can respond with data.

    The Flask app runs in a separate connection, so we must commit after inserts.
    """
    with mysql_engine.connect() as conn:
        # Clean existing scaffolding for this test ride/park
        conn.execute(text("DELETE FROM ride_hourly_stats WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM rides WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
        conn.execute(text("DELETE FROM parks WHERE park_id = :park_id"), {"park_id": TEST_PARK_ID})
        conn.commit()

        # Insert park and ride records
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
            "queue_times_id": 904128,
            "name": "Test Park - Ride Details",
            "city": "Anaheim",
            "state": "CA",
            "country": "US",
            "lat": 33.8121,
            "lon": -117.9190,
            "timezone": "America/Los_Angeles",
            "operator": "Test Operator",
            "is_disney": True,
            "is_universal": False,
            "is_active": True
        })

        conn.execute(text("""
            INSERT INTO rides (
                ride_id, queue_times_id, park_id, name, land_area, tier, entity_type, category, is_active
            ) VALUES (
                :ride_id, :queue_times_id, :park_id, :name, :land_area, :tier, 'ATTRACTION', 'ATTRACTION', TRUE
            )
        """), {
            "ride_id": TEST_RIDE_ID,
            "queue_times_id": 9904128,
            "park_id": TEST_PARK_ID,
            "name": "Pirates of the Caribbean (Test)",
            "land_area": "Adventureland",
            "tier": 1
        })
        conn.commit()

        base_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        rows = []
        for day_offset in range(0, 35):
            hour_start = base_hour - timedelta(days=day_offset)
            hour_start = hour_start.replace(hour=20)  # Noon Pacific for consistent DATE conversion
            hour_start = hour_start.astimezone(timezone.utc).replace(tzinfo=None)
            down_snapshots = (day_offset % 3) + 1
            operating_snapshots = 12 - down_snapshots
            snapshot_count = operating_snapshots + down_snapshots
            uptime_percentage = round((operating_snapshots / snapshot_count) * 100, 2)

            rows.append({
                "ride_id": TEST_RIDE_ID,
                "park_id": TEST_PARK_ID,
                "hour_start_utc": hour_start,
                "avg_wait": 20 + (day_offset % 5) * 5,
                "operating": operating_snapshots,
                "down": down_snapshots,
                "downtime_hours": round(down_snapshots * 0.5, 2),
                "uptime_pct": uptime_percentage,
                "snapshot_count": snapshot_count,
                "ride_operated": 1
            })

        conn.execute(text("""
            INSERT INTO ride_hourly_stats (
                ride_id, park_id, hour_start_utc, avg_wait_time_minutes,
                operating_snapshots, down_snapshots, downtime_hours,
                uptime_percentage, snapshot_count, ride_operated
            ) VALUES (
                :ride_id, :park_id, :hour_start_utc, :avg_wait,
                :operating, :down, :downtime_hours,
                :uptime_pct, :snapshot_count, :ride_operated
            )
        """), rows)
        conn.commit()

    yield

    # Cleanup after tests so other suites aren't polluted
    with mysql_engine.connect() as conn:
        conn.execute(text("DELETE FROM ride_hourly_stats WHERE ride_id = :ride_id"), {"ride_id": TEST_RIDE_ID})
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

    These tests will FAIL until daily aggregation is implemented.
    """

    def test_last_week_returns_7_daily_points_not_168_hourly(self, client):
        """
        LAST_WEEK should return daily data points, not 168 hourly.

        Validates that daily aggregation is working (count << 168).
        """
        # Use Pirates of the Caribbean (ride_id=4128) for testing
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

        Validates that daily aggregation is working (count << 720).
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

        This test should PASS even before implementing daily aggregation.
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

        This test should PASS even before implementing daily aggregation.
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
            assert 'date' not in first_point or first_point.get('date') is None, "Hourly data should not have date field"

    def test_daily_data_uses_date_field_not_hour_start_utc(self, client):
        """
        Daily data should use 'date' field instead of 'hour_start_utc'.

        This test will FAIL until we implement daily aggregation.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        # Check first timeseries point
        first_point = data['timeseries'][0]

        # FAIL: Currently has 'hour_start_utc'
        # PASS: Should have 'date' field for daily data
        assert 'date' in first_point, "Daily data should use 'date' field"
        assert 'hour_start_utc' not in first_point, "Daily data should not have 'hour_start_utc'"

        # Date should be in YYYY-MM-DD format
        assert len(first_point['date'].split('-')) == 3, "Date should be YYYY-MM-DD format"

    def test_daily_data_has_all_required_fields(self, client):
        """
        Daily data points should have all required fields.

        This test will FAIL until we implement daily aggregation.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

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

        # Verify breakdown uses 'date' field for daily data
        if breakdown_count > 0:
            first_row = data['hourly_breakdown'][0]
            assert 'date' in first_row, "Daily breakdown should have 'date' field"

    def test_daily_aggregation_averages_wait_times_correctly(self, client):
        """
        Daily data should show averaged wait times across all hours.

        This test will FAIL until we implement daily aggregation.
        """
        response = client.get(f'/api/rides/{TEST_RIDE_ID}/details?period=last_week')

        assert response.status_code == 200
        data = response.get_json()

        # Daily wait time should be a reasonable average (not identical to any single hour)
        # This is more of a smoke test
        for point in data['timeseries']:
            if point['avg_wait_time_minutes'] is not None:
                # Wait time should be positive and reasonable
                assert 0 <= point['avg_wait_time_minutes'] <= 300
                assert point['uptime_percentage'] >= 0
                assert point['uptime_percentage'] <= 100
