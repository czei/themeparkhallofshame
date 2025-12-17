"""
Integration Tests for Heatmap API Endpoints
============================================

Tests the /api/trends/heatmap-data endpoint that transforms chart query
responses into heatmap matrix format.

ARCHITECTURE: These tests verify that the endpoint correctly:
1. Reuses existing chart query infrastructure
2. Transforms Chart.js format to heatmap matrix format
3. Includes required metadata (entity_id, location, tier, park_name)
4. Rejects LIVE period
5. Returns proper data types (numbers, not strings)

Test Database: themepark_test (via mysql_connection fixture)
"""

import pytest
from datetime import date, datetime, timezone, timedelta
from freezegun import freeze_time
from flask import Flask
from api.app import create_app

# Mock current time for deterministic tests
MOCKED_NOW_UTC = datetime(2025, 12, 16, 20, 0, 0, tzinfo=timezone.utc)  # 12 PM PST Dec 16th
TODAY_PST = date(2025, 12, 16)
YESTERDAY_PST = date(2025, 12, 15)


class TestHeatmapAPIStructure:
    """Test basic API structure and parameter validation."""

    @pytest.fixture
    def client(self):
        """Create Flask test client."""
        app = create_app()
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_missing_period_returns_400(self, client):
        """Endpoint should return 400 if period parameter is missing."""
        response = client.get('/api/trends/heatmap-data?type=parks')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'period' in data['error'].lower()

    def test_missing_type_returns_400(self, client):
        """Endpoint should return 400 if type parameter is missing."""
        response = client.get('/api/trends/heatmap-data?period=today')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'type' in data['error'].lower()

    def test_invalid_period_returns_400(self, client):
        """Endpoint should return 400 for invalid periods."""
        response = client.get('/api/trends/heatmap-data?period=invalid&type=parks')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False

    def test_live_period_returns_400(self, client):
        """LIVE period should be rejected with clear error message."""
        response = client.get('/api/trends/heatmap-data?period=live&type=parks')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'live' in data['error'].lower()
        assert 'not supported' in data['error'].lower()

    def test_invalid_type_returns_400(self, client):
        """Endpoint should return 400 for invalid types."""
        response = client.get('/api/trends/heatmap-data?period=today&type=invalid')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'type' in data['error'].lower()

    def test_invalid_filter_returns_400(self, client):
        """Endpoint should return 400 for invalid filters."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&filter=invalid')
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'filter' in data['error'].lower()


@freeze_time(MOCKED_NOW_UTC)
class TestHeatmapResponseStructure:
    """Test that heatmap responses have correct structure and data types."""

    @pytest.fixture
    def client(self):
        """Create Flask test client."""
        app = create_app()
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_parks_today_response_structure(self, client):
        """Parks heatmap for TODAY should have correct structure."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=5')
        assert response.status_code == 200
        data = response.get_json()

        # Required top-level fields
        assert data['success'] is True
        assert data['period'] == 'today'
        assert data['granularity'] == 'hourly'
        assert data['metric'] == 'avg_wait_time_minutes'
        assert data['metric_unit'] == 'minutes'
        assert data['timezone'] == 'America/Los_Angeles'
        assert 'title' in data
        assert 'entities' in data
        assert 'time_labels' in data
        assert 'matrix' in data

    def test_parks_heatmap_entities_structure(self, client):
        """Parks heatmap entities should include entity_id and location."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=3')
        assert response.status_code == 200
        data = response.get_json()

        if data['entities']:  # Only test if there's data
            entity = data['entities'][0]
            # Required fields for parks
            assert 'entity_id' in entity
            assert 'entity_name' in entity
            assert 'location' in entity
            assert 'rank' in entity
            assert 'total_value' in entity

            # Verify data types
            assert isinstance(entity['entity_id'], int)
            assert isinstance(entity['entity_name'], str)
            assert isinstance(entity['location'], str)
            assert isinstance(entity['rank'], int)
            # total_value should be a number (int or float), not a string
            assert isinstance(entity['total_value'], (int, float)), \
                f"total_value should be numeric, got {type(entity['total_value'])}: {entity['total_value']}"

    def test_rides_downtime_entities_structure(self, client):
        """Rides downtime heatmap entities should include tier and park_name."""
        response = client.get('/api/trends/heatmap-data?period=today&type=rides-downtime&limit=3')
        assert response.status_code == 200
        data = response.get_json()

        if data['entities']:  # Only test if there's data
            entity = data['entities'][0]
            # Required fields for rides
            assert 'entity_id' in entity
            assert 'entity_name' in entity
            assert 'park_name' in entity
            assert 'tier' in entity
            assert 'rank' in entity
            assert 'total_value' in entity

            # Verify data types
            assert isinstance(entity['entity_id'], int)
            assert isinstance(entity['entity_name'], str)
            assert isinstance(entity['park_name'], str)
            assert entity['tier'] is None or isinstance(entity['tier'], int)
            assert isinstance(entity['rank'], int)
            assert isinstance(entity['total_value'], (int, float)), \
                f"total_value should be numeric, got {type(entity['total_value'])}: {entity['total_value']}"

    def test_heatmap_matrix_data_types(self, client):
        """Matrix values should be numbers or None, NOT strings."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=3')
        assert response.status_code == 200
        data = response.get_json()

        if data['matrix'] and data['matrix'][0]:  # Only test if there's data
            for row in data['matrix']:
                for value in row:
                    # Values should be None (for missing data) or numeric (int/float)
                    # CRITICAL: Values should NOT be strings like "45" or "56.8"
                    assert value is None or isinstance(value, (int, float)), \
                        f"Matrix values should be None or numeric, got {type(value)}: {value}"

    def test_hourly_granularity_for_today(self, client):
        """TODAY period should use hourly granularity with 18 time labels."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks')
        assert response.status_code == 200
        data = response.get_json()

        assert data['granularity'] == 'hourly'
        assert len(data['time_labels']) == 18  # 6am-11pm = 18 hours
        assert data['time_labels'][0] == '6:00'
        assert data['time_labels'][-1] == '23:00'

    def test_hourly_granularity_for_yesterday(self, client):
        """YESTERDAY period should use hourly granularity."""
        response = client.get('/api/trends/heatmap-data?period=yesterday&type=parks')
        assert response.status_code == 200
        data = response.get_json()

        assert data['granularity'] == 'hourly'
        assert len(data['time_labels']) == 18

    def test_daily_granularity_for_last_week(self, client):
        """LAST_WEEK period should use daily granularity with 7 time labels."""
        response = client.get('/api/trends/heatmap-data?period=last_week&type=parks')
        assert response.status_code == 200
        data = response.get_json()

        assert data['granularity'] == 'daily'
        assert len(data['time_labels']) == 7  # 7 days
        # Labels should be formatted like "Dec 10", "Dec 11", etc.
        assert ' ' in data['time_labels'][0]  # Format: "Mon DD"

    def test_daily_granularity_for_last_month(self, client):
        """LAST_MONTH period should use daily granularity with ~30 time labels."""
        response = client.get('/api/trends/heatmap-data?period=last_month&type=parks')
        assert response.status_code == 200
        data = response.get_json()

        assert data['granularity'] == 'daily'
        # Last month should have ~28-31 days
        assert 28 <= len(data['time_labels']) <= 31


@freeze_time(MOCKED_NOW_UTC)
class TestHeatmapTypes:
    """Test all three heatmap types with different configurations."""

    @pytest.fixture
    def client(self):
        """Create Flask test client."""
        app = create_app()
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_parks_wait_times_heatmap(self, client):
        """Parks wait times heatmap should return avg_wait_time_minutes metric."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks')
        assert response.status_code == 200
        data = response.get_json()

        assert data['metric'] == 'avg_wait_time_minutes'
        assert data['metric_unit'] == 'minutes'
        assert 'parks' in data['title'].lower() or 'wait' in data['title'].lower()

    def test_rides_downtime_heatmap(self, client):
        """Rides downtime heatmap should return downtime_hours metric."""
        response = client.get('/api/trends/heatmap-data?period=today&type=rides-downtime')
        assert response.status_code == 200
        data = response.get_json()

        assert data['metric'] == 'downtime_hours'
        assert data['metric_unit'] == 'hours'
        assert 'downtime' in data['title'].lower()

    def test_rides_wait_times_heatmap(self, client):
        """Rides wait times heatmap should return avg_wait_time_minutes metric."""
        response = client.get('/api/trends/heatmap-data?period=today&type=rides-waittimes')
        assert response.status_code == 200
        data = response.get_json()

        assert data['metric'] == 'avg_wait_time_minutes'
        assert data['metric_unit'] == 'minutes'
        assert 'wait' in data['title'].lower()


@freeze_time(MOCKED_NOW_UTC)
class TestHeatmapFiltersAndLimits:
    """Test filter and limit parameters."""

    @pytest.fixture
    def client(self):
        """Create Flask test client."""
        app = create_app()
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_disney_universal_filter(self, client):
        """Disney/Universal filter should be applied."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&filter=disney-universal')
        assert response.status_code == 200
        # Just verify it doesn't error - actual filtering is tested in chart query tests

    def test_all_parks_filter(self, client):
        """All parks filter should be applied."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&filter=all-parks')
        assert response.status_code == 200

    def test_limit_parameter(self, client):
        """Limit parameter should restrict number of entities."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=3')
        assert response.status_code == 200
        data = response.get_json()

        # If there's data, verify limit is respected
        if data['entities']:
            assert len(data['entities']) <= 3
            assert len(data['matrix']) <= 3

    def test_limit_boundary_min(self, client):
        """Limit below 1 should be clamped to 1."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=0')
        assert response.status_code == 200
        # Endpoint should handle this gracefully (clamped to 1)

    def test_limit_boundary_max(self, client):
        """Limit above 20 should be clamped to 20."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=100')
        assert response.status_code == 200
        data = response.get_json()

        # If there's data, verify it doesn't exceed max limit
        if data['entities']:
            assert len(data['entities']) <= 20


@freeze_time(MOCKED_NOW_UTC)
class TestHeatmapMatrixConsistency:
    """Test that matrix dimensions match entities and time labels."""

    @pytest.fixture
    def client(self):
        """Create Flask test client."""
        app = create_app()
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client

    def test_matrix_rows_match_entities(self, client):
        """Number of matrix rows should match number of entities."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=5')
        assert response.status_code == 200
        data = response.get_json()

        num_entities = len(data['entities'])
        num_rows = len(data['matrix'])

        assert num_rows == num_entities, \
            f"Matrix rows ({num_rows}) should match entities count ({num_entities})"

    def test_matrix_columns_match_time_labels(self, client):
        """Number of matrix columns should match number of time labels."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=5')
        assert response.status_code == 200
        data = response.get_json()

        if data['matrix']:  # Only test if there's data
            num_time_labels = len(data['time_labels'])
            num_columns = len(data['matrix'][0])

            assert num_columns == num_time_labels, \
                f"Matrix columns ({num_columns}) should match time labels count ({num_time_labels})"

    def test_entity_ranks_are_sequential(self, client):
        """Entity ranks should be sequential starting from 1."""
        response = client.get('/api/trends/heatmap-data?period=today&type=parks&limit=5')
        assert response.status_code == 200
        data = response.get_json()

        if data['entities']:  # Only test if there's data
            ranks = [e['rank'] for e in data['entities']]
            expected_ranks = list(range(1, len(data['entities']) + 1))

            assert ranks == expected_ranks, \
                f"Ranks should be sequential [1, 2, 3, ...], got {ranks}"
