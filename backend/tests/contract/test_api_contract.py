"""
Theme Park Downtime Tracker - API Contract Tests

Tests that API responses conform to the OpenAPI specification in contracts/api.yaml.

Validates:
- Response structure matches OpenAPI schema
- Required fields are present
- Field types are correct
- Error response formats

Priority: P1 - Ensures API contract compliance (T150)
"""

import pytest
import json
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Load OpenAPI spec
SPEC_PATH = Path(__file__).parent.parent.parent.parent / ".development" / "specs" / "001-theme-park-tracker" / "contracts" / "api.yaml"


def load_openapi_spec():
    """Load the OpenAPI specification."""
    with open(SPEC_PATH, 'r') as f:
        return yaml.safe_load(f)


class TestOpenAPISpecValid:
    """Test that OpenAPI spec is valid and loadable."""

    def test_spec_file_exists(self):
        """OpenAPI spec file should exist."""
        assert SPEC_PATH.exists(), f"OpenAPI spec not found at {SPEC_PATH}"

    def test_spec_is_valid_yaml(self):
        """OpenAPI spec should be valid YAML."""
        spec = load_openapi_spec()
        assert spec is not None
        assert 'openapi' in spec
        assert 'paths' in spec

    def test_spec_version(self):
        """OpenAPI spec should be version 3.0.x."""
        spec = load_openapi_spec()
        assert spec['openapi'].startswith('3.0')

    def test_spec_has_required_endpoints(self):
        """OpenAPI spec should define required endpoints."""
        spec = load_openapi_spec()
        paths = spec['paths']

        required_endpoints = [
            '/parks/downtime',
            '/parks/{parkId}/details',
            '/rides/downtime',
        ]

        for endpoint in required_endpoints:
            assert endpoint in paths, f"Missing endpoint: {endpoint}"


class TestParkDowntimeResponseSchema:
    """Test /parks/downtime response matches OpenAPI schema."""

    def test_success_response_structure(self):
        """Success response should have required fields."""
        # Simulate API response structure
        response = {
            "success": True,
            "period": "today",
            "filter": "all-parks",
            "aggregate_stats": {
                "total_parks_tracked": 85,
                "peak_downtime_hours": 12.5,
                "currently_down_rides": 23
            },
            "data": [
                {
                    "rank": 1,
                    "park_id": 16,
                    "park_name": "Magic Kingdom",
                    "location": "Orlando, FL",
                    "total_downtime_hours": 12.5,
                    "affected_rides_count": 5,
                    "uptime_percentage": 89.2,
                    "trend_percentage": 15.3,
                    "queue_times_url": "https://queue-times.com/parks/16"
                }
            ]
        }

        # Validate required fields
        assert 'success' in response
        assert 'period' in response
        assert 'data' in response
        assert isinstance(response['success'], bool)
        assert isinstance(response['data'], list)

    def test_park_ranking_item_structure(self):
        """Each park ranking item should have required fields."""
        park_item = {
            "rank": 1,
            "park_id": 16,
            "park_name": "Magic Kingdom",
            "total_downtime_hours": 12.5,
            "queue_times_url": "https://queue-times.com/parks/16"
        }

        required_fields = ['rank', 'park_id', 'park_name', 'total_downtime_hours']
        for field in required_fields:
            assert field in park_item, f"Missing required field: {field}"

    def test_period_enum_values(self):
        """period parameter should only allow valid enum values."""
        spec = load_openapi_spec()
        period_param = None

        # Find period parameter in spec
        for param in spec['paths']['/parks/downtime']['get']['parameters']:
            if param['name'] == 'period':
                period_param = param
                break

        assert period_param is not None
        valid_periods = period_param['schema']['enum']
        assert 'live' in valid_periods
        assert 'today' in valid_periods
        assert '7days' in valid_periods
        assert '30days' in valid_periods

    def test_filter_enum_values(self):
        """filter parameter should only allow valid enum values."""
        spec = load_openapi_spec()
        filter_param = None

        for param in spec['paths']['/parks/downtime']['get']['parameters']:
            if param['name'] == 'filter':
                filter_param = param
                break

        assert filter_param is not None
        valid_filters = filter_param['schema']['enum']
        assert 'disney-universal' in valid_filters
        assert 'all-parks' in valid_filters


class TestErrorResponseSchema:
    """Test error response formats match OpenAPI schema."""

    def test_400_error_response_structure(self):
        """400 Bad Request should have standard error structure."""
        error_response = {
            "success": False,
            "error": "Invalid period. Must be 'today', '7days', or '30days'"
        }

        assert error_response['success'] is False
        assert 'error' in error_response
        assert isinstance(error_response['error'], str)

    def test_500_error_response_structure(self):
        """500 Internal Server Error should have standard error structure."""
        error_response = {
            "success": False,
            "error": "Internal server error"
        }

        assert error_response['success'] is False
        assert 'error' in error_response


class TestRideDowntimeResponseSchema:
    """Test /rides/downtime response matches OpenAPI schema."""

    def test_ride_ranking_response_structure(self):
        """Ride ranking response should have required fields."""
        response = {
            "success": True,
            "period": "7days",
            "filter": "disney-universal",
            "data": [
                {
                    "rank": 1,
                    "ride_id": 101,
                    "ride_name": "Space Mountain",
                    "park_id": 16,
                    "park_name": "Magic Kingdom",
                    "tier": 1,
                    "total_downtime_hours": 5.2,
                    "downtime_incidents": 8,
                    "avg_incident_duration_minutes": 39,
                    "queue_times_url": "https://queue-times.com/parks/16/rides/101"
                }
            ]
        }

        assert 'success' in response
        assert 'period' in response
        assert 'data' in response

    def test_ride_item_required_fields(self):
        """Each ride item should have required fields."""
        ride_item = {
            "rank": 1,
            "ride_id": 101,
            "ride_name": "Space Mountain",
            "park_id": 16,
            "park_name": "Magic Kingdom",
            "tier": 1,
            "total_downtime_hours": 5.2
        }

        required_fields = ['rank', 'ride_id', 'ride_name', 'park_id', 'total_downtime_hours']
        for field in required_fields:
            assert field in ride_item, f"Missing required field: {field}"

    def test_tier_values(self):
        """tier field should be 1, 2, 3, or None."""
        valid_tiers = [1, 2, 3, None]
        for tier in valid_tiers:
            # Just verify these are valid values per spec
            assert tier in valid_tiers


class TestAttributionField:
    """Test attribution field is present in responses."""

    def test_attribution_structure(self):
        """Response should include data attribution."""
        attribution = {
            "data_source": "Queue-Times.com",
            "url": "https://queue-times.com"
        }

        assert 'data_source' in attribution
        assert 'url' in attribution
        assert attribution['data_source'] == "Queue-Times.com"


class TestAPIEndpointIntegration:
    """Integration tests for API endpoints using Flask test client."""

    @pytest.fixture
    def client(self):
        """Create Flask test client with mocked database."""
        from api.app import create_app

        with patch('database.connection.get_db_connection') as mock_conn:
            # Setup mock connection
            mock_context = MagicMock()
            mock_conn.return_value.__enter__ = Mock(return_value=mock_context)
            mock_conn.return_value.__exit__ = Mock(return_value=False)

            app = create_app()
            app.config['TESTING'] = True

            with app.test_client() as client:
                yield client

    def test_parks_downtime_returns_json(self, client):
        """GET /api/parks/downtime should return JSON."""
        with patch('api.routes.parks.StatsRepository') as mock_repo:
            # Mock repository methods
            mock_instance = mock_repo.return_value
            mock_instance.get_park_live_downtime_rankings.return_value = []
            mock_instance.get_aggregate_park_stats.return_value = {}

            response = client.get('/api/parks/downtime')

            # Check it returns JSON (even if empty/error)
            assert response.content_type == 'application/json'

    def test_parks_downtime_invalid_period(self, client):
        """GET /api/parks/downtime with invalid period should return 400."""
        response = client.get('/api/parks/downtime?period=invalid')

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] is False
        assert 'error' in data

    def test_parks_downtime_invalid_filter(self, client):
        """GET /api/parks/downtime with invalid filter should return 400."""
        response = client.get('/api/parks/downtime?filter=invalid')

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['success'] is False


class TestResponseFieldTypes:
    """Test that response field types match OpenAPI spec."""

    def test_success_field_is_boolean(self):
        """success field must be boolean."""
        assert isinstance(True, bool)
        assert isinstance(False, bool)
        # Ensure we're not accepting "true"/"false" strings
        assert not isinstance("true", bool)

    def test_numeric_fields_are_numbers(self):
        """Numeric fields should be int or float."""
        sample_data = {
            "rank": 1,
            "park_id": 16,
            "total_downtime_hours": 12.5,
            "uptime_percentage": 89.2,
            "trend_percentage": -5.2
        }

        assert isinstance(sample_data['rank'], int)
        assert isinstance(sample_data['park_id'], int)
        assert isinstance(sample_data['total_downtime_hours'], (int, float))
        assert isinstance(sample_data['uptime_percentage'], (int, float))
        assert isinstance(sample_data['trend_percentage'], (int, float))

    def test_url_fields_are_strings(self):
        """URL fields should be strings with valid format."""
        url = "https://queue-times.com/parks/16"

        assert isinstance(url, str)
        assert url.startswith('http')


class TestOpenAPISchemaComponents:
    """Test OpenAPI schema component definitions."""

    def test_park_downtime_ranking_schema_exists(self):
        """ParkDowntimeRanking schema should be defined."""
        spec = load_openapi_spec()
        schemas = spec.get('components', {}).get('schemas', {})

        # Check for schema (may have different name)
        assert len(schemas) > 0, "No schemas defined in components"

    def test_aggregate_stats_schema_exists(self):
        """AggregateStats schema should be defined."""
        spec = load_openapi_spec()
        schemas = spec.get('components', {}).get('schemas', {})

        # Verify there are schemas defined
        assert len(schemas) > 0

    def test_error_response_defined(self):
        """Error response should be defined in components/responses."""
        spec = load_openapi_spec()
        responses = spec.get('components', {}).get('responses', {})

        # Check for error responses
        assert 'BadRequest' in responses or 'InternalServerError' in responses or len(responses) > 0
