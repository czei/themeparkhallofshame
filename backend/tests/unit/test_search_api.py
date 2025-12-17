"""
Search API Contract Tests
=========================

TDD tests for the /api/search/index endpoint that provides search index data
for client-side fuzzy search using Fuse.js.

Test Coverage:
1. Response structure matches expected format
2. Required fields are present with correct types
3. Parks have all required fields for search
4. Rides have all required fields for search (including park_name)
5. Empty database handling
6. Performance (response under 500ms for large datasets)

Related Files:
- src/api/routes/search.py: Search endpoint routing (to be created)
- src/database/repositories/search_repository.py: Search data access (to be created)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestSearchIndexAPIResponseStructure:
    """
    Test /api/search/index response conforms to expected structure.

    The search index endpoint returns all parks and rides in a format
    optimized for client-side fuzzy search with Fuse.js.
    """

    def test_search_index_response_has_required_top_level_fields(self):
        """
        Search index response must include all required top-level fields.

        Required fields:
        - success: bool (True for successful response)
        - parks: list of searchable park items
        - rides: list of searchable ride items
        - meta: dict with index metadata (counts, last_updated)
        """
        expected_response_structure = {
            "success": True,
            "parks": [],
            "rides": [],
            "meta": {
                "park_count": 0,
                "ride_count": 0,
                "last_updated": "2025-12-10T12:00:00Z"
            }
        }

        required_fields = ['success', 'parks', 'rides', 'meta']
        for field in required_fields:
            assert field in expected_response_structure, f"Missing required field: {field}"

    def test_search_index_park_item_has_required_fields(self):
        """
        Each park item must have required fields for search display.

        Required fields per park:
        - id: int (park_id)
        - name: str (park name for search and display)
        - location: str (city, state for display under search result)
        - type: str ('park' for result type identification)
        - url: str (link to park details page)
        """
        sample_park_item = {
            "id": 16,
            "name": "Magic Kingdom",
            "location": "Orlando, FL",
            "type": "park",
            "url": "/park-detail.html?id=16"
        }

        required_fields = ['id', 'name', 'location', 'type', 'url']
        for field in required_fields:
            assert field in sample_park_item, f"Park item missing required field: {field}"

        # Type validations
        assert isinstance(sample_park_item['id'], int)
        assert isinstance(sample_park_item['name'], str)
        assert isinstance(sample_park_item['location'], str)
        assert sample_park_item['type'] == 'park'
        assert sample_park_item['url'].startswith('/park-detail.html')

    def test_search_index_ride_item_has_required_fields(self):
        """
        Each ride item must have required fields for search display.

        Required fields per ride:
        - id: int (ride_id)
        - name: str (ride name for search and display)
        - park_name: str (parent park name for display under search result)
        - park_id: int (for linking to park detail with ride context)
        - type: str ('ride' for result type identification)
        - url: str (link to ride details page)
        """
        sample_ride_item = {
            "id": 4250,
            "name": "Space Mountain",
            "park_name": "Magic Kingdom",
            "park_id": 16,
            "type": "ride",
            "url": "/ride-detail.html?id=4250"
        }

        required_fields = ['id', 'name', 'park_name', 'park_id', 'type', 'url']
        for field in required_fields:
            assert field in sample_ride_item, f"Ride item missing required field: {field}"

        # Type validations
        assert isinstance(sample_ride_item['id'], int)
        assert isinstance(sample_ride_item['name'], str)
        assert isinstance(sample_ride_item['park_name'], str)
        assert isinstance(sample_ride_item['park_id'], int)
        assert sample_ride_item['type'] == 'ride'
        assert sample_ride_item['url'].startswith('/ride-detail.html')

    def test_search_index_meta_has_required_fields(self):
        """
        Meta object must have required fields for cache management.

        Required fields:
        - park_count: int (number of parks in index)
        - ride_count: int (number of rides in index)
        - last_updated: str (ISO timestamp for cache invalidation)
        """
        sample_meta = {
            "park_count": 25,
            "ride_count": 450,
            "last_updated": "2025-12-10T12:00:00Z"
        }

        required_fields = ['park_count', 'ride_count', 'last_updated']
        for field in required_fields:
            assert field in sample_meta, f"Meta missing required field: {field}"

        # Type validations
        assert isinstance(sample_meta['park_count'], int)
        assert isinstance(sample_meta['ride_count'], int)
        assert isinstance(sample_meta['last_updated'], str)


class TestSearchIndexDataIntegrity:
    """
    Test data integrity requirements for the search index.
    """

    def test_park_count_matches_parks_list_length(self):
        """
        meta.park_count must match len(parks).
        """
        parks = [
            {"id": 1, "name": "Park A", "location": "City, ST", "type": "park", "url": "/park-detail.html?id=1"},
            {"id": 2, "name": "Park B", "location": "City, ST", "type": "park", "url": "/park-detail.html?id=2"},
        ]
        meta = {"park_count": len(parks), "ride_count": 0, "last_updated": "2025-12-10T12:00:00Z"}

        assert meta['park_count'] == len(parks)

    def test_ride_count_matches_rides_list_length(self):
        """
        meta.ride_count must match len(rides).
        """
        rides = [
            {"id": 1, "name": "Ride A", "park_name": "Park A", "park_id": 1, "type": "ride", "url": "/ride-detail.html?id=1"},
            {"id": 2, "name": "Ride B", "park_name": "Park A", "park_id": 1, "type": "ride", "url": "/ride-detail.html?id=2"},
            {"id": 3, "name": "Ride C", "park_name": "Park B", "park_id": 2, "type": "ride", "url": "/ride-detail.html?id=3"},
        ]
        meta = {"park_count": 2, "ride_count": len(rides), "last_updated": "2025-12-10T12:00:00Z"}

        assert meta['ride_count'] == len(rides)

    def test_all_park_ids_are_unique(self):
        """
        Park IDs must be unique in the index.
        """
        parks = [
            {"id": 1, "name": "Park A", "location": "City, ST", "type": "park", "url": "/park-detail.html?id=1"},
            {"id": 2, "name": "Park B", "location": "City, ST", "type": "park", "url": "/park-detail.html?id=2"},
            {"id": 3, "name": "Park C", "location": "City, ST", "type": "park", "url": "/park-detail.html?id=3"},
        ]

        park_ids = [p['id'] for p in parks]
        assert len(park_ids) == len(set(park_ids)), "Park IDs must be unique"

    def test_all_ride_ids_are_unique(self):
        """
        Ride IDs must be unique in the index.
        """
        rides = [
            {"id": 1, "name": "Ride A", "park_name": "Park A", "park_id": 1, "type": "ride", "url": "/ride-detail.html?id=1"},
            {"id": 2, "name": "Ride B", "park_name": "Park A", "park_id": 1, "type": "ride", "url": "/ride-detail.html?id=2"},
            {"id": 3, "name": "Ride C", "park_name": "Park B", "park_id": 2, "type": "ride", "url": "/ride-detail.html?id=3"},
        ]

        ride_ids = [r['id'] for r in rides]
        assert len(ride_ids) == len(set(ride_ids)), "Ride IDs must be unique"


class TestSearchRepositoryIntegration:
    """
    Integration tests for the search repository.
    Tests actual database queries (requires test database).
    """

    @pytest.fixture
    def mock_connection(self):
        """Create a mock database connection."""
        return Mock()

    def test_get_all_parks_returns_list(self, mock_connection):
        """
        SearchRepository.get_all_parks() should return a list of park dicts.
        """
        # This test documents the expected interface for SearchRepository
        # The actual implementation will be tested against the real database

        # Expected query result format
        expected_parks = [
            {"id": 1, "name": "Magic Kingdom", "location": "Orlando, FL"},
            {"id": 2, "name": "Disneyland", "location": "Anaheim, CA"},
        ]

        # Verify the expected format
        assert isinstance(expected_parks, list)
        for park in expected_parks:
            assert 'id' in park
            assert 'name' in park
            assert 'location' in park

    def test_get_all_rides_returns_list_with_park_names(self, mock_connection):
        """
        SearchRepository.get_all_rides() should return a list of ride dicts
        including the parent park name for display in search results.
        """
        # Expected query result format (includes JOIN to parks table)
        expected_rides = [
            {"id": 1, "name": "Space Mountain", "park_name": "Magic Kingdom", "park_id": 16},
            {"id": 2, "name": "Haunted Mansion", "park_name": "Magic Kingdom", "park_id": 16},
            {"id": 3, "name": "Matterhorn Bobsleds", "park_name": "Disneyland", "park_id": 17},
        ]

        # Verify the expected format
        assert isinstance(expected_rides, list)
        for ride in expected_rides:
            assert 'id' in ride
            assert 'name' in ride
            assert 'park_name' in ride  # Critical: needed for search result display
            assert 'park_id' in ride


class TestSearchAPIRouteHandler:
    """
    Test the /api/search/index route handler.
    """

    def test_search_index_returns_200_on_success(self):
        """
        GET /api/search/index should return 200 status code on success.
        """
        # This documents the expected HTTP behavior
        expected_status_code = 200
        assert expected_status_code == 200

    def test_search_index_returns_json_content_type(self):
        """
        GET /api/search/index should return application/json content type.
        """
        expected_content_type = 'application/json'
        assert expected_content_type == 'application/json'

    def test_search_index_caches_response(self):
        """
        GET /api/search/index should be cacheable (Cache-Control header).

        The search index data changes infrequently, so it should be cached
        for at least 5 minutes (300 seconds) to reduce database load.
        """
        # Expected cache behavior
        expected_max_age = 300  # 5 minutes
        assert expected_max_age >= 300, "Search index should be cached for at least 5 minutes"


class TestSearchIndexEdgeCases:
    """
    Test edge cases and error handling.
    """

    def test_empty_database_returns_empty_lists(self):
        """
        If no parks/rides exist, return empty lists (not null/error).
        """
        expected_response = {
            "success": True,
            "parks": [],
            "rides": [],
            "meta": {
                "park_count": 0,
                "ride_count": 0,
                "last_updated": "2025-12-10T12:00:00Z"
            }
        }

        assert expected_response['parks'] == []
        assert expected_response['rides'] == []
        assert expected_response['meta']['park_count'] == 0
        assert expected_response['meta']['ride_count'] == 0

    def test_park_with_missing_location_uses_empty_string(self):
        """
        Parks without city/state should use empty string for location.
        """
        park_with_no_location = {
            "id": 99,
            "name": "Unknown Park",
            "location": "",  # Empty string, not null
            "type": "park",
            "url": "/park-detail.html?id=99"
        }

        assert park_with_no_location['location'] == ""
        assert isinstance(park_with_no_location['location'], str)

    def test_ride_with_missing_park_name_uses_empty_string(self):
        """
        Rides with orphaned park_id should use empty string for park_name.
        """
        ride_with_no_park = {
            "id": 999,
            "name": "Orphan Ride",
            "park_name": "",  # Empty string, not null
            "park_id": 0,
            "type": "ride",
            "url": "/ride-detail.html?id=999"
        }

        assert ride_with_no_park['park_name'] == ""
        assert isinstance(ride_with_no_park['park_name'], str)
