"""
Unit Tests: ID Mapper
Tests for IDMapper UUID reconciliation with fuzzy matching.
Feature: 004-themeparks-data-collection
Task: T031
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from importer.id_mapper import IDMapper, MappingResult


class TestMappingResult:
    """Tests for MappingResult dataclass."""

    def test_exact_uuid_match(self):
        """MappingResult for exact UUID match."""
        result = MappingResult(
            ride_id=123,
            park_id=456,
            match_type='exact_uuid',
            confidence=1.0,
            matched_name="Test Ride"
        )

        assert result.ride_id == 123
        assert result.park_id == 456
        assert result.match_type == 'exact_uuid'
        assert result.confidence == 1.0

    def test_fuzzy_match_with_distance(self):
        """MappingResult for fuzzy name match."""
        result = MappingResult(
            ride_id=123,
            park_id=456,
            match_type='fuzzy_name',
            confidence=0.85,
            matched_name="Test Ride",
            distance=2
        )

        assert result.match_type == 'fuzzy_name'
        assert result.distance == 2

    def test_not_found(self):
        """MappingResult for unmatched entity."""
        result = MappingResult(
            ride_id=None,
            park_id=456,
            match_type='not_found',
            confidence=0.0
        )

        assert result.ride_id is None
        assert result.match_type == 'not_found'


class TestIDMapper:
    """Tests for IDMapper class."""

    @pytest.fixture
    def mock_session(self):
        """Create mock SQLAlchemy session."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_session):
        """Create IDMapper with mock session."""
        return IDMapper(mock_session, auto_create=False)

    def test_normalize_name(self, mapper):
        """Normalize name for comparison."""
        # Basic normalization
        assert mapper._normalize_name("Test Ride") == "test ride"

        # Special characters removed
        assert mapper._normalize_name("Test-Ride!") == "testride"

        # Multiple spaces collapsed
        assert mapper._normalize_name("Test   Ride") == "test ride"

        # Leading/trailing whitespace removed
        assert mapper._normalize_name("  Test Ride  ") == "test ride"

    def test_map_ride_exact_uuid_from_cache(self, mapper):
        """Map ride using cached UUID."""
        mapper._ride_uuid_cache["uuid-123"] = 456

        result = mapper.map_ride("uuid-123", "Test Ride", park_id=789)

        assert result.ride_id == 456
        assert result.match_type == 'exact_uuid'
        assert result.confidence == 1.0
        assert mapper.stats['cache_hits'] == 1

    def test_map_ride_exact_uuid_from_database(self, mapper, mock_session):
        """Map ride using UUID from database."""
        # Mock database query
        mock_ride = MagicMock()
        mock_ride.ride_id = 456
        mock_ride.park_id = 789
        mock_ride.ride_name = "Test Ride"

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_ride
        mock_session.execute.return_value = mock_result

        result = mapper.map_ride("uuid-123", "Test Ride", park_id=789)

        assert result.ride_id == 456
        assert result.match_type == 'exact_uuid'
        assert mapper._ride_uuid_cache["uuid-123"] == 456

    def test_map_ride_exact_name_from_cache(self, mapper, mock_session):
        """Map ride using cached exact name match."""
        # Cache the name mapping
        mapper._ride_name_cache[(789, "test ride")] = 456

        # UUID query returns nothing
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = mapper.map_ride("uuid-new", "Test Ride", park_id=789)

        assert result.ride_id == 456
        assert result.match_type == 'exact_name'

    def test_map_ride_fuzzy_match(self, mapper, mock_session):
        """Map ride using fuzzy name match."""
        # UUID query returns nothing
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Pre-populate ride cache (bypassing database load)
        # "test rid" is close to "test ride" - Levenshtein distance = 1
        mapper._rides_by_park[789] = [(456, "test rid")]

        result = mapper.map_ride("uuid-new", "Test Ride", park_id=789)

        # Should find fuzzy match with distance <= 3
        assert result.ride_id == 456
        assert result.match_type == 'fuzzy_name'
        assert result.distance is not None
        assert result.distance == 1
        assert result.confidence >= 0.8

    def test_map_ride_fuzzy_no_match(self, mapper, mock_session):
        """No fuzzy match when distance too high."""
        # UUID query returns nothing
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Add a ride with very different name
        mapper._rides_by_park[789] = [(456, "completely different name")]

        result = mapper.map_ride("uuid-new", "Test Ride", park_id=789)

        assert result.ride_id is None
        assert result.match_type == 'not_found'
        assert result.confidence == 0.0

    def test_map_ride_auto_create(self, mock_session):
        """Auto-create new ride when no match found."""
        # Create mapper with auto_create enabled
        mapper = IDMapper(mock_session, auto_create=True)

        # Pre-populate the park cache to avoid database load
        mapper._rides_by_park[789] = []

        # UUID query returns nothing
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Capture the ride when it's added to the session
        created_ride = None

        def capture_add(ride):
            nonlocal created_ride
            created_ride = ride
            # Simulate ID assignment that happens after flush
            ride.ride_id = 999

        mock_session.add = capture_add
        mock_session.flush = MagicMock()

        result = mapper.map_ride("uuid-new", "New Ride", park_id=789)

        # Verify Ride was created with correct attributes
        assert created_ride is not None
        assert created_ride.park_id == 789
        assert created_ride.name == "New Ride"
        assert created_ride.themeparks_wiki_id == "uuid-new"
        assert created_ride.queue_times_id is not None  # Generated from hash

        assert result.match_type == 'created'
        assert result.confidence == 1.0
        assert result.ride_id == 999

    def test_map_park_from_uuid_cache(self, mapper):
        """Map park using cached UUID."""
        mapper._park_uuid_cache["park-uuid-123"] = 456

        result = mapper.map_park(park_uuid="park-uuid-123")

        assert result == 456
        assert mapper.stats['cache_hits'] == 1

    def test_map_park_from_slug_cache(self, mapper):
        """Map park using cached slug."""
        mapper._park_slug_cache["magic-kingdom"] = 789

        result = mapper.map_park(park_slug="magic-kingdom")

        assert result == 789

    def test_map_park_from_database(self, mapper, mock_session):
        """Map park from database by UUID."""
        mock_park = MagicMock()
        mock_park.park_id = 456

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_park
        mock_session.execute.return_value = mock_result

        result = mapper.map_park(park_uuid="park-uuid-123")

        assert result == 456
        assert mapper._park_uuid_cache["park-uuid-123"] == 456

    def test_map_park_not_found(self, mapper, mock_session):
        """Return None when park not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = mapper.map_park(park_uuid="unknown-uuid")

        assert result is None

    def test_map_entity_from_event(self, mapper, mock_session):
        """Map entity from archive event data."""
        # Mock park mapping
        mock_park = MagicMock()
        mock_park.park_id = 789

        # Mock ride mapping
        mock_ride = MagicMock()
        mock_ride.ride_id = 456
        mock_ride.park_id = 789
        mock_ride.ride_name = "Test Ride"

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.side_effect = [mock_park, mock_ride]
        mock_session.execute.return_value = mock_result

        result = mapper.map_entity_from_event(
            entity_id="ride-uuid",
            name="Test Ride",
            park_id="park-uuid",
            park_slug=None
        )

        assert result.ride_id == 456
        assert result.park_id == 789

    def test_map_entity_from_event_park_not_found(self, mapper, mock_session):
        """Return not_found when park cannot be mapped."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        result = mapper.map_entity_from_event(
            entity_id="ride-uuid",
            name="Test Ride",
            park_id="unknown-park-uuid",
            park_slug=None
        )

        assert result.ride_id is None
        assert result.park_id is None
        assert result.match_type == 'not_found'

    def test_bulk_map(self, mapper, mock_session):
        """Map multiple entities in bulk."""
        # Pre-populate caches
        mapper._ride_uuid_cache["uuid-1"] = 101
        mapper._ride_uuid_cache["uuid-2"] = 102

        entities = [
            ("uuid-1", "Ride 1", 789),
            ("uuid-2", "Ride 2", 789)
        ]

        results = mapper.bulk_map(entities)

        assert len(results) == 2
        assert results["uuid-1"].ride_id == 101
        assert results["uuid-2"].ride_id == 102

    def test_clear_caches(self, mapper):
        """Clear all internal caches."""
        mapper._ride_uuid_cache["uuid"] = 123
        mapper._ride_name_cache[("key",)] = 456
        mapper._park_uuid_cache["park"] = 789

        mapper.clear_caches()

        assert len(mapper._ride_uuid_cache) == 0
        assert len(mapper._ride_name_cache) == 0
        assert len(mapper._park_uuid_cache) == 0

    def test_reset_stats(self, mapper):
        """Reset mapping statistics."""
        mapper._stats['exact_uuid'] = 10
        mapper._stats['cache_hits'] = 20

        mapper.reset_stats()

        assert mapper.stats['exact_uuid'] == 0
        assert mapper.stats['cache_hits'] == 0

    def test_stats_property_returns_copy(self, mapper):
        """Stats property returns copy, not original."""
        stats = mapper.stats
        stats['exact_uuid'] = 999

        assert mapper._stats['exact_uuid'] == 0


class TestLevenshteinDistance:
    """Tests for Levenshtein distance calculation."""

    def test_exact_match_distance_zero(self):
        """Exact match has distance 0."""
        from importer.id_mapper import levenshtein_distance

        assert levenshtein_distance("test", "test") == 0

    def test_single_char_difference(self):
        """Single character difference."""
        from importer.id_mapper import levenshtein_distance

        assert levenshtein_distance("test", "tast") == 1
        assert levenshtein_distance("test", "test1") == 1

    def test_multiple_char_differences(self):
        """Multiple character differences."""
        from importer.id_mapper import levenshtein_distance

        assert levenshtein_distance("kitten", "sitting") == 3

    def test_empty_strings(self):
        """Handle empty strings."""
        from importer.id_mapper import levenshtein_distance

        assert levenshtein_distance("", "") == 0
        assert levenshtein_distance("test", "") == 4
        assert levenshtein_distance("", "test") == 4
