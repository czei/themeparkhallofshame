"""
Integration Tests: Metadata Collection (Feature 004)

Tests for the MetadataCollector that syncs entity metadata
from ThemeParks.wiki API to the local database.

Feature: 004-themeparks-data-collection
Task: T055
"""

import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch

from collector.metadata_collector import MetadataCollector, INDOOR_TAGS, OUTDOOR_TAGS
from models.orm_metadata import EntityMetadata
from models.orm_ride import Ride
from models.orm_park import Park


@pytest.mark.integration
class TestMetadataCollectorClassification:
    """Tests for indoor/outdoor classification logic."""

    def test_classify_indoor_from_dark_ride_tag(self, mysql_session):
        """Should classify as INDOOR when dark ride tag present."""
        collector = MetadataCollector(mysql_session)
        result = collector.classify_indoor_outdoor(['dark ride', 'classic'])
        assert result == 'INDOOR'

    def test_classify_outdoor_from_coaster_tag(self, mysql_session):
        """Should classify as OUTDOOR when coaster tag present."""
        collector = MetadataCollector(mysql_session)
        result = collector.classify_indoor_outdoor(['roller coaster', 'thrill'])
        assert result == 'OUTDOOR'

    def test_classify_hybrid_when_both_tags(self, mysql_session):
        """Should classify as HYBRID when both indoor and outdoor tags present."""
        collector = MetadataCollector(mysql_session)
        result = collector.classify_indoor_outdoor(['dark ride', 'water ride'])
        assert result == 'HYBRID'

    def test_classify_none_for_unknown_tags(self, mysql_session):
        """Should return None for unknown tags."""
        collector = MetadataCollector(mysql_session)
        result = collector.classify_indoor_outdoor(['family', 'adventure'])
        assert result is None

    def test_classify_none_for_empty_tags(self, mysql_session):
        """Should return None for empty tag list."""
        collector = MetadataCollector(mysql_session)
        result = collector.classify_indoor_outdoor([])
        assert result is None


@pytest.mark.integration
class TestMetadataCollectorHeightParsing:
    """Tests for height requirement parsing."""

    def test_parse_height_min_only(self, mysql_session):
        """Should parse minimum height requirement."""
        collector = MetadataCollector(mysql_session)
        min_h, max_h = collector.parse_height_requirement({
            'height': {'min': 102, 'unit': 'cm'}
        })
        assert min_h == 102
        assert max_h is None

    def test_parse_height_min_and_max(self, mysql_session):
        """Should parse both min and max height."""
        collector = MetadataCollector(mysql_session)
        min_h, max_h = collector.parse_height_requirement({
            'height': {'min': 102, 'max': 195, 'unit': 'cm'}
        })
        assert min_h == 102
        assert max_h == 195

    def test_parse_height_convert_inches(self, mysql_session):
        """Should convert inches to centimeters."""
        collector = MetadataCollector(mysql_session)
        min_h, max_h = collector.parse_height_requirement({
            'height': {'min': 40, 'unit': 'inches'}
        })
        # 40 inches * 2.54 = 101.6, truncated to 101
        assert min_h == 101

    def test_parse_height_empty_restrictions(self, mysql_session):
        """Should return None for empty restrictions."""
        collector = MetadataCollector(mysql_session)
        min_h, max_h = collector.parse_height_requirement({})
        assert min_h is None
        assert max_h is None


@pytest.mark.integration
class TestMetadataCollectorCoordinates:
    """Tests for coordinate extraction."""

    def test_extract_coordinates_valid(self, mysql_session):
        """Should extract valid coordinates."""
        collector = MetadataCollector(mysql_session)
        lat, lon = collector.extract_coordinates({
            'location': {
                'latitude': 28.4177,
                'longitude': -81.5812
            }
        })
        assert lat == Decimal('28.4177')
        assert lon == Decimal('-81.5812')

    def test_extract_coordinates_missing_location(self, mysql_session):
        """Should return None for missing location."""
        collector = MetadataCollector(mysql_session)
        lat, lon = collector.extract_coordinates({})
        assert lat is None
        assert lon is None

    def test_extract_coordinates_partial_location(self, mysql_session):
        """Should return None for partial location."""
        collector = MetadataCollector(mysql_session)
        lat, lon = collector.extract_coordinates({
            'location': {'latitude': 28.4177}  # Missing longitude
        })
        assert lat is None
        assert lon is None


@pytest.mark.integration
class TestMetadataCollectorSync:
    """Tests for metadata sync operations."""

    def _create_test_park(self, session, name="Test Park", wiki_id="park-uuid-123"):
        """Helper to create a test park."""
        # Get a unique queue_times_id
        from sqlalchemy import func
        max_id = session.query(func.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_id + 1,
            name=name,
            city="Test City",
            state_province="CA",
            country="US",
            themeparks_wiki_id=wiki_id,
            timezone='America/Los_Angeles'
        )
        session.add(park)
        session.flush()
        return park

    def _create_test_ride(self, session, park, name="Test Ride", wiki_id="ride-uuid-456"):
        """Helper to create a test ride."""
        from sqlalchemy import func
        max_id = session.query(func.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_id + 1,
            name=name,
            park_id=park.park_id,
            themeparks_wiki_id=wiki_id,
            tier=2
        )
        session.add(ride)
        session.flush()
        return ride

    @patch('collector.metadata_collector.get_themeparks_wiki_client')
    def test_sync_ride_creates_metadata(self, mock_client_factory, mysql_session):
        """Should create new EntityMetadata record for ride."""
        park = self._create_test_park(mysql_session)
        ride = self._create_test_ride(mysql_session, park)

        # Mock API response
        mock_client = Mock()
        mock_client.get_entity.return_value = {
            'id': ride.themeparks_wiki_id,
            'name': 'Test Ride',
            'entityType': 'ATTRACTION',
            'location': {'latitude': 28.4177, 'longitude': -81.5812},
            'tags': ['dark ride', 'classic'],
            'restrictions': {'height': {'min': 102, 'unit': 'cm'}}
        }
        mock_client_factory.return_value = mock_client

        collector = MetadataCollector(mysql_session, client=mock_client)
        metadata = collector.sync_ride_metadata(ride)

        assert metadata is not None
        assert metadata.ride_id == ride.ride_id
        assert metadata.entity_name == 'Test Ride'
        assert metadata.entity_type == 'ATTRACTION'
        assert metadata.latitude == Decimal('28.4177')
        assert metadata.longitude == Decimal('-81.5812')
        assert metadata.indoor_outdoor == 'INDOOR'
        assert metadata.height_min_cm == 102

    @patch('collector.metadata_collector.get_themeparks_wiki_client')
    def test_sync_ride_updates_existing_metadata(self, mock_client_factory, mysql_session):
        """Should update existing EntityMetadata record."""
        park = self._create_test_park(mysql_session)
        ride = self._create_test_ride(mysql_session, park)

        # Create existing metadata
        existing = EntityMetadata(
            ride_id=ride.ride_id,
            themeparks_wiki_id=ride.themeparks_wiki_id,
            entity_name='Old Name',
            entity_type='ATTRACTION',
            version=1
        )
        mysql_session.add(existing)
        mysql_session.flush()

        # Mock API response with updated name
        mock_client = Mock()
        mock_client.get_entity.return_value = {
            'id': ride.themeparks_wiki_id,
            'name': 'Updated Name',
            'entityType': 'ATTRACTION',
            'location': {'latitude': 28.4177, 'longitude': -81.5812},
            'tags': []
        }
        mock_client_factory.return_value = mock_client

        collector = MetadataCollector(mysql_session, client=mock_client)
        metadata = collector.sync_ride_metadata(ride)

        assert metadata.entity_name == 'Updated Name'
        assert metadata.latitude == Decimal('28.4177')
        assert metadata.version == 2  # Version incremented

    @patch('collector.metadata_collector.get_themeparks_wiki_client')
    def test_sync_ride_without_wiki_id_returns_none(self, mock_client_factory, mysql_session):
        """Should return None for ride without themeparks_wiki_id."""
        from sqlalchemy import func
        park = self._create_test_park(mysql_session)
        max_id = mysql_session.query(func.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_id + 1,
            name='No Wiki ID Ride',
            park_id=park.park_id,
            themeparks_wiki_id=None,  # No wiki ID
            tier=2
        )
        mysql_session.add(ride)
        mysql_session.flush()

        collector = MetadataCollector(mysql_session)
        result = collector.sync_ride_metadata(ride)

        assert result is None


@pytest.mark.integration
class TestMetadataCollectorCoverage:
    """Tests for coverage statistics."""

    def _create_test_data(self, session):
        """Create test park and rides."""
        from sqlalchemy import func
        max_park_id = session.query(func.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_park_id + 1,
            name="Coverage Test Park",
            city="Test City",
            state_province="CA",
            country="US",
            themeparks_wiki_id="coverage-park-uuid",
            timezone='America/Los_Angeles'
        )
        session.add(park)
        session.flush()

        max_ride_id = session.query(func.max(Ride.queue_times_id)).scalar() or 80000
        rides = []
        for i in range(5):
            ride = Ride(
                queue_times_id=max_ride_id + i + 1,
                name=f"Ride {i}",
                park_id=park.park_id,
                themeparks_wiki_id=f"ride-uuid-{i}",
                tier=2
            )
            session.add(ride)
            rides.append(ride)
        session.flush()

        # Add metadata for some rides
        for i in range(3):
            metadata = EntityMetadata(
                ride_id=rides[i].ride_id,
                themeparks_wiki_id=rides[i].themeparks_wiki_id,
                entity_name=f"Ride {i}",
                entity_type='ATTRACTION',
                latitude=Decimal('28.4177') if i < 2 else None,
                longitude=Decimal('-81.5812') if i < 2 else None,
                height_min_cm=102 if i == 0 else None,
                indoor_outdoor='INDOOR' if i == 0 else None
            )
            session.add(metadata)
        session.flush()

        return park, rides

    def test_get_coverage_stats(self, mysql_session):
        """Should return accurate coverage statistics."""
        park, rides = self._create_test_data(mysql_session)

        collector = MetadataCollector(mysql_session)
        stats = collector.get_coverage_stats()

        # We created 5 rides with wiki IDs, 3 have metadata
        assert stats['total_rides'] >= 5
        assert stats['with_metadata'] >= 3
        assert stats['with_coordinates'] >= 2
        assert stats['with_height_requirement'] >= 1
        assert stats['with_indoor_outdoor'] >= 1


@pytest.mark.integration
class TestEntityMetadataModel:
    """Tests for EntityMetadata model methods."""

    def test_has_coordinates_true(self, mysql_session):
        """Should return True when coordinates are set."""
        metadata = EntityMetadata(
            ride_id=1,
            themeparks_wiki_id='test-uuid',
            entity_name='Test',
            entity_type='ATTRACTION',
            latitude=Decimal('28.4177'),
            longitude=Decimal('-81.5812')
        )
        assert metadata.has_coordinates is True

    def test_has_coordinates_false_when_missing(self, mysql_session):
        """Should return False when coordinates are missing."""
        metadata = EntityMetadata(
            ride_id=1,
            themeparks_wiki_id='test-uuid',
            entity_name='Test',
            entity_type='ATTRACTION',
            latitude=None,
            longitude=None
        )
        assert metadata.has_coordinates is False

    def test_height_requirement_text_with_min_only(self, mysql_session):
        """Should format height text with min only."""
        metadata = EntityMetadata(
            ride_id=1,
            themeparks_wiki_id='test-uuid',
            entity_name='Test',
            entity_type='ATTRACTION',
            height_min_cm=102
        )
        assert metadata.height_requirement_text == "102+ cm"

    def test_height_requirement_text_with_range(self, mysql_session):
        """Should format height text with range."""
        metadata = EntityMetadata(
            ride_id=1,
            themeparks_wiki_id='test-uuid',
            entity_name='Test',
            entity_type='ATTRACTION',
            height_min_cm=102,
            height_max_cm=195
        )
        assert metadata.height_requirement_text == "102-195 cm"

    def test_height_requirement_text_none_when_not_set(self, mysql_session):
        """Should return None when no height requirement."""
        metadata = EntityMetadata(
            ride_id=1,
            themeparks_wiki_id='test-uuid',
            entity_name='Test',
            entity_type='ATTRACTION'
        )
        assert metadata.height_requirement_text is None
