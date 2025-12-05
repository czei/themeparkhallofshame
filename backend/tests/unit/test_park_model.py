"""
Theme Park Downtime Tracker - Park Model Unit Tests

Tests Park dataclass:
- Dataclass field validation
- location property (with/without state)
- is_disney_or_universal property
- queue_times_url property
- to_dict() method
- from_row() class method (dict and Row-like object)

Priority: P1 - Quick win for coverage increase (81% → 100%)
"""

from datetime import datetime
from models.park import Park


class TestParkDataclass:
    """Test Park dataclass creation and fields."""

    def test_create_park_all_fields(self):
        """Park should create instance with all fields."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        assert park.park_id == 1
        assert park.queue_times_id == 101
        assert park.name == "Magic Kingdom"
        assert park.city == "Lake Buena Vista"
        assert park.state_province == "Florida"
        assert park.country == "USA"
        assert park.latitude == 28.4177
        assert park.longitude == -81.5812
        assert park.timezone == "America/New_York"
        assert park.operator == "Walt Disney World"
        assert park.is_disney is True
        assert park.is_universal is False
        assert park.is_active is True
        assert park.created_at == datetime(2024, 1, 1, 0, 0, 0)
        assert park.updated_at == datetime(2024, 1, 15, 10, 30, 0)

    def test_create_park_optional_fields_none(self):
        """Park should allow None for optional fields."""
        park = Park(
            park_id=2,
            queue_times_id=102,
            name="Small Theme Park",
            city="Springfield",
            state_province=None,  # Optional
            country="USA",
            latitude=None,  # Optional
            longitude=None,  # Optional
            timezone="America/Chicago",
            operator=None,  # Optional
            is_disney=False,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.state_province is None
        assert park.latitude is None
        assert park.longitude is None
        assert park.operator is None


class TestParkProperties:
    """Test Park computed properties."""

    def test_location_with_state_province(self):
        """location should return 'City, State' when state_province is set."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.location == "Lake Buena Vista, Florida"

    def test_location_without_state_province(self):
        """location should return 'City, Country' when state_province is None."""
        park = Park(
            park_id=2,
            queue_times_id=102,
            name="Disneyland Paris",
            city="Marne-la-Vallée",
            state_province=None,  # No state for international parks
            country="France",
            latitude=48.8675,
            longitude=2.7866,
            timezone="Europe/Paris",
            operator="Euro Disney",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.location == "Marne-la-Vallée, France"

    def test_is_disney_or_universal_disney_park(self):
        """is_disney_or_universal should return True for Disney parks."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.is_disney_or_universal is True

    def test_is_disney_or_universal_universal_park(self):
        """is_disney_or_universal should return True for Universal parks."""
        park = Park(
            park_id=2,
            queue_times_id=102,
            name="Universal Studios Florida",
            city="Orlando",
            state_province="Florida",
            country="USA",
            latitude=28.4793,
            longitude=-81.4689,
            timezone="America/New_York",
            operator="Universal Parks & Resorts",
            is_disney=False,
            is_universal=True,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.is_disney_or_universal is True

    def test_is_disney_or_universal_neither(self):
        """is_disney_or_universal should return False for independent parks."""
        park = Park(
            park_id=3,
            queue_times_id=103,
            name="Six Flags Magic Mountain",
            city="Valencia",
            state_province="California",
            country="USA",
            latitude=34.4244,
            longitude=-118.5974,
            timezone="America/Los_Angeles",
            operator="Six Flags",
            is_disney=False,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.is_disney_or_universal is False

    def test_queue_times_url(self):
        """queue_times_url should return Queue-Times.com URL."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.queue_times_url == "https://queue-times.com/parks/101"


class TestParkToDict:
    """Test Park to_dict() method."""

    def test_to_dict_all_fields(self):
        """to_dict() should return dictionary with all API fields."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        result = park.to_dict()

        assert result['park_id'] == 1
        assert result['queue_times_id'] == 101
        assert result['name'] == "Magic Kingdom"
        assert result['city'] == "Lake Buena Vista"
        assert result['state_province'] == "Florida"
        assert result['country'] == "USA"
        assert result['latitude'] == 28.4177
        assert result['longitude'] == -81.5812
        assert result['timezone'] == "America/New_York"
        assert result['operator'] == "Walt Disney World"
        assert result['is_disney'] is True
        assert result['is_universal'] is False
        assert result['is_active'] is True
        assert result['location'] == "Lake Buena Vista, Florida"
        assert result['queue_times_url'] == "https://queue-times.com/parks/101"

    def test_to_dict_excludes_timestamps(self):
        """to_dict() should not include created_at/updated_at."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Magic Kingdom",
            city="Lake Buena Vista",
            state_province="Florida",
            country="USA",
            latitude=28.4177,
            longitude=-81.5812,
            timezone="America/New_York",
            operator="Walt Disney World",
            is_disney=True,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        result = park.to_dict()

        assert 'created_at' not in result
        assert 'updated_at' not in result


class TestParkFromRow:
    """Test Park from_row() class method."""

    def test_from_row_dict(self):
        """from_row() should create Park from dictionary."""
        row = {
            'park_id': 1,
            'queue_times_id': 101,
            'name': "Magic Kingdom",
            'city': "Lake Buena Vista",
            'state_province': "Florida",
            'country': "USA",
            'latitude': 28.4177,
            'longitude': -81.5812,
            'timezone': "America/New_York",
            'operator': "Walt Disney World",
            'is_disney': True,
            'is_universal': False,
            'is_active': True,
            'created_at': datetime(2024, 1, 1, 0, 0, 0),
            'updated_at': datetime(2024, 1, 15, 10, 30, 0)
        }

        park = Park.from_row(row)

        assert park.park_id == 1
        assert park.name == "Magic Kingdom"
        assert park.city == "Lake Buena Vista"
        assert park.is_disney is True

    def test_from_row_object(self):
        """from_row() should create Park from Row-like object."""
        class MockRow:
            def __init__(self):
                self.park_id = 1
                self.queue_times_id = 101
                self.name = "Magic Kingdom"
                self.city = "Lake Buena Vista"
                self.state_province = "Florida"
                self.country = "USA"
                self.latitude = 28.4177
                self.longitude = -81.5812
                self.timezone = "America/New_York"
                self.operator = "Walt Disney World"
                self.is_disney = True
                self.is_universal = False
                self.is_active = True
                self.created_at = datetime(2024, 1, 1, 0, 0, 0)
                self.updated_at = datetime(2024, 1, 15, 10, 30, 0)

        row = MockRow()
        park = Park.from_row(row)

        assert park.park_id == 1
        assert park.name == "Magic Kingdom"
        assert park.city == "Lake Buena Vista"
        assert park.is_disney is True


class TestParkEdgeCases:
    """Test edge cases for Park model."""

    def test_park_with_empty_state_province(self):
        """Park should handle empty string state_province as None."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Test Park",
            city="Test City",
            state_province="",  # Empty string
            country="USA",
            latitude=None,
            longitude=None,
            timezone="America/New_York",
            operator=None,
            is_disney=False,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        # Empty string is falsy, so location should use country
        assert park.location == "Test City, USA"

    def test_inactive_park(self):
        """Park should support is_active=False."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Closed Park",
            city="Abandoned City",
            state_province=None,
            country="USA",
            latitude=None,
            longitude=None,
            timezone="America/New_York",
            operator=None,
            is_disney=False,
            is_universal=False,
            is_active=False,  # Inactive park
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.is_active is False

    def test_negative_coordinates(self):
        """Park should support negative latitude/longitude."""
        park = Park(
            park_id=1,
            queue_times_id=101,
            name="Southern Hemisphere Park",
            city="Sydney",
            state_province="NSW",
            country="Australia",
            latitude=-33.8688,  # Negative
            longitude=151.2093,
            timezone="Australia/Sydney",
            operator=None,
            is_disney=False,
            is_universal=False,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert park.latitude == -33.8688
        assert park.longitude == 151.2093
