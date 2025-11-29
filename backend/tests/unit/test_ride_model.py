"""
Theme Park Downtime Tracker - Ride Model Unit Tests

Tests Ride dataclass:
- Dataclass field validation
- tier_weight property (Tier 1/2/3, unclassified)
- tier_label property
- queue_times_url property
- to_dict() method
- from_row() class method (dict and Row-like object)

Priority: P1 - Quick win for coverage increase (77% → 100%)
"""

import pytest
from datetime import datetime
from models.ride import Ride


class TestRideDataclass:
    """Test Ride dataclass creation and fields."""

    def test_create_ride_all_fields(self):
        """Ride should create instance with all fields."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            category='ATTRACTION',
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        assert ride.ride_id == 1
        assert ride.queue_times_id == 1001
        assert ride.park_id == 101
        assert ride.name == "Space Mountain"
        assert ride.land_area == "Tomorrowland"
        assert ride.tier == 1
        assert ride.category == 'ATTRACTION'
        assert ride.is_active is True
        assert ride.created_at == datetime(2024, 1, 1, 0, 0, 0)
        assert ride.updated_at == datetime(2024, 1, 15, 10, 30, 0)

    def test_create_ride_optional_fields_none(self):
        """Ride should allow None for optional fields."""
        ride = Ride(
            ride_id=2,
            queue_times_id=1002,
            park_id=101,
            name="Unclassified Ride",
            land_area=None,  # Optional
            tier=None,  # Optional (unclassified)
            category=None,  # Optional
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.land_area is None
        assert ride.tier is None
        assert ride.category is None


class TestRideTierProperties:
    """Test Ride tier-related properties."""

    def test_tier_weight_tier_1(self):
        """tier_weight should return 3 for Tier 1 (major attractions)."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_weight == 3

    def test_tier_weight_tier_2(self):
        """tier_weight should return 2 for Tier 2 (standard attractions)."""
        ride = Ride(
            ride_id=2,
            queue_times_id=1002,
            park_id=101,
            name="Buzz Lightyear",
            land_area="Tomorrowland",
            tier=2,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_weight == 2

    def test_tier_weight_tier_3(self):
        """tier_weight should return 1 for Tier 3 (minor attractions)."""
        ride = Ride(
            ride_id=3,
            queue_times_id=1003,
            park_id=101,
            name="Carousel",
            land_area="Fantasyland",
            tier=3,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_weight == 1

    def test_tier_weight_unclassified(self):
        """tier_weight should return 1 for unclassified rides (tier=None)."""
        ride = Ride(
            ride_id=4,
            queue_times_id=1004,
            park_id=101,
            name="New Ride",
            land_area=None,
            tier=None,  # Unclassified
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_weight == 1

    def test_tier_label_tier_1(self):
        """tier_label should return 'Tier 1 (Major)' for tier 1."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_label == "Tier 1 (Major)"

    def test_tier_label_tier_2(self):
        """tier_label should return 'Tier 2 (Standard)' for tier 2."""
        ride = Ride(
            ride_id=2,
            queue_times_id=1002,
            park_id=101,
            name="Buzz Lightyear",
            land_area="Tomorrowland",
            tier=2,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_label == "Tier 2 (Standard)"

    def test_tier_label_tier_3(self):
        """tier_label should return 'Tier 3 (Minor)' for tier 3."""
        ride = Ride(
            ride_id=3,
            queue_times_id=1003,
            park_id=101,
            name="Carousel",
            land_area="Fantasyland",
            tier=3,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_label == "Tier 3 (Minor)"

    def test_tier_label_unclassified(self):
        """tier_label should return 'Unclassified' for tier=None."""
        ride = Ride(
            ride_id=4,
            queue_times_id=1004,
            park_id=101,
            name="New Ride",
            land_area=None,
            tier=None,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.tier_label == "Unclassified"

    def test_queue_times_url(self):
        """queue_times_url should return Queue-Times.com URL."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.queue_times_url == "https://queue-times.com/ride/1001"


class TestRideToDict:
    """Test Ride to_dict() method."""

    def test_to_dict_all_fields(self):
        """to_dict() should return dictionary with all API fields."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        result = ride.to_dict()

        assert result['ride_id'] == 1
        assert result['queue_times_id'] == 1001
        assert result['park_id'] == 101
        assert result['name'] == "Space Mountain"
        assert result['land_area'] == "Tomorrowland"
        assert result['tier'] == 1
        assert result['tier_weight'] == 3
        assert result['tier_label'] == "Tier 1 (Major)"
        assert result['is_active'] is True
        assert result['queue_times_url'] == "https://queue-times.com/ride/1001"

    def test_to_dict_excludes_timestamps(self):
        """to_dict() should not include created_at/updated_at."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Space Mountain",
            land_area="Tomorrowland",
            tier=1,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0)
        )

        result = ride.to_dict()

        assert 'created_at' not in result
        assert 'updated_at' not in result

    def test_to_dict_unclassified_ride(self):
        """to_dict() should handle unclassified rides (tier=None)."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="New Ride",
            land_area=None,
            tier=None,  # Unclassified
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        result = ride.to_dict()

        assert result['tier'] is None
        assert result['tier_weight'] == 1  # Default weight
        assert result['tier_label'] == "Unclassified"


class TestRideFromRow:
    """Test Ride from_row() class method."""

    def test_from_row_dict(self):
        """from_row() should create Ride from dictionary."""
        row = {
            'ride_id': 1,
            'queue_times_id': 1001,
            'park_id': 101,
            'name': "Space Mountain",
            'land_area': "Tomorrowland",
            'tier': 1,
            'is_active': True,
            'created_at': datetime(2024, 1, 1, 0, 0, 0),
            'updated_at': datetime(2024, 1, 15, 10, 30, 0)
        }

        ride = Ride.from_row(row)

        assert ride.ride_id == 1
        assert ride.name == "Space Mountain"
        assert ride.tier == 1

    def test_from_row_object(self):
        """from_row() should create Ride from Row-like object."""
        class MockRow:
            def __init__(self):
                self.ride_id = 1
                self.queue_times_id = 1001
                self.park_id = 101
                self.name = "Space Mountain"
                self.land_area = "Tomorrowland"
                self.tier = 1
                self.is_active = True
                self.created_at = datetime(2024, 1, 1, 0, 0, 0)
                self.updated_at = datetime(2024, 1, 15, 10, 30, 0)

        row = MockRow()
        ride = Ride.from_row(row)

        assert ride.ride_id == 1
        assert ride.name == "Space Mountain"
        assert ride.tier == 1


class TestRideEdgeCases:
    """Test edge cases for Ride model."""

    def test_inactive_ride(self):
        """Ride should support is_active=False."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Closed Attraction",
            land_area="Adventureland",
            tier=2,
            is_active=False,  # Inactive
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.is_active is False

    def test_ride_without_land_area(self):
        """Ride should handle None land_area."""
        ride = Ride(
            ride_id=1,
            queue_times_id=1001,
            park_id=101,
            name="Park-Wide Experience",
            land_area=None,  # No specific land
            tier=2,
            is_active=True,
            created_at=datetime(2024, 1, 1, 0, 0, 0),
            updated_at=datetime(2024, 1, 1, 0, 0, 0)
        )

        assert ride.land_area is None
        result = ride.to_dict()
        assert result['land_area'] is None

    def test_all_tier_values(self):
        """Ride should handle all valid tier values (1, 2, 3, None)."""
        tier_1 = Ride(
            ride_id=1, queue_times_id=1001, park_id=101, name="Tier 1 Ride",
            land_area=None, tier=1, is_active=True,
            created_at=datetime.now(), updated_at=datetime.now()
        )
        tier_2 = Ride(
            ride_id=2, queue_times_id=1002, park_id=101, name="Tier 2 Ride",
            land_area=None, tier=2, is_active=True,
            created_at=datetime.now(), updated_at=datetime.now()
        )
        tier_3 = Ride(
            ride_id=3, queue_times_id=1003, park_id=101, name="Tier 3 Ride",
            land_area=None, tier=3, is_active=True,
            created_at=datetime.now(), updated_at=datetime.now()
        )
        unclassified = Ride(
            ride_id=4, queue_times_id=1004, park_id=101, name="Unclassified Ride",
            land_area=None, tier=None, is_active=True,
            created_at=datetime.now(), updated_at=datetime.now()
        )

        assert tier_1.tier_weight == 3
        assert tier_2.tier_weight == 2
        assert tier_3.tier_weight == 1
        assert unclassified.tier_weight == 1

        assert tier_1.tier_label == "Tier 1 (Major)"
        assert tier_2.tier_label == "Tier 2 (Standard)"
        assert tier_3.tier_label == "Tier 3 (Minor)"
        assert unclassified.tier_label == "Unclassified"
