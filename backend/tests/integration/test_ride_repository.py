"""
Theme Park Downtime Tracker - Ride Repository Unit Tests

Tests the RideRepository with in-memory SQLite database:
- CRUD operations (create, get_by_id, update)
- Query operations (get_by_park_id, get_all_active, get_unclassified_rides)
- Tier classification filtering

Priority: P1 - CRITICAL (Ride data is core to the application)
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.ride_repository import RideRepository
from models.ride import Ride


# ============================================================================
# FIXTURES - Cleanup
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def cleanup_before_ride_repository_tests(mysql_connection):
    """Clean up all test data once at start of this test module."""
    from sqlalchemy import text
    mysql_connection.execute(text("DELETE FROM ride_status_snapshots"))
    mysql_connection.execute(text("DELETE FROM ride_status_changes"))
    mysql_connection.execute(text("DELETE FROM park_activity_snapshots"))
    mysql_connection.execute(text("DELETE FROM ride_daily_stats"))
    mysql_connection.execute(text("DELETE FROM ride_weekly_stats"))
    mysql_connection.execute(text("DELETE FROM ride_monthly_stats"))
    mysql_connection.execute(text("DELETE FROM park_daily_stats"))
    mysql_connection.execute(text("DELETE FROM park_weekly_stats"))
    mysql_connection.execute(text("DELETE FROM park_monthly_stats"))
    mysql_connection.execute(text("DELETE FROM ride_classifications"))
    mysql_connection.execute(text("DELETE FROM rides"))
    mysql_connection.execute(text("DELETE FROM parks"))
    mysql_connection.commit()
    yield


# ============================================================================
# Test Class: CRUD Operations
# ============================================================================

class TestRideRepositoryCRUD:
    """
    Test basic CRUD operations for RideRepository.

    Priority: P0 - Foundation for all other operations
    """

    def test_create_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        Create a new ride record.

        Given: Valid ride data
        When: create() is called
        Then: Return Ride object with assigned ride_id
        """
        from tests.conftest import insert_sample_park

        # First create a park
        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        ride = repo.create(sample_ride_data)

        assert ride is not None
        assert ride.ride_id is not None
        assert ride.name == sample_ride_data['name']
        assert ride.queue_times_id == sample_ride_data['queue_times_id']
        assert ride.park_id == park_id
        assert ride.tier == sample_ride_data['tier']

    def test_get_by_id_existing_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        Fetch ride by ID.

        Given: Ride exists in database
        When: get_by_id() is called
        Then: Return Ride object
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        created_ride = repo.create(sample_ride_data)

        fetched_ride = repo.get_by_id(created_ride.ride_id)

        assert fetched_ride is not None
        assert fetched_ride.ride_id == created_ride.ride_id
        assert fetched_ride.name == created_ride.name

    def test_get_by_id_nonexistent_ride(self, mysql_connection):
        """
        Fetch ride by ID when ride doesn't exist.

        Given: Ride ID 999 doesn't exist
        When: get_by_id(999) is called
        Then: Return None
        """
        repo = RideRepository(mysql_connection)

        ride = repo.get_by_id(999)

        assert ride is None

    def test_get_by_queue_times_id(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        Fetch ride by Queue-Times.com external ID.

        Given: Ride exists with queue_times_id=1001
        When: get_by_queue_times_id(1001) is called
        Then: Return Ride object
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        created_ride = repo.create(sample_ride_data)

        fetched_ride = repo.get_by_queue_times_id(sample_ride_data['queue_times_id'])

        assert fetched_ride is not None
        assert fetched_ride.queue_times_id == sample_ride_data['queue_times_id']
        assert fetched_ride.ride_id == created_ride.ride_id

    def test_update_ride(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        Update existing ride record.

        Given: Ride exists
        When: update() is called with new data
        Then: Return updated Ride object
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        created_ride = repo.create(sample_ride_data)

        update_data = {'name': 'Updated Space Mountain', 'tier': 2}
        updated_ride = repo.update(created_ride.ride_id, update_data)

        assert updated_ride is not None
        assert updated_ride.name == 'Updated Space Mountain'
        assert updated_ride.tier == 2
        # Other fields unchanged
        assert updated_ride.land_area == sample_ride_data['land_area']

    def test_update_nonexistent_ride(self, mysql_connection):
        """
        Update ride that doesn't exist.

        Given: Ride ID 999 doesn't exist
        When: update(999, {...}) is called
        Then: Return None
        """
        repo = RideRepository(mysql_connection)

        result = repo.update(999, {'name': 'Ghost Ride'})

        assert result is None


# ============================================================================
# Test Class: Query Operations
# ============================================================================

class TestRideRepositoryQueries:
    """
    Test query operations for RideRepository.

    Priority: P1 - Used for ride listings and filtering
    """

    def test_get_by_park_id_returns_park_rides(self, mysql_connection, sample_park_data):
        """
        get_by_park_id() should return all rides for a specific park.

        Given: Park 1 has 2 rides, Park 2 has 1 ride
        When: get_by_park_id(park1_id) is called
        Then: Return only Park 1's 2 rides
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        # Create 2 parks
        park1_id = insert_sample_park(mysql_connection, sample_park_data)

        park2_data = sample_park_data.copy()
        park2_data['queue_times_id'] = 102
        park2_data['name'] = 'Epcot'
        park2_id = insert_sample_park(mysql_connection, park2_data)

        # Create rides for Park 1
        ride1 = {
            'queue_times_id': 1001, 'park_id': park1_id, 'name': 'Space Mountain',
            'land_area': 'Tomorrowland', 'tier': 1, 'is_active': 1
        }
        ride2 = {
            'queue_times_id': 1002, 'park_id': park1_id, 'name': 'Big Thunder',
            'land_area': 'Frontierland', 'tier': 1, 'is_active': 1
        }
        insert_sample_ride(mysql_connection, ride1)
        insert_sample_ride(mysql_connection, ride2)

        # Create ride for Park 2
        ride3 = {
            'queue_times_id': 2001, 'park_id': park2_id, 'name': 'Test Track',
            'land_area': 'Future World', 'tier': 2, 'is_active': 1
        }
        insert_sample_ride(mysql_connection, ride3)

        repo = RideRepository(mysql_connection)
        rides = repo.get_by_park_id(park1_id)

        assert len(rides) == 2
        ride_names = [r.name for r in rides]
        assert 'Big Thunder' in ride_names  # Alphabetically first
        assert 'Space Mountain' in ride_names
        assert 'Test Track' not in ride_names

    def test_get_by_park_id_active_only(self, mysql_connection, sample_park_data):
        """
        get_by_park_id() with active_only=True should filter inactive rides.

        Given: Park has 2 active rides and 1 inactive ride
        When: get_by_park_id(park_id, active_only=True) is called
        Then: Return only 2 active rides
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create 2 active rides
        ride1 = {
            'queue_times_id': 1001, 'park_id': park_id, 'name': 'Active Ride 1',
            'land_area': 'Area 1', 'tier': 1, 'is_active': 1
        }
        ride2 = {
            'queue_times_id': 1002, 'park_id': park_id, 'name': 'Active Ride 2',
            'land_area': 'Area 2', 'tier': 2, 'is_active': 1
        }
        insert_sample_ride(mysql_connection, ride1)
        insert_sample_ride(mysql_connection, ride2)

        # Create 1 inactive ride
        ride3 = {
            'queue_times_id': 1003, 'park_id': park_id, 'name': 'Closed Ride',
            'land_area': 'Area 3', 'tier': 3, 'is_active': 0
        }
        insert_sample_ride(mysql_connection, ride3)

        repo = RideRepository(mysql_connection)
        rides = repo.get_by_park_id(park_id, active_only=True)

        assert len(rides) == 2
        ride_names = [r.name for r in rides]
        assert 'Active Ride 1' in ride_names
        assert 'Active Ride 2' in ride_names
        assert 'Closed Ride' not in ride_names

    def test_get_by_park_id_include_inactive(self, mysql_connection, sample_park_data):
        """
        get_by_park_id() with active_only=False should return all rides.

        Given: Park has 2 active rides and 1 inactive ride
        When: get_by_park_id(park_id, active_only=False) is called
        Then: Return all 3 rides
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create active and inactive rides
        for idx in range(3):
            ride = {
                'queue_times_id': 1001 + idx,
                'park_id': park_id,
                'name': f'Ride {idx}',
                'land_area': f'Area {idx}',
                'tier': 1,
                'is_active': 1 if idx < 2 else 0
            }
            insert_sample_ride(mysql_connection, ride)

        repo = RideRepository(mysql_connection)
        rides = repo.get_by_park_id(park_id, active_only=False)

        assert len(rides) == 3

    def test_get_all_active_returns_only_active_rides(self, mysql_connection, sample_park_data):
        """
        get_all_active() should return only active rides.

        Given: 2 active rides and 1 inactive ride
        When: get_all_active() is called
        Then: Return only the 2 active rides
        """
        from tests.conftest import insert_sample_park, insert_sample_ride

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create rides
        for idx in range(3):
            ride = {
                'queue_times_id': 1001 + idx,
                'park_id': park_id,
                'name': f'Ride {idx}',
                'land_area': f'Area {idx}',
                'tier': 1,
                'is_active': 1 if idx < 2 else 0
            }
            insert_sample_ride(mysql_connection, ride)

        repo = RideRepository(mysql_connection)
        rides = repo.get_all_active()

        assert len(rides) == 2
        assert all(r.is_active in (True, 1) for r in rides)

    def test_get_unclassified_rides(self, mysql_connection, sample_park_data):
        """
        get_unclassified_rides() should return rides without tier classification.

        Given: 2 rides without classification, 1 ride with classification
        When: get_unclassified_rides() is called
        Then: Return only the 2 unclassified rides
        """
        from tests.conftest import insert_sample_park, insert_sample_ride
        from sqlalchemy import text

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        # Create 3 rides
        ride1_id = insert_sample_ride(mysql_connection, {
            'queue_times_id': 1001, 'park_id': park_id, 'name': 'Unclassified 1',
            'land_area': 'Area 1', 'tier': None, 'is_active': 1
        })
        ride2_id = insert_sample_ride(mysql_connection, {
            'queue_times_id': 1002, 'park_id': park_id, 'name': 'Unclassified 2',
            'land_area': 'Area 2', 'tier': None, 'is_active': 1
        })
        ride3_id = insert_sample_ride(mysql_connection, {
            'queue_times_id': 1003, 'park_id': park_id, 'name': 'Classified',
            'land_area': 'Area 3', 'tier': 1, 'is_active': 1
        })

        # Add classification for ride3
        mysql_connection.execute(text("""
            INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score)
            VALUES (:ride_id, 1, 3, 'manual_override', 1.0)
        """), {'ride_id': ride3_id})

        repo = RideRepository(mysql_connection)
        rides = repo.get_unclassified_rides()

        assert len(rides) == 2
        ride_names = [r.name for r in rides]
        assert 'Unclassified 1' in ride_names
        assert 'Unclassified 2' in ride_names
        assert 'Classified' not in ride_names


# ============================================================================
# Test Class: Row to Ride Conversion
# ============================================================================

class TestRideRepositoryRowConversion:
    """
    Test _row_to_ride() conversion method.

    Priority: P2 - Internal method but critical for data integrity
    """

    def test_row_to_ride_conversion(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        _row_to_ride() should correctly convert database row to Ride object.

        Given: Ride record in database
        When: Fetched and converted
        Then: Ride object has all correct fields
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        created_ride = repo.create(sample_ride_data)

        # Verify all fields converted correctly
        assert isinstance(created_ride, Ride)
        assert created_ride.ride_id is not None
        assert created_ride.queue_times_id == sample_ride_data['queue_times_id']
        assert created_ride.park_id == park_id
        assert created_ride.name == sample_ride_data['name']
        assert created_ride.land_area == sample_ride_data['land_area']
        assert created_ride.tier == sample_ride_data['tier']
        assert created_ride.is_active in (True, 1)
        assert created_ride.created_at is not None
        assert created_ride.updated_at is not None


# ============================================================================
# Edge Cases & Error Handling
# ============================================================================

class TestRideRepositoryEdgeCases:
    """
    Test edge cases and error scenarios.

    Priority: P2 - Robustness
    """

    def test_create_ride_with_minimal_data(self, mysql_connection, sample_park_data):
        """
        Create ride with only required fields.

        Given: Minimal ride data
        When: create() is called
        Then: Ride created successfully with defaults
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        repo = RideRepository(mysql_connection)

        minimal_data = {
            'queue_times_id': 9999,
            'park_id': park_id,
            'name': 'Minimal Ride',
            'land_area': None,
            'tier': None,
            'is_active': True
        }

        ride = repo.create(minimal_data)

        assert ride is not None
        assert ride.name == 'Minimal Ride'
        assert ride.tier is None
        assert ride.land_area is None

    def test_update_only_tier(self, mysql_connection, sample_park_data, sample_ride_data):
        """
        Update ride with single field change (tier).

        Given: Ride exists
        When: update() with only 'tier' field
        Then: Only tier should change
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)
        sample_ride_data['park_id'] = park_id

        repo = RideRepository(mysql_connection)
        created_ride = repo.create(sample_ride_data)
        original_name = created_ride.name

        updated_ride = repo.update(created_ride.ride_id, {'tier': 3})

        assert updated_ride.tier == 3
        assert updated_ride.name == original_name  # Unchanged

    def test_get_by_park_id_empty_park(self, mysql_connection, sample_park_data):
        """
        get_by_park_id() should return empty list for park with no rides.

        Given: Park exists but has no rides
        When: get_by_park_id(park_id) is called
        Then: Return empty list
        """
        from tests.conftest import insert_sample_park

        park_id = insert_sample_park(mysql_connection, sample_park_data)

        repo = RideRepository(mysql_connection)
        rides = repo.get_by_park_id(park_id)

        assert rides == []
