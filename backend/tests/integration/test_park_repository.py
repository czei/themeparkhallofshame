"""
Theme Park Downtime Tracker - Park Repository Unit Tests

Tests the ParkRepository with in-memory SQLite database:
- CRUD operations (create, get_by_id, update)
- Query operations (get_all_active, get_disney_universal_parks)
- Rankings by downtime (FR-010) - daily/weekly/monthly/yearly
- Rankings by weighted downtime (FR-024) with tier weighting

Priority: P1 - CRITICAL (Complex SQL queries with rankings and aggregations)
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.park_repository import ParkRepository
from models.park import Park


# ============================================================================
# FIXTURES - Cleanup
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def cleanup_before_park_repository_tests(mysql_engine):
    """Clean up all test data once at start of this test module."""
    from sqlalchemy import text
    with mysql_engine.connect() as conn:
        conn.execute(text("DELETE FROM ride_status_snapshots"))
        conn.execute(text("DELETE FROM ride_status_changes"))
        conn.execute(text("DELETE FROM park_activity_snapshots"))
        conn.execute(text("DELETE FROM ride_daily_stats"))
        conn.execute(text("DELETE FROM ride_weekly_stats"))
        conn.execute(text("DELETE FROM ride_monthly_stats"))
        conn.execute(text("DELETE FROM park_daily_stats"))
        conn.execute(text("DELETE FROM park_weekly_stats"))
        conn.execute(text("DELETE FROM park_monthly_stats"))
        conn.execute(text("DELETE FROM ride_classifications"))
        conn.execute(text("DELETE FROM rides"))
        conn.execute(text("DELETE FROM parks"))
        conn.commit()
    yield


# ============================================================================
# Test Class: CRUD Operations
# ============================================================================

class TestParkRepositoryCRUD:
    """
    Test basic CRUD operations for ParkRepository.

    Priority: P0 - Foundation for all other operations
    """

    def test_create_park(self, mysql_connection, sample_park_data):
        """
        Create a new park record.

        Given: Valid park data
        When: create() is called
        Then: Return Park object with assigned park_id
        """
        repo = ParkRepository(mysql_connection)

        park = repo.create(sample_park_data)

        assert park is not None
        assert park.park_id is not None
        assert park.name == sample_park_data['name']
        assert park.queue_times_id == sample_park_data['queue_times_id']
        assert park.is_disney == sample_park_data['is_disney']

    def test_get_by_id_existing_park(self, mysql_connection, sample_park_data):
        """
        Fetch park by ID.

        Given: Park exists in database
        When: get_by_id() is called
        Then: Return Park object
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)

        fetched_park = repo.get_by_id(created_park.park_id)

        assert fetched_park is not None
        assert fetched_park.park_id == created_park.park_id
        assert fetched_park.name == created_park.name

    def test_get_by_id_nonexistent_park(self, mysql_connection):
        """
        Fetch park by ID when park doesn't exist.

        Given: Park ID 999 doesn't exist
        When: get_by_id(999) is called
        Then: Return None
        """
        repo = ParkRepository(mysql_connection)

        park = repo.get_by_id(999)

        assert park is None

    def test_get_by_queue_times_id(self, mysql_connection, sample_park_data):
        """
        Fetch park by Queue-Times.com external ID.

        Given: Park exists with queue_times_id=601
        When: get_by_queue_times_id(601) is called
        Then: Return Park object
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)

        fetched_park = repo.get_by_queue_times_id(sample_park_data['queue_times_id'])

        assert fetched_park is not None
        assert fetched_park.queue_times_id == sample_park_data['queue_times_id']
        assert fetched_park.park_id == created_park.park_id

    def test_update_park(self, mysql_connection, sample_park_data):
        """
        Update existing park record.

        Given: Park exists
        When: update() is called with new data
        Then: Return updated Park object
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)

        update_data = {'name': 'Updated Magic Kingdom', 'city': 'Lake Buena Vista'}
        updated_park = repo.update(created_park.park_id, update_data)

        assert updated_park is not None
        assert updated_park.name == 'Updated Magic Kingdom'
        assert updated_park.city == 'Lake Buena Vista'
        # Other fields unchanged
        assert updated_park.state_province == sample_park_data['state_province']

    def test_update_nonexistent_park(self, mysql_connection):
        """
        Update park that doesn't exist.

        Given: Park ID 999 doesn't exist
        When: update(999, {...}) is called
        Then: Return None
        """
        repo = ParkRepository(mysql_connection)

        result = repo.update(999, {'name': 'Ghost Park'})

        assert result is None

    def test_update_with_no_fields(self, mysql_connection, sample_park_data):
        """
        Update park with empty data dictionary.

        Given: Park exists
        When: update() is called with empty dict
        Then: Return unchanged Park object
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)

        result = repo.update(created_park.park_id, {})

        assert result is not None
        assert result.name == created_park.name


# ============================================================================
# Test Class: Query Operations
# ============================================================================

class TestParkRepositoryQueries:
    """
    Test query operations for ParkRepository.

    Priority: P1 - Used for park listings
    """

    def test_get_all_active_returns_only_active_parks(self, mysql_connection):
        """
        get_all_active() should return only active parks.

        Given: 2 active parks and 1 inactive park
        When: get_all_active() is called
        Then: Return only the 2 active parks
        """
        from tests.conftest import insert_sample_park

        repo = ParkRepository(mysql_connection)

        # Create 2 active parks
        park1_data = {
            'queue_times_id': 601, 'name': 'Magic Kingdom', 'city': 'Orlando',
            'state_province': 'FL', 'country': 'US', 'latitude': 28.385,
            'longitude': -81.563, 'timezone': 'America/New_York',
            'operator': 'Disney', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        }
        park2_data = {
            'queue_times_id': 102, 'name': 'Epcot', 'city': 'Orlando',
            'state_province': 'FL', 'country': 'US', 'latitude': 28.374,
            'longitude': -81.549, 'timezone': 'America/New_York',
            'operator': 'Disney', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        }
        insert_sample_park(mysql_connection, park1_data)
        insert_sample_park(mysql_connection, park2_data)

        # Create 1 inactive park
        park3_data = park1_data.copy()
        park3_data['queue_times_id'] = 103
        park3_data['name'] = 'Closed Park'
        park3_data['is_active'] = 0
        insert_sample_park(mysql_connection, park3_data)

        parks = repo.get_all_active()

        assert len(parks) == 2
        assert all(park.is_active for park in parks)
        park_names = [p.name for p in parks]
        assert 'Epcot' in park_names
        assert 'Magic Kingdom' in park_names
        assert 'Closed Park' not in park_names

    def test_get_all_active_ordered_by_name(self, mysql_connection):
        """
        get_all_active() should return parks ordered by name.

        Given: Parks 'Zion', 'Atlantis', 'Midway'
        When: get_all_active() is called
        Then: Return in alphabetical order
        """
        from tests.conftest import insert_sample_park

        repo = ParkRepository(mysql_connection)

        for idx, name in enumerate(['Zion Park', 'Atlantis Park', 'Midway Park'], start=101):
            park_data = {
                'queue_times_id': idx, 'name': name, 'city': 'Test',
                'state_province': 'TX', 'country': 'US', 'latitude': 30.0,
                'longitude': -95.0, 'timezone': 'America/Chicago',
                'operator': 'Independent', 'is_disney': 0, 'is_universal': 0, 'is_active': 1
            }
            insert_sample_park(mysql_connection, park_data)

        parks = repo.get_all_active()

        park_names = [p.name for p in parks]
        assert park_names == ['Atlantis Park', 'Midway Park', 'Zion Park']

    def test_get_disney_universal_parks(self, mysql_connection):
        """
        get_disney_universal_parks() should return only Disney/Universal parks.

        Given: 2 Disney parks, 1 Universal park, 1 Independent park
        When: get_disney_universal_parks() is called
        Then: Return only Disney and Universal parks (3 total)
        """
        from tests.conftest import insert_sample_park

        repo = ParkRepository(mysql_connection)

        # Disney parks
        disney1 = {
            'queue_times_id': 601, 'name': 'Magic Kingdom', 'city': 'Orlando',
            'state_province': 'FL', 'country': 'US', 'latitude': 28.385,
            'longitude': -81.563, 'timezone': 'America/New_York',
            'operator': 'Disney', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        }
        disney2 = {
            'queue_times_id': 102, 'name': 'Disneyland', 'city': 'Anaheim',
            'state_province': 'CA', 'country': 'US', 'latitude': 33.812,
            'longitude': -117.919, 'timezone': 'America/Los_Angeles',
            'operator': 'Disney', 'is_disney': 1, 'is_universal': 0, 'is_active': 1
        }

        # Universal park
        universal1 = {
            'queue_times_id': 201, 'name': 'Universal Studios Florida', 'city': 'Orlando',
            'state_province': 'FL', 'country': 'US', 'latitude': 28.475,
            'longitude': -81.467, 'timezone': 'America/New_York',
            'operator': 'Universal', 'is_disney': 0, 'is_universal': 1, 'is_active': 1
        }

        # Independent park
        independent = {
            'queue_times_id': 301, 'name': 'Six Flags', 'city': 'Arlington',
            'state_province': 'TX', 'country': 'US', 'latitude': 32.755,
            'longitude': -97.070, 'timezone': 'America/Chicago',
            'operator': 'Six Flags', 'is_disney': 0, 'is_universal': 0, 'is_active': 1
        }

        insert_sample_park(mysql_connection, disney1)
        insert_sample_park(mysql_connection, disney2)
        insert_sample_park(mysql_connection, universal1)
        insert_sample_park(mysql_connection, independent)

        parks = repo.get_disney_universal_parks()

        assert len(parks) == 3
        park_names = [p.name for p in parks]
        assert 'Magic Kingdom' in park_names
        assert 'Disneyland' in park_names
        assert 'Universal Studios Florida' in park_names
        assert 'Six Flags' not in park_names


# ============================================================================
# Test Class: Row to Park Conversion
# ============================================================================

class TestParkRepositoryRowConversion:
    """
    Test _row_to_park() conversion method.

    Priority: P2 - Internal method but critical for data integrity
    """

    def test_row_to_park_conversion(self, mysql_connection, sample_park_data):
        """
        _row_to_park() should correctly convert database row to Park object.

        Given: Park record in database
        When: Fetched and converted
        Then: Park object has all correct fields
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)

        # Verify all fields converted correctly
        assert isinstance(created_park, Park)
        assert created_park.park_id is not None
        assert created_park.queue_times_id == sample_park_data['queue_times_id']
        assert created_park.name == sample_park_data['name']
        assert created_park.city == sample_park_data['city']
        assert created_park.state_province == sample_park_data['state_province']
        assert created_park.country == sample_park_data['country']
        assert created_park.latitude == sample_park_data['latitude']
        assert created_park.longitude == sample_park_data['longitude']
        assert created_park.timezone == sample_park_data['timezone']
        assert created_park.operator == sample_park_data['operator']
        assert created_park.is_disney == sample_park_data['is_disney']
        assert created_park.is_universal == sample_park_data['is_universal']
        # SQLite returns integers for booleans (0/1 instead of True/False)
        assert created_park.is_active in (True, 1)
        assert created_park.created_at is not None
        assert created_park.updated_at is not None


# ============================================================================
# Test Class: Rankings by Downtime (FR-010)
# ============================================================================

class TestParkRankingsByDowntime:
    """
    Test get_rankings_by_downtime() for different periods.

    Priority: P1 - CRITICAL (Core feature FR-010)

    Note: These tests require MySQL-specific SQL functions and stats tables.
    Full integration tests with MySQL database are in tests/integration/.
    """

    def test_get_rankings_daily_period(self, mysql_connection):
        """
        get_rankings_by_downtime('daily') should call _get_daily_rankings.

        Given: Valid database connection
        When: get_rankings_by_downtime('daily') is called
        Then: Execute without error (integration tests verify full data flow)
        """
        repo = ParkRepository(mysql_connection)

        # Should not raise ValueError
        result = repo.get_rankings_by_downtime(period='daily', limit=10)
        assert isinstance(result, list)

    def test_get_rankings_weekly_period(self, mysql_connection):
        """
        get_rankings_by_downtime('weekly') should call _get_weekly_rankings.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_downtime(period='weekly', limit=10)
        assert isinstance(result, list)

    def test_get_rankings_monthly_period(self, mysql_connection):
        """
        get_rankings_by_downtime('monthly') should call _get_monthly_rankings.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_downtime(period='monthly', limit=10)
        assert isinstance(result, list)

    def test_get_rankings_yearly_period(self, mysql_connection):
        """
        get_rankings_by_downtime('yearly') should call _get_yearly_rankings.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_downtime(period='yearly', limit=10)
        assert isinstance(result, list)

    def test_get_rankings_invalid_period_raises_error(self, mysql_connection):
        """
        get_rankings_by_downtime() should raise ValueError for invalid period.

        Given: Invalid period 'invalid'
        When: get_rankings_by_downtime('invalid') is called
        Then: Raise ValueError
        """
        repo = ParkRepository(mysql_connection)

        with pytest.raises(ValueError) as exc_info:
            repo.get_rankings_by_downtime(period='invalid')

        assert 'Invalid period' in str(exc_info.value)


# ============================================================================
# Test Class: Rankings by Weighted Downtime (FR-024)
# ============================================================================

class TestParkRankingsByWeightedDowntime:
    """
    Test get_rankings_by_weighted_downtime() with tier weighting.

    Priority: P1 - CRITICAL (Core feature FR-024)

    Note: These tests require MySQL-specific SQL functions and stats tables.
    Full integration tests with MySQL database are in tests/integration/.

    Weighted scoring:
    - Tier 1 rides: 3x weight
    - Tier 2 rides: 2x weight
    - Tier 3 rides: 1x weight
    """

    def test_get_weighted_rankings_weekly_period(self, mysql_connection):
        """
        get_rankings_by_weighted_downtime('weekly') should execute successfully.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_weighted_downtime(period='weekly', limit=10)
        assert isinstance(result, list)

    def test_get_weighted_rankings_monthly_period(self, mysql_connection):
        """
        get_rankings_by_weighted_downtime('monthly') should execute successfully.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_weighted_downtime(period='monthly', limit=10)
        assert isinstance(result, list)

    def test_get_weighted_rankings_yearly_period(self, mysql_connection):
        """
        get_rankings_by_weighted_downtime('yearly') should execute successfully.
        """
        repo = ParkRepository(mysql_connection)

        result = repo.get_rankings_by_weighted_downtime(period='yearly', limit=10)
        assert isinstance(result, list)

    def test_get_weighted_rankings_invalid_period_raises_error(self, mysql_connection):
        """
        get_rankings_by_weighted_downtime() should raise ValueError for invalid period.

        Given: Invalid period 'daily' (not meaningful for weighted scores)
        When: get_rankings_by_weighted_downtime('daily') is called
        Then: Raise ValueError
        """
        repo = ParkRepository(mysql_connection)

        with pytest.raises(ValueError) as exc_info:
            repo.get_rankings_by_weighted_downtime(period='daily')

        assert 'Invalid period for weighted rankings' in str(exc_info.value)


# ============================================================================
# Edge Cases & Error Handling
# ============================================================================

class TestParkRepositoryEdgeCases:
    """
    Test edge cases and error scenarios.

    Priority: P2 - Robustness
    """

    def test_create_park_with_minimal_data(self, mysql_connection):
        """
        Create park with only required fields.

        Given: Minimal park data
        When: create() is called
        Then: Park created successfully with defaults
        """
        repo = ParkRepository(mysql_connection)

        minimal_data = {
            'queue_times_id': 999,
            'name': 'Minimal Park',
            'city': 'Test City',
            'state_province': '',
            'country': 'US',
            'latitude': None,
            'longitude': None,
            'timezone': 'UTC',
            'operator': None,
            'is_disney': False,
            'is_universal': False
        }

        park = repo.create(minimal_data)

        assert park is not None
        assert park.name == 'Minimal Park'
        assert park.operator is None

    def test_update_only_one_field(self, mysql_connection, sample_park_data):
        """
        Update park with single field change.

        Given: Park exists
        When: update() with only 'city' field
        Then: Only city should change
        """
        repo = ParkRepository(mysql_connection)
        created_park = repo.create(sample_park_data)
        original_name = created_park.name

        updated_park = repo.update(created_park.park_id, {'city': 'New City'})

        assert updated_park.city == 'New City'
        assert updated_park.name == original_name  # Unchanged
