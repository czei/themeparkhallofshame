"""
Theme Park Downtime Tracker - Schedule Repository Integration Tests

Tests the ScheduleRepository with MySQL database:
- Storing schedule entries
- Checking if park is open based on schedule
- Schedule refresh logic
- Fallback to heuristic when no schedule data

Priority: P1 - Critical for park open detection
"""

import pytest
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.repositories.schedule_repository import ScheduleRepository


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="module", autouse=True)
def cleanup_before_schedule_tests(mysql_engine):
    """Clean up schedule data before tests."""
    from sqlalchemy import text
    with mysql_engine.connect() as conn:
        conn.execute(text("DELETE FROM park_schedules"))
        conn.commit()
    yield


@pytest.fixture
def sample_park_id(mysql_session):
    """Create a sample park and return its ID."""
    from sqlalchemy import text

    # Check if park already exists
    result = mysql_session.execute(
        text("SELECT park_id FROM parks WHERE queue_times_id = 99901 LIMIT 1")
    )
    row = result.fetchone()
    if row:
        return row[0]

    # Create new park
    mysql_session.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, themeparks_wiki_id, name, city, state_province,
                          country, timezone, operator, is_disney, is_universal, is_active)
        VALUES (99901, 99901, 'test-uuid-12345', 'Test Schedule Park', 'Orlando', 'FL',
                'US', 'America/New_York', 'Test', 0, 0, 1)
    """))
    mysql_session.commit()

    result = mysql_session.execute(
        text("SELECT park_id FROM parks WHERE queue_times_id = 99901")
    )
    return result.fetchone()[0]


# ============================================================================
# Test Class: Store Schedule Entry
# ============================================================================

class TestScheduleRepositoryStore:
    """
    Test storing schedule entries.

    Priority: P1 - Foundation for schedule-based park detection
    """

    def test_store_schedule_entry_upsert(self, mysql_session, sample_park_id):
        """
        Store a schedule entry using upsert logic.

        Given: Valid schedule entry
        When: _store_schedule_entry() is called
        Then: Entry is stored in database
        """
        repo = ScheduleRepository(mysql_session)

        entry = {
            "date": "2025-12-15",
            "openingTime": "2025-12-15T09:00:00-05:00",
            "closingTime": "2025-12-15T22:00:00-05:00",
            "type": "OPERATING"
        }

        result = repo._store_schedule_entry(sample_park_id, entry, "America/New_York")

        assert result is True

        # Verify in database
        from sqlalchemy import text
        db_result = mysql_session.execute(text("""
            SELECT schedule_date, opening_time, closing_time, schedule_type
            FROM park_schedules
            WHERE park_id = :park_id AND schedule_date = '2025-12-15'
        """), {"park_id": sample_park_id})
        row = db_result.fetchone()

        assert row is not None
        assert row.schedule_type == "OPERATING"
        # Opening time should be converted to UTC (14:00 UTC)
        assert row.opening_time.hour == 14

    def test_store_schedule_entry_updates_existing(self, mysql_session, sample_park_id):
        """
        Storing duplicate schedule entry should update, not create duplicate.

        Given: Schedule entry already exists
        When: Same date/type entry is stored with different times
        Then: Existing entry is updated
        """
        repo = ScheduleRepository(mysql_session)

        # Store initial entry
        entry1 = {
            "date": "2025-12-16",
            "openingTime": "2025-12-16T09:00:00-05:00",
            "closingTime": "2025-12-16T20:00:00-05:00",
            "type": "OPERATING"
        }
        repo._store_schedule_entry(sample_park_id, entry1, "America/New_York")

        # Store updated entry (later closing time)
        entry2 = {
            "date": "2025-12-16",
            "openingTime": "2025-12-16T09:00:00-05:00",
            "closingTime": "2025-12-16T23:00:00-05:00",  # Changed
            "type": "OPERATING"
        }
        repo._store_schedule_entry(sample_park_id, entry2, "America/New_York")

        # Verify only one entry exists
        from sqlalchemy import text
        count_result = mysql_session.execute(text("""
            SELECT COUNT(*) as count
            FROM park_schedules
            WHERE park_id = :park_id AND schedule_date = '2025-12-16'
        """), {"park_id": sample_park_id})
        assert count_result.fetchone().count == 1

        # Verify closing time was updated (04:00 UTC = 23:00 EST next day)
        db_result = mysql_session.execute(text("""
            SELECT closing_time FROM park_schedules
            WHERE park_id = :park_id AND schedule_date = '2025-12-16'
        """), {"park_id": sample_park_id})
        row = db_result.fetchone()
        assert row.closing_time.hour == 4  # 23:00 EST = 04:00 UTC next day


# ============================================================================
# Test Class: Is Park Open Now
# ============================================================================

class TestScheduleRepositoryIsOpen:
    """
    Test is_park_open_now() schedule checking.

    Priority: P1 - Core functionality for park open detection
    """

    def test_is_park_open_within_hours(self, mysql_session, sample_park_id):
        """
        Park should be open when current time is within schedule.

        Given: Schedule for today 9am-10pm EST
        When: Current time is 2pm EST (19:00 UTC)
        Then: is_park_open_now() returns True
        """
        repo = ScheduleRepository(mysql_session)

        # Insert schedule for today
        today = date.today()
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time, schedule_type)
            VALUES (:park_id, :schedule_date, :opening, :closing, 'OPERATING')
            ON DUPLICATE KEY UPDATE opening_time = VALUES(opening_time), closing_time = VALUES(closing_time)
        """), {
            "park_id": sample_park_id,
            "schedule_date": today,
            "opening": datetime.combine(today, datetime.min.time().replace(hour=14)),  # 9am EST = 14:00 UTC
            "closing": datetime.combine(today, datetime.min.time().replace(hour=3)) + timedelta(days=1)  # 10pm EST = 03:00 UTC next day
        })
        mysql_session.commit()

        # Check at 2pm EST = 19:00 UTC
        test_time = datetime.combine(today, datetime.min.time().replace(hour=19))
        result = repo.is_park_open_now(sample_park_id, now_utc=test_time)

        assert result is True

    def test_is_park_closed_before_hours(self, mysql_session, sample_park_id):
        """
        Park should be closed when current time is before opening.

        Given: Schedule for today 9am-10pm EST
        When: Current time is 7am EST (12:00 UTC)
        Then: is_park_open_now() returns False
        """
        repo = ScheduleRepository(mysql_session)

        today = date.today()
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time, schedule_type)
            VALUES (:park_id, :schedule_date, :opening, :closing, 'OPERATING')
            ON DUPLICATE KEY UPDATE opening_time = VALUES(opening_time), closing_time = VALUES(closing_time)
        """), {
            "park_id": sample_park_id,
            "schedule_date": today,
            "opening": datetime.combine(today, datetime.min.time().replace(hour=14)),  # 9am EST = 14:00 UTC
            "closing": datetime.combine(today, datetime.min.time().replace(hour=3)) + timedelta(days=1)
        })
        mysql_session.commit()

        # Check at 7am EST = 12:00 UTC (before opening)
        test_time = datetime.combine(today, datetime.min.time().replace(hour=12))
        result = repo.is_park_open_now(sample_park_id, now_utc=test_time)

        assert result is False

    def test_is_park_closed_no_schedule(self, mysql_session):
        """
        Park with no schedule should return False.

        Given: No schedule data for park
        When: is_park_open_now() is called
        Then: Returns False
        """
        repo = ScheduleRepository(mysql_session)

        # Use a non-existent park ID
        result = repo.is_park_open_now(999999)

        assert result is False


# ============================================================================
# Test Class: Has Recent Schedule
# ============================================================================

class TestScheduleRepositoryHasRecent:
    """
    Test has_recent_schedule() checking.

    Priority: P2 - Used for fallback logic
    """

    def test_has_recent_schedule_true(self, mysql_session, sample_park_id):
        """
        Should return True when schedule was fetched recently.

        Given: Schedule fetched within last 24 hours
        When: has_recent_schedule(park_id) is called
        Then: Returns True
        """
        repo = ScheduleRepository(mysql_session)

        # Insert schedule with recent fetched_at
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time,
                                       schedule_type, fetched_at)
            VALUES (:park_id, CURDATE() + INTERVAL 7 DAY, NOW(), NOW() + INTERVAL 12 HOUR,
                    'OPERATING', NOW())
            ON DUPLICATE KEY UPDATE fetched_at = NOW()
        """), {"park_id": sample_park_id})
        mysql_session.commit()

        result = repo.has_recent_schedule(sample_park_id, max_age_hours=24)

        assert result is True

    def test_has_recent_schedule_false_old_data(self, mysql_session):
        """
        Should return False when schedule is stale.

        Given: No schedule fetched in last 24 hours
        When: has_recent_schedule(park_id) is called
        Then: Returns False
        """
        repo = ScheduleRepository(mysql_session)

        # Create a separate test park for this test to avoid interference
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT IGNORE INTO parks (park_id, queue_times_id, name, city, country, timezone, is_active, is_disney, is_universal)
            VALUES (99902, 99902, 'Old Schedule Park', 'Test', 'US', 'America/New_York', 1, 0, 0)
        """))
        mysql_session.commit()
        result = mysql_session.execute(
            text("SELECT park_id FROM parks WHERE queue_times_id = 99902")
        )
        old_park_id = result.fetchone()[0]

        # Insert schedule with old fetched_at (48 hours ago)
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time,
                                       schedule_type, fetched_at)
            VALUES (:park_id, CURDATE() + INTERVAL 14 DAY, NOW(), NOW() + INTERVAL 12 HOUR,
                    'OPERATING', NOW() - INTERVAL 48 HOUR)
            ON DUPLICATE KEY UPDATE fetched_at = NOW() - INTERVAL 48 HOUR
        """), {"park_id": old_park_id})
        mysql_session.commit()

        result = repo.has_recent_schedule(old_park_id, max_age_hours=24)

        assert result is False


# ============================================================================
# Test Class: Get Schedule For Date
# ============================================================================

class TestScheduleRepositoryGetForDate:
    """
    Test get_schedule_for_date() retrieval.

    Priority: P2 - Used for displaying park hours
    """

    def test_get_schedule_for_date_exists(self, mysql_session, sample_park_id):
        """
        Should return schedule when it exists.

        Given: Schedule exists for the date
        When: get_schedule_for_date() is called
        Then: Returns schedule dict
        """
        repo = ScheduleRepository(mysql_session)

        target_date = date.today() + timedelta(days=5)

        # Insert schedule
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time, schedule_type)
            VALUES (:park_id, :schedule_date, NOW(), NOW() + INTERVAL 12 HOUR, 'OPERATING')
            ON DUPLICATE KEY UPDATE opening_time = NOW()
        """), {"park_id": sample_park_id, "schedule_date": target_date})
        mysql_session.commit()

        result = repo.get_schedule_for_date(sample_park_id, target_date)

        assert result is not None
        assert result["park_id"] == sample_park_id
        assert result["schedule_type"] == "OPERATING"

    def test_get_schedule_for_date_not_exists(self, mysql_session, sample_park_id):
        """
        Should return None when schedule doesn't exist.

        Given: No schedule for the date
        When: get_schedule_for_date() is called
        Then: Returns None
        """
        repo = ScheduleRepository(mysql_session)

        # Far future date unlikely to have schedule
        future_date = date.today() + timedelta(days=365)

        result = repo.get_schedule_for_date(sample_park_id, future_date)

        assert result is None


# ============================================================================
# Test Class: Fetch and Store from API (with mocking)
# ============================================================================

class TestScheduleRepositoryFetchFromAPI:
    """
    Test fetch_and_store_schedule() with mocked API.

    Priority: P1 - Tests API integration logic
    """

    @patch('database.repositories.schedule_repository.get_themeparks_wiki_client')
    def test_fetch_and_store_schedule_success(self, mock_get_client, mysql_session, sample_park_id):
        """
        Should fetch and store schedule from API.

        Given: API returns schedule data
        When: fetch_and_store_schedule() is called
        Then: Schedule entries are stored
        """
        # Mock API response
        mock_client = MagicMock()
        mock_client.get_entity_schedule.return_value = {
            "timezone": "America/New_York",
            "schedule": [
                {
                    "date": "2025-12-20",
                    "openingTime": "2025-12-20T09:00:00-05:00",
                    "closingTime": "2025-12-20T22:00:00-05:00",
                    "type": "OPERATING"
                },
                {
                    "date": "2025-12-21",
                    "openingTime": "2025-12-21T08:00:00-05:00",
                    "closingTime": "2025-12-21T23:00:00-05:00",
                    "type": "OPERATING"
                }
            ]
        }
        mock_get_client.return_value = mock_client

        repo = ScheduleRepository(mysql_session)

        count = repo.fetch_and_store_schedule(sample_park_id, "test-wiki-id")

        assert count == 2
        mock_client.get_entity_schedule.assert_called_once_with("test-wiki-id")

    @patch('database.repositories.schedule_repository.get_themeparks_wiki_client')
    def test_fetch_and_store_schedule_empty_response(self, mock_get_client, mysql_session, sample_park_id):
        """
        Should handle empty schedule response.

        Given: API returns empty schedule
        When: fetch_and_store_schedule() is called
        Then: Returns 0, no entries stored
        """
        mock_client = MagicMock()
        mock_client.get_entity_schedule.return_value = {
            "timezone": "America/New_York",
            "schedule": []
        }
        mock_get_client.return_value = mock_client

        repo = ScheduleRepository(mysql_session)

        count = repo.fetch_and_store_schedule(sample_park_id, "test-wiki-id")

        assert count == 0


# ============================================================================
# Test Class: Cleanup Old Schedules
# ============================================================================

class TestScheduleRepositoryCleanup:
    """
    Test cleanup_old_schedules() maintenance.

    Priority: P3 - Database maintenance
    """

    def test_cleanup_old_schedules(self, mysql_session, sample_park_id):
        """
        Should delete schedules older than specified days.

        Given: Old schedule entries exist
        When: cleanup_old_schedules(days_to_keep=7) is called
        Then: Old entries are deleted
        """
        repo = ScheduleRepository(mysql_session)

        # Insert old schedule (10 days ago)
        from sqlalchemy import text
        mysql_session.execute(text("""
            INSERT INTO park_schedules (park_id, schedule_date, opening_time, closing_time, schedule_type)
            VALUES (:park_id, CURDATE() - INTERVAL 10 DAY, NOW(), NOW() + INTERVAL 12 HOUR, 'OPERATING')
            ON DUPLICATE KEY UPDATE opening_time = NOW()
        """), {"park_id": sample_park_id})
        mysql_session.commit()

        deleted = repo.cleanup_old_schedules(days_to_keep=7)

        assert deleted >= 1
