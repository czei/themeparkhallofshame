"""
Integration Tests: Queue Data Collection (Feature 004)

Verifies that extended queue data (Lightning Lane, Virtual Queue,
Single Rider, etc.) is collected and stored correctly.

Feature: 004-themeparks-data-collection
Task: T039
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import text
from unittest.mock import MagicMock

# These imports are used for the unit-style tests within integration context
import sys
from pathlib import Path

backend_src = Path(__file__).parent.parent.parent / 'src'
if str(backend_src.absolute()) not in sys.path:
    sys.path.insert(0, str(backend_src.absolute()))

from collector.queue_data_collector import QueueDataCollector
from collector.themeparks_wiki_client import QueueData as ClientQueueData, LiveRideData


class TestQueueDataStorage:
    """
    Integration tests for storing queue data to the database.
    """

    @pytest.fixture
    def setup_test_park(self, mysql_session):
        """Create test park."""
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, is_active)
            VALUES (99003, 'Queue Data Test Park', 'Test City', 'CA', 'US', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99003"
        ))
        return result.scalar()

    @pytest.fixture
    def setup_test_ride(self, mysql_session, setup_test_park):
        """Create test ride."""
        park_id = setup_test_park

        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990003, :park_id, 'Queue Data Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990003"
        ))
        return result.scalar()

    @pytest.fixture
    def setup_test_snapshot(self, mysql_session, setup_test_ride):
        """Create test snapshot for queue data attachment."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': now})
        mysql_session.flush()

        result = mysql_session.execute(text("""
            SELECT snapshot_id FROM ride_status_snapshots
            WHERE ride_id = :ride_id ORDER BY snapshot_id DESC LIMIT 1
        """), {'ride_id': ride_id})
        snapshot_id = result.scalar()

        return {'snapshot_id': snapshot_id, 'recorded_at': now}

    def test_single_rider_queue_storage(self, mysql_session, setup_test_snapshot):
        """SINGLE_RIDER queue data is stored correctly."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Insert single rider queue data
        mysql_session.execute(text("""
            INSERT INTO queue_data
            (snapshot_id, queue_type, wait_time_minutes, recorded_at)
            VALUES (:snapshot_id, 'SINGLE_RIDER', 15, :recorded_at)
        """), {'snapshot_id': snapshot_id, 'recorded_at': recorded_at})
        mysql_session.flush()

        # Verify the data
        result = mysql_session.execute(text("""
            SELECT queue_type, wait_time_minutes
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == 'SINGLE_RIDER'
        assert row[1] == 15

    def test_paid_return_time_with_pricing(self, mysql_session, setup_test_snapshot):
        """PAID_RETURN_TIME (Lightning Lane) stores pricing info."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']
        return_start = recorded_at + timedelta(hours=2)
        return_end = recorded_at + timedelta(hours=3)

        # Insert Lightning Lane queue data with pricing
        mysql_session.execute(text("""
            INSERT INTO queue_data
            (snapshot_id, queue_type, wait_time_minutes, return_time_start, return_time_end,
             price_amount, price_currency, recorded_at)
            VALUES (:snapshot_id, 'PAID_RETURN_TIME', NULL, :return_start, :return_end,
                    25.00, 'USD', :recorded_at)
        """), {
            'snapshot_id': snapshot_id,
            'recorded_at': recorded_at,
            'return_start': return_start,
            'return_end': return_end
        })
        mysql_session.flush()

        # Verify the data
        result = mysql_session.execute(text("""
            SELECT queue_type, price_amount, price_currency, return_time_start, return_time_end
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == 'PAID_RETURN_TIME'
        assert float(row[1]) == 25.00
        assert row[2] == 'USD'
        assert row[3] is not None  # return_time_start
        assert row[4] is not None  # return_time_end

    def test_boarding_group_queue(self, mysql_session, setup_test_snapshot):
        """BOARDING_GROUP (Virtual Queue) stores group status."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Insert boarding group data
        mysql_session.execute(text("""
            INSERT INTO queue_data
            (snapshot_id, queue_type, boarding_group_status, boarding_group_current, recorded_at)
            VALUES (:snapshot_id, 'BOARDING_GROUP', 'DISTRIBUTING', 'Group 42', :recorded_at)
        """), {'snapshot_id': snapshot_id, 'recorded_at': recorded_at})
        mysql_session.flush()

        # Verify the data
        result = mysql_session.execute(text("""
            SELECT queue_type, boarding_group_status, boarding_group_current
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == 'BOARDING_GROUP'
        assert row[1] == 'DISTRIBUTING'
        assert row[2] == 'Group 42'

    def test_return_time_window(self, mysql_session, setup_test_snapshot):
        """RETURN_TIME stores return window correctly."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']
        return_start = datetime(2025, 1, 15, 14, 0, 0)
        return_end = datetime(2025, 1, 15, 15, 0, 0)

        # Insert return time data
        mysql_session.execute(text("""
            INSERT INTO queue_data
            (snapshot_id, queue_type, return_time_start, return_time_end, recorded_at)
            VALUES (:snapshot_id, 'RETURN_TIME', :return_start, :return_end, :recorded_at)
        """), {
            'snapshot_id': snapshot_id,
            'recorded_at': recorded_at,
            'return_start': return_start,
            'return_end': return_end
        })
        mysql_session.flush()

        # Verify the data
        result = mysql_session.execute(text("""
            SELECT return_time_start, return_time_end
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row[0] == return_start
        assert row[1] == return_end

    def test_multiple_queue_types_per_snapshot(self, mysql_session, setup_test_snapshot):
        """A snapshot can have multiple queue types (e.g., SINGLE_RIDER + PAID_RETURN_TIME)."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Insert multiple queue types for same snapshot
        mysql_session.execute(text("""
            INSERT INTO queue_data
            (snapshot_id, queue_type, wait_time_minutes, recorded_at)
            VALUES
            (:snapshot_id, 'SINGLE_RIDER', 10, :recorded_at),
            (:snapshot_id, 'PAID_RETURN_TIME', NULL, :recorded_at)
        """), {'snapshot_id': snapshot_id, 'recorded_at': recorded_at})
        mysql_session.flush()

        # Verify count
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        count = result.scalar()

        assert count == 2


class TestQueueDataCollectorIntegration:
    """
    Integration tests for QueueDataCollector class with real database.
    """

    @pytest.fixture
    def setup_test_park(self, mysql_session):
        """Create test park."""
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, is_active)
            VALUES (99004, 'Collector Test Park', 'Test City', 'CA', 'US', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99004"
        ))
        return result.scalar()

    @pytest.fixture
    def setup_test_ride(self, mysql_session, setup_test_park):
        """Create test ride."""
        park_id = setup_test_park

        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990004, :park_id, 'Collector Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990004"
        ))
        return result.scalar()

    @pytest.fixture
    def setup_test_snapshot(self, mysql_session, setup_test_ride):
        """Create test snapshot."""
        ride_id = setup_test_ride
        now = datetime.utcnow()

        mysql_session.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
            VALUES (:ride_id, :recorded_at, 30, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
        """), {'ride_id': ride_id, 'recorded_at': now})
        mysql_session.flush()

        result = mysql_session.execute(text("""
            SELECT snapshot_id FROM ride_status_snapshots
            WHERE ride_id = :ride_id ORDER BY snapshot_id DESC LIMIT 1
        """), {'ride_id': ride_id})
        snapshot_id = result.scalar()

        return {'snapshot_id': snapshot_id, 'recorded_at': now}

    def test_collector_saves_single_rider(self, mysql_session, setup_test_snapshot):
        """QueueDataCollector correctly saves SINGLE_RIDER queue data."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Create collector
        collector = QueueDataCollector(mysql_session)

        # Create mock ride data with SINGLE_RIDER queue
        ride_data = LiveRideData(
            entity_id="test-entity-001",
            name="Test Ride",
            entity_type="ATTRACTION",
            status="OPERATING",
            wait_time=30,
            operating_hours=None,
            last_updated=None,
            queues=[
                ClientQueueData(
                    queue_type='SINGLE_RIDER',
                    wait_time=15
                )
            ]
        )

        # Save queue data
        records = collector.save_queue_data(snapshot_id, ride_data, recorded_at)
        mysql_session.flush()  # Ensure records are written before query

        # Verify record was created
        assert len(records) == 1

        # Verify in database
        result = mysql_session.execute(text("""
            SELECT queue_type, wait_time_minutes
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == 'SINGLE_RIDER'
        assert row[1] == 15

    def test_collector_skips_standby(self, mysql_session, setup_test_snapshot):
        """QueueDataCollector skips STANDBY queue (already in snapshot)."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Create collector
        collector = QueueDataCollector(mysql_session)

        # Create mock ride data with STANDBY queue (should be skipped)
        ride_data = LiveRideData(
            entity_id="test-entity-002",
            name="Test Ride",
            entity_type="ATTRACTION",
            status="OPERATING",
            wait_time=30,
            operating_hours=None,
            last_updated=None,
            queues=[
                ClientQueueData(
                    queue_type='STANDBY',
                    wait_time=30
                )
            ]
        )

        # Save queue data
        records = collector.save_queue_data(snapshot_id, ride_data, recorded_at)

        # Verify no records created (STANDBY is skipped)
        assert len(records) == 0

    def test_collector_handles_paid_queue(self, mysql_session, setup_test_snapshot):
        """QueueDataCollector saves PAID_RETURN_TIME with pricing."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Create collector
        collector = QueueDataCollector(mysql_session)

        # Create mock ride data with paid queue
        ride_data = LiveRideData(
            entity_id="test-entity-003",
            name="Test Ride",
            entity_type="ATTRACTION",
            status="OPERATING",
            wait_time=30,
            operating_hours=None,
            last_updated=None,
            queues=[
                ClientQueueData(
                    queue_type='PAID_RETURN_TIME',
                    wait_time=None,
                    return_start='2025-01-15T14:00:00Z',
                    return_end='2025-01-15T15:00:00Z',
                    price_amount=25.00,
                    price_currency='USD'
                )
            ]
        )

        # Save queue data
        records = collector.save_queue_data(snapshot_id, ride_data, recorded_at)
        mysql_session.flush()  # Ensure records are written before query

        # Verify record was created
        assert len(records) == 1

        # Verify in database
        result = mysql_session.execute(text("""
            SELECT queue_type, price_amount, price_currency
            FROM queue_data
            WHERE snapshot_id = :snapshot_id
        """), {'snapshot_id': snapshot_id})
        row = result.fetchone()

        assert row is not None
        assert row[0] == 'PAID_RETURN_TIME'
        assert float(row[1]) == 25.00
        assert row[2] == 'USD'

    def test_collector_stats_tracking(self, mysql_session, setup_test_snapshot):
        """QueueDataCollector tracks statistics correctly."""
        snapshot = setup_test_snapshot
        snapshot_id = snapshot['snapshot_id']
        recorded_at = snapshot['recorded_at']

        # Create collector
        collector = QueueDataCollector(mysql_session)

        # Create mock ride data with multiple queue types
        ride_data = LiveRideData(
            entity_id="test-entity-004",
            name="Test Ride",
            entity_type="ATTRACTION",
            status="OPERATING",
            wait_time=30,
            operating_hours=None,
            last_updated=None,
            queues=[
                ClientQueueData(queue_type='STANDBY', wait_time=30),  # Will be skipped
                ClientQueueData(queue_type='SINGLE_RIDER', wait_time=10),
                ClientQueueData(queue_type='PAID_RETURN_TIME', price_amount=25.00)
            ]
        )

        # Save queue data
        collector.save_queue_data(snapshot_id, ride_data, recorded_at)

        # Check stats
        stats = collector.stats
        assert stats['queues_processed'] == 3  # All 3 processed
        assert stats['queues_saved'] == 2  # Only 2 saved (STANDBY skipped)
        assert stats['errors'] == 0


class TestQueueDataQueries:
    """
    Test query patterns for queue data analysis.
    """

    @pytest.fixture
    def setup_test_data(self, mysql_session):
        """Create park, ride, and snapshots with queue data."""
        # Create park
        mysql_session.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, is_active)
            VALUES (99005, 'Query Test Park', 'Test City', 'CA', 'US', 'America/Los_Angeles', 1)
        """))
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT park_id FROM parks WHERE queue_times_id = 99005"
        ))
        park_id = result.scalar()

        # Create ride
        mysql_session.execute(text("""
            INSERT INTO rides (queue_times_id, park_id, name, is_active)
            VALUES (990005, :park_id, 'Query Test Ride', 1)
        """), {'park_id': park_id})
        mysql_session.flush()

        result = mysql_session.execute(text(
            "SELECT ride_id FROM rides WHERE queue_times_id = 990005"
        ))
        ride_id = result.scalar()

        # Create multiple snapshots with queue data
        now = datetime.utcnow()
        snapshot_ids = []

        for i in range(5):
            ts = now - timedelta(hours=i)
            mysql_session.execute(text("""
                INSERT INTO ride_status_snapshots
                (ride_id, recorded_at, wait_time, is_open, computed_is_open, status, last_updated_api, data_source)
                VALUES (:ride_id, :recorded_at, :wait_time, 1, 1, 'OPERATING', :recorded_at, 'LIVE')
            """), {'ride_id': ride_id, 'recorded_at': ts, 'wait_time': 30 + i * 5})
            mysql_session.flush()

            result = mysql_session.execute(text("""
                SELECT snapshot_id FROM ride_status_snapshots
                WHERE ride_id = :ride_id ORDER BY snapshot_id DESC LIMIT 1
            """), {'ride_id': ride_id})
            snapshot_id = result.scalar()
            snapshot_ids.append(snapshot_id)

            # Add queue data with varying prices
            mysql_session.execute(text("""
                INSERT INTO queue_data
                (snapshot_id, queue_type, wait_time_minutes, price_amount, price_currency, recorded_at)
                VALUES
                (:snapshot_id, 'SINGLE_RIDER', :sr_wait, NULL, NULL, :recorded_at),
                (:snapshot_id, 'PAID_RETURN_TIME', NULL, :price, 'USD', :recorded_at)
            """), {
                'snapshot_id': snapshot_id,
                'recorded_at': ts,
                'sr_wait': 10 + i * 2,
                'price': 20.00 + i * 5
            })
            mysql_session.flush()

        return {'ride_id': ride_id, 'snapshot_ids': snapshot_ids}

    def test_query_average_lightning_lane_price(self, mysql_session, setup_test_data):
        """Query average Lightning Lane price."""
        result = mysql_session.execute(text("""
            SELECT AVG(price_amount) as avg_price
            FROM queue_data
            WHERE queue_type = 'PAID_RETURN_TIME'
        """))
        avg_price = result.scalar()

        # Prices are 20, 25, 30, 35, 40 = avg 30
        assert float(avg_price) == 30.00

    def test_query_single_rider_usage(self, mysql_session, setup_test_data):
        """Query single rider wait times over time."""
        result = mysql_session.execute(text("""
            SELECT COUNT(*) as count,
                   MIN(wait_time_minutes) as min_wait,
                   MAX(wait_time_minutes) as max_wait
            FROM queue_data
            WHERE queue_type = 'SINGLE_RIDER'
        """))
        row = result.fetchone()

        assert row[0] == 5  # 5 records
        assert row[1] == 10  # min wait
        assert row[2] == 18  # max wait (10, 12, 14, 16, 18)

    def test_join_queue_data_to_snapshots(self, mysql_session, setup_test_data):
        """Join queue_data to ride_status_snapshots."""
        ride_id = setup_test_data['ride_id']

        result = mysql_session.execute(text("""
            SELECT
                rss.wait_time as standby_wait,
                qd.wait_time_minutes as single_rider_wait,
                qd.price_amount as ll_price
            FROM ride_status_snapshots rss
            JOIN queue_data qd ON rss.snapshot_id = qd.snapshot_id
            WHERE rss.ride_id = :ride_id
            ORDER BY rss.recorded_at DESC
        """), {'ride_id': ride_id})
        rows = result.fetchall()

        # Should have 10 rows (5 snapshots × 2 queue types each)
        assert len(rows) == 10
