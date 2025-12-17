# backend/tests/integration/test_today_api_contract.py

import pytest
from datetime import datetime, timezone
from sqlalchemy import text
from freezegun import freeze_time
import sys
from pathlib import Path

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
if str(backend_src.absolute()) not in sys.path:
    sys.path.insert(0, str(backend_src.absolute()))

from api.app import create_app
from utils import metrics as metrics_module

# Define a fixed point in time for all tests to ensure "today" is consistent.
# This is late in the day in Pacific Time (UTC-8).
MOCKED_NOW_UTC = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)  # 8 PM PST on Dec 5th
TODAY_START_UTC = datetime(2025, 12, 5, 8, 0, 0, tzinfo=timezone.utc) # Midnight PST on Dec 5th

@pytest.fixture(scope="module")
def app():
    """Create Flask app for testing."""
    app = create_app()
    app.config['TESTING'] = True
    # Disable caching for contract tests to always hit the database
    app.config['CACHE_TYPE'] = 'NullCache'
    return app

@pytest.fixture(scope="module")
def client(app):
    """Create Flask test client."""
    return app.test_client()

@pytest.fixture(scope="function")
def today_api_test_data(mysql_connection):
    """
    Set up a controlled dataset for the 'today' API contract test.

    This fixture creates:
    - 3 active parks with varying downtime to test sorting and filtering.
    - 1 park with zero downtime (should be excluded from results).
    - 1 inactive park (should be excluded).
    - Snapshots and hourly stats for a deterministic "today" window.
    """
    conn = mysql_connection

    # Use a unique high-ID range to avoid conflicts
    park_qt_ids = {'disney': 9101, 'universal': 9102, 'other': 9103, 'zero_shame': 9104, 'inactive': 9105}
    ride_qt_id_start = 91000

    # Clean up previous test runs
    conn.execute(text("DELETE FROM park_hourly_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 9100)"))
    conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 9100)"))
    conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id >= 91001"))
    conn.execute(text("DELETE FROM ride_classifications WHERE ride_id >= 91001"))
    conn.execute(text("DELETE FROM rides WHERE ride_id >= 91001"))
    conn.execute(text("DELETE FROM parks WHERE queue_times_id >= 9100"))

    # Create parks
    parks_to_create = [
        (park_qt_ids['disney'], 'Contract Test Park (Disney)', 'Disney', True, False, True),
        (park_qt_ids['universal'], 'Contract Test Park (Universal)', 'Universal', False, True, True),
        (park_qt_ids['other'], 'Contract Test Park (Other)', 'Other', False, False, True),
        (park_qt_ids['zero_shame'], 'Contract Test Park (Zero Shame)', 'Other', False, False, True),
        (park_qt_ids['inactive'], 'Contract Test Park (Inactive)', 'Other', False, False, False),
    ]

    park_id_map = {}
    for i, (qt_id, name, operator, is_disney, is_universal, is_active) in enumerate(parks_to_create, 1):
        park_id = 9100 + i
        park_id_map[qt_id] = park_id
        conn.execute(text("""
            INSERT INTO parks (park_id, queue_times_id, name, operator, is_disney, is_universal, is_active, city, state_province, country, timezone)
            VALUES (:park_id, :qt_id, :name, :op, :is_d, :is_u, :is_a, 'Test City', 'TC', 'US', 'America/Los_Angeles')
        """), {'park_id': park_id, 'qt_id': qt_id, 'name': name, 'op': operator, 'is_d': is_disney, 'is_u': is_universal, 'is_a': is_active})

    # Create rides and classifications
    # Each park gets one T1, one T2 ride. T1 weight=3, T2 weight=2. Total weight = 5.
    rides_to_create = []
    ride_id_map = {}
    ride_id_counter = 91001
    for qt_id, park_id in park_id_map.items():
        for tier, tier_weight in [(1, 3), (2, 2)]:
            ride_id_map[(park_id, tier)] = ride_id_counter
            rides_to_create.append({
                'ride_id': ride_id_counter, 'qt_id': ride_qt_id_start + ride_id_counter, 'park_id': park_id,
                'name': f'Ride T{tier} for Park {park_id}', 'tier': tier, 'tier_weight': tier_weight
            })
            ride_id_counter += 1

    for ride in rides_to_create:
        conn.execute(text("""
            INSERT INTO rides (ride_id, queue_times_id, park_id, name, is_active, category)
            VALUES (:ride_id, :qt_id, :park_id, :name, TRUE, 'ATTRACTION')
        """), ride)
        conn.execute(text("INSERT INTO ride_classifications (ride_id, tier, tier_weight) VALUES (:ride_id, :tier, :tier_weight)"), ride)

    # Generate snapshots and hourly stats
    # 20 hours of data = 240 snapshots (one every 5 mins)
    total_snapshots = (MOCKED_NOW_UTC - TODAY_START_UTC).seconds // 300

    # Park Data Scenarios:
    # Disney (9101): T1 ride down 50% of time. Shame = (3/5)*10 * 50% = 3.0
    # Universal (9102): T1 ride down 25% of time. Shame = (3/5)*10 * 25% = 1.5
    # Other (9103): T2 ride down 10% of time. Shame = (2/5)*10 * 10% = 0.4
    # Zero Shame (9104): No rides down. Shame = 0.
    scenarios = {
        park_id_map[park_qt_ids['disney']]: {'down_ride_tier': 1, 'down_fraction': 0.50, 'total_weight': 5, 'expected_shame': 3.0, 'expected_downtime_hours': 10.0},
        park_id_map[park_qt_ids['universal']]: {'down_ride_tier': 1, 'down_fraction': 0.25, 'total_weight': 5, 'expected_shame': 1.5, 'expected_downtime_hours': 5.0},
        park_id_map[park_qt_ids['other']]: {'down_ride_tier': 2, 'down_fraction': 0.10, 'total_weight': 5, 'expected_shame': 0.4, 'expected_downtime_hours': 4.0}, # T2 down for 20 hours * 0.1 = 2 hours/ride * 2 rides = 4h
        park_id_map[park_qt_ids['zero_shame']]: {'down_ride_tier': None, 'down_fraction': 0, 'total_weight': 5, 'expected_shame': 0.0, 'expected_downtime_hours': 0.0},
        park_id_map[park_qt_ids['inactive']]: {'down_ride_tier': 1, 'down_fraction': 1.0, 'total_weight': 5, 'expected_shame': 6.0, 'expected_downtime_hours': 20.0},
    }

    from datetime import timedelta
    for park_id, scenario in scenarios.items():
        ride_t1 = ride_id_map[(park_id, 1)]
        ride_t2 = ride_id_map[(park_id, 2)]

        down_snapshots = int(total_snapshots * scenario['down_fraction'])

        hourly_stats = {}

        for i in range(total_snapshots):
            ts = TODAY_START_UTC + timedelta(minutes=i * 5)
            hour_start = ts.replace(minute=0, second=0, microsecond=0)
            if hour_start not in hourly_stats:
                hourly_stats[hour_start] = {'shame_scores': [], 'total_downtime_hours': 0, 'weighted_downtime_hours': 0, 'rides_operating': 0, 'rides_down': 0, 'snapshot_count': 0}

            # T1 is the down ride
            is_t1_down = scenario['down_ride_tier'] == 1 and i < down_snapshots
            # T2 is the down ride
            is_t2_down = scenario['down_ride_tier'] == 2 and i < down_snapshots

            shame_score = 0
            if is_t1_down: shame_score = (3 / scenario['total_weight']) * 10
            if is_t2_down: shame_score = (2 / scenario['total_weight']) * 10

            rides_open = (0 if is_t1_down else 1) + (0 if is_t2_down else 1)
            rides_closed = 2 - rides_open
            avg_wait = 10 + i
            conn.execute(
                text("""
                    INSERT INTO park_activity_snapshots (
                        park_id,
                        recorded_at,
                        total_rides_tracked,
                        rides_open,
                        rides_closed,
                        avg_wait_time,
                        max_wait_time,
                        park_appears_open,
                        shame_score
                    ) VALUES (
                        :pid,
                        :ts,
                        :total_tracked,
                        :open_count,
                        :closed_count,
                        :avg_wait,
                        :max_wait,
                        TRUE,
                        :shame
                    )
                """),
                {
                    'pid': park_id,
                    'ts': ts,
                    'total_tracked': 2,
                    'open_count': rides_open,
                    'closed_count': rides_closed,
                    'avg_wait': avg_wait,
                    'max_wait': avg_wait + 5,
                    'shame': shame_score,
                }
            )
            conn.execute(text("INSERT INTO ride_status_snapshots (ride_id, recorded_at, status, computed_is_open) VALUES (:rid, :ts, :st, :cio)"), {'rid': ride_t1, 'ts': ts, 'st': 'DOWN' if is_t1_down else 'OPERATING', 'cio': not is_t1_down})
            conn.execute(text("INSERT INTO ride_status_snapshots (ride_id, recorded_at, status, computed_is_open) VALUES (:rid, :ts, :st, :cio)"), {'rid': ride_t2, 'ts': ts, 'st': 'DOWN' if is_t2_down else 'OPERATING', 'cio': not is_t2_down})

            # For hourly aggregation
            hourly_stats[hour_start]['shame_scores'].append(shame_score)
            hourly_stats[hour_start]['snapshot_count'] += 1
            if is_t1_down:
                hourly_stats[hour_start]['total_downtime_hours'] += (5/60.0)
                hourly_stats[hour_start]['weighted_downtime_hours'] += (5/60.0) * 3
                hourly_stats[hour_start]['rides_down'] += 1
            else:
                hourly_stats[hour_start]['rides_operating'] += 1
            if is_t2_down:
                hourly_stats[hour_start]['total_downtime_hours'] += (5/60.0)
                hourly_stats[hour_start]['weighted_downtime_hours'] += (5/60.0) * 2
                hourly_stats[hour_start]['rides_down'] += 1
            else:
                hourly_stats[hour_start]['rides_operating'] += 1

        for hour, stats in hourly_stats.items():
            if not stats['shame_scores']: continue
            avg_shame = sum(stats['shame_scores']) / len(stats['shame_scores'])
            conn.execute(text("""
                INSERT INTO park_hourly_stats (
                    park_id,
                    hour_start_utc,
                    shame_score,
                    avg_wait_time_minutes,
                    rides_operating,
                    rides_down,
                    total_downtime_hours,
                    weighted_downtime_hours,
                    effective_park_weight,
                    snapshot_count,
                    park_was_open
                ) VALUES (
                    :pid,
                    :hour,
                    :shame,
                    :avg_wait,
                    :ro,
                    :rd,
                    :tdh,
                    :wdh,
                    :weight,
                    :sc,
                    TRUE
                )
            """), {
                'pid': park_id,
                'hour': hour,
                'shame': avg_shame,
                'avg_wait': 10.0,
                'ro': stats['rides_operating'],
                'rd': stats['rides_down'],
                'tdh': stats['total_downtime_hours'],
                'wdh': stats['weighted_downtime_hours'],
                'weight': scenario['total_weight'],
                'sc': stats['snapshot_count'],
            })

    conn.commit()
    yield scenarios
    # Teardown is handled by transaction rollback in mysql_connection fixture

def _validate_ranking_item_contract(item: dict):
    """Assert that a single park ranking item matches the OpenAPI contract."""
    # Required fields from spec
    required_fields = [
        'rank', 'park_id', 'park_name', 'shame_score', 'total_downtime_hours',
        'weighted_downtime_hours', 'rides_operating', 'rides_down',
        'uptime_percentage', 'effective_park_weight', 'snapshot_count'
    ]
    for field in required_fields:
        assert field in item, f"Missing required field: {field}"
        assert item[field] is not None, f"Field '{field}' should not be null"

    # Data types
    assert isinstance(item['rank'], int)
    assert isinstance(item['park_id'], int)
    assert isinstance(item['park_name'], str)
    assert isinstance(item['shame_score'], float)
    assert isinstance(item['total_downtime_hours'], float)
    assert isinstance(item['weighted_downtime_hours'], float)
    assert isinstance(item['rides_operating'], int)
    assert isinstance(item['rides_down'], int)
    assert isinstance(item['uptime_percentage'], float)
    assert isinstance(item['effective_park_weight'], float)
    assert isinstance(item['snapshot_count'], int)

    # Value ranges and precision
    assert 0 <= item['shame_score'] <= 10
    # Check for 1 decimal place precision
    assert round(item['shame_score'], 1) == item['shame_score']

    assert 0 <= item['uptime_percentage'] <= 100
    assert item['snapshot_count'] > 0

def _validate_today_downtime_response(response, expected_park_ids: list):
    """Comprehensive validation for the /parks/downtime?period=today response."""
    assert response.status_code == 200
    data = response.get_json()

    # Top-level contract validation
    assert data['success'] is True
    assert data['period'] == 'today'
    assert 'filter' in data
    assert 'weighted' in data
    assert 'sort_by' in data
    assert 'data' in data
    assert 'attribution' in data
    assert data['attribution']['data_source'] == "ThemeParks.wiki"
    assert data['attribution']['url'] == "https://themeparks.wiki"

    # Check that only the expected parks are present and in the correct order
    assert len(data['data']) == len(expected_park_ids)
    actual_park_ids = [p['park_id'] for p in data['data']]
    assert actual_park_ids == expected_park_ids, f"Expected park IDs {expected_park_ids}, but got {actual_park_ids}"

    # Validate each item in the data array against the contract
    for item in data['data']:
        _validate_ranking_item_contract(item)

@pytest.mark.parametrize("use_hourly_tables", [False, True], ids=["FallbackQuery", "HybridQuery"])
@freeze_time(MOCKED_NOW_UTC)
def test_today_happy_path_default_parameters(client, today_api_test_data, use_hourly_tables):
    """
    Covers: Happy Path - Default Parameters
    - No query params defaults to period=today, filter=all-parks
    - Validates response structure, data types, and required fields.
    - Tests BOTH the fallback query and the new hybrid query.
    """
    metrics_module.USE_HOURLY_TABLES = use_hourly_tables

    response = client.get('/api/parks/downtime?period=today')

    # Expected order by shame_score DESC: Disney (3.0), Universal (1.5), Other (0.4)
    expected_park_ids = [9101, 9102, 9103]
    _validate_today_downtime_response(response, expected_park_ids)

    data = response.get_json()
    assert data['filter'] == 'all-parks'
    assert data['sort_by'] == 'shame_score'

    # Check specific shame scores
    park_shame_scores = {p['park_id']: p['shame_score'] for p in data['data']}
    assert park_shame_scores[9101] == 3.0
    assert park_shame_scores[9102] == 1.5
    assert park_shame_scores[9103] == 0.4


@pytest.mark.parametrize("use_hourly_tables", [False, True], ids=["FallbackQuery", "HybridQuery"])
@freeze_time(MOCKED_NOW_UTC)
def test_today_filter_parameter(client, today_api_test_data, use_hourly_tables):
    """
    Covers: Filter Parameter
    - filter=disney-universal returns only those parks.
    - filter=all-parks returns all (tested in happy path).
    """
    metrics_module.USE_HOURLY_TABLES = use_hourly_tables

    response = client.get('/api/parks/downtime?period=today&filter=disney-universal')

    # Expected order: Disney (3.0), Universal (1.5)
    expected_park_ids = [9101, 9102]
    _validate_today_downtime_response(response, expected_park_ids)
    assert response.get_json()['filter'] == 'disney-universal'

@pytest.mark.parametrize("use_hourly_tables", [False, True], ids=["FallbackQuery", "HybridQuery"])
@freeze_time(MOCKED_NOW_UTC)
def test_today_sort_parameter(client, today_api_test_data, use_hourly_tables):
    """
    Covers: Sort Parameter
    - sort_by=total_downtime_hours sorts correctly.
    """
    metrics_module.USE_HOURLY_TABLES = use_hourly_tables

    response = client.get('/api/parks/downtime?period=today&sort_by=total_downtime_hours')

    # Expected downtime hours:
    # Disney (9101): T1 ride (weight 3) down for 50% of 20 hours = 10 hours
    # Universal (9102): T1 ride (weight 3) down for 25% of 20 hours = 5 hours
    # Other (9103): T2 ride (weight 2) down for 10% of 20 hours * 2 rides = 4 hours
    # Expected order by downtime DESC: Disney (10h), Universal (5h), Other (4h)
    expected_park_ids = [9101, 9102, 9103]
    _validate_today_downtime_response(response, expected_park_ids)
    assert response.get_json()['sort_by'] == 'total_downtime_hours'

@pytest.mark.parametrize("use_hourly_tables", [False, True], ids=["FallbackQuery", "HybridQuery"])
@freeze_time(MOCKED_NOW_UTC)
def test_today_limit_parameter(client, today_api_test_data, use_hourly_tables):
    """
    Covers: Limit Parameter
    - limit=2 returns a maximum of 2 results.
    """
    metrics_module.USE_HOURLY_TABLES = use_hourly_tables

    response = client.get('/api/parks/downtime?period=today&limit=2')

    # Expected top 2 by default sort (shame_score): Disney, Universal
    expected_park_ids = [9101, 9102]
    _validate_today_downtime_response(response, expected_park_ids)

@pytest.mark.parametrize("use_hourly_tables", [False, True], ids=["FallbackQuery", "HybridQuery"])
@freeze_time(MOCKED_NOW_UTC)
def test_today_edge_case_exclusions(client, today_api_test_data, use_hourly_tables):
    """
    Covers: Edge Cases
    - Parks with zero downtime (shame_score=0) are excluded.
    - Inactive parks are excluded.
    - The test data fixture includes parks for both scenarios. This test
      confirms they are NOT present in the response.
    """
    metrics_module.USE_HOURLY_TABLES = use_hourly_tables

    response = client.get('/api/parks/downtime?period=today')
    data = response.get_json()

    # The main validation already checks that only 3 parks are returned.
    # This test makes the reason for exclusion explicit.
    returned_park_ids = {p['park_id'] for p in data['data']}

    # Park 9104 was created with zero downtime
    assert 9104 not in returned_park_ids, "Park with zero shame score should be excluded"

    # Park 9105 was created as inactive
    assert 9105 not in returned_park_ids, "Inactive park should be excluded"
