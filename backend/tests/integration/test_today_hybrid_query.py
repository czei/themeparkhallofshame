# backend/tests/integration/test_today_hybrid_query.py
"""
Integration tests for TODAY hybrid query logic.

NOTE: These tests are OBSOLETE as of 2025-12 refactoring.

The TodayParkRankingsQuery was refactored to use ONLY pre-aggregated park_hourly_stats
tables. The "hybrid" approach (combining hourly tables with raw snapshots) has been
removed for performance reasons. The following features no longer exist:
- USE_HOURLY_TABLES flag
- get_today_range_to_now_utc() import
- _query_hourly_tables() method
- _query_raw_snapshots() method
- _combine_hourly_and_raw() method
- _build_rankings_from_combined_data() method

These tests remain for historical reference but are all skipped.

Original purpose:
Tests TodayParkRankingsQuery's hybrid approach:
- _query_hourly_tables(): Fetches complete hours from park_hourly_stats
- _query_raw_snapshots(): Fetches current hour from park_activity_snapshots
- _combine_hourly_and_raw(): Combines both sources with averaging
- _build_rankings_from_combined_data(): Builds final rankings

Key Test Scenarios:
1. Normal hybrid: Complete hours + current partial hour
2. Early morning: No complete hours, only current hour
3. Hour boundary: Current time exactly on hour (no raw data)
4. Multiple parks: Independent aggregation
5. Rides down bug: current hour rides_down ignored
"""

import pytest

# Mark entire module as obsolete
pytestmark = pytest.mark.skip(
    reason="OBSOLETE: TodayParkRankingsQuery hybrid approach was refactored out in 2025-12. "
           "Query now uses ONLY pre-aggregated park_hourly_stats tables."
)

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from database.queries.today.today_park_rankings import TodayParkRankingsQuery
from database.repositories.stats_repository import StatsRepository
from utils import metrics as metrics_module
from utils import timezone as timezone_module


# context_start_text: TodayParkRankingsQuery._combine_hourly_and_raw definition
# def _combine_hourly_and_raw(
#     self,
#     hourly_data: Dict[int, Dict[str, Any]],
#     raw_data: Dict[int, Dict[str, Any]]
# ) -> Dict[int, Dict[str, Any]]:
#     ...
#     if all_shame_scores:
#         avg_shame_score = round(sum(all_shame_scores) / len(all_shame_scores), 1)
# context_end_text


@pytest.fixture
def today_hybrid_schema(mysql_connection):
    """
    Ensure minimal schema for TODAY hybrid query integration tests.

    This fixture creates only the tables needed by TodayParkRankingsQuery hybrid path:
    - parks
    - rides
    - ride_classifications
    - ride_status_snapshots
    - park_activity_snapshots
    - park_hourly_stats

    Tables are truncated before each test to avoid cross-test contamination.
    """
    conn = mysql_connection

    # Create minimal schema pieces if they do not exist
    # NOTE: These definitions must be compatible with production schema columns
    # that TodayParkRankingsQuery and StatsRepository actually read.

    # parks
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS parks (
            park_id INT PRIMARY KEY,
            queue_times_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            city VARCHAR(255) DEFAULT 'Test City',
            state_province VARCHAR(255) DEFAULT 'TS',
            country VARCHAR(64) DEFAULT 'US',
            timezone VARCHAR(64) DEFAULT 'America/Los_Angeles',
            operator VARCHAR(255) DEFAULT 'Test Operator',
            is_active BOOLEAN DEFAULT TRUE,
            is_disney BOOLEAN DEFAULT FALSE,
            is_universal BOOLEAN DEFAULT FALSE
        )
        """
        )
    )

    # rides
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS rides (
            ride_id INT PRIMARY KEY,
            queue_times_id INT NOT NULL,
            park_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(32) DEFAULT 'ATTRACTION',
            is_active BOOLEAN DEFAULT TRUE
        )
        """
        )
    )

    # ride_classifications
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS ride_classifications (
            ride_id INT PRIMARY KEY,
            tier INT,
            tier_weight DECIMAL(5,2),
            classification_method VARCHAR(64),
            confidence_score DECIMAL(5,2)
        )
        """
        )
    )

    # ride_status_snapshots
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS ride_status_snapshots (
            snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
            ride_id INT NOT NULL,
            recorded_at DATETIME(6) NOT NULL,
            status VARCHAR(32),
            wait_time INT,
            computed_is_open BOOLEAN
        )
        """
        )
    )

    # park_activity_snapshots
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS park_activity_snapshots (
            snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
            park_id INT NOT NULL,
            recorded_at DATETIME(6) NOT NULL,
            park_appears_open BOOLEAN NOT NULL,
            shame_score DECIMAL(5,2)
        )
        """
        )
    )

    # park_hourly_stats
    conn.execute(
        text(
            """
        CREATE TABLE IF NOT EXISTS park_hourly_stats (
            stat_id INT AUTO_INCREMENT PRIMARY KEY,
            park_id INT NOT NULL,
            hour_start_utc DATETIME(6) NOT NULL,
            shame_score DECIMAL(5,2),
            rides_operating INT,
            rides_down INT,
            total_downtime_hours DECIMAL(8,2),
            weighted_downtime_hours DECIMAL(8,2),
            effective_park_weight DECIMAL(8,2),
            snapshot_count INT,
            park_was_open BOOLEAN
        )
        """
        )
    )

    conn.commit()

    # Truncate data before each test
    for table in [
        "park_hourly_stats",
        "park_activity_snapshots",
        "ride_status_snapshots",
        "ride_classifications",
        "rides",
        "parks",
    ]:
        conn.execute(text(f"DELETE FROM {table}"))
    conn.commit()

    return conn


def _seed_single_park_with_hourly_and_raw(
    conn,
    *,
    park_id: int,
    queue_times_id: int,
    hourly_shames: list[float],
    hourly_snapshot_counts: list[int],
    raw_shames: list[float],
    now_utc: datetime,
):
    """
    Seed deterministic data for a single park:

    - N complete hours in park_hourly_stats, each with:
        - shame_score (stored but NOT used in hybrid averaging)
        - snapshot_count used only for metadata aggregation

    - Raw snapshots in park_activity_snapshots for current incomplete hour,
      each with specific shame_score values.

    NOTE: TodayParkRankingsQuery._combine_hourly_and_raw currently does:
        all_shame_scores = hourly['shame_scores'] + raw['shame_scores']
        avg = mean(all_shame_scores)

    Because hourly['shame_scores'] is itself the list of per-hour *averages*
    (from park_hourly_stats.shame_score) and raw['shame_scores'] are per-snapshot
    values, this is a bug from a "true weighted by snapshot count" perspective.

    Our tests purposely surface this mismatch against a "ground truth"
    snapshot-weighted average computed independently in test code.
    """
    # Create park
    conn.execute(
        text(
            """
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (:park_id, :qt_id, 'Test Park', 'Test City', 'TS', 'US', 'America/Los_Angeles', 'Test Operator', TRUE)
        """
        ),
        {"park_id": park_id, "qt_id": queue_times_id},
    )

    # Create a single ride so CTEs in _query_raw_snapshots don't filter everything out
    conn.execute(
        text(
            """
        INSERT INTO rides (ride_id, queue_times_id, park_id, name, is_active)
        VALUES (:ride_id, :queue_times_id, :park_id, 'Test Ride', TRUE)
        """
        ),
        {"ride_id": 1000 + park_id, "queue_times_id": 9000 + park_id, "park_id": park_id},
    )

    # Give it a classification (weight doesn't matter for shame_score here)
    conn.execute(
        text(
            """
        INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score)
        VALUES (:ride_id, 2, 2.0, 'manual_override', 1.0)
        """
        ),
        {"ride_id": 1000 + park_id},
    )

    # Establish base midnight (Pacific day start converted to UTC) so that
    # TodayParkRankingsQuery.get_rankings() with get_today_range_to_now_utc()
    # will include our data. For test determinism we treat `now_utc` as end time.
    #
    # We subtract len(hourly_shames) hours from current hour start so that
    # they will all lie between start_utc and current_hour_start.
    current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
    first_hour_start = current_hour_start - timedelta(hours=len(hourly_shames))

    # Seed hourly stats (complete hours 0..N-1)
    for idx, (shame, snapshots) in enumerate(zip(hourly_shames, hourly_snapshot_counts)):
        hour_start = first_hour_start + timedelta(hours=idx)
        conn.execute(
            text(
                """
            INSERT INTO park_hourly_stats (
                park_id,
                hour_start_utc,
                shame_score,
                rides_operating,
                rides_down,
                total_downtime_hours,
                weighted_downtime_hours,
                effective_park_weight,
                snapshot_count,
                park_was_open
            ) VALUES (
                :park_id,
                :hour_start,
                :shame_score,
                10,
                1,
                1.0,
                1.0,
                10.0,
                :snapshots,
                TRUE
            )
            """
            ),
            {
                "park_id": park_id,
                "hour_start": hour_start,
                "shame_score": shame,
                "snapshots": snapshots,
            },
        )

    # Seed raw snapshots for current hour: each snapshot must be "park open"
    # and must match the join condition used by _query_raw_snapshots.
    #
    # That method does:
    #   FROM park_activity_snapshots pas
    #   WHERE pas.recorded_at BETWEEN start_time AND end_time
    #     AND pas.park_appears_open = TRUE
    #     AND pas.shame_score IS NOT NULL
    #
    # BUT it also builds a rides_operated CTE joining ride_status_snapshots
    # to park_activity_snapshots on EXACT recorded_at. To ensure no filtering
    # occurs, we insert matching ride_status_snapshots for each pas row.
    raw_count = len(raw_shames)
    # Spread snapshots 5 minutes apart inside the current hour
    for idx, shame in enumerate(raw_shames):
        recorded_at = current_hour_start + timedelta(minutes=5 * idx)
        conn.execute(
            text(
                """
            INSERT INTO park_activity_snapshots (
                park_id, recorded_at, park_appears_open, shame_score
            )
            VALUES (:park_id, :recorded_at, TRUE, :shame_score)
            """
            ),
            {
                "park_id": park_id,
                "recorded_at": recorded_at,
                "shame_score": shame,
            },
        )
        conn.execute(
            text(
                """
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, status, wait_time, computed_is_open
            ) VALUES (
                :ride_id, :recorded_at, 'OPERATING', 10, TRUE
            )
            """
            ),
            {
                "ride_id": 1000 + park_id,
                "recorded_at": recorded_at,
            },
        )

    conn.commit()

    return {
        "current_hour_start": current_hour_start,
        "first_hour_start": first_hour_start,
    }


def _compute_snapshot_weighted_average(
    hourly_shames: list[float],
    hourly_snapshot_counts: list[int],
    raw_shames: list[float],
) -> float:
    """
    Ground-truth snapshot-weighted average (what we expect TODAY to mean):

    (sum(hour_i_shame * hourly_snapshot_count_i) + sum(raw_shame_j)) /
    (sum(hourly_snapshot_count_i) + len(raw_shames))

    Returns value rounded to 1 decimal place to match query rounding.
    """
    numerator = 0.0
    total_snapshots = 0

    for shame, count in zip(hourly_shames, hourly_snapshot_counts):
        numerator += shame * count
        total_snapshots += count

    for shame in raw_shames:
        numerator += shame
        total_snapshots += 1

    if total_snapshots == 0:
        return 0.0

    return round(numerator / total_snapshots, 1)


def _compute_simple_average(hourly_shames: list[float], raw_shames: list[float]) -> float:
    """
    Simple unweighted average over all per-hour and per-snapshot shame scores:

    This matches the current implementation in _combine_hourly_and_raw:

        all_shame_scores = hourly['shame_scores'] + raw['shame_scores']
        avg = mean(all_shame_scores)

    I.e., each *hour* and each *snapshot* get equal weight.
    """
    all_scores = list(hourly_shames) + list(raw_shames)
    if not all_scores:
        return 0.0
    return round(sum(all_scores) / len(all_scores), 1)


def _force_use_hourly_tables(monkeypatch):
    """
    Force TodayParkRankingsQuery to take the hybrid path by setting
    USE_HOURLY_TABLES = True in the query module where it's actually used.

    CRITICAL: Must patch the query module, not the metrics module, because
    the query does 'from utils.metrics import USE_HOURLY_TABLES' which creates
    a local binding that won't be affected by patching the source module.
    """
    # Patch where the value is USED, not where it's DEFINED
    monkeypatch.setattr(
        "database.queries.today.today_park_rankings.USE_HOURLY_TABLES",
        True,
        raising=False
    )


def _freeze_today_range(monkeypatch, start_utc: datetime, now_utc: datetime):
    """
    Freeze get_today_range_to_now_utc() so that TodayParkRankingsQuery
    uses the same [start_utc, now_utc) window we used when inserting data.

    CRITICAL: Must patch the query module, not the timezone module, because
    the query does 'from utils.timezone import get_today_range_to_now_utc' which
    creates a local binding that won't be affected by patching the source module.
    """
    def _fake_today_range():
        return start_utc, now_utc

    # Patch where the function is USED, not where it's DEFINED
    monkeypatch.setattr(
        "database.queries.today.today_park_rankings.get_today_range_to_now_utc",
        _fake_today_range,
        raising=False,
    )


def test_today_hybrid_simple_vs_weighted_average_single_park(today_hybrid_schema, monkeypatch):
    """
    GIVEN:
        - 1 park with 3 complete hours in park_hourly_stats
          (shame scores [2.0, 4.0, 6.0] and snapshot counts [12, 6, 3])
        - current incomplete hour with 2 raw snapshots (shame scores [8.0, 10.0])

    WHEN:
        - TodayParkRankingsQuery.get_rankings() is executed with USE_HOURLY_TABLES=True

    THEN:
        - The "true" snapshot-weighted average shame score is:

              numerator = 2*12 + 4*6 + 6*3 + 8 + 10
                        = 24 + 24 + 18 + 8 + 10 = 84
              denom    = 12 + 6 + 3 + 2 = 23
              weighted = 84 / 23 ≈ 3.7 (rounded 1 decimal)

        - The CURRENT implementation _combine_hourly_and_raw uses a *simple* average:

              hourly averages: [2, 4, 6]  (1 weight each)
              raw snapshots:   [8, 10]    (1 weight each)
              simple = (2 + 4 + 6 + 8 + 10) / 5 = 6.0

        - The integration test asserts the implementation result (6.0) but also
          computes and asserts the *expected* weighted value separately so
          a future refactor can safely change the implementation and update
          this assertion to lock in correct behavior.
    """
    conn = today_hybrid_schema

    # Controlled "now" for this test
    now_utc = datetime(2025, 1, 2, 15, 34, 0, tzinfo=timezone.utc)

    hourly_shames = [2.0, 4.0, 6.0]
    hourly_snapshot_counts = [12, 6, 3]  # total 21 snapshots
    raw_shames = [8.0, 10.0]  # 2 snapshots in current hour

    seed_meta = _seed_single_park_with_hourly_and_raw(
        conn,
        park_id=1,
        queue_times_id=9001,
        hourly_shames=hourly_shames,
        hourly_snapshot_counts=hourly_snapshot_counts,
        raw_shames=raw_shames,
        now_utc=now_utc,
    )

    # Freeze time range and feature flag so TodayParkRankingsQuery
    # executes the HYBRID path deterministically
    _force_use_hourly_tables(monkeypatch)

    # start_utc at the first hourly bucket; end_utc at now
    start_utc = seed_meta["first_hour_start"]
    _freeze_today_range(monkeypatch, start_utc=start_utc, now_utc=now_utc)

    query = TodayParkRankingsQuery(conn)
    rankings = query.get_rankings(
        filter_disney_universal=False,
        limit=10,
        sort_by="shame_score",
    )

    # Sanity: exactly one park result
    assert len(rankings) == 1
    result = rankings[0]

    # Implementation behavior: simple average of per-hour averages + raw snapshots
    implementation_expected = _compute_simple_average(hourly_shames, raw_shames)
    assert pytest.approx(float(result["shame_score"]), rel=1e-9) == implementation_expected

    # Ground-truth snapshot-weighted average (documented expectation)
    weighted_expected = _compute_snapshot_weighted_average(
        hourly_shames=hourly_shames,
        hourly_snapshot_counts=hourly_snapshot_counts,
        raw_shames=raw_shames,
    )

    # Document current deviation: this will be intentionally different until
    # _combine_hourly_and_raw is fixed to use snapshot weights.
    assert weighted_expected != implementation_expected

    # For future refactor:
    #   When _combine_hourly_and_raw is fixed, update this assertion to:
    #       assert result["shame_score"] == weighted_expected
    #   and remove the inequality assertion above.
    #
    # For now, keep both values visible for debugging.
    print(
        f"\n[DEBUG] TODAY hybrid simple_avg={implementation_expected}, "
        f"snapshot_weighted={weighted_expected}"
    )


def test_today_hybrid_empty_hourly_only_raw_current_hour(today_hybrid_schema, monkeypatch):
    """
    Edge case: early morning shortly after midnight.

    GIVEN:
        - No complete hours in park_hourly_stats (park opened this hour)
        - 3 raw snapshots in current incomplete hour (shame scores [3.0, 5.0, 7.0])

    WHEN:
        - TodayParkRankingsQuery.get_rankings() hybrid path runs

    THEN:
        - Implementation should average over ONLY the raw snapshots:

              simple_avg = (3 + 5 + 7) / 3 = 5.0

        - Snapshot-weighted average is identical in this specific case since
          all inputs are from raw snapshots with equal weight 1.

    This ensures _query_hourly_tables() returning an empty dict does not break
    the hybrid combination.
    """
    conn = today_hybrid_schema
    now_utc = datetime(2025, 1, 2, 0, 17, 0, tzinfo=timezone.utc)

    hourly_shames: list[float] = []
    hourly_snapshot_counts: list[int] = []
    raw_shames = [3.0, 5.0, 7.0]

    seed_meta = _seed_single_park_with_hourly_and_raw(
        conn,
        park_id=2,
        queue_times_id=9002,
        hourly_shames=hourly_shames,
        hourly_snapshot_counts=hourly_snapshot_counts,
        raw_shames=raw_shames,
        now_utc=now_utc,
    )

    _force_use_hourly_tables(monkeypatch)

    # start_utc at first_hour_start == current_hour_start because no hourly data
    start_utc = seed_meta["first_hour_start"]
    _freeze_today_range(monkeypatch, start_utc=start_utc, now_utc=now_utc)

    query = TodayParkRankingsQuery(conn)
    rankings = query.get_rankings(limit=10)

    assert len(rankings) == 1
    result = rankings[0]

    simple_expected = _compute_simple_average(hourly_shames, raw_shames)
    weighted_expected = _compute_snapshot_weighted_average(
        hourly_shames, hourly_snapshot_counts, raw_shames
    )

    assert pytest.approx(float(result["shame_score"]), rel=1e-9) == simple_expected
    assert simple_expected == weighted_expected == 5.0


def test_today_hybrid_missing_raw_current_hour_uses_hourly_only(today_hybrid_schema, monkeypatch):
    """
    Edge case: no snapshots collected yet for current hour.

    GIVEN:
        - 2 complete hours in park_hourly_stats:
            hour0 shame=1.0 (snapshot_count=6)
            hour1 shame=3.0 (snapshot_count=12)
        - No raw snapshots for current hour

    WHEN:
        - TodayParkRankingsQuery hybrid path runs

    THEN:
        - Implementation should average over ONLY hourly stats:

              simple_avg = (1.0 + 3.0) / 2 = 2.0

        - Snapshot-weighted average would be:

              weighted = (1*6 + 3*12) / (6+12) = (6 + 36)/18 = 42/18 = 2.3̅

        This test locks in behavior that missing current-hour data does not crash
        and that only hourly contributions are considered.
    """
    conn = today_hybrid_schema
    now_utc = datetime(2025, 1, 2, 10, 5, 0, tzinfo=timezone.utc)

    hourly_shames = [1.0, 3.0]
    hourly_snapshot_counts = [6, 12]
    raw_shames: list[float] = []

    seed_meta = _seed_single_park_with_hourly_and_raw(
        conn,
        park_id=3,
        queue_times_id=9003,
        hourly_shames=hourly_shames,
        hourly_snapshot_counts=hourly_snapshot_counts,
        raw_shames=raw_shames,
        now_utc=now_utc,
    )

    # Intentionally DELETE all raw snapshots for current hour to simulate
    # "no data yet" for this hour.
    conn.execute(
        text(
            """
        DELETE FROM park_activity_snapshots
        WHERE park_id = :park_id
        """
        ),
        {"park_id": 3},
    )
    conn.execute(
        text(
            """
        DELETE FROM ride_status_snapshots
        WHERE ride_id = :ride_id
        """
        ),
        {"ride_id": 1003},
    )
    conn.commit()

    _force_use_hourly_tables(monkeypatch)
    start_utc = seed_meta["first_hour_start"]
    _freeze_today_range(monkeypatch, start_utc=start_utc, now_utc=now_utc)

    query = TodayParkRankingsQuery(conn)
    rankings = query.get_rankings(limit=10)

    assert len(rankings) == 1
    result = rankings[0]

    simple_expected = _compute_simple_average(hourly_shames, raw_shames)
    weighted_expected = _compute_snapshot_weighted_average(
        hourly_shames, hourly_snapshot_counts, raw_shames
    )

    assert pytest.approx(float(result["shame_score"]), rel=1e-9) == simple_expected
    assert simple_expected == 2.0
    assert weighted_expected == pytest.approx(2.3, rel=1e-2)


def test_today_hybrid_multiple_parks_independent_aggregation(today_hybrid_schema, monkeypatch):
    """
    Regression: ensure multiple parks' hourly/raw data do not bleed into each other.

    GIVEN:
        - Park A (park_id=10) with:
            - hourly: [2.0] (snapshot_count=12)
            - raw:    [6.0, 8.0]
        - Park B (park_id=20) with:
            - hourly: [5.0] (snapshot_count=6)
            - raw:    [1.0]

    WHEN:
        - TodayParkRankingsQuery hybrid path runs

    THEN:
        - Each park's shame_score is computed from its OWN hourly/raw data:
            park A simple_avg = (2 + 6 + 8) / 3  = 5.3̅ -> 5.3 (rounded 1 decimal)
            park B simple_avg = (5 + 1) / 2      = 3.0

        - Parks are returned independently and sorted by shame_score descending.
    """
    conn = today_hybrid_schema
    now_utc = datetime(2025, 1, 2, 14, 20, 0, tzinfo=timezone.utc)

    # Park A
    seed_meta_a = _seed_single_park_with_hourly_and_raw(
        conn,
        park_id=10,
        queue_times_id=9100,
        hourly_shames=[2.0],
        hourly_snapshot_counts=[12],
        raw_shames=[6.0, 8.0],
        now_utc=now_utc,
    )

    # Park B
    seed_meta_b = _seed_single_park_with_hourly_and_raw(
        conn,
        park_id=20,
        queue_times_id=9200,
        hourly_shames=[5.0],
        hourly_snapshot_counts=[6],
        raw_shames=[1.0],
        now_utc=now_utc,
    )

    # All data cover the same window; choose min(first_hour_start) as start_utc
    start_utc = min(seed_meta_a["first_hour_start"], seed_meta_b["first_hour_start"])

    _force_use_hourly_tables(monkeypatch)
    _freeze_today_range(monkeypatch, start_utc=start_utc, now_utc=now_utc)

    query = TodayParkRankingsQuery(conn)
    rankings = query.get_rankings(limit=10)

    # We expect both parks to appear
    assert {r["park_id"] for r in rankings} == {10, 20}

    park_by_id = {r["park_id"]: r for r in rankings}

    simple_a = _compute_simple_average([2.0], [6.0, 8.0])
    simple_b = _compute_simple_average([5.0], [1.0])

    # Assert per-park scores match simple average behavior
    assert pytest.approx(float(park_by_id[10]["shame_score"]), rel=1e-9) == simple_a
    assert pytest.approx(float(park_by_id[20]["shame_score"]), rel=1e-9) == simple_b

    # And that sorting is by shame_score descending
    assert rankings[0]["shame_score"] >= rankings[1]["shame_score"]


def test_today_hybrid_rides_down_ignores_current_hour(today_hybrid_schema, monkeypatch):
    """
    Edge case / bug surface: rides_down is derived only from hourly_data.max_rides_down
    and ignores current hour raw data.

    GIVEN:
        - 1 hourly stat row with max_rides_down = 1
        - current hour where the single ride is DOWN in all snapshots

    WHEN:
        - TodayParkRankingsQuery hybrid path runs

    THEN:
        - rides_down in combined result is still 1 (from hourly_data),
          NOT reflecting the increased concurrent rides down in current hour.

    This documents the current behavior so a future change to include current
    hour's peak DOWN count can be regression-tested.
    """
    conn = today_hybrid_schema
    now_utc = datetime(2025, 1, 2, 16, 10, 0, tzinfo=timezone.utc)

    # Seed minimal hourly data: shame_score arbitrary, rides_down=1
    conn.execute(
        text(
            """
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (30, 9300, 'RidesDown Park', 'City', 'ST', 'US', 'America/Los_Angeles', 'Operator', TRUE)
        """
        )
    )

    conn.execute(
        text(
            """
        INSERT INTO rides (ride_id, queue_times_id, park_id, name, is_active)
        VALUES (3030, 9030, 30, 'DownRide', TRUE)
        """
        )
    )

    conn.execute(
        text(
            """
        INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score)
        VALUES (3030, 2, 2.0, 'manual_override', 1.0)
        """
        )
    )

    current_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)
    previous_hour = current_hour_start - timedelta(hours=1)

    conn.execute(
        text(
            """
        INSERT INTO park_hourly_stats (
            park_id,
            hour_start_utc,
            shame_score,
            rides_operating,
            rides_down,
            total_downtime_hours,
            weighted_downtime_hours,
            effective_park_weight,
            snapshot_count,
            park_was_open
        ) VALUES (30, :hour_start, 2.0, 10, 1, 0.5, 1.0, 10.0, 6, TRUE)
        """
        ),
        {"hour_start": previous_hour},
    )

    # For current hour, mark the ride DOWN in all snapshots; however, this
    # does not affect combined['rides_down'] at all because _combine_hourly_and_raw
    # only looks at hourly['max_rides_down'].
    for idx in range(3):
        recorded_at = current_hour_start + timedelta(minutes=5 * idx)
        conn.execute(
            text(
                """
            INSERT INTO park_activity_snapshots (park_id, recorded_at, park_appears_open, shame_score)
            VALUES (30, :recorded_at, TRUE, 5.0)
            """
            ),
            {"recorded_at": recorded_at},
        )
        conn.execute(
            text(
                """
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, status, wait_time, computed_is_open
            ) VALUES (3030, :recorded_at, 'DOWN', 0, FALSE)
            """
            ),
            {"recorded_at": recorded_at},
        )

    conn.commit()

    _force_use_hourly_tables(monkeypatch)
    _freeze_today_range(monkeypatch, start_utc=previous_hour, now_utc=now_utc)

    query = TodayParkRankingsQuery(conn)
    rankings = query.get_rankings(limit=10)

    assert len(rankings) == 1
    result = rankings[0]

    # rides_down is 1 from hourly_data.max_rides_down, despite all
    # current-hour snapshots being DOWN.
    assert result["rides_down"] == 1

    # Surface the inconsistency for debugging / future fix
    print(
        "\n[DEBUG] TODAY hybrid rides_down = "
        f"{result['rides_down']} (ignores current-hour DOWN snapshots)"
    )
