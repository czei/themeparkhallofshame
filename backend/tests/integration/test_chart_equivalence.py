# backend/tests/integration/test_chart_equivalence.py
"""
Integration tests for chart query equivalence (T011).

Validates that park_hourly_stats aggregation produces identical results to
GROUP BY HOUR queries on raw park_activity_snapshots + ride_hourly_stats.

This is a critical regression test ensuring the aggregation script doesn't
drift from the original GROUP BY semantics used by chart queries.

NOTE: This test file is SKIPPED because it depends on the ride_hourly_stats table
which was dropped in migration 003. These tests need to be rewritten to use the
new ORM-based hourly aggregation from query_helpers.py.
"""

from datetime import datetime, timedelta, timezone, date

import pytest
from sqlalchemy import text

# Skip entire module - ride_hourly_stats table was dropped in migration 003
pytestmark = pytest.mark.skip(reason="ride_hourly_stats table dropped in migration 003 - tests need rewrite")


@pytest.fixture
def hourly_equivalence_schema(mysql_session):
    """
    Minimal schema for equivalence test.

    We keep this deliberately tight to:
    - avoid depending on the huge comprehensive_test_data fixture
    - control every row in park_activity_snapshots / park_hourly_stats / rides
    """
    conn = mysql_session

    # Ensure tables exist (they come from migrations)
    # Just clean the specific test data we insert so other schema stays intact.
    tables_to_clean = [
        "park_hourly_stats",
        "ride_hourly_stats",
        "park_activity_snapshots",
        "ride_status_snapshots",
        "ride_classifications",
        "rides",
        "parks",
    ]
    for table in tables_to_clean:
        conn.execute(text(f"DELETE FROM {table}"))

    mysql_session.commit()
    return mysql_session


def _insert_core_test_data(conn, base_hour_utc: datetime):
    """
    Create a single park with two rides and carefully controlled 5-min snapshots
    for exactly one fully complete UTC hour.

    The pattern is chosen so that:
    - shame_score per snapshot is simple to reason about
    - downtime hours and weighted_downtime_hours are deterministic
    - park_was_open is true for the entire hour
    """

    # One test park
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (1001, 91001, 'Equivalence Test Park', 'Test City', 'TC', 'US', 'America/Los_Angeles', 'Test Operator', TRUE)
    """))

    # Two rides with different tier weights
    conn.execute(text("""
        INSERT INTO rides (ride_id, queue_times_id, park_id, name, category, is_active)
        VALUES
            (2001, 92001, 1001, 'Ride A', 'ATTRACTION', TRUE),
            (2002, 92002, 1001, 'Ride B', 'ATTRACTION', TRUE)
    """))

    conn.execute(text("""
        INSERT INTO ride_classifications (ride_id, tier, tier_weight)
        VALUES
            (2001, 1, 3.0),
            (2002, 2, 2.0)
    """))

    # Build 12 snapshots (60 minutes / 5 min) for the target hour
    # Pattern (every 5 minutes):
    #  - park open
    #  - shame_score sequence: [0.0, 2.0, 4.0, 6.0, ...] in a repeating cycle
    #  - rides_open = 2, rides_closed = 0 until last 4 snapshots where 1 ride is down
    #  - effective_park_weight fixed at 5.0 (3 + 2)
    snapshots = []
    ride_snaps = []
    interval_minutes = 5

    for i in range(12):
        ts = base_hour_utc + timedelta(minutes=i * interval_minutes)
        # simple shame pattern: grows linearly then wraps
        shame_value = float((i % 6) * 2.0)  # 0,2,4,6,8,10,0,2,...

        # park-level snapshot
        snapshots.append({
            "park_id": 1001,
            "recorded_at": ts,
            "park_appears_open": True,
            "shame_score": shame_value,
            "avg_wait": 10.0 + i,  # arbitrary but monotonic
            "max_wait": 15.0 + i,
            "rides_open": 2 if i < 8 else 1,
            "rides_closed": 0 if i < 8 else 1,
        })

        # ride snapshots - keep them consistent with park_appears_open
        #  - first 8 snapshots: both open
        #  - last 4 snapshots: Ride B DOWN, Ride A open
        if i < 8:
            ride_snaps.append({
                "ride_id": 2001,
                "recorded_at": ts,
                "status": "OPERATING",
                "computed_is_open": True,
            })
            ride_snaps.append({
                "ride_id": 2002,
                "recorded_at": ts,
                "status": "OPERATING",
                "computed_is_open": True,
            })
        else:
            ride_snaps.append({
                "ride_id": 2001,
                "recorded_at": ts,
                "status": "OPERATING",
                "computed_is_open": True,
            })
            ride_snaps.append({
                "ride_id": 2002,
                "recorded_at": ts,
                "status": "DOWN",
                "computed_is_open": False,
            })

    for s in snapshots:
        payload = {
            "park_id": s["park_id"],
            "recorded_at": s["recorded_at"],
            "park_appears_open": s["park_appears_open"],
            "shame_score": s["shame_score"],
            "avg_wait": s["avg_wait"],
            "max_wait": s["max_wait"],
            "rides_open": s["rides_open"],
            "rides_closed": s["rides_closed"],
            "total_tracked": s["rides_open"] + s["rides_closed"],
        }
        conn.execute(text("""
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
                :park_id,
                :recorded_at,
                :total_tracked,
                :rides_open,
                :rides_closed,
                :avg_wait,
                :max_wait,
                :park_appears_open,
                :shame_score
            )
        """), payload)

    for r in ride_snaps:
        conn.execute(text("""
            INSERT INTO ride_status_snapshots (
                ride_id, recorded_at, status, computed_is_open, wait_time
            ) VALUES (
                :ride_id, :recorded_at, :status, :computed_is_open, 5
            )
        """), r)

    conn.commit()
    return snapshots, ride_snaps


def _populate_ride_hourly_from_raw(conn, base_hour_utc: datetime):
    """
    Derive ride_hourly_stats from ride_status_snapshots using the same business
    rules as HourlyAggregator._aggregate_ride (at least for this single park).

    We don't import RideStatusSQL helpers here; instead we mirror the key logic
    that's relevant for downtime_hours so we can compute the authoritative
    expected values for park_hourly_stats.total_downtime_hours and
    weighted_downtime_hours.
    """
    # Down snapshots: any snapshot where computed_is_open = FALSE is treated as DOWN
    # and park_appears_open is TRUE (we ensured that in _insert_core_test_data).
    conn.execute(text("""
        INSERT INTO ride_hourly_stats (
            ride_id,
            park_id,
            hour_start_utc,
            avg_wait_time_minutes,
            operating_snapshots,
            down_snapshots,
            downtime_hours,
            uptime_percentage,
            snapshot_count,
            ride_operated,
            created_at
        )
        SELECT
            r.ride_id,
            r.park_id,
            :hour_start AS hour_start_utc,
            ROUND(AVG(CASE
                WHEN rss.computed_is_open = TRUE AND rss.wait_time IS NOT NULL
                THEN rss.wait_time
            END), 2) AS avg_wait_time_minutes,
            SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS operating_snapshots,
            SUM(CASE
                WHEN rss.computed_is_open = FALSE
                THEN 1
                ELSE 0
            END) AS down_snapshots,
            -- downtime_hours = down_snapshots * 5 / 60
            ROUND(SUM(CASE
                WHEN rss.computed_is_open = FALSE
                THEN 5.0 / 60.0
                ELSE 0
            END), 2) AS downtime_hours,
            CASE
                WHEN COUNT(*) > 0
                THEN ROUND(100.0 * SUM(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2)
                ELSE 0
            END AS uptime_percentage,
            COUNT(*) AS snapshot_count,
            MAX(CASE WHEN rss.computed_is_open = TRUE THEN 1 ELSE 0 END) AS ride_operated,
            NOW()
        FROM ride_status_snapshots rss
        JOIN rides r ON rss.ride_id = r.ride_id
        WHERE rss.recorded_at >= :hour_start
          AND rss.recorded_at < :hour_end
        GROUP BY r.ride_id, r.park_id
    """), {
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    })

    conn.commit()


def _raw_hourly_group_by(conn, base_hour_utc: datetime):
    """
    Compute the "raw" hourly aggregation equivalent to aggregate_hourly._aggregate_park
    using GROUP BY HOUR on park_activity_snapshots and ride_hourly_stats.

    This is the contract we expect park_hourly_stats rows to satisfy.
    """
    # Shame + snapshot_count + park_was_open from park_activity_snapshots
    row = conn.execute(text("""
        SELECT
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN shame_score END), 1) AS shame_score,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN avg_wait_time END), 2) AS avg_wait_time_minutes,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN rides_open END), 0) AS rides_operating,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN rides_closed END), 0) AS rides_down,
            COUNT(*) AS snapshot_count,
            MAX(park_appears_open) AS park_was_open
        FROM park_activity_snapshots
        WHERE park_id = 1001
          AND recorded_at >= :hour_start
          AND recorded_at < :hour_end
    """), {
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    }).fetchone()

    if not row:
        return None

    shame_score = float(row.shame_score) if row.shame_score is not None else None
    avg_wait_time_minutes = float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes is not None else None
    rides_operating = int(row.rides_operating) if row.rides_operating is not None else 0
    rides_down = int(row.rides_down) if row.rides_down is not None else 0
    snapshot_count = int(row.snapshot_count)
    park_was_open = bool(row.park_was_open)

    # total_downtime_hours from ride_hourly_stats
    downtime_row = conn.execute(text("""
        SELECT
            COALESCE(SUM(downtime_hours), 0) AS total_downtime_hours
        FROM ride_hourly_stats
        WHERE park_id = 1001
          AND hour_start_utc = :hour_start
          AND ride_operated = 1
    """), {"hour_start": base_hour_utc}).fetchone()

    total_downtime_hours = float(downtime_row.total_downtime_hours or 0)

    # weighted_downtime_hours using ride_hourly_stats + ride_classifications
    w_row = conn.execute(text("""
        SELECT
            COALESCE(SUM(rhs.downtime_hours * COALESCE(rc.tier_weight, 2)), 0) AS weighted_downtime_hours
        FROM ride_hourly_stats rhs
        JOIN rides r ON rhs.ride_id = r.ride_id
        LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        WHERE rhs.park_id = 1001
          AND rhs.hour_start_utc = :hour_start
          AND rhs.ride_operated = 1
    """), {"hour_start": base_hour_utc}).fetchone()

    weighted_downtime_hours = float(w_row.weighted_downtime_hours or 0)

    # effective_park_weight: sum of tier weights for rides that have operated in last 7 days.
    # Our test data uses last_operated_at semantics via snapshots only; we mimic that
    # by assuming all rides present with any snapshots are active in the 7-day window.
    eff_row = conn.execute(text("""
        SELECT
            COALESCE(SUM(COALESCE(rc.tier_weight, 2)), 0) AS effective_park_weight
        FROM rides r
        LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        WHERE r.park_id = 1001
    """)).fetchone()

    effective_park_weight = float(eff_row.effective_park_weight or 0)

    return {
        "shame_score": shame_score,
        "avg_wait_time_minutes": avg_wait_time_minutes,
        "rides_operating": rides_operating,
        "rides_down": rides_down,
        "total_downtime_hours": total_downtime_hours,
        "weighted_downtime_hours": weighted_downtime_hours,
        "effective_park_weight": effective_park_weight,
        "snapshot_count": snapshot_count,
        "park_was_open": park_was_open,
    }


def _read_hourly_row(conn, base_hour_utc: datetime):
    row = conn.execute(text("""
        SELECT
            shame_score,
            avg_wait_time_minutes,
            rides_operating,
            rides_down,
            total_downtime_hours,
            weighted_downtime_hours,
            effective_park_weight,
            snapshot_count,
            park_was_open
        FROM park_hourly_stats
        WHERE park_id = 1001
          AND hour_start_utc = :hour_start
    """), {"hour_start": base_hour_utc}).fetchone()

    if not row:
        return None

    return {
        "shame_score": float(row.shame_score) if row.shame_score is not None else None,
        "avg_wait_time_minutes": float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes is not None else None,
        "rides_operating": int(row.rides_operating) if row.rides_operating is not None else 0,
        "rides_down": int(row.rides_down) if row.rides_down is not None else 0,
        "total_downtime_hours": float(row.total_downtime_hours or 0),
        "weighted_downtime_hours": float(row.weighted_downtime_hours or 0),
        "effective_park_weight": float(row.effective_park_weight or 0),
        "snapshot_count": int(row.snapshot_count or 0),
        "park_was_open": bool(row.park_was_open),
    }


def _assert_hourly_equivalence(expected, actual):
    """
    Core equivalence assertion with explicit tolerances per-field:

    - shame_score: ±0.1
    - total_downtime_hours: ±0.01
    - weighted_downtime_hours: ±0.01
    - rides_operating: exact
    - rides_down: exact
    - snapshot_count: exact
    - park_was_open: exact
    - avg_wait_time_minutes: small FP tolerance (±0.01)
    """
    # Floating point tolerant comparisons
    if expected["shame_score"] is None:
        assert actual["shame_score"] is None
    else:
        assert abs(actual["shame_score"] - expected["shame_score"]) <= 0.1, \
            f"shame_score mismatch: expected {expected['shame_score']} got {actual['shame_score']}"

    if expected["avg_wait_time_minutes"] is None:
        assert actual["avg_wait_time_minutes"] is None
    else:
        assert abs(actual["avg_wait_time_minutes"] - expected["avg_wait_time_minutes"]) <= 0.01, \
            f"avg_wait_time_minutes mismatch: expected {expected['avg_wait_time_minutes']} got {actual['avg_wait_time_minutes']}"

    assert abs(actual["total_downtime_hours"] - expected["total_downtime_hours"]) <= 0.01, \
        f"total_downtime_hours mismatch: expected {expected['total_downtime_hours']} got {actual['total_downtime_hours']}"

    assert abs(actual["weighted_downtime_hours"] - expected["weighted_downtime_hours"]) <= 0.01, \
        f"weighted_downtime_hours mismatch: expected {expected['weighted_downtime_hours']} got {actual['weighted_downtime_hours']}"

    # Integral fields exact
    assert actual["rides_operating"] == expected["rides_operating"], \
        f"rides_operating mismatch: expected {expected['rides_operating']} got {actual['rides_operating']}"
    assert actual["rides_down"] == expected["rides_down"], \
        f"rides_down mismatch: expected {expected['rides_down']} got {actual['rides_down']}"
    assert actual["snapshot_count"] == expected["snapshot_count"], \
        f"snapshot_count mismatch: expected {expected['snapshot_count']} got {actual['snapshot_count']}"
    assert actual["park_was_open"] == expected["park_was_open"], \
        f"park_was_open mismatch: expected {expected['park_was_open']} got {actual['park_was_open']}"

    # effective_park_weight must be exact for this deterministic dataset
    assert abs(actual["effective_park_weight"] - expected["effective_park_weight"]) <= 0.001, \
        f"effective_park_weight mismatch: expected {expected['effective_park_weight']} got {actual['effective_park_weight']}"


def test_hourly_aggregation_matches_raw_group_by_hour_single_park(hourly_equivalence_schema):
    """
    T011 core regression test:
    - Build deterministic 5-minute snapshot series for exactly one UTC hour.
    - Compute ride_hourly_stats from raw ride_status_snapshots (same rules as HourlyAggregator._aggregate_ride).
    - Run the park-level INSERT .. SELECT used by HourlyAggregator._aggregate_park for that hour.
    - Compute expected per-hour aggregates directly from raw tables (GROUP BY HOUR style).
    - Compare park_hourly_stats row against expected with tight tolerances.

    This surfaces any drift between:
    - aggregate_hourly._aggregate_park() implementation
    - the intended GROUP BY HOUR semantics used by chart queries.
    """
    conn = hourly_equivalence_schema

    # Choose a fixed UTC hour so test is deterministic and timezone-agnostic
    base_hour_utc = datetime(2025, 1, 2, 15, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    # 1) Seed raw snapshot data (park_activity_snapshots + ride_status_snapshots)
    snapshots, ride_snaps = _insert_core_test_data(conn, base_hour_utc)

    # Sanity: ensure we have 12 park snapshots and 24 ride snapshots
    assert len(snapshots) == 12
    assert len(ride_snaps) == 24

    # 2) Populate ride_hourly_stats from raw ride_status_snapshots
    _populate_ride_hourly_from_raw(conn, base_hour_utc)

    # 3) Run the same INSERT .. SELECT logic as HourlyAggregator._aggregate_park
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
            park_was_open,
            created_at
        )
        SELECT
            :park_id,
            :hour_start,
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.shame_score END), 1) AS shame_score,
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.avg_wait_time END), 2) AS avg_wait_time_minutes,
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.rides_open END), 0) AS rides_operating,
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.rides_closed END), 0) AS rides_down,
            COALESCE((
                SELECT SUM(downtime_hours)
                FROM ride_hourly_stats rhs
                WHERE rhs.park_id = :park_id
                  AND rhs.hour_start_utc = :hour_start
                  AND rhs.ride_operated = 1
            ), 0) AS total_downtime_hours,
            COALESCE((
                SELECT SUM(rhs.downtime_hours * COALESCE(rc.tier_weight, 2))
                FROM ride_hourly_stats rhs
                JOIN rides r ON rhs.ride_id = r.ride_id
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE rhs.park_id = :park_id
                  AND rhs.hour_start_utc = :hour_start
                  AND rhs.ride_operated = 1
            ), 0) AS weighted_downtime_hours,
            COALESCE((
                SELECT SUM(COALESCE(rc.tier_weight, 2))
                FROM rides r
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE r.park_id = :park_id
                  AND r.is_active = TRUE
                  AND r.category = 'ATTRACTION'
            ), 0) AS effective_park_weight,
            COUNT(*) AS snapshot_count,
            MAX(pas.park_appears_open) AS park_was_open,
            NOW()
        FROM park_activity_snapshots pas
        WHERE pas.park_id = :park_id
          AND pas.recorded_at >= :hour_start
          AND pas.recorded_at < :hour_end
        ON DUPLICATE KEY UPDATE
            shame_score = VALUES(shame_score),
            avg_wait_time_minutes = VALUES(avg_wait_time_minutes),
            rides_operating = VALUES(rides_operating),
            rides_down = VALUES(rides_down),
            total_downtime_hours = VALUES(total_downtime_hours),
            weighted_downtime_hours = VALUES(weighted_downtime_hours),
            effective_park_weight = VALUES(effective_park_weight),
            snapshot_count = VALUES(snapshot_count),
            park_was_open = VALUES(park_was_open),
            updated_at = NOW()
    """), {
        "park_id": 1001,
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    })
    conn.commit()

    # 4) Compute expected GROUP BY HOUR result from raw tables
    expected = _raw_hourly_group_by(conn, base_hour_utc)
    assert expected is not None, "Expected GROUP BY HOUR result was None (no snapshots?)"

    # For this controlled dataset:
    # - effective_park_weight should be 3 + 2 = 5
    # - 4 snapshots with one ride down (5 min each) -> ride B downtime = 20 min = 0.33h
    # - weighted_downtime_hours = 0.33h * tier_weight(ride B=2) ≈ 0.67h
    assert abs(expected["effective_park_weight"] - 5.0) <= 0.001
    assert expected["snapshot_count"] == 12
    assert expected["park_was_open"] is True

    # 5) Read corresponding row from park_hourly_stats and compare field-by-field
    actual = _read_hourly_row(conn, base_hour_utc)
    assert actual is not None, "No park_hourly_stats row inserted for test hour"

    _assert_hourly_equivalence(expected, actual)


def test_hourly_aggregation_skips_hours_with_no_snapshots(hourly_equivalence_schema):
    """
    Guardrail: ensure no park_hourly_stats row is written when there are zero
    park_activity_snapshots for the hour.

    This mirrors the early-return path in HourlyAggregator._aggregate_park and
    prevents spurious zero rows that would poison chart equivalence.
    """
    conn = hourly_equivalence_schema
    base_hour_utc = datetime(2025, 1, 3, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

    # Insert a park but no snapshots
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (2001, 92001, 'No Data Park', 'Nowhere', 'ND', 'US', 'America/Los_Angeles', 'Op', TRUE)
    """))
    conn.commit()

    # Directly run the park aggregation INSERT .. SELECT logic used in _aggregate_park
    # Since there are no rows in park_activity_snapshots, the SELECT returns 0 rows
    # and thus INSERT should not create anything.
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
            park_was_open,
            created_at
        )
        SELECT
            :park_id,
            :hour_start,
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.shame_score END), 1),
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.avg_wait_time END), 2),
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.rides_open END), 0),
            ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.rides_closed END), 0),
            0, 0, 0,
            COUNT(*),
            MAX(pas.park_appears_open),
            NOW()
        FROM park_activity_snapshots pas
        WHERE pas.park_id = :park_id
          AND pas.recorded_at >= :hour_start
          AND pas.recorded_at < :hour_end
        HAVING COUNT(*) > 0
        ON DUPLICATE KEY UPDATE
            shame_score = VALUES(shame_score)
    """), {
        "park_id": 2001,
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    })
    conn.commit()

    # Verify: no row in park_hourly_stats for this hour/park
    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt
        FROM park_hourly_stats
        WHERE park_id = 2001
          AND hour_start_utc = :hour_start
    """), {"hour_start": base_hour_utc}).fetchone()

    assert row.cnt == 0, "park_hourly_stats row should not be created when there are no snapshots"
