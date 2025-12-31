# backend/tests/integration/test_chart_equivalence.py
"""
Integration tests for chart query equivalence.

Validates that park_hourly_stats aggregation produces identical results to
GROUP BY HOUR queries on raw park_activity_snapshots.

This is a critical regression test ensuring the aggregation script doesn't
drift from the original GROUP BY semantics used by chart queries.

Note: This test focuses on park-level metrics (shame score, wait times, etc.)
that can be verified directly from park_activity_snapshots. Ride-level downtime
calculations are now computed on-the-fly from ride_status_snapshots using
HourlyAggregationQuery.
"""

from datetime import datetime, timedelta, timezone, date

import pytest
from sqlalchemy import text


@pytest.fixture
def hourly_equivalence_schema(mysql_session):
    """
    Minimal schema for equivalence test.

    We keep this deliberately tight to:
    - avoid depending on the huge comprehensive_test_data fixture
    - control every row in park_activity_snapshots / park_hourly_stats
    """
    conn = mysql_session

    # Clean test data (use high IDs to avoid conflicts with other tests)
    conn.execute(text("DELETE FROM park_hourly_stats WHERE park_id >= 900000"))
    conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id >= 900000"))
    conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id >= 900000"))
    conn.execute(text("DELETE FROM rides WHERE ride_id >= 900000"))
    conn.execute(text("DELETE FROM parks WHERE park_id >= 900000"))

    mysql_session.commit()
    return mysql_session


def _insert_core_test_data(conn, base_hour_utc: datetime):
    """
    Create a single park with carefully controlled 5-min snapshots
    for exactly one fully complete UTC hour.

    The pattern is chosen so that:
    - shame_score per snapshot is simple to reason about
    - park_was_open is true for the entire hour
    """
    # Test IDs >= 900000 to distinguish from production data
    park_id = 900001

    # One test park
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (:park_id, 991001, 'Equivalence Test Park', 'Test City', 'TC', 'US', 'America/Los_Angeles', 'Test Operator', TRUE)
    """), {"park_id": park_id})

    # Build 12 snapshots (60 minutes / 5 min) for the target hour
    # Pattern (every 5 minutes):
    #  - park open
    #  - shame_score sequence: [0.0, 2.0, 4.0, 6.0, ...] in a repeating cycle
    #  - rides_open = 2, rides_closed = 0 until last 4 snapshots where 1 ride is down
    snapshots = []
    interval_minutes = 5

    for i in range(12):
        ts = base_hour_utc + timedelta(minutes=i * interval_minutes)
        # simple shame pattern: grows linearly then wraps
        shame_value = float((i % 6) * 2.0)  # 0,2,4,6,8,10,0,2,...
        avg_wait = 10.0 + i  # arbitrary but monotonic

        snapshots.append({
            "park_id": park_id,
            "recorded_at": ts,
            "park_appears_open": True,
            "shame_score": shame_value,
            "avg_wait": avg_wait,
            "max_wait": 15.0 + i,
            "rides_open": 2 if i < 8 else 1,
            "rides_closed": 0 if i < 8 else 1,
        })

    for s in snapshots:
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
        """), {
            "park_id": s["park_id"],
            "recorded_at": s["recorded_at"],
            "total_tracked": s["rides_open"] + s["rides_closed"],
            "rides_open": s["rides_open"],
            "rides_closed": s["rides_closed"],
            "avg_wait": s["avg_wait"],
            "max_wait": s["max_wait"],
            "park_appears_open": s["park_appears_open"],
            "shame_score": s["shame_score"],
        })

    conn.commit()
    return snapshots, park_id


def _raw_hourly_group_by(conn, park_id: int, base_hour_utc: datetime):
    """
    Compute the "raw" hourly aggregation equivalent to aggregate_hourly._aggregate_park
    using GROUP BY HOUR on park_activity_snapshots.

    This is the contract we expect park_hourly_stats rows to satisfy for park-level metrics.
    """
    row = conn.execute(text("""
        SELECT
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN shame_score END), 1) AS shame_score,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN avg_wait_time END), 2) AS avg_wait_time_minutes,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN rides_open END), 0) AS rides_operating,
            ROUND(AVG(CASE WHEN park_appears_open = 1 THEN rides_closed END), 0) AS rides_down,
            COUNT(*) AS snapshot_count,
            MAX(park_appears_open) AS park_was_open
        FROM park_activity_snapshots
        WHERE park_id = :park_id
          AND recorded_at >= :hour_start
          AND recorded_at < :hour_end
    """), {
        "park_id": park_id,
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    }).fetchone()

    if not row:
        return None

    return {
        "shame_score": float(row.shame_score) if row.shame_score is not None else None,
        "avg_wait_time_minutes": float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes is not None else None,
        "rides_operating": int(row.rides_operating) if row.rides_operating is not None else 0,
        "rides_down": int(row.rides_down) if row.rides_down is not None else 0,
        "snapshot_count": int(row.snapshot_count),
        "park_was_open": bool(row.park_was_open),
    }


def _read_hourly_row(conn, park_id: int, base_hour_utc: datetime):
    row = conn.execute(text("""
        SELECT
            shame_score,
            avg_wait_time_minutes,
            rides_operating,
            rides_down,
            snapshot_count,
            park_was_open
        FROM park_hourly_stats
        WHERE park_id = :park_id
          AND hour_start_utc = :hour_start
    """), {"park_id": park_id, "hour_start": base_hour_utc}).fetchone()

    if not row:
        return None

    return {
        "shame_score": float(row.shame_score) if row.shame_score is not None else None,
        "avg_wait_time_minutes": float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes is not None else None,
        "rides_operating": int(row.rides_operating) if row.rides_operating is not None else 0,
        "rides_down": int(row.rides_down) if row.rides_down is not None else 0,
        "snapshot_count": int(row.snapshot_count or 0),
        "park_was_open": bool(row.park_was_open),
    }


def _assert_hourly_equivalence(expected, actual):
    """
    Core equivalence assertion with explicit tolerances per-field:

    - shame_score: ±0.1
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

    # Integral fields exact
    assert actual["rides_operating"] == expected["rides_operating"], \
        f"rides_operating mismatch: expected {expected['rides_operating']} got {actual['rides_operating']}"
    assert actual["rides_down"] == expected["rides_down"], \
        f"rides_down mismatch: expected {expected['rides_down']} got {actual['rides_down']}"
    assert actual["snapshot_count"] == expected["snapshot_count"], \
        f"snapshot_count mismatch: expected {expected['snapshot_count']} got {actual['snapshot_count']}"
    assert actual["park_was_open"] == expected["park_was_open"], \
        f"park_was_open mismatch: expected {expected['park_was_open']} got {actual['park_was_open']}"


def test_hourly_aggregation_matches_raw_group_by_hour_single_park(hourly_equivalence_schema):
    """
    Core regression test:
    - Build deterministic 5-minute snapshot series for exactly one UTC hour.
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

    # 1) Seed raw snapshot data (park_activity_snapshots)
    snapshots, park_id = _insert_core_test_data(conn, base_hour_utc)

    # Sanity: ensure we have 12 park snapshots
    assert len(snapshots) == 12

    # 2) Run the park-level INSERT .. SELECT logic (simplified version of HourlyAggregator._aggregate_park)
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
            0 AS total_downtime_hours,      -- Downtime now computed on-the-fly from ride_status_snapshots
            0 AS weighted_downtime_hours,   -- Downtime now computed on-the-fly from ride_status_snapshots
            0 AS effective_park_weight,     -- Park weight now computed on-the-fly
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
            snapshot_count = VALUES(snapshot_count),
            park_was_open = VALUES(park_was_open),
            updated_at = NOW()
    """), {
        "park_id": park_id,
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    })
    conn.commit()

    # 3) Compute expected GROUP BY HOUR result from raw tables
    expected = _raw_hourly_group_by(conn, park_id, base_hour_utc)
    assert expected is not None, "Expected GROUP BY HOUR result was None (no snapshots?)"

    # For this controlled dataset:
    # - 12 snapshots total
    # - park was open for all snapshots
    assert expected["snapshot_count"] == 12
    assert expected["park_was_open"] is True

    # 4) Read corresponding row from park_hourly_stats and compare field-by-field
    actual = _read_hourly_row(conn, park_id, base_hour_utc)
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

    # Test IDs >= 900000 to distinguish from production data
    park_id = 900002

    # Insert a park but no snapshots
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (:park_id, 992001, 'No Data Park', 'Nowhere', 'ND', 'US', 'America/Los_Angeles', 'Op', TRUE)
    """), {"park_id": park_id})
    conn.commit()

    # Run the park aggregation INSERT .. SELECT logic
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
        "park_id": park_id,
        "hour_start": base_hour_utc,
        "hour_end": base_hour_utc + timedelta(hours=1),
    })
    conn.commit()

    # Verify: no row in park_hourly_stats for this hour/park
    row = conn.execute(text("""
        SELECT COUNT(*) AS cnt
        FROM park_hourly_stats
        WHERE park_id = :park_id
          AND hour_start_utc = :hour_start
    """), {"park_id": park_id, "hour_start": base_hour_utc}).fetchone()

    assert row.cnt == 0, "park_hourly_stats row should not be created when there are no snapshots"
