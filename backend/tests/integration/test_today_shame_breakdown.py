# backend/tests/integration/test_today_shame_breakdown.py
"""
Integration test for TODAY shame breakdown API.

Bug: get_park_today_shame_breakdown() doesn't return a 'rides' array,
causing the frontend to display "No downtime data available for today yet"
even when shame_score > 0.

Fix: Query ride_hourly_stats table to get ride-level downtime data for today.

This is a TDD regression test - written RED first to expose the bug.
"""

from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import text


@pytest.fixture
def today_shame_test_data(mysql_session):
    """
    Insert test data for today shame breakdown testing.

    Creates:
    - 1 test park
    - 3 test rides with classifications
    - RideHourlyStats entries with downtime (for today)
    - ParkHourlyStats entries (for shame_score)
    """
    conn = mysql_session

    # Use high IDs to avoid conflicts
    park_id = 990002
    ride_ids = [990002, 990003, 990004]

    # Clean up any existing test data
    for rid in ride_ids:
        conn.execute(text("DELETE FROM ride_hourly_stats WHERE ride_id = :rid"), {"rid": rid})
        conn.execute(text("DELETE FROM ride_classifications WHERE ride_id = :rid"), {"rid": rid})
        conn.execute(text("DELETE FROM rides WHERE ride_id = :rid"), {"rid": rid})
    conn.execute(text("DELETE FROM park_hourly_stats WHERE park_id = :park_id"), {"park_id": park_id})
    conn.execute(text("DELETE FROM parks WHERE park_id = :park_id"), {"park_id": park_id})

    # Create test park
    conn.execute(text("""
        INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_active)
        VALUES (:park_id, 990002, 'Today Shame Test Park', 'Test City', 'TC', 'US', 'America/Los_Angeles', 'Test Operator', TRUE)
    """), {"park_id": park_id})

    # Create test rides with classifications
    # tier_weight in ride_classifications is stored as 1, 2, or 3 (actual multipliers applied at calculation time)
    rides = [
        (ride_ids[0], "Test Coaster Alpha", 1, 3),  # Tier 1, stored weight=3 (applied as 10x)
        (ride_ids[1], "Test Coaster Beta", 2, 2),   # Tier 2, stored weight=2 (applied as 5x)
        (ride_ids[2], "Test Spinner Gamma", 3, 1),  # Tier 3, stored weight=1 (applied as 2x)
    ]

    for rid, name, tier, weight in rides:
        conn.execute(text("""
            INSERT INTO rides (ride_id, park_id, queue_times_id, name, is_active)
            VALUES (:ride_id, :park_id, :queue_times_id, :name, TRUE)
        """), {"ride_id": rid, "park_id": park_id, "queue_times_id": rid, "name": name})

        conn.execute(text("""
            INSERT INTO ride_classifications (ride_id, tier, tier_weight, category, classification_method)
            VALUES (:ride_id, :tier, :tier_weight, 'ATTRACTION', 'manual_override')
        """), {"ride_id": rid, "tier": tier, "tier_weight": weight})

    # Create ride_hourly_stats entries with downtime for today (current UTC hour)
    # Test Coaster Alpha: 2.0 hours downtime
    # Test Coaster Beta: 1.5 hours downtime
    # Test Spinner Gamma: 1.0 hours downtime
    now_utc = datetime.utcnow()
    hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

    ride_downtimes = [
        (ride_ids[0], 2.0, True),
        (ride_ids[1], 1.5, True),
        (ride_ids[2], 1.0, True),
    ]

    for rid, downtime, operated in ride_downtimes:
        conn.execute(text("""
            INSERT INTO ride_hourly_stats (
                ride_id, park_id, hour_start_utc,
                avg_wait_time_minutes, operating_snapshots, down_snapshots,
                downtime_hours, uptime_percentage, snapshot_count, ride_operated
            )
            VALUES (
                :ride_id, :park_id, :hour_start_utc,
                0, 0, 12,
                :downtime_hours, 0, 12, :ride_operated
            )
        """), {
            "ride_id": rid,
            "park_id": park_id,
            "hour_start_utc": hour_start,
            "downtime_hours": downtime,
            "ride_operated": operated,
        })

    # Create park_hourly_stats entry for today (needed for shame_score)
    # Applied tier weights: Tier 1 = 10, Tier 2 = 5, Tier 3 = 2
    # Total weighted downtime: 2.0*10 + 1.5*5 + 1.0*2 = 20 + 7.5 + 2 = 29.5
    # Effective park weight: 10 + 5 + 2 = 17
    # Shame score = 29.5 / 17 * 10 = 17.35 (capped at 10) → 10.0
    conn.execute(text("""
        INSERT INTO park_hourly_stats (
            park_id, hour_start_utc, shame_score,
            total_downtime_hours, weighted_downtime_hours, effective_park_weight,
            rides_operating, rides_down, avg_wait_time_minutes,
            snapshot_count, park_was_open
        )
        VALUES (
            :park_id, :hour_start_utc, :shame_score,
            :total_downtime_hours, :weighted_downtime_hours, :effective_park_weight,
            0, 3, 0,
            12, TRUE
        )
    """), {
        "park_id": park_id,
        "hour_start_utc": hour_start,
        "shame_score": 10.0,  # Capped shame score
        "total_downtime_hours": 4.5,  # 2.0 + 1.5 + 1.0
        "weighted_downtime_hours": 29.5,
        "effective_park_weight": 17.0,
    })

    conn.commit()

    return {
        "park_id": park_id,
        "ride_ids": ride_ids,
        "expected_rides": [
            {"ride_id": ride_ids[0], "ride_name": "Test Coaster Alpha", "tier": 1, "downtime_hours": 2.0},
            {"ride_id": ride_ids[1], "ride_name": "Test Coaster Beta", "tier": 2, "downtime_hours": 1.5},
            {"ride_id": ride_ids[2], "ride_name": "Test Spinner Gamma", "tier": 3, "downtime_hours": 1.0},
        ],
        "expected_shame_score": 10.0,
        "expected_total_downtime": 4.5,
        "expected_weighted_downtime": 29.5,
    }


class TestTodayShameBreakdown:
    """
    Test that get_park_today_shame_breakdown returns rides array.

    Bug: The method currently only returns shame_score statistics,
    but the frontend expects a 'rides' array to display the breakdown.
    """

    def test_returns_rides_array(self, mysql_session, today_shame_test_data):
        """
        TDD RED: Verify get_park_today_shame_breakdown returns a 'rides' array.

        The bug causes this to fail because 'rides' key is missing.
        """
        from database.repositories.stats_repository import StatsRepository

        park_id = today_shame_test_data["park_id"]

        repo = StatsRepository(mysql_session)
        result = repo.get_park_today_shame_breakdown(park_id)

        # CRITICAL: Must have 'rides' key
        assert "rides" in result, (
            f"get_park_today_shame_breakdown must return a 'rides' array. "
            f"Got keys: {list(result.keys())}"
        )

        # rides should be a list
        assert isinstance(result["rides"], list), (
            f"'rides' must be a list, got {type(result['rides'])}"
        )

    def test_rides_array_contains_downtime_data(self, mysql_session, today_shame_test_data):
        """
        TDD RED: Verify rides array contains the expected downtime data.
        """
        from database.repositories.stats_repository import StatsRepository

        park_id = today_shame_test_data["park_id"]
        expected_rides = today_shame_test_data["expected_rides"]

        repo = StatsRepository(mysql_session)
        result = repo.get_park_today_shame_breakdown(park_id)

        rides = result.get("rides", [])

        # Should have 3 rides with downtime
        assert len(rides) >= 3, (
            f"Expected at least 3 rides with downtime, got {len(rides)}: {rides}"
        )

        # Verify ride structure
        for ride in rides:
            assert "ride_id" in ride, "Each ride must have 'ride_id'"
            assert "ride_name" in ride, "Each ride must have 'ride_name'"
            assert "tier" in ride, "Each ride must have 'tier'"
            assert "downtime_hours" in ride, "Each ride must have 'downtime_hours'"
            assert "weighted_contribution" in ride, "Each ride must have 'weighted_contribution'"

    def test_rides_sorted_by_weighted_contribution_desc(self, mysql_session, today_shame_test_data):
        """
        TDD RED: Verify rides are sorted by weighted contribution (highest first).
        """
        from database.repositories.stats_repository import StatsRepository

        park_id = today_shame_test_data["park_id"]

        repo = StatsRepository(mysql_session)
        result = repo.get_park_today_shame_breakdown(park_id)

        rides = result.get("rides", [])

        if len(rides) >= 2:
            # Verify descending order by weighted_contribution
            for i in range(len(rides) - 1):
                assert rides[i]["weighted_contribution"] >= rides[i + 1]["weighted_contribution"], (
                    f"Rides should be sorted by weighted_contribution descending. "
                    f"Got {rides[i]['weighted_contribution']} before {rides[i + 1]['weighted_contribution']}"
                )

    def test_shame_score_rounded_to_one_decimal(self, mysql_session, today_shame_test_data):
        """
        TDD RED: Verify shame_score is rounded to 1 decimal place.

        Bug: Rankings uses round(x, 1), Details uses round(x, 2), causing mismatch.
        """
        from database.repositories.stats_repository import StatsRepository

        park_id = today_shame_test_data["park_id"]

        repo = StatsRepository(mysql_session)
        result = repo.get_park_today_shame_breakdown(park_id)

        shame_score = result.get("shame_score", 0)

        # Verify it's rounded to 1 decimal place
        # A number rounded to 1 decimal will equal itself when rounded again
        assert round(shame_score, 1) == shame_score, (
            f"shame_score should be rounded to 1 decimal place. "
            f"Got {shame_score} which rounds to {round(shame_score, 1)}"
        )

    def test_consistency_with_rankings_shame_score(self, mysql_session, today_shame_test_data):
        """
        Verify shame_score matches what rankings API would return.

        The whole point of the ORM refactoring was single source of truth.
        """
        from database.repositories.stats_repository import StatsRepository

        park_id = today_shame_test_data["park_id"]
        expected_shame_score = today_shame_test_data["expected_shame_score"]

        repo = StatsRepository(mysql_session)
        result = repo.get_park_today_shame_breakdown(park_id)

        # Both should return the same value (from park_hourly_stats)
        assert result.get("shame_score") == expected_shame_score, (
            f"Breakdown shame_score ({result.get('shame_score')}) should match "
            f"rankings shame_score ({expected_shame_score})"
        )
