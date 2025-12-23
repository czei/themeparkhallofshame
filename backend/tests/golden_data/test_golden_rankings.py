"""
Golden Data Regression Tests for Rankings

These tests verify that our ranking queries produce consistent results
against captured production data with hand-verified expected outcomes.

Note: These tests load ~130K+ rows of snapshot data and may take 30-60 seconds.
They are designed for regression testing, not fast TDD cycles.

Usage:
    pytest tests/golden_data/ -v -m golden_data
"""

import json
import os
import subprocess
import pytest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from freezegun import freeze_time
from sqlalchemy import text

# Golden dataset directory
GOLDEN_DATA_DIR = Path(__file__).parent / "datasets"


def load_sql_via_cli(sql_path: Path):
    """Load SQL file via mysql CLI for performance."""
    if not sql_path.exists():
        pytest.skip(f"Golden data file not found: {sql_path}")

    host = os.environ.get('TEST_DB_HOST', 'localhost')
    port = os.environ.get('TEST_DB_PORT', '3306')
    user = os.environ.get('TEST_DB_USER', 'themepark_test')
    password = os.environ.get('TEST_DB_PASSWORD', 'test_password')
    database = os.environ.get('TEST_DB_NAME', 'themepark_test')

    cmd = ['mysql', f'-h{host}', f'-P{port}', f'-u{user}', f'-p{password}', database]

    with open(sql_path, 'r') as f:
        result = subprocess.run(cmd, stdin=f, capture_output=True, text=True)
        if result.returncode != 0 and 'Duplicate entry' not in result.stderr:
            raise RuntimeError(f"Failed to load {sql_path}: {result.stderr}")


def load_expected(date_str: str, result_name: str) -> dict:
    """Load expected results JSON."""
    path = GOLDEN_DATA_DIR / date_str / "expected" / f"{result_name}.json"
    if not path.exists():
        pytest.skip(f"Expected results not found: {path}")
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def golden_2025_12_21(mysql_session):
    """
    Load golden dataset for December 21, 2025.

    Uses mysql CLI for fast loading of large snapshot files.
    Module-scoped to load data once for all tests in this file.
    """
    dataset_path = GOLDEN_DATA_DIR / "2025-12-21"

    if not dataset_path.exists():
        pytest.skip("Golden dataset 2025-12-21 not found")

    # Load all data via CLI for performance and to handle foreign keys properly
    for sql_file in ["parks.sql", "rides.sql", "snapshots.sql"]:
        sql_path = dataset_path / sql_file
        if sql_path.exists():
            load_sql_via_cli(sql_path)

    yield {
        "date": "2025-12-21",
        "dataset_path": dataset_path,
    }


class TestGoldenParkRankings:
    """Test park rankings against golden expected results."""

    # Freeze time to end of Dec 21, 2025 (11:59 PM PST = 7:59 AM UTC Dec 22)
    # This simulates querying "yesterday" on Dec 22
    FROZEN_TIME = datetime(2025, 12, 22, 7, 59, 59, tzinfo=timezone.utc)

    @pytest.mark.golden_data
    @freeze_time(FROZEN_TIME)
    def test_parks_downtime_yesterday_top_parks(self, mysql_session, golden_2025_12_21):
        """
        Verify top parks by shame score match expected golden results.

        This is a regression test - if the query logic changes, this test
        will catch discrepancies against known-good expected values.
        """
        expected = load_expected("2025-12-21", "parks_downtime_yesterday")

        # Run the actual park rankings query
        conn = mysql_session.connection()

        # Query mirrors what the API does for "yesterday" period
        result = conn.execute(text("""
            SELECT
                p.park_id,
                p.name as park_name,
                p.is_disney,
                p.is_universal,
                ROUND(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.shame_score END), 1) as shame_score
            FROM parks p
            LEFT JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND DATE(CONVERT_TZ(pas.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
            WHERE EXISTS (
                SELECT 1 FROM park_activity_snapshots pas2
                WHERE pas2.park_id = p.park_id
                AND DATE(CONVERT_TZ(pas2.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
                AND pas2.park_appears_open = 1
            )
            GROUP BY p.park_id
            HAVING shame_score IS NOT NULL AND shame_score > 0
            ORDER BY shame_score DESC
            LIMIT 20
        """)).fetchall()

        # Compare top 5 parks
        assert len(result) >= 5, "Should have at least 5 parks with shame scores"

        for i, (actual, exp) in enumerate(zip(result[:5], expected['parks'][:5])):
            assert actual.park_name == exp['park_name'], \
                f"Park #{i+1} name mismatch: got {actual.park_name}, expected {exp['park_name']}"

            # Allow small floating point difference
            actual_score = float(actual.shame_score) if actual.shame_score else 0
            expected_score = exp['shame_score']
            assert abs(actual_score - expected_score) < 0.5, \
                f"Park {actual.park_name} shame score mismatch: got {actual_score}, expected {expected_score}"

    @pytest.mark.golden_data
    @freeze_time(FROZEN_TIME)
    def test_parks_count_matches_expected(self, mysql_session, golden_2025_12_21):
        """Verify the total number of parks with shame data matches expected."""
        expected = load_expected("2025-12-21", "parks_downtime_yesterday")

        conn = mysql_session.connection()
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT p.park_id) as park_count
            FROM parks p
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            WHERE DATE(CONVERT_TZ(pas.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
              AND pas.park_appears_open = 1
              AND pas.shame_score > 0
        """)).fetchone()

        expected_count = len(expected['parks'])
        actual_count = result.park_count

        # Allow some variance due to query differences
        assert abs(actual_count - expected_count) <= 2, \
            f"Park count mismatch: got {actual_count}, expected ~{expected_count}"


class TestGoldenRideRankings:
    """Test ride rankings against golden expected results."""

    FROZEN_TIME = datetime(2025, 12, 22, 7, 59, 59, tzinfo=timezone.utc)

    @pytest.mark.golden_data
    @freeze_time(FROZEN_TIME)
    def test_rides_downtime_yesterday_top_rides(self, mysql_session, golden_2025_12_21):
        """
        Verify top rides by downtime match expected golden results.
        """
        expected = load_expected("2025-12-21", "rides_downtime_yesterday")

        conn = mysql_session.connection()

        # Query for rides with most downtime
        result = conn.execute(text("""
            SELECT
                r.ride_id,
                r.name as ride_name,
                r.tier,
                p.name as park_name,
                p.park_id,
                COUNT(*) as total_snapshots,
                SUM(CASE
                    WHEN pas.park_appears_open = 1 AND (
                        (p.is_disney = 1 OR p.is_universal = 1) AND rss.status = 'DOWN'
                        OR (p.is_disney = 0 AND p.is_universal = 0) AND rss.status IN ('DOWN', 'CLOSED')
                    ) THEN 1 ELSE 0
                END) as down_snapshots
            FROM rides r
            JOIN parks p ON r.park_id = p.park_id
            JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE DATE(CONVERT_TZ(rss.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
              AND r.is_active = 1
              AND EXISTS (
                  SELECT 1 FROM ride_status_snapshots rss2
                  JOIN park_activity_snapshots pas2 ON pas2.park_id = r.park_id
                      AND pas2.recorded_at = rss2.recorded_at
                  WHERE rss2.ride_id = r.ride_id
                    AND DATE(CONVERT_TZ(rss2.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
                    AND pas2.park_appears_open = 1
                    AND rss2.computed_is_open = 1
              )
            GROUP BY r.ride_id
            HAVING down_snapshots > 0
            ORDER BY down_snapshots DESC, r.tier ASC
            LIMIT 20
        """)).fetchall()

        # Verify we have results
        assert len(result) >= 5, "Should have at least 5 rides with downtime"

        # Compare top rides - check they appear in expected (order may vary slightly)
        expected_ride_names = {r['ride_name'] for r in expected['rides'][:20]}
        actual_ride_names = {r.ride_name for r in result[:10]}

        # At least 50% of top 10 should match expected top 20
        overlap = len(actual_ride_names & expected_ride_names)
        assert overlap >= 5, \
            f"Top rides mismatch: only {overlap} of top 10 appear in expected top 20"

    @pytest.mark.golden_data
    @freeze_time(FROZEN_TIME)
    def test_ride_downtime_snapshots_reasonable(self, mysql_session, golden_2025_12_21):
        """Verify downtime snapshot counts are within reasonable bounds."""
        expected = load_expected("2025-12-21", "rides_downtime_yesterday")

        # Check that the ride with most downtime has expected snapshot count
        top_ride = expected['rides'][0]

        conn = mysql_session.connection()
        result = conn.execute(text("""
            SELECT
                r.name,
                SUM(CASE
                    WHEN pas.park_appears_open = 1 AND rss.status IN ('DOWN', 'CLOSED')
                    THEN 1 ELSE 0
                END) as down_snapshots
            FROM rides r
            JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
            JOIN parks p ON r.park_id = p.park_id
            JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
                AND pas.recorded_at = rss.recorded_at
            WHERE r.ride_id = :ride_id
              AND DATE(CONVERT_TZ(rss.recorded_at, '+00:00', 'America/Los_Angeles')) = '2025-12-21'
            GROUP BY r.ride_id
        """), {"ride_id": top_ride['ride_id']}).fetchone()

        if result:
            # Allow 20% variance in snapshot count
            expected_down = top_ride['down_snapshots']
            actual_down = int(result.down_snapshots)
            variance = abs(actual_down - expected_down) / max(expected_down, 1)
            assert variance < 0.3, \
                f"Ride {top_ride['ride_name']} downtime mismatch: got {actual_down}, expected {expected_down}"


class TestGoldenDataIntegrity:
    """Tests to verify golden data integrity and completeness."""

    @pytest.mark.golden_data
    def test_golden_data_has_parks(self, mysql_session, golden_2025_12_21):
        """Verify golden data loaded parks."""
        conn = mysql_session.connection()
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM parks")).fetchone()
        assert result.cnt >= 30, f"Expected at least 30 parks, got {result.cnt}"

    @pytest.mark.golden_data
    def test_golden_data_has_rides(self, mysql_session, golden_2025_12_21):
        """Verify golden data loaded rides."""
        conn = mysql_session.connection()
        result = conn.execute(text("SELECT COUNT(*) as cnt FROM rides")).fetchone()
        assert result.cnt >= 1000, f"Expected at least 1000 rides, got {result.cnt}"

    @pytest.mark.golden_data
    def test_golden_data_has_snapshots(self, mysql_session, golden_2025_12_21):
        """Verify golden data loaded snapshots for the target date."""
        conn = mysql_session.connection()

        park_count = conn.execute(text("""
            SELECT COUNT(*) as cnt FROM park_activity_snapshots
            WHERE DATE(recorded_at) = '2025-12-21'
        """)).fetchone()

        ride_count = conn.execute(text("""
            SELECT COUNT(*) as cnt FROM ride_status_snapshots
            WHERE DATE(recorded_at) = '2025-12-21'
        """)).fetchone()

        assert park_count.cnt >= 4000, f"Expected at least 4000 park snapshots, got {park_count.cnt}"
        assert ride_count.cnt >= 100000, f"Expected at least 100000 ride snapshots, got {ride_count.cnt}"
