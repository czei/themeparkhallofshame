"""
Single Source of Truth Tests
============================

These tests verify that all query files use the centralized SQL helpers
from utils/sql_helpers.py instead of implementing their own status logic.

CANONICAL RULE: If a park is closed, ignore ALL ride statuses.
This is enforced by RideStatusSQL.rides_that_operated_cte() which checks
both ride status AND park_appears_open.

Any query that determines ride downtime/reliability MUST use this helper.
"""

import ast
import os
import re
from pathlib import Path

import pytest


class TestSingleSourceOfTruth:
    """
    Verify that query files use centralized helpers instead of inline logic.
    """

    @pytest.fixture
    def query_files(self):
        """Get all Python files in the queries directory."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries"
        return list(queries_dir.rglob("*.py"))

    @pytest.fixture
    def today_query_files(self):
        """Get query files in today/ directory that need rides_that_operated CTE."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today"
        return [f for f in queries_dir.glob("*.py") if f.name != "__init__.py"]

    @pytest.fixture
    def trends_query_files(self):
        """Get query files in trends/ directory that need rides_that_operated CTE."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends"
        # Only least_reliable_rides.py needs the CTE for today period
        return [f for f in queries_dir.glob("least_reliable_rides.py")]

    def test_rides_that_operated_cte_exists_in_sql_helpers(self):
        """Verify the centralized CTE helper exists."""
        from utils.sql_helpers import RideStatusSQL

        assert hasattr(RideStatusSQL, "rides_that_operated_cte"), (
            "RideStatusSQL.rides_that_operated_cte() must exist as the "
            "single source of truth for determining which rides operated"
        )

    def test_rides_that_operated_cte_checks_park_status(self):
        """Verify the CTE helper checks park_appears_open."""
        from utils.sql_helpers import RideStatusSQL

        cte_sql = RideStatusSQL.rides_that_operated_cte()

        assert "park_activity_snapshots" in cte_sql, (
            "rides_that_operated_cte must join park_activity_snapshots "
            "to check park status"
        )
        assert "park_appears_open" in cte_sql, (
            "rides_that_operated_cte must check park_appears_open = TRUE "
            "to enforce Rule 1: Park status takes precedence over ride status"
        )

    def test_rides_that_operated_cte_checks_ride_operating(self):
        """Verify the CTE helper checks ride operating status."""
        from utils.sql_helpers import RideStatusSQL

        cte_sql = RideStatusSQL.rides_that_operated_cte()

        # Should check for OPERATING status or computed_is_open
        assert "OPERATING" in cte_sql or "computed_is_open" in cte_sql, (
            "rides_that_operated_cte must check for OPERATING status or "
            "computed_is_open to determine if ride operated"
        )

    def test_today_ride_rankings_uses_centralized_cte(self):
        """Verify today_ride_rankings.py uses the centralized CTE."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today"
        file_path = queries_dir / "today_ride_rankings.py"

        content = file_path.read_text()

        assert "rides_that_operated_cte" in content, (
            f"{file_path.name} must use RideStatusSQL.rides_that_operated_cte() "
            "instead of implementing its own has_operated logic"
        )

    def test_today_park_rankings_uses_centralized_cte(self):
        """Verify today_park_rankings.py uses the centralized CTE."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today"
        file_path = queries_dir / "today_park_rankings.py"

        content = file_path.read_text()

        assert "rides_that_operated_cte" in content, (
            f"{file_path.name} must use RideStatusSQL.rides_that_operated_cte() "
            "instead of implementing its own has_operated logic"
        )

    def test_least_reliable_rides_uses_centralized_cte(self):
        """Verify least_reliable_rides.py uses the centralized CTE."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "trends"
        file_path = queries_dir / "least_reliable_rides.py"

        content = file_path.read_text()

        assert "rides_that_operated_cte" in content, (
            f"{file_path.name} must use RideStatusSQL.rides_that_operated_cte() "
            "instead of implementing its own has_operated logic"
        )

    def test_no_inline_has_operated_subquery_in_today_rankings(self):
        """
        Verify today ranking files don't use inline has_operated subqueries.

        They should use the centralized CTE instead.
        """
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today"

        for file_path in queries_dir.glob("*.py"):
            if file_path.name == "__init__.py":
                continue

            content = file_path.read_text()

            # Check for inline has_operated subquery pattern that doesn't check park status
            # The old pattern looked like: EXISTS(SELECT 1 FROM ride_status_snapshots WHERE status='OPERATING')
            # without joining to park_activity_snapshots

            # If they use has_operated_subquery, it must be with park_id_expr parameter
            if "has_operated_subquery" in content:
                # Make sure it's the version with park_id_expr or they use the CTE
                assert "rides_that_operated_cte" in content or "park_id_expr" in content, (
                    f"{file_path.name} uses has_operated_subquery without park_id_expr. "
                    "Use RideStatusSQL.rides_that_operated_cte() instead to ensure "
                    "park status is checked."
                )


class TestParkStatusPrecedence:
    """
    Test that the canonical rule is enforced:
    "If a park is closed, ignore ALL ride statuses."
    """

    def test_cte_requires_park_open_for_ride_to_count(self):
        """
        The rides_that_operated CTE should only include rides where
        both the ride was operating AND the park was open.
        """
        from utils.sql_helpers import RideStatusSQL

        cte_sql = RideStatusSQL.rides_that_operated_cte()

        # The CTE must have both conditions
        has_ride_operating_check = (
            "status = 'OPERATING'" in cte_sql or
            "computed_is_open = TRUE" in cte_sql
        )
        has_park_open_check = "park_appears_open = TRUE" in cte_sql

        assert has_ride_operating_check and has_park_open_check, (
            "rides_that_operated_cte must require BOTH:\n"
            "1. Ride status is OPERATING (or computed_is_open=TRUE)\n"
            "2. Park is open (park_appears_open=TRUE)\n"
            "This enforces: 'If a park is closed, ignore ALL ride statuses.'"
        )

    def test_cte_joins_park_activity_snapshots(self):
        """
        The CTE must join park_activity_snapshots to get park status
        at the same timestamp as the ride snapshot.
        """
        from utils.sql_helpers import RideStatusSQL

        cte_sql = RideStatusSQL.rides_that_operated_cte()

        # Should join on both park_id and recorded_at
        assert "park_activity_snapshots" in cte_sql, (
            "CTE must join park_activity_snapshots"
        )
        assert "recorded_at" in cte_sql, (
            "CTE must match park status timestamp with ride snapshot timestamp"
        )


class TestNoViolationsOfDRY:
    """
    Scan query files for patterns that would violate DRY principles
    by implementing their own ride status logic.
    """

    @pytest.fixture
    def query_file_contents(self):
        """Load all query file contents."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries"
        contents = {}
        for file_path in queries_dir.rglob("*.py"):
            if file_path.name != "__init__.py":
                contents[file_path.name] = file_path.read_text()
        return contents

    def test_no_inline_exists_operating_without_park_check(self, query_file_contents):
        """
        Detect inline EXISTS subqueries that check ride status without
        also checking park status.

        BAD: EXISTS(SELECT 1 FROM ride_status_snapshots WHERE status='OPERATING')
        GOOD: Use RideStatusSQL.rides_that_operated_cte()
        """
        # Pattern for inline EXISTS that checks OPERATING but might miss park status
        dangerous_pattern = re.compile(
            r"EXISTS\s*\(\s*SELECT.*ride_status_snapshots.*OPERATING",
            re.IGNORECASE | re.DOTALL
        )

        for filename, content in query_file_contents.items():
            # Skip if file uses the centralized CTE
            if "rides_that_operated_cte" in content:
                continue

            # Check for dangerous pattern
            if dangerous_pattern.search(content):
                # Allow if it also checks park_appears_open in the same subquery
                if "park_appears_open" not in content:
                    pytest.fail(
                        f"{filename} has inline EXISTS subquery checking OPERATING status "
                        "without checking park_appears_open. Use "
                        "RideStatusSQL.rides_that_operated_cte() instead."
                    )
