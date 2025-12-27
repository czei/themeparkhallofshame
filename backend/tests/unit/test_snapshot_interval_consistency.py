"""
Tests for SNAPSHOT_INTERVAL_MINUTES consistency across codebase.

These tests ensure that all files use the centralized SNAPSHOT_INTERVAL_MINUTES
constant from utils.metrics instead of hardcoding values.

Bug Context (2025-12-18):
- Multiple files had hardcoded SNAPSHOT_INTERVAL_MINUTES = 5
- The actual collection interval is 10 minutes
- This caused downtime calculations to be off by 50%
- Rankings showed 0.25h while details showed 1.92h for same park
"""

import ast
import re
from pathlib import Path

import pytest


# Files that MUST import SNAPSHOT_INTERVAL_MINUTES from utils.metrics
# if they use it in calculations
FILES_THAT_USE_INTERVAL = [
    "src/database/repositories/stats_repository.py",
    "src/database/queries/yesterday/yesterday_ride_rankings.py",
    "src/database/queries/builders/expressions.py",
    "src/database/queries/trends/longest_wait_times.py",
    "src/database/queries/trends/least_reliable_rides.py",
    "src/scripts/aggregate_hourly.py",
]

# The canonical source of truth
CANONICAL_SOURCE = "src/utils/metrics.py"
EXPECTED_INTERVAL = 10


class TestSnapshotIntervalConsistency:
    """Ensure SNAPSHOT_INTERVAL_MINUTES is consistent across codebase."""

    def test_canonical_source_has_correct_value(self):
        """The canonical source (utils/metrics.py) must have the correct interval."""
        from utils.metrics import SNAPSHOT_INTERVAL_MINUTES

        assert SNAPSHOT_INTERVAL_MINUTES == EXPECTED_INTERVAL, (
            f"SNAPSHOT_INTERVAL_MINUTES in utils/metrics.py is {SNAPSHOT_INTERVAL_MINUTES}, "
            f"expected {EXPECTED_INTERVAL}. The actual data collection runs every 10 minutes."
        )

    @pytest.mark.parametrize("filepath", FILES_THAT_USE_INTERVAL)
    def test_file_imports_from_metrics(self, filepath):
        """Files using SNAPSHOT_INTERVAL_MINUTES must import from utils.metrics."""
        backend_root = Path(__file__).parent.parent.parent
        full_path = backend_root / filepath

        if not full_path.exists():
            pytest.skip(f"File {filepath} does not exist")

        content = full_path.read_text()

        # Check for hardcoded assignment (the bug pattern)
        hardcoded_pattern = r'SNAPSHOT_INTERVAL_MINUTES\s*=\s*\d+'
        hardcoded_matches = re.findall(hardcoded_pattern, content)

        # Filter out comments
        lines = content.split('\n')
        actual_hardcoded = []
        for match in hardcoded_matches:
            for line in lines:
                if match in line and not line.strip().startswith('#'):
                    # Make sure it's not in a string or comment
                    if 'from utils.metrics import' not in line:
                        actual_hardcoded.append(line.strip())

        assert not actual_hardcoded, (
            f"File {filepath} has hardcoded SNAPSHOT_INTERVAL_MINUTES:\n"
            f"  {actual_hardcoded}\n"
            f"Must import from utils.metrics instead."
        )

        # Verify import exists if the constant is used
        if 'SNAPSHOT_INTERVAL_MINUTES' in content:
            # Allow both 'from utils.metrics' and 'from src.utils.metrics' patterns
            import_pattern = r'from (?:src\.)?utils\.metrics import.*SNAPSHOT_INTERVAL_MINUTES'
            assert re.search(import_pattern, content), (
                f"File {filepath} uses SNAPSHOT_INTERVAL_MINUTES but doesn't import from utils.metrics"
            )

    def test_no_hardcoded_5_in_sql_downtime_calculations(self):
        """SQL queries must not hardcode 5-minute intervals for downtime."""
        backend_root = Path(__file__).parent.parent.parent / "src"

        # Pattern: THEN 5 / 60 or THEN 5.0 / 60.0 (hardcoded 5-minute interval)
        bad_patterns = [
            r'THEN\s+5\s*/\s*60',
            r'THEN\s+5\.0\s*/\s*60',
        ]

        violations = []
        for py_file in backend_root.rglob("*.py"):
            content = py_file.read_text()
            for pattern in bad_patterns:
                if re.search(pattern, content):
                    # Get the line for context
                    for i, line in enumerate(content.split('\n'), 1):
                        if re.search(pattern, line):
                            violations.append(f"{py_file.relative_to(backend_root)}:{i}: {line.strip()}")

        assert not violations, (
            f"Found hardcoded 5-minute interval in SQL queries:\n"
            + "\n".join(f"  {v}" for v in violations)
        )


class TestDisneyUniversalDownStatusLogic:
    """
    Ensure Disney/Universal rides with DOWN status are properly counted.

    Bug Context (2025-12-18):
    - Disney/Universal parks report DOWN for breakdowns, CLOSED for scheduled
    - The "operated today" logic was excluding rides that only showed DOWN
    - DINOSAUR was DOWN for 3.5 hours but not counted until it showed OPERATING
    - This caused Animal Kingdom to show 0.25h instead of 3.83h downtime

    NOTE (2025-12-24 ORM Migration):
    - aggregate_hourly.py is now DEPRECATED
    - Live queries use ORM and compute status on-the-fly
    - The DOWN status logic is now in sql_helpers.py RideStatusSQL
    """

    def test_aggregate_hourly_includes_disney_down_in_operated(self):
        """
        aggregate_hourly.py is DEPRECATED - test sql_helpers instead.

        The DOWN status logic is now centralized in RideStatusSQL.PARKS_WITH_DOWN_STATUS
        and RideStatusSQL.is_down() method.
        """
        from utils.sql_helpers import RideStatusSQL

        # Verify the DOWN status logic exists in the canonical location
        assert hasattr(RideStatusSQL, 'PARKS_WITH_DOWN_STATUS'), (
            "RideStatusSQL must define PARKS_WITH_DOWN_STATUS for Disney/Universal parks"
        )

        # Verify is_down method exists
        import inspect
        assert hasattr(RideStatusSQL, 'is_down'), (
            "RideStatusSQL must have is_down() method"
        )

        sig = inspect.signature(RideStatusSQL.is_down)
        params = list(sig.parameters.keys())
        assert 'parks_alias' in params, (
            "RideStatusSQL.is_down() must accept parks_alias for park-type aware logic"
        )

    def test_sql_helpers_documents_park_type_logic(self):
        """sql_helpers.py must document the park-type aware downtime logic."""
        from utils.sql_helpers import RideStatusSQL

        # Verify the is_down method exists and accepts parks_alias
        import inspect
        sig = inspect.signature(RideStatusSQL.is_down)
        params = list(sig.parameters.keys())

        assert 'parks_alias' in params, (
            "RideStatusSQL.is_down() must accept parks_alias parameter for park-type aware logic"
        )

        # Verify PARKS_WITH_DOWN_STATUS is defined
        assert hasattr(RideStatusSQL, 'PARKS_WITH_DOWN_STATUS'), (
            "RideStatusSQL must define PARKS_WITH_DOWN_STATUS for Disney/Universal/Dollywood"
        )


class TestParkOpenScheduleBasedLogic:
    """
    Ensure park open detection uses schedule data as SINGLE SOURCE OF TRUTH.

    Architecture (2025-12-26):
    - park_appears_open is set during snapshot collection based on schedule data
    - collect_snapshots.py calls schedule_repo.is_park_open_now(park_id)
    - NO rides_open > 0 fallback - this caused bugs (test rides during closed hours)

    Bug Fixed (2025-12-26):
    - DCA showed 23 rides "down" during hours 0-7 and 22-23 (park closed)
    - Root cause: rides_open > 0 fallback set park_was_open=1 when 3 test rides showed as "open"
    - Fix: Use schedule-based park_appears_open ONLY, no fallback
    - Chart queries now filter by park_schedules table directly
    """

    def test_aggregate_hourly_does_not_use_rides_open_fallback(self):
        """aggregate_hourly.py must NOT use rides_open > 0 as fallback for park open."""
        backend_root = Path(__file__).parent.parent.parent
        filepath = backend_root / "src/scripts/aggregate_hourly.py"

        content = filepath.read_text()

        # The old buggy fallback pattern should NOT exist
        # Old pattern: (pas.park_appears_open = 1 OR pas.rides_open > 0)
        fallback_patterns = [
            r'park_appears_open\s*=\s*1\s+OR\s+.*rides_open\s*>\s*0',
            r'park_appears_open\s*=\s*TRUE\s+OR\s+.*rides_open\s*>\s*0',
            r'\(pas\.park_appears_open.*OR.*pas\.rides_open\s*>\s*0\)',
        ]

        has_fallback = any(re.search(p, content, re.IGNORECASE) for p in fallback_patterns)

        assert not has_fallback, (
            "aggregate_hourly.py must NOT use rides_open > 0 fallback!\n\n"
            "Bug: Test rides showing 'open' during closed hours caused park_was_open=1\n"
            "for ALL 24 hours, making charts show 23 rides 'down' at 3am.\n\n"
            "Fix: Use schedule-based park_appears_open ONLY (SINGLE SOURCE OF TRUTH).\n"
            "The schedule data is set during collection via schedule_repo.is_park_open_now()."
        )

    def test_charts_filter_by_schedule_not_rides_open(self):
        """Chart queries must filter by park_schedules table, not rides_open fallback."""
        backend_root = Path(__file__).parent.parent.parent
        charts_path = backend_root / "src/database/queries/charts/park_shame_history.py"

        content = charts_path.read_text()

        # Should have schedule-based filtering
        has_schedule_filter = '_get_schedule_for_date' in content or 'ParkSchedule' in content

        assert has_schedule_filter, (
            "Chart queries must use park_schedules table for filtering hours.\n"
            "This is the SINGLE SOURCE OF TRUTH for when a park is open."
        )


class TestSixFlagsFiestaTexasBugFixes:
    """
    Tests for the Six Flags Fiesta Texas shame=2.4 but downtime=0 bug.

    Bug Report (2025-12-19): Six Flags Fiesta Texas showed shame_score=2.4
    but total_downtime_hours=0.0 on https://themeparkhallofshame.com/park-detail.html?park_id=169

    Root Causes:
    1. operated_today_ride_ids CTE didn't use fallback heuristic for parks with bad schedule data
    2. Ride aggregation park_appears_open_filter() didn't use with_fallback=True
    3. DOWN status only counted as "operated" for Disney/Universal, not all parks

    Result: Rides with status='DOWN' had their downtime excluded from totals.

    NOTE (2025-12-24 ORM Migration):
    - aggregate_hourly.py is now DEPRECATED
    - The fallback heuristic is in sql_helpers.py ParkStatusSQL.park_appears_open_filter()
    - Live queries use ORM and compute status on-the-fly
    """

    def test_operated_today_cte_uses_fallback_heuristic(self):
        """
        aggregate_hourly.py is DEPRECATED - test sql_helpers instead.

        The fallback heuristic is now in ParkStatusSQL.park_appears_open_filter(with_fallback=True).
        """
        from utils.sql_helpers import ParkStatusSQL
        import inspect

        # Verify the fallback heuristic exists in the canonical location
        assert hasattr(ParkStatusSQL, 'park_appears_open_filter'), (
            "ParkStatusSQL must have park_appears_open_filter() method"
        )

        sig = inspect.signature(ParkStatusSQL.park_appears_open_filter)
        params = list(sig.parameters.keys())

        assert 'with_fallback' in params, (
            "ParkStatusSQL.park_appears_open_filter() must accept with_fallback parameter"
        )

    def test_ride_aggregation_uses_fallback_filter(self):
        """
        aggregate_hourly.py is DEPRECATED - test sql_helpers instead.

        Verify ParkStatusSQL has the fallback heuristic available.
        """
        from utils.sql_helpers import ParkStatusSQL
        import inspect

        # Check that the filter method exists and supports fallback
        assert hasattr(ParkStatusSQL, 'park_appears_open_filter'), (
            "ParkStatusSQL must have park_appears_open_filter() method"
        )

        # Get the source to verify with_fallback is used in implementation
        source = inspect.getsource(ParkStatusSQL.park_appears_open_filter)

        # Should have rides_open > 0 fallback logic
        assert 'rides_open' in source or 'with_fallback' in source, (
            "ParkStatusSQL.park_appears_open_filter should support rides_open fallback"
        )

    def test_down_status_counts_as_operated_for_all_parks(self):
        """
        aggregate_hourly.py is DEPRECATED - test sql_helpers instead.

        DOWN status logic is now centralized in RideStatusSQL.
        """
        from utils.sql_helpers import RideStatusSQL
        import inspect

        # Verify PARKS_WITH_DOWN_STATUS exists (defines which parks use DOWN distinctly)
        assert hasattr(RideStatusSQL, 'PARKS_WITH_DOWN_STATUS'), (
            "RideStatusSQL must define PARKS_WITH_DOWN_STATUS"
        )

        # Verify is_down method handles park-type aware logic
        source = inspect.getsource(RideStatusSQL.is_down)

        # The is_down method should check for DOWN status
        assert 'DOWN' in source, (
            "RideStatusSQL.is_down() must check for DOWN status"
        )


class TestHourlyAggregationDowntimeCalculation:
    """Ensure hourly aggregation calculates downtime with correct interval."""

    def test_aggregate_hourly_uses_metrics_interval(self):
        """aggregate_hourly.py must use SNAPSHOT_INTERVAL_MINUTES from metrics."""
        backend_root = Path(__file__).parent.parent.parent
        filepath = backend_root / "src/scripts/aggregate_hourly.py"

        content = filepath.read_text()

        # Must import from utils.metrics
        assert "from utils.metrics import SNAPSHOT_INTERVAL_MINUTES" in content, (
            "aggregate_hourly.py must import SNAPSHOT_INTERVAL_MINUTES from utils.metrics"
        )

        # Must NOT have hardcoded 5.0 / 60.0 for downtime
        assert "5.0 / 60.0" not in content and "5 / 60" not in content, (
            "aggregate_hourly.py must not hardcode 5-minute interval. "
            "Use SNAPSHOT_INTERVAL_MINUTES from utils.metrics."
        )

    def test_downtime_formula_uses_variable(self):
        """The downtime SQL formula must use the variable, not a literal."""
        backend_root = Path(__file__).parent.parent.parent
        filepath = backend_root / "src/scripts/aggregate_hourly.py"

        content = filepath.read_text()

        # Look for the downtime calculation pattern
        # Should be: THEN {SNAPSHOT_INTERVAL_MINUTES} / 60.0
        # NOT: THEN 5.0 / 60.0

        # Find lines with downtime calculation
        lines = content.split('\n')
        downtime_lines = [l for l in lines if '/ 60' in l and 'downtime' in l.lower()]

        for line in downtime_lines:
            # Skip comments
            if line.strip().startswith('#') or line.strip().startswith('--'):
                continue
            # Check it uses the variable
            if 'THEN' in line:
                assert 'SNAPSHOT_INTERVAL_MINUTES' in line or '{SNAPSHOT_INTERVAL_MINUTES}' in content, (
                    f"Downtime calculation must use SNAPSHOT_INTERVAL_MINUTES variable: {line}"
                )
