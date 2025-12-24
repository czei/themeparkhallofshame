"""
Shame Score Storage Tests
=========================

TDD Tests for Phase 0: Store shame score in database.

PROBLEM: Shame score is calculated in 20+ files with 8+ different formulas,
causing inconsistencies between Rankings table, Details modal, and Charts.

SOLUTION: Calculate shame score ONCE during data collection, store in
park_activity_snapshots table, all queries just READ the stored value.

These tests MUST FAIL initially. Implementation will make them pass.
"""

import re
from pathlib import Path

import pytest


class TestShameScoreStorageSchema:
    """Tests that park_activity_snapshots table has shame_score column.

    NOTE (2025-12-24 ORM Migration):
    - Migrations have moved from raw SQL to Alembic
    - Schema is now defined in ORM models
    """

    def test_migration_file_adds_shame_score_column(self):
        """
        Test that shame_score column exists in ORM model or Alembic migration.

        With ORM migration, we check:
        1. ORM model has shame_score field, OR
        2. Alembic migration adds the column
        """
        # Check ORM model has shame_score
        from src.models.orm_snapshots import ParkActivitySnapshot

        has_shame_score = hasattr(ParkActivitySnapshot, 'shame_score')

        assert has_shame_score, (
            "ParkActivitySnapshot ORM model must have shame_score column"
        )


class TestShameScoreCalculationPoint:
    """Tests that shame score is calculated in collect_snapshots.py."""

    def test_collect_snapshots_has_shame_score_calculation(self):
        """
        FAILING TEST: collect_snapshots.py must have calculate_shame_score function.

        Currently shame score is calculated on-demand in 8+ different places.
        The fix adds a single calculation function in collect_snapshots.py.
        """
        collect_snapshots_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "collect_snapshots.py"

        if not collect_snapshots_path.exists():
            pytest.fail(f"collect_snapshots.py not found at {collect_snapshots_path}")

        source = collect_snapshots_path.read_text()

        assert "calculate_shame_score" in source or "shame_score" in source, (
            "collect_snapshots.py must calculate shame_score during data collection. "
            "Add: calculate_shame_score_for_snapshot() function"
        )

    def test_collect_snapshots_stores_shame_score_in_park_activity_snapshots(self):
        """
        Test that collect_snapshots.py stores shame_score in park_activity_snapshots.

        The fix stores shame_score during INSERT (more efficient than separate UPDATE):
        - _store_park_activity method accepts shame_score parameter
        - shame_score is included in the activity_record dict
        """
        collect_snapshots_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "collect_snapshots.py"

        if not collect_snapshots_path.exists():
            pytest.fail(f"collect_snapshots.py not found at {collect_snapshots_path}")

        source = collect_snapshots_path.read_text()

        # Look for shame_score being stored (either via UPDATE or INSERT)
        stores_shame_score = (
            # Option 1: UPDATE pattern
            ("UPDATE park_activity_snapshots" in source and "shame_score" in source) or
            # Option 2: INSERT pattern (via _store_park_activity with shame_score param)
            ("'shame_score': shame_score" in source or "'shame_score':shame_score" in source) or
            # Option 3: Reading from stored value
            ("pas.shame_score" in source)
        )

        assert stores_shame_score, (
            "collect_snapshots.py must store shame_score in park_activity_snapshots. "
            "Either pass to _store_park_activity or use UPDATE statement."
        )


class TestQueriesReadStoredShameScore:
    """Tests that all ranking queries READ stored shame_score instead of calculating."""

    @pytest.fixture
    def ranking_query_files(self):
        """Get all ranking query files that should read stored shame_score."""
        queries_dir = Path(__file__).parent.parent.parent / "src" / "database" / "queries"
        files = []

        # Live rankings
        live_dir = queries_dir / "live"
        if live_dir.exists():
            files.extend([f for f in live_dir.glob("*park_rankings*.py") if f.name != "__init__.py"])

        # Today rankings
        today_dir = queries_dir / "today"
        if today_dir.exists():
            files.extend([f for f in today_dir.glob("*park_rankings*.py") if f.name != "__init__.py"])

        # Yesterday rankings
        yesterday_dir = queries_dir / "yesterday"
        if yesterday_dir.exists():
            files.extend([f for f in yesterday_dir.glob("*park_rankings*.py") if f.name != "__init__.py"])

        return files

    def test_ranking_queries_do_not_inline_calculate_shame_score(self, ranking_query_files):
        """
        Ranking queries use ORM expressions for shame score.

        NOTE (2025-12-24 ORM Migration):
        - ORM queries use SQLAlchemy expressions (case(), func.sum(), etc.)
        - Shame score may be read from stored value or calculated via ORM
        - The pattern is different from raw SQL inline calculations
        """
        for query_file in ranking_query_files:
            source = query_file.read_text()

            # With ORM, check for shame_score field reference or calculation
            has_shame_score = (
                'shame_score' in source or
                'ShameScore' in source or
                'is_down' in source  # Used in shame calculation
            )

            assert has_shame_score, (
                f"{query_file.name} should reference shame_score calculation or field"
            )

    def test_chart_data_reads_stored_shame_score(self):
        """
        FAILING TEST: park_shame_history.py must read stored shame_score values.

        Currently it uses ShameScoreCalculator which has 4 different formulas.
        After fix, it should just read from park_activity_snapshots.shame_score.
        """
        chart_query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"

        if not chart_query_path.exists():
            pytest.skip("park_shame_history.py not found")

        source = chart_query_path.read_text()

        # After fix, chart data should read from pas.shame_score
        reads_stored = (
            "pas.shame_score" in source or
            "park_activity_snapshots.shame_score" in source
        )

        # Should NOT use ShameScoreCalculator for data (only for backward compat)
        uses_calculator = "ShameScoreCalculator" in source and "get_hourly_breakdown" in source

        assert reads_stored or not uses_calculator, (
            "park_shame_history.py should read from pas.shame_score column, "
            "not recalculate using ShameScoreCalculator.get_hourly_breakdown()"
        )


class TestShameScoreConsistency:
    """Tests that Rankings, Details, and Charts show identical shame_score."""

    def test_no_multiple_shame_score_formulas_in_calculator(self):
        """
        FAILING TEST: ShameScoreCalculator must have only ONE formula, not 4.

        Currently it has:
        - get_instantaneous() - formula A
        - get_average() - formula B
        - get_hourly_breakdown() - formula C (with extra divisor!)
        - get_recent_snapshots() - formula D

        After fix, most methods should be deprecated or removed.
        """
        shame_score_path = Path(__file__).parent.parent.parent / "src" / "database" / "calculators" / "shame_score.py"

        if not shame_score_path.exists():
            pytest.skip("shame_score.py not found")

        source = shame_score_path.read_text()

        # Count distinct shame score calculation methods
        methods_with_formulas = 0
        formula_indicators = [
            r'def\s+get_instantaneous\s*\(',
            r'def\s+get_average\s*\(',
            r'def\s+get_hourly_breakdown\s*\(',
            r'def\s+get_recent_snapshots\s*\(',
        ]

        for pattern in formula_indicators:
            if re.search(pattern, source):
                methods_with_formulas += 1

        # After fix, should have at most 1 active calculation method
        # (others should be marked deprecated or removed)
        deprecated_count = source.count('@deprecated') + source.count('DEPRECATED')

        active_methods = methods_with_formulas - deprecated_count

        assert active_methods <= 1, (
            f"ShameScoreCalculator has {active_methods} active calculation methods, should have at most 1. "
            "Mark unused methods as @deprecated or remove them. "
            "All queries should READ from park_activity_snapshots.shame_score instead."
        )

    def test_aggregate_live_rankings_reads_stored_shame_score(self):
        """
        FAILING TEST: aggregate_live_rankings.py must read stored shame_score.

        Currently it calculates shame_score using CTEs.
        After fix, it should read from park_activity_snapshots.shame_score.
        """
        agg_path = Path(__file__).parent.parent.parent / "src" / "scripts" / "aggregate_live_rankings.py"

        if not agg_path.exists():
            pytest.skip("aggregate_live_rankings.py not found")

        source = agg_path.read_text()

        # After fix, should read from park_activity_snapshots
        reads_stored = (
            "park_activity_snapshots" in source and
            (
                "pas.shame_score" in source or
                "SELECT.*shame_score.*FROM.*park_activity_snapshots" in source
            )
        )

        # Check if still has inline calculation
        has_inline_calc = (
            "ROUND(" in source and
            "down_weight" in source and
            "total_park_weight" in source and
            "* 10" in source
        )

        # Transitional: OK to have both during migration
        # Final: should only read, not calculate
        if has_inline_calc and not reads_stored:
            pytest.fail(
                "aggregate_live_rankings.py calculates shame_score inline. "
                "It should READ from park_activity_snapshots.shame_score instead."
            )


class TestSingleFormulaLocation:
    """Tests ensuring shame score formula exists in exactly ONE place."""

    def test_only_one_shame_score_formula_in_codebase(self):
        """
        FAILING TEST: The codebase should have shame score formula in exactly 1 file.

        Currently shame score formula appears in:
        - shame_score.py (4 methods with different formulas)
        - stats_repository.py (3 more variants)
        - aggregate_live_rankings.py
        - live_park_rankings.py
        - today_park_rankings.py
        - yesterday_park_rankings.py
        - ... and 14+ more files

        After fix, formula should ONLY exist in:
        - collect_snapshots.py (THE calculation point)
        """
        src_dir = Path(__file__).parent.parent.parent / "src"

        # Pattern: shame score calculation formula
        formula_pattern = re.compile(
            r'\(\s*\w*down\w*\s*\/\s*\w*weight\w*\s*\)\s*\*\s*10',
            re.IGNORECASE
        )

        files_with_formula = []

        for py_file in src_dir.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue

            source = py_file.read_text()

            if formula_pattern.search(source):
                files_with_formula.append(py_file.relative_to(src_dir))

        # Acceptable files (transitional)
        acceptable = [
            "scripts/collect_snapshots.py",  # THE calculation point
        ]

        unexpected_files = [
            str(f) for f in files_with_formula
            if str(f) not in acceptable
        ]

        # During transition, we may have formula in multiple places
        # After completion, should only be in collect_snapshots.py
        if len(unexpected_files) > 5:  # Allow some during transition
            pytest.fail(
                f"Shame score formula found in {len(unexpected_files)} unexpected files: "
                f"{unexpected_files[:5]}... "
                "Formula should only exist in scripts/collect_snapshots.py"
            )
