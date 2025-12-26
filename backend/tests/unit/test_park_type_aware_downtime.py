"""
Unit tests for park-type aware downtime logic.

CRITICAL BUSINESS RULE:
- Disney/Universal parks: Only 'DOWN' status counts as downtime
  (CLOSED = scheduled closure, not a malfunction)
- Other parks: Both 'DOWN' and 'CLOSED' count as downtime
  (They don't distinguish between scheduled closures and breakdowns)

This ensures parks like DCA don't show inflated downtime from
scheduled ride closures (e.g., meal breaks, weather holds).
"""

import pytest
from unittest.mock import MagicMock


class TestParkTypeAwareIsDown:
    """Tests for RideStatusExpressions.is_down() park-type aware logic."""

    def test_disney_park_down_status_counts_as_down(self):
        """
        For Disney parks, status='DOWN' should count as downtime.
        """
        from src.utils.query_helpers import RideStatusExpressions

        # Create mock objects
        mock_snapshot = MagicMock()
        mock_snapshot.status = 'DOWN'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = True
        mock_park.is_universal = False

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is True, "Disney park with DOWN status should be counted as down"

    def test_disney_park_closed_status_not_counted_as_down(self):
        """
        For Disney parks, status='CLOSED' should NOT count as downtime.
        Disney distinguishes between DOWN (breakdown) and CLOSED (scheduled).
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'CLOSED'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = True
        mock_park.is_universal = False

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is False, "Disney park with CLOSED status should NOT be counted as down"

    def test_universal_park_down_status_counts_as_down(self):
        """
        For Universal parks, status='DOWN' should count as downtime.
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'DOWN'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = False
        mock_park.is_universal = True

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is True, "Universal park with DOWN status should be counted as down"

    def test_universal_park_closed_status_not_counted_as_down(self):
        """
        For Universal parks, status='CLOSED' should NOT count as downtime.
        Universal distinguishes between DOWN and CLOSED.
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'CLOSED'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = False
        mock_park.is_universal = True

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is False, "Universal park with CLOSED status should NOT be counted as down"

    def test_other_park_down_status_counts_as_down(self):
        """
        For non-Disney/Universal parks, status='DOWN' should count as downtime.
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'DOWN'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = False
        mock_park.is_universal = False

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is True, "Non-Disney/Universal park with DOWN status should be counted as down"

    def test_other_park_closed_status_counts_as_down(self):
        """
        For non-Disney/Universal parks, status='CLOSED' SHOULD count as downtime.
        These parks don't distinguish between breakdowns and scheduled closures.
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'CLOSED'
        mock_snapshot.computed_is_open = False

        mock_park = MagicMock()
        mock_park.is_disney = False
        mock_park.is_universal = False

        result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
        assert result is True, "Non-Disney/Universal park with CLOSED status SHOULD be counted as down"

    def test_operating_ride_never_counted_as_down(self):
        """
        A ride with status='OPERATING' should never be counted as down,
        regardless of park type.
        """
        from src.utils.query_helpers import RideStatusExpressions

        mock_snapshot = MagicMock()
        mock_snapshot.status = 'OPERATING'
        mock_snapshot.computed_is_open = True

        # Test all park types
        for is_disney, is_universal in [(True, False), (False, True), (False, False)]:
            mock_park = MagicMock()
            mock_park.is_disney = is_disney
            mock_park.is_universal = is_universal

            result = RideStatusExpressions.is_down_python(mock_snapshot, mock_park)
            assert result is False, f"OPERATING ride should not be down (disney={is_disney}, universal={is_universal})"


class TestRideStatusSQLGeneration:
    """Tests for SQL generation of park-type aware logic."""

    def test_is_down_sql_includes_park_type_check(self):
        """
        RideStatusSQL.is_down() should generate SQL that checks park type.
        """
        from src.utils.sql_helpers import RideStatusSQL

        sql = RideStatusSQL.is_down("rss", parks_alias="p")

        # Should reference the park type flags
        assert "is_disney" in sql or "is_universal" in sql, \
            "SQL should check park type for Disney/Universal distinction"

    def test_is_down_sql_has_different_logic_for_park_types(self):
        """
        The SQL should have conditional logic for different park types.
        """
        from src.utils.sql_helpers import RideStatusSQL

        sql = RideStatusSQL.is_down("rss", parks_alias="p")

        # Should have CASE or conditional logic
        assert "CASE" in sql.upper() or "OR" in sql.upper() or "AND" in sql.upper(), \
            "SQL should have conditional logic for park types"
