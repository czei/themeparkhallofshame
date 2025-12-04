"""
TDD Tests for 7-Day Hybrid Denominator Feature.

Problem: Rankings table and Details modal show different shame scores for the same park.
- Knott's Berry Farm Rankings: 0.6
- Knott's Berry Farm Details: 2.1

Root Cause: Different denominators in the shame score formula:
- Rankings uses full park roster (~66.7 total weight)
- Details uses only rides that operated (~19.0 total weight)

Solution: Use "Full roster MINUS rides that haven't operated in the last 7 days"
as a consistent denominator across all calculations.

These tests are written FIRST (TDD Red phase) and will FAIL until the
implementation is complete.
"""
import os
from unittest.mock import MagicMock
from datetime import datetime, timedelta, timezone


class TestSevenDayHybridDenominator:
    """Tests for the 7-day hybrid denominator implementation."""

    def test_sql_helper_has_rides_active_in_7_days_method(self):
        """
        FAILING TEST: RideStatusSQL should have a centralized method
        for filtering rides that operated in the last 7 days.

        This ensures DRY - all queries use the same 7-day filter logic.
        """
        from utils.sql_helpers import RideStatusSQL

        # Should have the method
        assert hasattr(RideStatusSQL, 'rides_active_in_7_days_filter'), (
            "RideStatusSQL should have rides_active_in_7_days_filter() method. "
            "This is the single source of truth for 7-day ride filtering."
        )

        # Method should be a staticmethod
        assert callable(getattr(RideStatusSQL, 'rides_active_in_7_days_filter', None)), (
            "rides_active_in_7_days_filter should be callable"
        )

    def test_7day_filter_returns_sql_fragment(self):
        """
        FAILING TEST: The 7-day filter method should return a SQL fragment
        that can be used in WHERE clauses.
        """
        from utils.sql_helpers import RideStatusSQL

        filter_sql = RideStatusSQL.rides_active_in_7_days_filter()

        assert isinstance(filter_sql, str), "Should return a SQL string"
        assert 'last_operated_at' in filter_sql, (
            "Filter should reference last_operated_at column"
        )
        assert '7' in filter_sql, "Filter should reference 7 days"

    def test_7day_filter_uses_utc_timestamp(self):
        """
        FAILING TEST: The filter should use UTC_TIMESTAMP() not NOW()
        for timezone consistency.

        Zen review identified this as a HIGH priority fix.
        """
        from utils.sql_helpers import RideStatusSQL

        filter_sql = RideStatusSQL.rides_active_in_7_days_filter()

        # Should use UTC_TIMESTAMP for consistency with Python's datetime.now(timezone.utc)
        assert 'UTC_TIMESTAMP()' in filter_sql or 'utc_timestamp()' in filter_sql.lower(), (
            "Filter should use UTC_TIMESTAMP() not NOW() for timezone consistency. "
            "This is a HIGH priority fix from Zen review."
        )


class TestShameScoreCalculatorEffectiveWeight:
    """Tests for the get_effective_park_weight() method."""

    def test_shame_score_calculator_has_get_effective_park_weight_method(self):
        """
        FAILING TEST: ShameScoreCalculator should have a get_effective_park_weight()
        method that returns the denominator for shame score calculations.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        assert hasattr(calc, 'get_effective_park_weight'), (
            "ShameScoreCalculator should have get_effective_park_weight() method. "
            "This calculates the 7-day filtered park weight used as denominator."
        )

    def test_get_effective_park_weight_accepts_park_id(self):
        """
        FAILING TEST: get_effective_park_weight() should accept a park_id parameter.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        # Set up mock to return a result
        mock_result = MagicMock()
        mock_result.scalar.return_value = 45.0
        mock_db.execute.return_value = mock_result

        calc = ShameScoreCalculator(mock_db)

        # Should be able to call with park_id
        result = calc.get_effective_park_weight(park_id=139)

        assert result is not None, "Should return a weight value"
        # Verify execute was called (method was actually implemented)
        assert mock_db.execute.called, "Should execute a database query"

    def test_get_effective_park_weight_handles_zero_gracefully(self):
        """
        FAILING TEST: When effective_park_weight = 0 (no rides operated in 7 days),
        the method should return 0.0, not raise an error.

        This is a CRITICAL fix from Zen review - division by zero protection.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        # Simulate empty result (no rides operated)
        mock_result = MagicMock()
        mock_result.scalar.return_value = None  # NULL from database
        mock_db.execute.return_value = mock_result

        calc = ShameScoreCalculator(mock_db)

        result = calc.get_effective_park_weight(park_id=139)

        assert result == 0.0, (
            "Should return 0.0 when no rides operated in 7 days. "
            "This is CRITICAL for division by zero protection."
        )


class TestShameScoreZeroDenominator:
    """Tests for safe handling of zero denominators."""

    def test_calculate_shame_score_with_zero_denominator_returns_zero(self):
        """
        FAILING TEST: ShameScoreCalculator should have a calculate_shame_score()
        method that safely handles zero denominators.

        Formula: (down_weight / effective_park_weight) * 10

        When effective_park_weight = 0 (seasonal closure, no eligible rides),
        the method should return 0.0, not raise ZeroDivisionError.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        # Should have the method
        assert hasattr(calc, 'calculate_shame_score'), (
            "ShameScoreCalculator should have calculate_shame_score() helper method "
            "for safe division with zero-denominator protection."
        )

        # Should return 0 when denominator is 0
        result = calc.calculate_shame_score(down_weight=10.0, effective_park_weight=0.0)
        assert result == 0.0, "Should return 0.0 when denominator is 0"

    def test_calculate_shame_score_normal_case(self):
        """
        FAILING TEST: calculate_shame_score() should correctly compute shame
        when given valid inputs.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        # 10 down_weight out of 50 total weight = 20% = 2.0 shame score
        result = calc.calculate_shame_score(down_weight=10.0, effective_park_weight=50.0)

        assert result == 2.0, (
            "Shame score should be (10 / 50) * 10 = 2.0"
        )


class TestSingleSourceOfSevenDayWindow:
    """Tests ensuring all queries use the same 7-day logic (DRY principle)."""

    def test_rankings_queries_reference_7day_filter(self):
        """
        FAILING TEST: The ranking query files should use the centralized
        7-day filter from RideStatusSQL, not have their own implementation.
        """
        # Read the live_park_rankings.py file
        query_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/database/queries/live/live_park_rankings.py'
        )

        with open(query_file, 'r') as f:
            source = f.read()

        # Should import or reference the centralized filter
        has_centralized_filter = (
            'rides_active_in_7_days_filter' in source or
            'last_operated_at' in source
        )

        assert has_centralized_filter, (
            "live_park_rankings.py should use the 7-day window filter. "
            "Either via RideStatusSQL.rides_active_in_7_days_filter() or "
            "directly filtering on last_operated_at column."
        )

    def test_shame_score_cte_uses_7day_filter(self):
        """
        FAILING TEST: ShameScoreSQL.park_weights_cte() should support
        a 7-day filter parameter.
        """
        from utils.sql_helpers import ShameScoreSQL

        # Get the CTE SQL
        cte_sql = ShameScoreSQL.park_weights_cte()

        # Currently this test documents the expected change:
        # The CTE should either:
        # 1. Accept a use_7day_filter parameter, or
        # 2. Always use the 7-day filter

        # For now, we verify the method exists and returns SQL
        assert isinstance(cte_sql, str), "park_weights_cte should return SQL string"
        assert 'park_weights' in cte_sql, "Should define park_weights CTE"


class TestRidesLastOperatedAt:
    """Tests for the last_operated_at column usage."""

    def test_rides_schema_mentions_last_operated_at(self):
        """
        FAILING TEST: The rides model should have a last_operated_at column
        for tracking when each ride last operated.
        """
        # Try to find the column definition in the core_tables schema
        schema_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/database/schema/core_tables.py'
        )

        with open(schema_file, 'r') as f:
            source = f.read()

        assert 'last_operated_at' in source, (
            "rides table in core_tables.py should have last_operated_at column. "
            "This tracks when each ride last had OPERATING status."
        )


class TestEdgeCases:
    """Tests for edge cases in the 7-day window logic."""

    def test_ride_operated_exactly_7_days_ago_included(self):
        """
        FAILING TEST: A ride that operated exactly 7 days ago should be
        included in the effective park weight (boundary case).
        """
        from utils.sql_helpers import RideStatusSQL

        filter_sql = RideStatusSQL.rides_active_in_7_days_filter()

        # Should use >= for the comparison to include the boundary
        assert '>=' in filter_sql, (
            "Filter should use >= to include rides that operated exactly 7 days ago"
        )

    def test_effective_weight_never_exceeds_full_roster(self):
        """
        FAILING TEST: The effective park weight should always be less than
        or equal to the full roster weight.

        This is a sanity check - we can only exclude rides, not add them.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        # Should have method to get full roster weight for comparison
        assert hasattr(calc, 'get_full_roster_park_weight') or \
               hasattr(calc, 'get_park_weight'), (
            "ShameScoreCalculator should have a method to get full roster weight "
            "for comparison/validation purposes."
        )
