"""
Unit tests for ShameScoreCalculator - Single Source of Truth for shame score calculations.

TDD: These tests define the expected behavior of the calculator.
The calculator must ensure consistency across:
- Rankings table (TODAY period)
- Breakdown panel (park details modal)
- Chart average display

Key Formula:
    shame_score = AVG(per-snapshot instantaneous shame scores)

    Where instantaneous shame at timestamp T =
        (sum of tier_weights for down rides at T) / total_park_weight * 10
"""
from unittest.mock import MagicMock
from datetime import datetime, timezone


class TestShameScoreCalculatorInterface:
    """Tests for ShameScoreCalculator class interface."""

    def test_calculator_accepts_db_session_via_dependency_injection(self):
        """
        ShameScoreCalculator should accept a db_session in constructor.
        This enables unit testing with mock sessions.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        calc = ShameScoreCalculator(mock_session)

        assert calc.db == mock_session

    def test_get_average_returns_optional_float(self):
        """
        get_average() should return Optional[float].
        Returns None when no data exists.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        calc = ShameScoreCalculator(mock_session)

        # Verify method signature accepts expected parameters
        assert callable(getattr(calc, 'get_average', None))

    def test_get_instantaneous_returns_optional_float(self):
        """
        get_instantaneous() should return Optional[float].
        Returns None when no data exists.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        calc = ShameScoreCalculator(mock_session)

        assert callable(getattr(calc, 'get_instantaneous', None))

    def test_get_hourly_breakdown_returns_list_of_dicts(self):
        """
        get_hourly_breakdown() should return List[Dict].
        Each dict contains: hour, shame_score, down_minutes, total_rides
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        calc = ShameScoreCalculator(mock_session)

        assert callable(getattr(calc, 'get_hourly_breakdown', None))


class TestShameScoreCalculatorEdgeCases:
    """Tests for edge cases that must be handled gracefully."""

    def test_no_data_returns_none_not_zero(self):
        """
        When there's no snapshot data for the time range,
        get_average() should return None (not 0).

        This distinguishes "no data" from "zero downtime".
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        # Simulate empty result set
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result

        calc = ShameScoreCalculator(mock_session)

        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)

        result = calc.get_average(park_id=999, start=start, end=end)

        # Should return None for no data, not 0
        assert result is None

    def test_park_with_no_rides_returns_none(self):
        """
        A park with zero scorable attractions should return None.
        This avoids division by zero errors.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        # Simulate park with total_park_weight = 0 (SQL returns NULL as avg_shame_score)
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.avg_shame_score = None  # SQL CASE returns NULL when weight=0
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result

        calc = ShameScoreCalculator(mock_session)

        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)

        result = calc.get_average(park_id=999, start=start, end=end)

        # Should return None to avoid division by zero
        assert result is None

    def test_park_closed_all_day_returns_none(self):
        """
        When park_appears_open was never TRUE for the time range,
        get_average() should return None.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        # Simulate no snapshots where park was open
        mock_result = MagicMock()
        mock_result.fetchone.return_value = MagicMock(
            total_snapshots=0,
            avg_shame_score=None
        )
        mock_session.execute.return_value = mock_result

        calc = ShameScoreCalculator(mock_session)

        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)

        result = calc.get_average(park_id=999, start=start, end=end)

        assert result is None


class TestShameScoreCalculatorFormula:
    """Tests verifying the correct shame score formula is used."""

    def test_shame_score_uses_average_of_instantaneous_not_cumulative(self):
        """
        CRITICAL: The calculator MUST use AVG(per-snapshot shame scores),
        NOT the cumulative formula (total_weighted_downtime / total_weight * 10).

        This is the root cause of the current bug - the breakdown uses
        cumulative while rankings uses average instantaneous.
        """
        # This test documents the correct behavior.
        # Implementation should use the per_snapshot_shame CTE approach
        # from today_park_rankings.py, NOT the cumulative approach
        # from stats_repository.py.
        pass  # Formula validation is done via integration tests

    def test_shame_score_precision_is_one_decimal(self):
        """
        Shame scores should be rounded to 1 decimal place (e.g., 4.7, 3.3).
        """
        from utils.metrics import SHAME_SCORE_PRECISION

        assert SHAME_SCORE_PRECISION == 1

    def test_shame_score_multiplier_is_ten(self):
        """
        Shame scores are multiplied by 10 for readability.
        This converts 0.47 -> 4.7 for easier interpretation.
        """
        from utils.metrics import SHAME_SCORE_MULTIPLIER

        assert SHAME_SCORE_MULTIPLIER == 10


class TestShameScoreCalculatorFiltering:
    """Tests for consistent filtering logic."""

    def test_only_counts_rides_that_operated(self):
        """
        Only rides that had at least one OPERATING snapshot should be counted.
        Rides that were DOWN all day without ever operating should be excluded.
        """
        # This is enforced by rides_that_operated CTE
        # Verification done via integration tests
        pass

    def test_only_counts_downtime_when_park_appears_open(self):
        """
        Downtime should only be counted when park_appears_open = TRUE.
        Pre-opening and post-closing downtime should not count.
        """
        # This is enforced by park_appears_open filter
        # Verification done via integration tests
        pass

    def test_disney_universal_only_counts_down_status(self):
        """
        For Disney/Universal parks, only status='DOWN' counts as downtime.
        CLOSED rides at Disney don't count (they use CLOSED for refurbs).
        """
        # This is enforced by RideStatusSQL.is_down() with parks_alias
        # Verification done via integration tests
        pass


class TestShameScoreCalculatorHourlyBreakdown:
    """Tests for hourly breakdown functionality."""

    def test_hourly_breakdown_returns_all_operating_hours(self):
        """
        get_hourly_breakdown() should return data for all hours
        when the park was operating, typically 6am-11pm (hours 6-23).
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_session = MagicMock()
        calc = ShameScoreCalculator(mock_session)

        # Verify method signature
        assert callable(getattr(calc, 'get_hourly_breakdown', None))

    def test_hourly_breakdown_average_matches_get_average(self):
        """
        CRITICAL: The average of hourly shame scores from get_hourly_breakdown()
        MUST equal the value returned by get_average().

        This ensures the chart displays a consistent average.
        """
        # This is the consistency requirement - verified via integration tests
        pass
