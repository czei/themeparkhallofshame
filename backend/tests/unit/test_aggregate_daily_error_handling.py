"""
Unit tests for aggregate_daily.py error handling.

TDD RED PHASE: These tests verify that the aggregation script handles
errors gracefully without crashing due to undefined variables.

Bug Report:
- log_id is referenced in exception handler but may not be defined
- If error occurs before _start_aggregation_log, log_id is undefined
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import date, datetime


class TestAggregationScriptErrorHandling:
    """Test error handling in DailyAggregator."""

    def test_exception_handler_works_when_log_id_not_set(self):
        """
        FAILING TEST: Exception handler must not crash when log_id is undefined.

        Bug: If an exception occurs before _start_aggregation_log() is called,
        log_id is undefined, causing UnboundLocalError in the exception handler.

        Expected behavior: Exception handler should check if log_id exists
        before trying to mark aggregation as failed.
        """
        from scripts.aggregate_daily import DailyAggregator

        aggregator = DailyAggregator(target_date=date(2025, 12, 25))

        # Create mock session that works for context manager
        mock_session_instance = MagicMock()

        call_count = [0]

        def session_side_effect():
            """First call works, second call (in except block) also works."""
            call_count[0] += 1
            mock_context = MagicMock()
            mock_context.__enter__ = MagicMock(return_value=mock_session_instance)
            mock_context.__exit__ = MagicMock(return_value=False)
            return mock_context

        with patch('src.scripts.aggregate_daily.get_db_session', side_effect=session_side_effect):
            with patch('src.scripts.aggregate_daily.AggregationLogRepository') as mock_repo_class:
                mock_repo = MagicMock()
                mock_repo_class.return_value = mock_repo

                # Make _check_already_aggregated raise an exception BEFORE log_id is set
                with patch.object(aggregator, '_check_already_aggregated',
                                  side_effect=Exception("Check failed before log_id set")):

                    # This should NOT raise UnboundLocalError
                    # It should handle the undefined log_id gracefully
                    with pytest.raises(SystemExit) as exc_info:
                        aggregator.run()

                    # Should exit with error code 1
                    assert exc_info.value.code == 1

                    # Verify we didn't crash trying to use undefined log_id
                    # The fail_aggregation_log should either:
                    # 1. Not be called at all (if log_id check added), or
                    # 2. Be called with a valid log_id
                    # Currently it crashes with UnboundLocalError

    def test_exception_handler_logs_failure_when_log_id_exists(self):
        """
        Verify that when log_id IS set, the exception handler marks it as failed.
        """
        from scripts.aggregate_daily import DailyAggregator

        aggregator = DailyAggregator(target_date=date(2025, 12, 25))

        with patch('src.scripts.aggregate_daily.get_db_session') as mock_session:
            mock_session_instance = MagicMock()
            mock_session.return_value.__enter__ = MagicMock(return_value=mock_session_instance)
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            # Mock the repositories
            with patch.object(aggregator, '_check_already_aggregated', return_value=False):
                with patch.object(aggregator, '_start_aggregation_log', return_value=123):
                    with patch.object(aggregator, '_aggregate_rides', side_effect=Exception("Ride aggregation failed")):
                        with patch.object(aggregator, '_fail_aggregation_log') as mock_fail:
                            with pytest.raises(SystemExit):
                                aggregator.run()

                            # Verify _fail_aggregation_log was called with the log_id
                            mock_fail.assert_called_once()
                            call_args = mock_fail.call_args
                            assert call_args[0][0] == 123  # log_id


class TestAggregationScriptCompletesSuccessfully:
    """Test that aggregation completes and marks log correctly."""

    def test_successful_aggregation_sets_completed_at(self):
        """
        Verify that successful aggregation sets completed_at timestamp.

        This prevents the "completed_at cannot be null" error.
        """
        from scripts.aggregate_daily import DailyAggregator

        aggregator = DailyAggregator(target_date=date(2025, 12, 25))

        with patch('src.scripts.aggregate_daily.get_db_session') as mock_session:
            mock_session_instance = MagicMock()
            mock_session.return_value.__enter__ = MagicMock(return_value=mock_session_instance)
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            # Mock all the methods to succeed
            with patch.object(aggregator, '_check_already_aggregated', return_value=False):
                with patch.object(aggregator, '_start_aggregation_log', return_value=123):
                    with patch.object(aggregator, '_aggregate_rides'):
                        with patch.object(aggregator, '_aggregate_parks'):
                            with patch.object(aggregator, '_complete_aggregation_log') as mock_complete:
                                aggregator.run()

                                # Verify _complete_aggregation_log was called
                                mock_complete.assert_called_once()


class TestShameScoreCalculation:
    """Test that shame_score is calculated correctly in daily aggregation."""

    def test_shame_score_uses_tier_weights_from_classifications(self):
        """
        Verify shame_score calculation joins ride_classifications for tier weights.

        Bug: If shame_score calculation doesn't join ride_classifications,
        it uses default tier_weight=2 for all rides instead of actual weights.
        """
        # This test validates the SQL in _aggregate_park includes the join
        from scripts.aggregate_daily import DailyAggregator
        import inspect

        # Get the source code of _aggregate_park
        source = inspect.getsource(DailyAggregator._aggregate_park)

        # Verify it joins ride_classifications
        assert 'RideClassification' in source or 'ride_classifications' in source, \
            "_aggregate_park must join ride_classifications for tier weights"

        # Verify it uses tier_weight
        assert 'tier_weight' in source, \
            "_aggregate_park must use tier_weight in shame_score calculation"

    def test_shame_score_formula_matches_single_source_of_truth(self):
        """
        Verify the shame_score formula in aggregate_daily matches metrics.py.

        Formula: shame_score = (weighted_downtime / effective_park_weight) * 10

        This test ensures we don't have formula drift between calculation locations.
        """
        from utils.metrics import SHAME_SCORE_MULTIPLIER

        # The multiplier should be 10
        assert SHAME_SCORE_MULTIPLIER == 10, \
            "SHAME_SCORE_MULTIPLIER should be 10 for 0-10 scale"

        # Check that aggregate_daily uses the same multiplier
        from scripts.aggregate_daily import DailyAggregator
        import inspect

        source = inspect.getsource(DailyAggregator._aggregate_park)

        # Should multiply by 10 (or use the constant)
        assert '* 10' in source or 'SHAME_SCORE_MULTIPLIER' in source, \
            "_aggregate_park should multiply by 10 for shame_score"
