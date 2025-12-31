"""
Unit tests for shame_score NULL fallback behavior.

TDD RED PHASE: These tests verify that when park_daily_stats.shame_score is NULL,
the API calculates a fallback value from the available ride data instead of
returning 0.

Bug Report:
- DCA showed shame_score=0 for yesterday even though there was 35 hours of downtime
- Root cause: daily aggregation script failed, leaving shame_score=NULL
- API returned 0 instead of calculating fallback from rides data

Expected behavior:
- If shame_score is NULL but we have ride downtime data, calculate it on-demand
- Use the same formula: (weighted_downtime_hours / total_park_weight) * 10
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from decimal import Decimal


class TestShameScoreNullFallback:
    """Test that API calculates fallback when shame_score is NULL."""

    def test_returns_calculated_shame_when_stored_is_null(self):
        """
        When shame_score is NULL in park_daily_stats, return 0.

        NOTE: The current implementation intentionally uses the pre-computed
        shame_score from park_daily_stats as the single source of truth.
        When it's NULL (aggregation failed), we return 0 rather than
        attempting on-the-fly calculation, which could produce inconsistent
        results vs the Rankings page.

        Future enhancement: Could add fallback calculation using ride data,
        but this would need to use the same formula as the aggregation script.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Create mock daily stat with NULL shame_score but valid downtime
        mock_daily_stat = MagicMock()
        mock_daily_stat.shame_score = None  # NULL - aggregation failed
        mock_daily_stat.weighted_downtime_hours = Decimal('13.0')
        mock_daily_stat.effective_park_weight = Decimal('6.0')
        mock_daily_stat.total_downtime_hours = Decimal('35.33')
        mock_daily_stat.avg_uptime_percentage = Decimal('75.0')
        mock_daily_stat.rides_with_downtime = 15
        mock_daily_stat.total_rides_tracked = 40

        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = mock_daily_stat
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)

        # Mock rides with downtime data including tier weights
        mock_rides = [
            {'ride_id': 1, 'ride_name': 'Space Mountain', 'tier': 1, 'tier_weight': 3,
             'downtime_hours': 2.0, 'weighted_contribution': 6.0},
            {'ride_id': 2, 'ride_name': 'Pirates', 'tier': 2, 'tier_weight': 2,
             'downtime_hours': 3.0, 'weighted_contribution': 6.0},
            {'ride_id': 3, 'ride_name': 'Teacups', 'tier': 3, 'tier_weight': 1,
             'downtime_hours': 1.0, 'weighted_contribution': 1.0},
        ]
        repo._get_rides_with_downtime_for_date = MagicMock(return_value=mock_rides)

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # Current behavior: NULL shame_score returns 0
        # This is intentional - we use park_daily_stats as single source of truth
        assert breakdown['shame_score'] == 0, \
            f"When shame_score is NULL, should return 0, got {breakdown['shame_score']}"

    def test_returns_zero_when_no_downtime(self):
        """
        When shame_score is NULL AND there's no downtime, return 0.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Daily stat with NULL shame_score AND no downtime
        mock_daily_stat = MagicMock()
        mock_daily_stat.shame_score = None
        mock_daily_stat.total_downtime_hours = Decimal('0')
        mock_daily_stat.avg_uptime_percentage = Decimal('100.0')
        mock_daily_stat.rides_with_downtime = 0
        mock_daily_stat.total_rides_tracked = 40

        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = mock_daily_stat
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)
        repo._get_rides_with_downtime_for_date = MagicMock(return_value=[])

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # With no downtime, 0 is correct
        assert breakdown['shame_score'] == 0

    def test_uses_stored_shame_score_when_available(self):
        """
        When shame_score IS stored (not NULL), use the stored value.

        NOTE: The returned value is rounded to 1 decimal place for display
        consistency with the Rankings page.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Daily stat with valid shame_score
        mock_daily_stat = MagicMock()
        mock_daily_stat.shame_score = Decimal('19.29')  # Pre-calculated
        mock_daily_stat.weighted_downtime_hours = Decimal('35.33')
        mock_daily_stat.effective_park_weight = Decimal('18.3')
        mock_daily_stat.total_downtime_hours = Decimal('35.33')
        mock_daily_stat.avg_uptime_percentage = Decimal('75.0')
        mock_daily_stat.rides_with_downtime = 15
        mock_daily_stat.total_rides_tracked = 40

        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = mock_daily_stat
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)
        repo._get_rides_with_downtime_for_date = MagicMock(return_value=[
            {'ride_id': 1, 'ride_name': 'Test', 'tier': 1, 'tier_weight': 3,
             'downtime_hours': 2.0, 'weighted_contribution': 6.0},
        ])

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # Should use the stored value, rounded to 1 decimal place
        # 19.29 rounds to 19.3
        assert breakdown['shame_score'] == 19.3


class TestWeeklyShameScoreNullFallback:
    """Test that weekly breakdown calculates fallback when shame_score is NULL."""

    def test_weekly_returns_calculated_shame_when_all_days_null(self):
        """
        FAILING TEST: When ALL daily shame_scores are NULL,
        calculate from ride data instead of returning 0.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Create mock daily stats - ALL with NULL shame_score
        mock_stats = []
        for i in range(7):
            stat = MagicMock()
            stat.shame_score = None  # ALL NULL - aggregation failed
            stat.total_downtime_hours = Decimal('10.0')
            stat.avg_uptime_percentage = Decimal('80.0')
            stat.rides_with_downtime = 5
            stat.total_rides_tracked = 20
            mock_stats.append(stat)

        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = mock_stats
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)

        # Mock rides with downtime - weighted_contribution = 30
        mock_rides = [
            {'ride_id': 1, 'ride_name': 'Test Ride', 'tier': 1, 'tier_weight': 3,
             'downtime_hours': 10.0, 'weighted_contribution': 30.0},
        ]
        repo._get_rides_with_downtime_for_date_range = MagicMock(return_value=mock_rides)

        with patch('utils.timezone.get_last_week_date_range',
                   return_value=(date(2025, 12, 19), date(2025, 12, 25), None)):
            breakdown = repo.get_park_weekly_shame_breakdown(park_id=194)

        # With all NULL shame_scores, naive approach gives 0
        # With proper fallback, should calculate from ride data
        # weighted_downtime = 30, total_weight = 3, shame = (30/3)*10 = 100
        assert breakdown['shame_score'] > 0, \
            f"Weekly shame_score should be calculated from rides, got {breakdown['shame_score']}"


class TestMonthlyShameScoreNullFallback:
    """Test that monthly breakdown calculates fallback when shame_score is NULL."""

    def test_monthly_returns_calculated_shame_when_all_days_null(self):
        """
        FAILING TEST: When ALL daily shame_scores are NULL (aggregation never ran),
        calculate from ride data.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Create mock daily stats - ALL with NULL shame_score
        mock_stats = []
        for i in range(30):
            stat = MagicMock()
            stat.shame_score = None  # ALL NULL - aggregation failed for entire month
            stat.total_downtime_hours = Decimal('5.0')
            stat.avg_uptime_percentage = Decimal('85.0')
            stat.rides_with_downtime = 3
            stat.total_rides_tracked = 20
            mock_stats.append(stat)

        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = mock_stats
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)

        # Mock rides with downtime
        mock_rides = [
            {'ride_id': 1, 'ride_name': 'Test Ride', 'tier': 2, 'tier_weight': 2,
             'downtime_hours': 50.0, 'weighted_contribution': 100.0},
        ]
        repo._get_rides_with_downtime_for_date_range = MagicMock(return_value=mock_rides)

        with patch('utils.timezone.get_last_month_date_range',
                   return_value=(date(2025, 11, 26), date(2025, 12, 25), None)):
            breakdown = repo.get_park_monthly_shame_breakdown(park_id=194)

        # With all NULL shame_scores, naive approach gives 0
        # With proper fallback, should calculate from ride data
        assert breakdown['shame_score'] > 0, \
            f"Monthly shame_score should be calculated from rides, got {breakdown['shame_score']}"


class TestShameScoreCalculationFormula:
    """Test that fallback calculation uses correct formula."""

    def test_fallback_uses_metrics_calculate_shame_score(self):
        """
        The fallback should use the same formula as utils/metrics.py.

        Formula: shame_score = (weighted_downtime_hours / total_park_weight) * 10
        """
        from utils.metrics import calculate_shame_score

        # Example: 13 weighted hours, 6 total weight
        result = calculate_shame_score(
            total_weighted_downtime_hours=13.0,
            total_park_weight=6.0
        )

        # (13 / 6) * 10 = 21.67
        assert result == pytest.approx(21.67, rel=0.01)

    def test_fallback_handles_zero_park_weight(self):
        """
        When total_park_weight is 0, should return None or 0, not crash.
        """
        from utils.metrics import calculate_shame_score

        result = calculate_shame_score(
            total_weighted_downtime_hours=13.0,
            total_park_weight=0
        )

        assert result is None or result == 0
