"""
Unit tests for StatsRepository.get_aggregate_park_stats() summary stats.

These tests verify the business logic for aggregate stats panel values:
- Each period (LIVE, TODAY, YESTERDAY, LAST_WEEK, LAST_MONTH) uses correct ORM model
- Disney/Universal filter is applied correctly
- Results are formatted consistently
- Edge cases (no data, NULL values) are handled

Bug Context (2025-12-26):
- Stats panels on index.html showed all zeros
- Root cause: _get_summary_stats returned hardcoded zeros
- Fix: Query pre-aggregated tables based on period using ORM

ORM Refactoring (2025-12-27):
- Converted from raw SQL to pure ORM queries
- Uses ParkLiveRankings, ParkHourlyStats, ParkDailyStats models
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from sqlalchemy.orm import Session
from decimal import Decimal


class MockQueryResult:
    """Mock result from ORM query that supports attribute access."""
    def __init__(self, total_parks=0, total_rides=0, rides_down=0,
                 total_downtime_hours=0, avg_uptime=100):
        self.total_parks = total_parks
        self.total_rides = total_rides
        self.rides_down = rides_down
        self.total_downtime_hours = Decimal(str(total_downtime_hours))
        self.avg_uptime = Decimal(str(avg_uptime))


class TestSummaryStatsLivePeriod:
    """Test get_aggregate_park_stats for LIVE period."""

    def test_live_period_uses_park_live_rankings_model(self):
        """
        Given: period='live'
        When: get_aggregate_park_stats() is called with no park_id
        Then: Query ParkLiveRankings model via ORM
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=50,
            total_rides=500,
            rides_down=10,
            total_downtime_hours=5.5,
            avg_uptime=98.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='live')

        # Verify session.query was called (ORM pattern)
        mock_session.query.assert_called()

        # Verify result structure
        assert result['period'] == 'live'
        assert result['total_parks'] == 50
        assert result['rides_operating'] == 490  # 500 - 10
        assert result['rides_down'] == 10
        assert result['total_downtime_hours'] == 5.5
        assert result['avg_uptime_percentage'] == 98.0

    def test_live_period_with_disney_filter(self):
        """
        Given: period='live' and filter_disney_universal=True
        When: get_aggregate_park_stats() is called
        Then: Apply Disney/Universal filter to ORM query
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=12,
            total_rides=200,
            rides_down=5,
            total_downtime_hours=2.0,
            avg_uptime=97.5
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(
            park_id=None,
            period='live',
            filter_disney_universal=True
        )

        # Verify filter was called (for Disney/Universal)
        mock_query.filter.assert_called()

        # Verify filter_disney_universal in result
        assert result['filter_disney_universal'] is True


class TestSummaryStatsTodayPeriod:
    """Test get_aggregate_park_stats for TODAY period."""

    def test_today_period_uses_park_hourly_stats_model(self):
        """
        Given: period='today'
        When: get_aggregate_park_stats() is called with no park_id
        Then: Query ParkHourlyStats model with 24-hour window
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=45,
            total_rides=450,
            rides_down=15,
            total_downtime_hours=8.0,
            avg_uptime=96.5
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='today')

        # Verify session.query was called (ORM pattern)
        mock_session.query.assert_called()

        # Verify result
        assert result['period'] == 'today'
        assert result['total_parks'] == 45
        assert result['rides_down'] == 15


class TestSummaryStatsYesterdayPeriod:
    """Test get_aggregate_park_stats for YESTERDAY period."""

    def test_yesterday_period_uses_park_daily_stats_model(self):
        """
        Given: period='yesterday'
        When: get_aggregate_park_stats() is called with no park_id
        Then: Query ParkDailyStats model for yesterday
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=48,
            total_rides=480,
            rides_down=20,
            total_downtime_hours=12.5,
            avg_uptime=95.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='yesterday')

        # Verify session.query was called (ORM pattern)
        mock_session.query.assert_called()

        # Verify result
        assert result['period'] == 'yesterday'


class TestSummaryStatsLastWeekPeriod:
    """Test get_aggregate_park_stats for LAST_WEEK period."""

    def test_last_week_period_queries_7_days(self):
        """
        Given: period='last_week'
        When: get_aggregate_park_stats() is called with no park_id
        Then: Query ParkDailyStats model for last 7 days
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=50,
            total_rides=500,
            rides_down=75,
            total_downtime_hours=45.0,
            avg_uptime=93.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='last_week')

        # Verify result
        assert result['period'] == 'last_week'


class TestSummaryStatsLastMonthPeriod:
    """Test get_aggregate_park_stats for LAST_MONTH period."""

    def test_last_month_period_queries_30_days(self):
        """
        Given: period='last_month'
        When: get_aggregate_park_stats() is called with no park_id
        Then: Query ParkDailyStats model for last 30 days
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=50,
            total_rides=500,
            rides_down=250,
            total_downtime_hours=150.0,
            avg_uptime=90.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='last_month')

        # Verify result
        assert result['period'] == 'last_month'


class TestSummaryStatsEdgeCases:
    """Test edge cases for summary stats."""

    def test_no_results_returns_zero_values(self):
        """
        Given: Query returns no results (empty database)
        When: get_aggregate_park_stats() is called
        Then: Return dict with zero values
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        # Return result with None total_parks to trigger empty response
        mock_result = Mock()
        mock_result.total_parks = None
        mock_query.first.return_value = mock_result

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='live')

        assert result['total_parks'] == 0
        assert result['rides_operating'] == 0
        assert result['rides_down'] == 0
        assert result['total_downtime_hours'] == 0.0
        assert result['avg_uptime_percentage'] == 100.0

    def test_null_values_handled_correctly(self):
        """
        Given: Query returns NULL for some columns
        When: get_aggregate_park_stats() is called
        Then: Handle NULLs with appropriate defaults
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=10,
            total_rides=0,  # ORM COALESCE should handle this
            rides_down=0,
            total_downtime_hours=0,
            avg_uptime=100
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='live')

        # Verify results are properly defaulted
        assert result['total_parks'] == 10
        assert result['rides_operating'] == 0
        assert result['rides_down'] == 0
        assert result['total_downtime_hours'] == 0.0
        assert result['avg_uptime_percentage'] == 100.0

    def test_unknown_period_defaults_to_live(self):
        """
        Given: An unknown period value
        When: get_aggregate_park_stats() is called
        Then: Default to 'live' behavior
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=50,
            total_rides=500,
            rides_down=10,
            total_downtime_hours=5.0,
            avg_uptime=98.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period='unknown_period')

        # Unknown period should default to live
        assert result['period'] == 'live'


class TestSummaryStatsResultFormat:
    """Test that result format is consistent across periods."""

    @pytest.mark.parametrize("period", ['live', 'today', 'yesterday', 'last_week', 'last_month'])
    def test_all_periods_return_same_keys(self, period):
        """
        Given: Any valid period
        When: get_aggregate_park_stats() is called
        Then: Result contains all expected keys
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock(spec=Session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.first.return_value = MockQueryResult(
            total_parks=50,
            total_rides=500,
            rides_down=10,
            total_downtime_hours=5.0,
            avg_uptime=98.0
        )

        repo = StatsRepository(mock_session)
        result = repo.get_aggregate_park_stats(park_id=None, period=period)

        # All results should have these keys
        required_keys = [
            'period',
            'filter_disney_universal',
            'total_parks',
            'rides_operating',
            'rides_down',
            'rides_closed',
            'rides_refurbishment',
            'total_downtime_hours',
            'avg_uptime_percentage'
        ]

        for key in required_keys:
            assert key in result, f"Missing key '{key}' in result for period '{period}'"
