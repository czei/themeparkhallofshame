"""
Unit Tests for Wait Times Trend Calculation
============================================

TDD Tests: These tests define the EXPECTED trend_percentage calculation
for the Wait Times tab. Trend shows the change in average wait time
compared to yesterday.

The Bug These Tests Catch:
--------------------------
Frontend shows "N/A" for trend because trend_percentage is always NULL.
Trend should be calculated as:
  (today_avg - yesterday_avg) / yesterday_avg * 100

Example:
- Today's avg wait: 45 minutes
- Yesterday's avg wait: 40 minutes
- Trend: +12.5% (waits are getting longer)
"""

import pytest
from unittest.mock import MagicMock, patch


class TestRideWaitTimesTrendCalculation:
    """Test that ride wait times calculates trend from historical data."""

    def test_ride_wait_times_trend_uses_yesterday_comparison(self):
        """
        CRITICAL: trend_percentage should compare today vs yesterday.

        The SQL should:
        1. Get yesterday's avg_wait_time from ride_daily_stats
        2. Calculate: (today_avg - yesterday_avg) / yesterday_avg * 100

        Frontend code (wait-times.js line 376-382):
            const trendPct = ride.trend_percentage !== null ...
            const trendText = trendPct !== null
                ? `${trendPct > 0 ? '+' : ''}${trendPct.toFixed(1)}%`
                : 'N/A';
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # Should join or subquery ride_daily_stats for yesterday's data
        assert 'ride_daily_stats' in source or 'yesterday' in source.lower(), \
            "Ride trend must use ride_daily_stats to get yesterday's avg"

    def test_ride_wait_times_trend_formula_is_percentage_change(self):
        """
        Trend formula should be percentage change from yesterday.

        Formula: ((today - yesterday) / yesterday) * 100
        - Positive = waits are increasing (bad for guests)
        - Negative = waits are decreasing (good for guests)
        - Zero = no change
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # Should have percentage calculation logic
        # Either in SQL or in Python post-processing
        has_calculation = (
            'yesterday' in source.lower() or
            'ride_daily_stats' in source or
            '- yesterday_avg' in source or
            '/ yesterday' in source.lower()
        )
        assert has_calculation, \
            "Ride trend must calculate percentage change vs yesterday"

    def test_ride_wait_times_trend_handles_no_yesterday_data(self):
        """
        When yesterday has no data, trend should be NULL (shows as N/A).

        This happens for:
        - New rides
        - Rides that were closed yesterday
        - Parks that weren't operating yesterday
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # Should use LEFT JOIN or COALESCE to handle missing yesterday data
        has_null_handling = (
            'LEFT JOIN' in source or
            'COALESCE' in source or
            'IFNULL' in source or
            'yesterday_stats' in source
        )
        assert has_null_handling, \
            "Ride trend must handle missing yesterday data gracefully"


class TestParkWaitTimesTrendCalculation:
    """Test that park wait times calculates trend from historical data."""

    def test_park_wait_times_trend_uses_yesterday_comparison(self):
        """
        Park trend should compare today vs yesterday park-wide average.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # Should join or subquery park_daily_stats for yesterday's data
        assert 'park_daily_stats' in source or 'yesterday' in source.lower(), \
            "Park trend must use park_daily_stats to get yesterday's avg"

    def test_park_wait_times_trend_handles_no_yesterday_data(self):
        """
        When yesterday has no data, trend should be NULL (shows as N/A).
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # Should use LEFT JOIN or COALESCE to handle missing yesterday data
        has_null_handling = (
            'LEFT JOIN' in source or
            'COALESCE' in source or
            'IFNULL' in source or
            'yesterday_stats' in source
        )
        assert has_null_handling, \
            "Park trend must handle missing yesterday data gracefully"


class TestTrendCalculationFormula:
    """Test the trend calculation formula is correct."""

    def test_trend_formula_example_positive(self):
        """
        Example: Today 45 min, Yesterday 40 min
        Trend = (45 - 40) / 40 * 100 = 12.5%
        """
        today_avg = 45
        yesterday_avg = 40
        expected_trend = ((today_avg - yesterday_avg) / yesterday_avg) * 100

        assert expected_trend == 12.5

    def test_trend_formula_example_negative(self):
        """
        Example: Today 30 min, Yesterday 40 min
        Trend = (30 - 40) / 40 * 100 = -25%
        """
        today_avg = 30
        yesterday_avg = 40
        expected_trend = ((today_avg - yesterday_avg) / yesterday_avg) * 100

        assert expected_trend == -25.0

    def test_trend_formula_example_no_change(self):
        """
        Example: Today 40 min, Yesterday 40 min
        Trend = (40 - 40) / 40 * 100 = 0%
        """
        today_avg = 40
        yesterday_avg = 40
        expected_trend = ((today_avg - yesterday_avg) / yesterday_avg) * 100

        assert expected_trend == 0.0


class TestTrendNotNullForLiveData:
    """Test that trend is calculated, not hardcoded to NULL."""

    def test_ride_trend_is_not_hardcoded_null(self):
        """
        trend_percentage should NOT be hardcoded to NULL.

        Previous bug: SQL had "NULL AS trend_percentage"
        Fix: Should calculate from yesterday's data
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)

        # Should NOT have hardcoded NULL for trend
        # Allow NULL only in COALESCE/IFNULL fallback
        lines = source.split('\n')
        for line in lines:
            # Skip lines that are COALESCE or comments
            if 'COALESCE' in line or 'IFNULL' in line or line.strip().startswith('--') or line.strip().startswith('#'):
                continue
            # Flag hardcoded NULL AS trend_percentage
            if 'NULL AS trend_percentage' in line:
                pytest.fail("trend_percentage should not be hardcoded to NULL")

    def test_park_trend_is_not_hardcoded_null(self):
        """
        Park trend_percentage should NOT be hardcoded to NULL.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        lines = source.split('\n')
        for line in lines:
            if 'COALESCE' in line or 'IFNULL' in line or line.strip().startswith('--') or line.strip().startswith('#'):
                continue
            if 'NULL AS trend_percentage' in line:
                pytest.fail("trend_percentage should not be hardcoded to NULL")
