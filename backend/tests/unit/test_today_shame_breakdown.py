"""
Tests for TODAY shame breakdown consistency.

BUG: get_park_today_shame_breakdown() uses get_pacific_day_range_utc() which
returns a full 24-hour range, while today rankings use get_today_range_to_now_utc()
which correctly returns midnight to NOW. This causes the details modal to show
zero shame score when the rankings table shows a value.

BUG 2: tier_weights hardcoded as 5/2/1 instead of 3/2/1.
"""


class TestTodayShameBreakdownTimeRange:
    """Tests for correct time range usage in today shame breakdown."""

    def test_today_shame_breakdown_uses_correct_time_range_function(self):
        """
        FAILING TEST: get_park_today_shame_breakdown should use get_today_range_to_now_utc()
        not get_pacific_day_range_utc() which returns full day including future times.

        The rankings query uses get_today_range_to_now_utc() - the details should match.
        """
        # Read the source file and check which function is used
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_today_shame_breakdown)

        # Should use get_today_range_to_now_utc, NOT get_pacific_day_range_utc
        assert 'get_today_range_to_now_utc' in source, (
            "get_park_today_shame_breakdown should use get_today_range_to_now_utc() "
            "to match the time range used by TodayParkRankingsQuery. "
            "Currently uses get_pacific_day_range_utc() which includes future times."
        )
        assert 'get_pacific_day_range_utc(today)' not in source, (
            "get_park_today_shame_breakdown should NOT use get_pacific_day_range_utc() "
            "because that returns the full 24-hour day including future times when "
            "data doesn't exist yet."
        )


class TestTodayShameBreakdownTierWeights:
    """Tests for correct tier weights in today shame breakdown response."""

    def test_tier_weights_use_correct_3x_2x_1x_system(self):
        """
        FAILING TEST: tier_weights in response should be 3/2/1, not 5/2/1.

        The database uses tier_weight values of 3, 2, 1 for Tier 1, 2, 3 respectively.
        The returned tier_weights dict should match.
        """
        # Read the source file and check the tier_weights dict
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_today_shame_breakdown)

        # Should return tier_weights with 1: 3 (not 1: 5)
        assert '1: 3' in source or '"1": 3' in source, (
            "tier_weights should use 3 for Tier 1 (flagship), not 5. "
            "The database uses tier_weight=3 for Tier 1 rides."
        )
        assert '1: 5' not in source and '"1": 5' not in source, (
            "tier_weights should NOT use 5 for Tier 1. "
            "The correct value is 3 (3x weight for flagship attractions)."
        )


class TestYesterdayShameBreakdownTierWeights:
    """Tests for correct tier weights in yesterday shame breakdown response."""

    def test_yesterday_tier_weights_use_correct_3x_2x_1x_system(self):
        """
        FAILING TEST: tier_weights in yesterday response should be 3/2/1, not 5/2/1.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_yesterday_shame_breakdown)

        # Should return tier_weights with 1: 3 (not 1: 5)
        assert '1: 3' in source or '"1": 3' in source, (
            "tier_weights should use 3 for Tier 1 (flagship), not 5."
        )


class TestLiveShameBreakdownTierWeights:
    """Tests for correct tier weights in live shame breakdown response."""

    def test_live_tier_weights_use_correct_3x_2x_1x_system(self):
        """
        FAILING TEST: tier_weights in live response should be 3/2/1, not 5/2/1.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        source = inspect.getsource(StatsRepository.get_park_shame_breakdown)

        # Should return tier_weights with 1: 3 (not 1: 5)
        assert '1: 3' in source or '"1": 3' in source, (
            "tier_weights should use 3 for Tier 1 (flagship), not 5."
        )


class TestChartDataAverageFormat:
    """Tests for chart_data.average numeric format.

    ARCHITECTURE CHANGE (Dec 2025):
    Shame score is now calculated ONCE during data collection and stored in
    park_activity_snapshots.shame_score. The chart's average is computed from
    these stored values, ensuring consistency with rankings and breakdown panels.
    """

    def test_park_shame_history_reads_stored_shame_score(self):
        """
        Verify that get_single_park_hourly READs from pas.shame_score.

        This ensures the chart average matches the rankings table value because
        all components read from the same stored source.
        """
        import inspect
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        source = inspect.getsource(ParkShameHistoryQuery.get_single_park_hourly)

        # The method should return an 'average' field
        assert 'average' in source, (
            "get_single_park_hourly should return an 'average' field in the response"
        )
        # Verify it reads from stored shame_score (single source of truth)
        assert 'pas.shame_score' in source, (
            "get_single_park_hourly should READ from pas.shame_score (stored value)"
        )
        # Should NOT use ShameScoreCalculator anymore
        assert 'ShameScoreCalculator' not in source, (
            "get_single_park_hourly should NOT use ShameScoreCalculator - "
            "it should READ from stored pas.shame_score instead"
        )


class TestChartAverageMatchesBreakdown:
    """Tests that chart_data.average matches shame_breakdown.shame_score.

    BUG: The chart's average is calculated differently from the breakdown's
    shame_score, causing inconsistent values to be displayed:
    - Chart average: Calculated from hourly data with restrictive filters
    - Breakdown shame_score: Calculated from cumulative data

    The fix should override chart_data.average with shame_breakdown.shame_score
    in the API response to ensure consistency.
    """

    def test_parks_route_overrides_chart_average_with_breakdown(self):
        """
        Verify that the parks details route overrides chart_data.average
        with the shame_breakdown.shame_score for consistency.
        """
        import os

        # Read the parks.py source file directly
        parks_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/parks.py'
        )
        with open(parks_file, 'r') as f:
            source = f.read()

        # Should override chart_data['average'] with breakdown's shame_score
        assert "chart_data['average']" in source or 'chart_data["average"]' in source, (
            "The parks details route should override chart_data['average'] "
            "with shame_breakdown['shame_score'] to ensure the chart displays "
            "the same average as the breakdown panel."
        )
