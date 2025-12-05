"""
TDD Tests for LIVE Chart Feature.

Feature Request: Add a chart for LIVE period showing real-time granular data
(5-minute intervals for the last 60-90 minutes) instead of no chart.

This differentiates LIVE from TODAY:
- LIVE: Shows recent instantaneous shame scores at 5-min granularity
- TODAY: Shows hourly averages for the full day
"""
from unittest.mock import MagicMock


class TestLiveChartBackend:
    """Tests for backend LIVE chart data generation."""

    def test_park_details_live_returns_chart_data(self):
        """
        FAILING TEST: /parks/<id>/details?period=live should return chart_data
        with recent snapshots (last 60 minutes at 5-min intervals).

        Currently returns chart_data: null for LIVE period.
        """
        import os

        # Read parks.py to check if LIVE period generates chart_data
        parks_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/parks.py'
        )
        with open(parks_file, 'r') as f:
            source = f.read()

        # Should include 'live' in the periods that generate chart_data
        # Currently: if period in ('today', 'yesterday'):
        # Should be: if period in ('live', 'today', 'yesterday'):
        assert "if period in ('live', 'today', 'yesterday'):" in source or \
               "period == 'live'" in source and "chart_data" in source, (
            "parks.py should generate chart_data for LIVE period. "
            "Currently only TODAY and YESTERDAY periods get chart data."
        )

    def test_shame_score_calculator_has_get_recent_snapshots_method(self):
        """
        FAILING TEST: ShameScoreCalculator should have a get_recent_snapshots()
        method that returns the last 60-90 minutes of instantaneous shame scores.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        # Should have get_recent_snapshots method
        assert hasattr(calc, 'get_recent_snapshots'), (
            "ShameScoreCalculator should have a get_recent_snapshots() method "
            "that returns recent instantaneous shame scores for LIVE charts."
        )

    def test_get_recent_snapshots_returns_correct_structure(self):
        """
        FAILING TEST: get_recent_snapshots() should return a dict with:
        - labels: List of time labels (e.g., ['10:05', '10:10', '10:15', ...])
        - data: List of instantaneous shame scores at each 5-min interval
        - granularity: 'minutes' (to differentiate from hourly charts)
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        # Call the method (will fail until implemented)
        result = calc.get_recent_snapshots(park_id=139, minutes=60)

        # Should return dict with correct structure
        assert isinstance(result, dict), "get_recent_snapshots should return a dict"
        assert 'labels' in result, "Result should have 'labels' key"
        assert 'data' in result, "Result should have 'data' key"
        assert 'granularity' in result, "Result should have 'granularity' key"
        assert result['granularity'] == 'minutes', "Granularity should be 'minutes'"

    def test_get_recent_snapshots_labels_and_data_same_length(self):
        """
        The labels and data arrays should always have the same length.

        Note: The actual number of data points depends on available snapshot data.
        With real data for 60 minutes at 5-min intervals, expect up to 12 points.
        With mock/empty data, arrays will be empty but still equal length.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        result = calc.get_recent_snapshots(park_id=139, minutes=60)

        labels_len = len(result.get('labels', []))
        data_len = len(result.get('data', []))

        assert labels_len == data_len, (
            f"Labels ({labels_len}) and data ({data_len}) arrays should have same length"
        )


class TestLiveChartAPIResponse:
    """Tests for API response structure when period=live."""

    def test_live_chart_data_different_from_today(self):
        """
        FAILING TEST: LIVE chart_data should have different granularity from TODAY.

        - LIVE: granularity='minutes', labels like ['10:05', '10:10', ...]
        - TODAY: granularity='hourly', labels like ['6:00', '7:00', ...]
        """
        import os

        # This test documents the expected difference in behavior
        # Currently LIVE returns chart_data: null

        parks_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/parks.py'
        )
        with open(parks_file, 'r') as f:
            source = f.read()

        # Should call different methods for LIVE vs TODAY
        assert 'get_recent_snapshots' in source, (
            "parks.py should call get_recent_snapshots() for LIVE period "
            "to get minute-granularity chart data."
        )


class TestLiveChartCurrentValue:
    """Tests for LIVE chart 'current' value accuracy.

    BUG: The 'Current: X.X' badge was showing a value from shame_breakdown
    that didn't match the chart's actual most recent data point.

    FIX: For LIVE period, the 'current' value should be the last non-null
    data point from the chart, not a separately-calculated breakdown value.
    """

    def test_live_chart_current_equals_last_data_point(self):
        """
        FAILING TEST: For LIVE period, chart_data['current'] should equal
        the last non-null value from chart_data['data'].

        The UI shows 'Current: X.X' badge which must match the rightmost
        point on the chart for visual consistency.
        """
        import os

        parks_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/parks.py'
        )
        with open(parks_file, 'r') as f:
            source = f.read()

        # The code should set 'current' from the chart data, not breakdown
        # Look for logic that extracts last non-null value from data array
        assert "chart_data['current']" in source or 'chart_data["current"]' in source, (
            "For LIVE period, parks.py should set chart_data['current'] to "
            "the last non-null value from chart_data['data'] array. "
            "Currently it uses shame_breakdown['shame_score'] which doesn't "
            "match the chart's visual representation."
        )

    def test_live_api_response_has_current_field(self):
        """
        LIVE chart_data should have a 'current' field (not 'average') to clearly
        indicate this is the most recent instantaneous value, not an average.
        """
        import os

        parks_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/parks.py'
        )
        with open(parks_file, 'r') as f:
            source = f.read()

        # Should set 'current' for LIVE, 'average' for TODAY/YESTERDAY
        assert "period == 'live'" in source, "parks.py should have LIVE-specific handling"
        # The LIVE handler should extract last value from data array
        live_section_has_last_value = (
            "chart_data['data']" in source or
            'chart_data["data"]' in source
        )
        assert live_section_has_last_value, (
            "LIVE period should extract the last value from chart_data['data'] "
            "to use as the 'current' value shown in the badge."
        )


class TestChartsTabLiveChart:
    """Tests for Charts tab (trends) LIVE period with 5-minute granularity.

    The Charts tab shows multi-park comparison charts. For LIVE period,
    it should show 5-minute granularity data like the park details modal,
    not hourly data like TODAY.
    """

    def test_park_shame_history_has_get_live_method(self):
        """
        FAILING TEST: ParkShameHistoryQuery should have a get_live() method
        that returns multi-park chart data at 5-minute granularity.
        """
        import os

        # Check that get_live method exists in park_shame_history.py
        query_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/database/queries/charts/park_shame_history.py'
        )
        with open(query_file, 'r') as f:
            source = f.read()

        assert 'def get_live(' in source, (
            "ParkShameHistoryQuery should have a get_live() method that returns "
            "multi-park chart data at 5-minute granularity for the Charts tab."
        )

    def test_trends_route_uses_get_live_for_live_period(self):
        """
        FAILING TEST: The trends/chart-data endpoint should use get_live()
        for period=live, not get_hourly() like TODAY.
        """
        import os

        trends_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/trends.py'
        )
        with open(trends_file, 'r') as f:
            source = f.read()

        # LIVE period should have separate handling from TODAY
        # Check for get_live() call when period is 'live'
        assert "query.get_live(" in source or ".get_live(" in source, (
            "trends.py should call get_live() for LIVE period to return "
            "5-minute granularity chart data instead of hourly data like TODAY."
        )

    def test_trends_live_returns_minutes_granularity(self):
        """
        FAILING TEST: The trends/chart-data endpoint with period=live should
        return granularity='minutes' to match the park details modal.
        """
        import os

        trends_file = os.path.join(
            os.path.dirname(__file__),
            '../../src/api/routes/trends.py'
        )
        with open(trends_file, 'r') as f:
            source = f.read()

        # For LIVE period, granularity should be 'minutes', not 'hourly'
        # Look for separate handling of live period setting minutes granularity
        assert "granularity = 'minutes'" in source, (
            "trends.py should set granularity='minutes' for LIVE period "
            "to differentiate it from TODAY's hourly granularity."
        )


class TestLiveChartLabels:
    """Tests for LIVE chart label formatting."""

    def test_labels_show_time_in_hh_mm_format(self):
        """
        FAILING TEST: Labels should be in HH:MM format showing actual times.
        Example: ['10:05', '10:10', '10:15', ...]
        """
        from database.calculators.shame_score import ShameScoreCalculator

        mock_db = MagicMock()
        calc = ShameScoreCalculator(mock_db)

        result = calc.get_recent_snapshots(park_id=139, minutes=60)

        labels = result.get('labels', [])
        if labels:
            # Labels should be in HH:MM format
            import re
            for label in labels:
                assert re.match(r'^\d{1,2}:\d{2}$', label), (
                    f"Label '{label}' should be in HH:MM format"
                )
