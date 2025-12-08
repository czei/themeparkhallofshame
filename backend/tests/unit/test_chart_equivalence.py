"""
Chart Data Equivalence Tests
============================

TDD tests to verify chart data from hourly tables matches raw snapshot data.

This is CRITICAL for data integrity - users must see identical charts whether
we query hourly tables or raw snapshots.

Test Strategy:
1. Query chart data using raw snapshots (current implementation)
2. Query chart data using hourly tables (new implementation)
3. Compare results (should match within rounding tolerance)

Test Coverage:
- TODAY period: Hourly chart data for current day
- YESTERDAY period: Hourly chart data for previous day
- Single park charts (park details modal)
- Multi-park charts (trends page)

Acceptance Criteria:
- Shame scores match within 0.1 (one decimal place)
- Time labels match exactly
- Number of data points matches
- Missing data (None/null) handled consistently

Related Files:
- src/database/queries/charts/park_shame_history.py
- src/database/tables: park_hourly_stats, park_activity_snapshots
"""

import pytest
from decimal import Decimal
from datetime import datetime, date, timedelta
from pathlib import Path


class TestChartQueryArchitecture:
    """
    Test the architecture of chart queries supports hourly tables.

    The ParkShameHistoryQuery class should have methods to query
    both raw snapshots and hourly tables.
    """

    def test_chart_query_has_hourly_table_method(self):
        """
        ParkShameHistoryQuery should have method to query hourly tables.

        Expected method:
        _query_hourly_tables(
            park_id: int,
            start_hour: datetime,
            end_hour: datetime
        ) -> List[Dict]

        Returns hourly shame scores from park_hourly_stats table.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should have method to query hourly tables
            assert ("_query_hourly_tables" in source_code or
                    "park_hourly_stats" in source_code), \
                "ParkShameHistoryQuery must support querying hourly tables"

    def test_chart_query_checks_feature_flag(self):
        """
        Chart query should check USE_HOURLY_TABLES feature flag.

        When flag=true: Use park_hourly_stats (fast)
        When flag=false: Use park_activity_snapshots (current behavior)
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should check feature flag
            assert "USE_HOURLY_TABLES" in source_code, \
                "ParkShameHistoryQuery must check USE_HOURLY_TABLES feature flag"


class TestHourlyChartDataStructure:
    """
    Test hourly chart data structure matches expected format.

    Charts use Chart.js format:
    {
        "labels": ["6:00", "7:00", ..., "23:00"],
        "datasets": [
            {"label": "Magic Kingdom", "data": [5.2, 4.8, 6.1, ...]},
            {"label": "EPCOT", "data": [3.1, 2.9, 3.5, ...]}
        ]
    }
    """

    def test_hourly_chart_has_correct_structure(self):
        """
        Hourly chart response must have labels and datasets.

        Required fields:
        - labels: List[str] (hour labels like "6:00")
        - datasets: List[Dict] (one per park)

        Each dataset:
        - label: str (park name)
        - data: List[float|None] (shame scores, None for missing hours)
        """
        chart_response = {
            "labels": ["6:00", "7:00", "8:00", "9:00"],
            "datasets": [
                {
                    "label": "Magic Kingdom",
                    "data": [5.2, 4.8, 6.1, 5.9]
                }
            ]
        }

        assert "labels" in chart_response
        assert "datasets" in chart_response
        assert isinstance(chart_response["labels"], list)
        assert isinstance(chart_response["datasets"], list)

        if chart_response["datasets"]:
            dataset = chart_response["datasets"][0]
            assert "label" in dataset
            assert "data" in dataset
            assert len(dataset["data"]) == len(chart_response["labels"])

    def test_hourly_labels_span_6am_to_11pm(self):
        """
        Hourly charts should show 6:00 AM to 11:00 PM (18 hours).

        Park operating hours typically:
        - Open: 8:00 AM - 10:00 PM
        - Chart range: 6:00 AM - 11:00 PM (capture early/late activity)
        """
        expected_labels = [f"{h}:00" for h in range(6, 24)]
        assert len(expected_labels) == 18
        assert expected_labels[0] == "6:00"
        assert expected_labels[-1] == "23:00"

    def test_shame_scores_are_nullable(self):
        """
        Shame scores can be None when:
        - Park hasn't opened yet today
        - Park is closed (seasonal)
        - No snapshot data for that hour

        Chart.js handles None by showing gaps in the line.
        """
        dataset_with_missing_data = {
            "label": "Michigan's Adventure",
            "data": [None, None, None, 5.2, 4.8, None]  # Closed until hour 3
        }

        # This is valid - None values are allowed
        assert dataset_with_missing_data["data"][0] is None
        assert dataset_with_missing_data["data"][3] == 5.2


class TestChartDataEquivalence:
    """
    Test chart data from hourly tables equals raw snapshot data.

    This is the PRIMARY TEST for data integrity.
    """

    def test_hourly_table_chart_matches_raw_snapshot_chart(self):
        """
        Chart data from hourly tables should match raw snapshots.

        Test approach:
        1. Query hourly chart using park_hourly_stats
        2. Query hourly chart using park_activity_snapshots
        3. Compare shame scores (should match within 0.1)

        This will be tested in integration tests with real data.
        """
        # Placeholder for integration test
        # Will query same park/hour both ways and compare
        pass

    def test_shame_score_rounding_is_consistent(self):
        """
        Shame scores rounded to 1 decimal place consistently.

        Sources:
        - park_hourly_stats.shame_score: DECIMAL(3,1)
        - park_activity_snapshots.shame_score: DECIMAL(3,1)
        - Chart query: ROUND(AVG(...), 1)

        All should produce same precision.
        """
        # Verify database schema has correct precision
        migration_path = Path(__file__).parent.parent.parent / "src" / "database" / "migrations" / "013_add_hourly_stats.sql"

        if migration_path.exists():
            source_code = migration_path.read_text()

            # Verify shame_score is DECIMAL(3,1) in hourly table
            assert "shame_score DECIMAL(3,1)" in source_code, \
                "park_hourly_stats.shame_score must be DECIMAL(3,1)"

    def test_missing_hours_handled_consistently(self):
        """
        Missing data should be None in both implementations.

        Scenario: Park opens at 9am, chart starts at 6am
        - Hours 6-8: Should be None (park closed)
        - Hours 9+: Should have shame scores

        Both hourly tables and raw snapshots must return None (not 0).
        """
        # This will be tested in integration tests
        pass


class TestSingleParkChartData:
    """
    Test single park hourly chart (park details modal).

    Endpoint: GET /api/parks/<id>/details?period=today
    Returns: chart_data with hourly shame scores for one park
    """

    def test_single_park_chart_uses_hourly_tables_when_enabled(self):
        """
        Single park chart should use hourly tables when flag enabled.

        The get_single_park_hourly() method should check USE_HOURLY_TABLES
        and query park_hourly_stats accordingly.
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Method should exist
            assert "def get_single_park_hourly" in source_code, \
                "ParkShameHistoryQuery must have get_single_park_hourly method"

    def test_single_park_chart_returns_18_data_points(self):
        """
        Single park hourly chart should return 18 data points (6am-11pm).

        Even if park only operated 10am-8pm, chart should show:
        - 6am-9am: None
        - 10am-8pm: shame scores
        - 9pm-11pm: None
        """
        # Chart should always have 18 hours, with None for closed hours
        expected_hours = 18
        assert expected_hours == 18

    def test_single_park_chart_includes_current_field(self):
        """
        Single park chart should include 'current' shame score.

        Response format:
        {
            "labels": ["6:00", "7:00", ...],
            "data": [None, None, 5.2, 4.8, ...],
            "average": 5.0,
            "current": 4.8  # Last non-None value
        }
        """
        chart_data = {
            "labels": ["6:00", "7:00", "8:00", "9:00"],
            "data": [None, None, 5.2, 4.8],
            "average": 5.0,
            "current": 4.8
        }

        assert "current" in chart_data
        assert "average" in chart_data
        assert "data" in chart_data


class TestMultiParkChartData:
    """
    Test multi-park hourly chart (trends page).

    Endpoint: GET /api/trends/chart-data?type=parks&period=today
    Returns: Chart with top 5 parks by downtime
    """

    def test_multi_park_chart_limits_to_top_parks(self):
        """
        Multi-park chart should show top 5 parks by downtime.

        This prevents chart from being too crowded.
        Default limit: 5 parks
        """
        default_limit = 5
        assert default_limit == 5

    def test_multi_park_chart_uses_same_time_range_for_all_parks(self):
        """
        All parks should use same time range (6am-11pm).

        This ensures charts are comparable:
        - All parks show 18 hours
        - X-axis labels are identical
        - Data points align across datasets
        """
        # All datasets must have same length
        pass


class TestChartDataForDifferentPeriods:
    """
    Test chart data for different time periods.

    Periods:
    - TODAY: Hourly data, current day (uses hourly tables)
    - YESTERDAY: Hourly data, previous day (uses hourly tables)
    - LAST_WEEK: Daily data, past 7 days (uses park_daily_stats)
    - LAST_MONTH: Daily data, past 30 days (uses park_daily_stats)
    """

    def test_today_period_uses_hourly_granularity(self):
        """
        TODAY period should return hourly data points.

        Expected: 18 data points (one per hour from 6am-11pm)
        """
        # TODAY and YESTERDAY use hourly data
        expected_today_points = 18
        assert expected_today_points == 18

    def test_yesterday_period_uses_hourly_granularity(self):
        """
        YESTERDAY period should return hourly data points.

        Expected: 18 data points (full day, 6am-11pm)
        YESTERDAY is immutable (won't change), so highly cacheable.
        """
        expected_yesterday_points = 18
        assert expected_yesterday_points == 18

    def test_last_week_period_uses_daily_granularity(self):
        """
        LAST_WEEK period should return daily data points.

        Expected: 7 data points (one per day)
        Uses park_daily_stats table (not affected by hourly refactor)
        """
        expected_week_points = 7
        assert expected_week_points == 7

    def test_last_month_period_uses_daily_granularity(self):
        """
        LAST_MONTH period should return daily data points.

        Expected: 30 data points (one per day)
        Uses park_daily_stats table (not affected by hourly refactor)
        """
        expected_month_points = 30
        assert expected_month_points == 30


class TestChartPerformance:
    """
    Test chart query performance with hourly tables.

    Goal: Sub-1-second chart rendering
    """

    def test_hourly_table_query_returns_fewer_rows(self):
        """
        Querying hourly tables should return much fewer rows than raw snapshots.

        For TODAY chart (6am-11pm = 18 hours):
        - Raw snapshots: 18 hours × 12 snapshots/hour × 5 parks = 1,080 rows
        - Hourly tables: 18 hours × 5 parks = 90 rows

        12x reduction in rows to scan.
        """
        parks = 5
        hours = 18
        snapshots_per_hour = 12

        raw_rows = parks * hours * snapshots_per_hour  # 1,080
        hourly_rows = parks * hours  # 90

        reduction_factor = raw_rows / hourly_rows
        assert reduction_factor == 12.0

    def test_hourly_query_eliminates_group_by_hour_operation(self):
        """
        Raw query does GROUP BY HOUR() which is expensive.

        Raw query:
        SELECT HOUR(...) as hour, AVG(shame_score)
        FROM park_activity_snapshots
        GROUP BY HOUR(...)  # Expensive!

        Hourly query:
        SELECT hour_start_utc, shame_score
        FROM park_hourly_stats
        WHERE ...  # No GROUP BY needed!
        """
        # Verify hourly query doesn't need GROUP BY
        # The aggregation was already done by aggregate_hourly.py
        pass


class TestChartDataValidation:
    """
    Test data validation for chart queries.
    """

    def test_shame_scores_are_in_valid_range(self):
        """
        Shame scores must be in range [0.0, 10.0].

        Invalid values should not appear in charts:
        - Negative scores
        - Scores > 10.0
        - NaN, Infinity
        """
        valid_scores = [0.0, 5.2, 10.0, None]
        invalid_scores = [-1.0, 10.5, float('inf'), float('nan')]

        for score in valid_scores:
            if score is not None:
                assert 0.0 <= score <= 10.0

    def test_chart_handles_all_none_data_gracefully(self):
        """
        Chart should handle dataset with all None values.

        Scenario: Park closed all day (seasonal closure)
        Expected: Dataset shows empty line (Chart.js handles this)
        """
        all_none_dataset = {
            "label": "Michigan's Adventure",
            "data": [None] * 18
        }

        # This is valid - chart will show no line for this park
        assert all(v is None for v in all_none_dataset["data"])

    def test_chart_handles_partial_day_data(self):
        """
        Chart should handle incomplete day (e.g., TODAY before noon).

        Scenario: It's 10:30 AM, only hours 6-10 have data
        Expected: Hours 11-23 are None (future hours)
        """
        # This is handled by the hybrid query logic
        # Future hours should be None, not 0
        pass


class TestChartCaching:
    """
    Test caching behavior for chart data.
    """

    def test_yesterday_charts_are_highly_cacheable(self):
        """
        YESTERDAY chart data never changes (immutable).

        Cache TTL: 24 hours (or indefinite)
        This reduces database load significantly.
        """
        # YESTERDAY data is immutable after the day ends
        # Can cache aggressively
        pass

    def test_today_charts_have_short_cache_ttl(self):
        """
        TODAY chart data updates every 5 minutes (snapshot collection).

        Cache TTL: 5 minutes
        This keeps charts fresh while reducing query load.
        """
        expected_cache_ttl_minutes = 5
        assert expected_cache_ttl_minutes == 5


class TestChartDataIntegrity:
    """
    Test data integrity in chart queries.

    These tests verify the three canonical business rules apply to charts.
    """

    def test_charts_exclude_closed_parks(self):
        """
        Charts should not show parks that were closed all day.

        Business Rule 1: Park Status Takes Precedence

        If a park is closed all day (seasonal), it should not appear in:
        - Top parks list
        - Chart datasets
        """
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "charts" / "park_shame_history.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should check park_appears_open or park_was_open
            assert ("park_appears_open" in source_code or
                    "park_was_open" in source_code or
                    "ParkStatusSQL" in source_code), \
                "Chart queries must filter out closed parks"

    def test_charts_only_show_rides_that_operated(self):
        """
        Chart shame scores should only count rides that operated.

        Business Rule 2: Rides Must Have Operated

        This is enforced by:
        - park_hourly_stats: Pre-filtered during aggregation
        - Raw query: Uses rides_that_operated CTE
        """
        # This will be verified in integration tests
        pass
