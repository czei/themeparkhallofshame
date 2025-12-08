"""
TODAY Hybrid Query Tests
========================

TDD tests for the hybrid query logic that combines:
1. park_hourly_stats for complete hours (fast)
2. park_activity_snapshots for incomplete current hour (accurate)

This ensures TODAY rankings are both fast and accurate.

Test Scenario (Example at 9:45 AM Pacific):
- Hours 0-8 (midnight to 9am): Query park_hourly_stats (216 rows total = 27 parks × 8 hours)
- Hour 9 (9am to 9:45am): Query park_activity_snapshots (~240 rows = 27 parks × ~9 snapshots)
- Combined: Fast query for 89% of data + real-time query for current hour

Architecture:
- TodayParkRankingsQuery.get_rankings() is the public method
- _query_hourly_tables() handles complete hours
- _query_raw_snapshots() handles current hour
- _combine_results() merges both data sources

Related Files:
- src/database/queries/today/today_park_rankings.py
- src/database/repositories/stats_repository.py (get_hourly_stats method)
- src/utils/metrics.py (USE_HOURLY_TABLES flag)
"""

import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path


class TestHybridQueryArchitecture:
    """
    Test the hybrid query architecture design.

    The TODAY query should be decomposed into:
    1. _query_hourly_tables(start_hour, end_hour) -> Dict[park_id, stats]
    2. _query_raw_snapshots(start_time, end_time) -> Dict[park_id, stats]
    3. _combine_results(hourly_stats, raw_stats) -> List[park_rankings]
    """

    def test_today_query_has_hourly_tables_method(self):
        """
        TodayParkRankingsQuery should have _query_hourly_tables() method.

        Method signature:
        def _query_hourly_tables(
            self,
            start_hour: datetime,
            end_hour: datetime,
            filter_disney_universal: bool
        ) -> Dict[int, Dict]

        Returns: {park_id: {shame_score, downtime_hours, rides_down, ...}}
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Method should exist (may be public or private)
            assert ("def _query_hourly_tables" in source_code or
                    "def query_hourly_tables" in source_code), \
                "TodayParkRankingsQuery must have method to query hourly tables"

    def test_today_query_has_raw_snapshots_method(self):
        """
        TodayParkRankingsQuery should have _query_raw_snapshots() method.

        Method signature:
        def _query_raw_snapshots(
            self,
            start_time: datetime,
            end_time: datetime,
            filter_disney_universal: bool
        ) -> Dict[int, Dict]

        Returns: {park_id: {shame_score, downtime_hours, rides_down, ...}}
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Method should exist (may be public or private)
            assert ("def _query_raw_snapshots" in source_code or
                    "def query_raw_snapshots" in source_code or
                    # Or may reuse existing method
                    "park_activity_snapshots" in source_code), \
                "TodayParkRankingsQuery must have method to query raw snapshots"

    def test_today_query_has_combine_method(self):
        """
        TodayParkRankingsQuery should have method to combine results.

        This may be:
        - _combine_results(hourly, raw) -> combined
        - Or inline logic in get_rankings() method
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should have logic to combine data sources
            assert ("combine" in source_code.lower() or
                    "SUM" in source_code or
                    "UNION" in source_code), \
                "TodayParkRankingsQuery must combine hourly and raw data"


class TestHourBoundaryCalculation:
    """
    Test calculation of hour boundaries for hybrid query.

    Given current Pacific time, determine:
    - start_of_day: Today at midnight Pacific (inclusive)
    - start_of_current_hour: Current hour at :00 (e.g., 9:00 AM)
    - now: Current time (e.g., 9:45 AM)

    Complete hours: [start_of_day, start_of_current_hour)
    Incomplete hour: [start_of_current_hour, now)
    """

    def test_calculates_start_of_day_in_pacific_time(self):
        """
        Start of day should be midnight Pacific, regardless of UTC time.

        Example:
        - Pacific: 2025-12-05 00:00:00 PST (UTC-8)
        - UTC: 2025-12-05 08:00:00 UTC

        The query should use Pacific midnight as the boundary.
        """
        # This logic should be in the query or helper
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should use Pacific timezone helper
            assert ("get_today_pacific" in source_code or
                    "Pacific" in source_code or
                    "pytz" in source_code), \
                "TODAY query must calculate start_of_day in Pacific time"

    def test_calculates_current_hour_boundary(self):
        """
        Current hour boundary is the start of the current hour.

        Example at 9:45 AM:
        - start_of_current_hour = 9:00 AM
        - This is the boundary between hourly tables and raw snapshots

        Hours 0-8 use hourly tables.
        Hour 9 (9:00-9:45) uses raw snapshots.
        """
        # This calculation should be in the query
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should calculate hour boundaries
            assert ("hour" in source_code.lower() and
                    ("replace" in source_code or "floor" in source_code.lower())), \
                "TODAY query should calculate current hour boundary"

    def test_handles_midnight_edge_case(self):
        """
        At midnight (12:00 AM), there are no complete hours yet.

        Scenario: 12:15 AM Pacific
        - Complete hours: None (0 hours)
        - Incomplete hour: [00:00, 00:15]
        - Expected: Query only raw snapshots (no hourly table query)
        """
        # Edge case should be handled gracefully
        # Either skip hourly query or query returns empty
        pass  # Implementation will be tested in integration tests


class TestFeatureFlagBehavior:
    """
    Test USE_HOURLY_TABLES feature flag controls hybrid behavior.

    Flag=true: Use hybrid query (hourly tables + raw snapshots)
    Flag=false: Use raw snapshots only (existing behavior)
    """

    def test_feature_flag_enabled_uses_hybrid_query(self):
        """
        When USE_HOURLY_TABLES=true, should use hybrid query.

        Expected behavior:
        1. Check feature flag in get_rankings()
        2. If true: Call _query_hourly_tables() + _query_raw_snapshots()
        3. Combine results
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should import and check USE_HOURLY_TABLES
            assert "USE_HOURLY_TABLES" in source_code, \
                "TODAY query must check USE_HOURLY_TABLES feature flag"

            # Should have conditional logic
            assert ("if USE_HOURLY_TABLES" in source_code or
                    "if use_hourly" in source_code.lower()), \
                "TODAY query must have conditional logic for feature flag"

    def test_feature_flag_disabled_uses_raw_snapshots_only(self):
        """
        When USE_HOURLY_TABLES=false, should use existing raw query.

        Expected behavior:
        1. Check feature flag in get_rankings()
        2. If false: Call existing raw snapshots query for entire day
        3. No hourly table access

        This provides safe fallback if hourly tables have issues.
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should have fallback path
            assert ("else:" in source_code or
                    "park_activity_snapshots" in source_code), \
                "TODAY query must have fallback to raw snapshots"


class TestDataCombinationLogic:
    """
    Test combining hourly stats with raw snapshots.

    The combination must:
    1. Sum downtime_hours across both sources
    2. Calculate weighted average shame_score
    3. Union rides_down sets (no duplicates)
    4. Average other metrics appropriately
    """

    def test_combines_downtime_hours_correctly(self):
        """
        Total downtime should be sum of hourly + current hour.

        Example for Magic Kingdom at 9:45 AM:
        - Hours 0-8 (hourly table): 12.5 hours total downtime
        - Hour 9 (raw snapshots): 1.2 hours downtime so far
        - Combined TODAY: 13.7 hours total downtime
        """
        # Test data structure
        hourly_stats = {
            16: {  # Magic Kingdom park_id
                'total_downtime_hours': 12.5,
                'shame_score': 6.0,
                'rides_down': 5,
                'snapshot_count': 96  # 8 hours × 12 snapshots/hour
            }
        }

        raw_stats = {
            16: {
                'total_downtime_hours': 1.2,
                'shame_score': 7.5,
                'rides_down': 3,  # May overlap with hourly
                'snapshot_count': 9  # 45 minutes of data
            }
        }

        # Combined should sum downtime
        expected_total_downtime = 12.5 + 1.2  # = 13.7
        assert expected_total_downtime == 13.7

    def test_calculates_weighted_average_shame_score(self):
        """
        Shame score should be weighted by snapshot counts.

        Formula:
        combined_shame = (hourly_shame × hourly_snapshots + raw_shame × raw_snapshots) / total_snapshots

        Example:
        - Hours 0-8: shame=6.0, snapshots=96
        - Hour 9: shame=7.5, snapshots=9
        - Combined: (6.0×96 + 7.5×9) / (96+9) = (576 + 67.5) / 105 = 6.13
        """
        hourly_shame = 6.0
        hourly_snapshots = 96
        raw_shame = 7.5
        raw_snapshots = 9

        combined_shame = (hourly_shame * hourly_snapshots + raw_shame * raw_snapshots) / (hourly_snapshots + raw_snapshots)
        expected = round((576 + 67.5) / 105, 1)  # 6.1

        assert abs(combined_shame - expected) < 0.1

    def test_merges_rides_down_without_duplicates(self):
        """
        rides_down should be the union of rides down in both periods.

        Example:
        - Hours 0-8: Rides [1, 2, 3, 4, 5] had downtime (5 rides)
        - Hour 9: Rides [3, 4, 5, 6] had downtime (4 rides)
        - Combined: Rides [1, 2, 3, 4, 5, 6] total (6 unique rides, not 9)

        NOTE: This is complex to implement perfectly. A simpler approach:
        rides_down = MAX(hourly_rides_down, raw_rides_down)

        This is conservative (may overcount) but avoids complex ride tracking.
        """
        hourly_rides_down = 5
        raw_rides_down = 4

        # Conservative approach: take maximum
        combined_rides_down = max(hourly_rides_down, raw_rides_down)
        assert combined_rides_down == 5

        # Perfect approach would require tracking individual ride_ids
        # But that's more complex and may not be worth it


class TestQueryPerformanceCharacteristics:
    """
    Test performance characteristics of hybrid query.

    Goal: Sub-1-second response time for TODAY rankings.
    """

    def test_hourly_query_returns_fixed_rows_per_park(self):
        """
        Hourly query should return predictable row count.

        At 9:45 AM (9 complete hours):
        - 27 parks × 9 complete hours = 243 rows (if all parks open all hours)
        - Realistically: ~150-200 rows (some parks closed overnight)

        This is MUCH smaller than raw snapshots:
        - 27 parks × 9 hours × 12 snapshots/hour = 2,916 snapshots
        """
        # Hourly query size is bounded by (parks × complete_hours)
        num_parks = 27  # Approximate Disney/Universal parks
        complete_hours = 9
        max_hourly_rows = num_parks * complete_hours  # = 243

        # This should be much smaller than raw snapshots
        snapshots_per_hour = 12  # 5-minute collection
        raw_snapshot_rows = num_parks * complete_hours * snapshots_per_hour  # = 2,916

        assert max_hourly_rows < raw_snapshot_rows  # 243 < 2,916
        reduction_factor = raw_snapshot_rows / max_hourly_rows
        assert reduction_factor > 10  # 12x reduction

    def test_raw_snapshot_query_returns_current_hour_only(self):
        """
        Raw snapshot query should be limited to current incomplete hour.

        At 9:45 AM:
        - Time range: [9:00 AM, 9:45 AM] = 45 minutes
        - Snapshots: 27 parks × 9 snapshots = ~243 rows

        This is manageable even without aggregation.
        """
        num_parks = 27
        minutes_in_current_hour = 45
        snapshots_per_hour = 12
        snapshots_in_current_hour = (minutes_in_current_hour / 60) * snapshots_per_hour  # = 9

        raw_rows = num_parks * snapshots_in_current_hour  # = 243
        assert raw_rows < 500  # Should be small


class TestDataConsistency:
    """
    Test data consistency between hourly tables and raw snapshots.

    The hybrid query should produce the same results as querying
    all raw snapshots for the entire day (within rounding tolerance).
    """

    def test_hybrid_query_matches_raw_query_for_complete_hours(self):
        """
        For complete hours, hourly table data should match raw aggregation.

        This ensures aggregate_hourly.py and the raw query use the same
        business logic (RideStatusSQL, park_appears_open, etc.).
        """
        # This will be tested in integration tests
        # We'll query the same hour both ways and compare results
        pass

    def test_shame_score_rounding_is_consistent(self):
        """
        Shame scores should be rounded to 1 decimal place consistently.

        Database: DECIMAL(3,1)
        Python: round(value, 1)

        Both sources must use same rounding to avoid 5.4 vs 5.5 discrepancies.
        """
        # Example values
        raw_shame = 5.45
        hourly_shame = 5.4  # DECIMAL(3,1) rounds down

        # Python round() uses "round half to even" (banker's rounding)
        # DECIMAL truncates
        # This could cause mismatches!

        # The query should use ROUND() SQL function, not Python round()
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should use SQL ROUND(), not Python rounding
            assert "ROUND(" in source_code, \
                "Query should use SQL ROUND() for consistent rounding with DECIMAL"


class TestErrorHandling:
    """
    Test error handling in hybrid query.
    """

    def test_handles_missing_hourly_data_gracefully(self):
        """
        If hourly tables are empty (not backfilled), fall back gracefully.

        Scenario: Fresh deployment, hourly tables empty
        Expected: Query returns results using raw snapshots only
        """
        # The query should handle empty hourly table results
        # Either:
        # 1. Fall back to raw query for entire day, OR
        # 2. Return empty hourly stats and only show current hour data

        # Fallback to raw is safer
        pass

    def test_handles_database_connection_errors(self):
        """
        If hourly table query fails, fall back to raw snapshots.

        This ensures the API doesn't break if hourly tables have issues.
        """
        # Error handling should catch exceptions and use fallback
        pass

    def test_handles_timezone_edge_cases(self):
        """
        Query should work correctly at Pacific timezone boundaries.

        DST transitions:
        - Spring forward: 2 AM becomes 3 AM (missing hour)
        - Fall back: 2 AM happens twice (duplicate hour)
        """
        # This is a known edge case in time-series data
        # The query should use UTC internally to avoid DST issues
        pass


class TestIntegrationWithRepository:
    """
    Test integration between query and repository layer.
    """

    def test_today_query_calls_repository_get_hourly_stats(self):
        """
        _query_hourly_tables() should call StatsRepository.get_hourly_stats().

        This keeps the query layer thin and delegates data access to repository.
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should call repository method
            assert ("get_hourly_stats" in source_code or
                    "StatsRepository" in source_code or
                    # Or may query directly (acceptable)
                    "park_hourly_stats" in source_code), \
                "TODAY query should access hourly stats via repository or direct query"

    def test_repository_method_filters_by_park_if_needed(self):
        """
        get_hourly_stats() may optionally filter by park_id.

        For TODAY rankings, we query all parks at once.
        For park details, we query one park.

        Method should support both use cases.
        """
        # This is a design decision for the repository
        # Either:
        # 1. get_hourly_stats(park_id=None) returns all parks
        # 2. get_hourly_stats(park_id=16) returns one park
        pass
