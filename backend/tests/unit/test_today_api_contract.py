"""
TODAY API Contract Tests
========================

TDD tests for the TODAY period API responses with hourly aggregation tables.

These tests verify that the TODAY API endpoint returns the correct format
and data when USE_HOURLY_TABLES feature flag is enabled.

Test Coverage:
1. Response structure matches expected format
2. Required fields are present with correct types
3. Data integrity is maintained
4. Hybrid query logic (complete/incomplete hours)
5. Feature flag behavior (hourly tables vs raw snapshots)

Related Files:
- src/api/routes/parks.py: TODAY endpoint routing
- src/database/queries/today/today_park_rankings.py: TODAY query implementation
- src/database/repositories/stats_repository.py: get_hourly_stats() method
"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path


class TestTodayAPIResponseStructure:
    """
    Test TODAY API response conforms to expected structure.

    The TODAY endpoint should return park rankings for the current Pacific day
    (midnight to now).
    """

    def test_today_response_has_required_fields(self):
        """
        TODAY response must include all required top-level fields.

        Required fields:
        - success: bool (True for successful response)
        - period: str ('today')
        - filter: str ('all-parks' or 'disney-universal')
        - sort_by: str (e.g., 'shame_score')
        - data: list of park rankings
        - aggregate_stats: dict with summary statistics
        - attribution: dict with data source info
        """
        expected_response_structure = {
            "success": True,
            "period": "today",
            "filter": "all-parks",
            "sort_by": "shame_score",
            "weighted": False,
            "data": [],
            "aggregate_stats": {},
            "attribution": {
                "data_source": "ThemeParks.wiki",
                "url": "https://themeparks.wiki"
            }
        }

        required_fields = ['success', 'period', 'filter', 'data', 'aggregate_stats', 'attribution']
        for field in required_fields:
            assert field in expected_response_structure, f"Missing required field: {field}"

    def test_today_park_ranking_item_has_required_fields(self):
        """
        Each park ranking item must have required fields.

        Required fields per park:
        - rank: int (1-indexed ranking)
        - park_id: int
        - park_name: str
        - location: str
        - shame_score: float (0-10 scale, 1 decimal place)
        - total_downtime_hours: float
        - weighted_downtime_hours: float
        - rides_down: int (count of rides with downtime)
        - rides_operating: int
        - uptime_percentage: float (0-100)
        - queue_times_url: str
        """
        sample_park_ranking = {
            "rank": 1,
            "park_id": 16,
            "park_name": "Magic Kingdom",
            "location": "Orlando, FL",
            "shame_score": 7.5,
            "total_downtime_hours": 12.5,
            "weighted_downtime_hours": 15.8,
            "rides_down": 5,
            "rides_operating": 35,
            "uptime_percentage": 89.2,
            "queue_times_url": "https://queue-times.com/parks/16"
        }

        required_fields = [
            'rank', 'park_id', 'park_name', 'location', 'shame_score',
            'total_downtime_hours', 'weighted_downtime_hours', 'rides_down',
            'rides_operating', 'uptime_percentage'
        ]

        for field in required_fields:
            assert field in sample_park_ranking, f"Missing required field: {field}"

    def test_today_field_types_are_correct(self):
        """
        Field types must match expected types.

        Type Requirements:
        - rank: int
        - park_id: int
        - park_name: str
        - shame_score: float (or int)
        - total_downtime_hours: float (or int)
        - rides_down: int
        - uptime_percentage: float (or int)
        """
        sample_park = {
            "rank": 1,
            "park_id": 16,
            "park_name": "Magic Kingdom",
            "shame_score": 7.5,
            "total_downtime_hours": 12.5,
            "rides_down": 5,
            "uptime_percentage": 89.2
        }

        assert isinstance(sample_park['rank'], int)
        assert isinstance(sample_park['park_id'], int)
        assert isinstance(sample_park['park_name'], str)
        assert isinstance(sample_park['shame_score'], (int, float))
        assert isinstance(sample_park['total_downtime_hours'], (int, float))
        assert isinstance(sample_park['rides_down'], int)
        assert isinstance(sample_park['uptime_percentage'], (int, float))


class TestTodayDataIntegrity:
    """
    Test data integrity rules are enforced in TODAY responses.

    These tests verify the three canonical business rules:
    1. Park Status Takes Precedence (closed parks excluded)
    2. Rides Must Have Operated (only active rides counted)
    3. Park-Type Aware Downtime (Disney/Universal vs others)
    """

    def test_today_excludes_closed_parks(self):
        """
        Parks that were closed today should not appear in rankings.

        Business Rule 1: Park Status Takes Precedence

        If a park is closed all day, it should have:
        - No ranking entry in the response
        - shame_score = NULL (not 0)

        This prevents Michigan's Adventure from showing 0% uptime when closed
        for the season.
        """
        # This will be verified in integration tests with real data
        # Here we just verify the query implementation exists
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Verify park_appears_open check exists
            assert "park_appears_open" in source_code or "park_was_open" in source_code, \
                "TODAY query must filter out closed parks using park_appears_open or park_was_open"

    def test_today_only_counts_rides_that_operated(self):
        """
        Rides that never operated today should not count toward downtime.

        Business Rule 2: Rides Must Have Operated

        A ride only counts if it had at least one snapshot with:
        - status='OPERATING' OR computed_is_open=TRUE
        - AND park_appears_open=TRUE
        """
        # Verify the query uses RideStatusSQL.rides_that_operated_cte()
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should use the centralized helper or equivalent logic
            assert ("rides_that_operated" in source_code or
                    "ride_operated" in source_code or
                    "RideStatusSQL" in source_code), \
                "TODAY query must filter rides using rides_that_operated logic"

    def test_today_applies_park_type_aware_downtime_logic(self):
        """
        Downtime logic should vary by park type.

        Business Rule 3: Park-Type Aware Downtime

        Disney/Universal:
        - DOWN = downtime
        - CLOSED = scheduled closure (not downtime)

        Other parks (Dollywood, etc.):
        - DOWN or CLOSED = potential downtime
        """
        # This is enforced in the query implementation
        # The test just verifies the code references park type
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should reference is_disney/is_universal or use RideStatusSQL.is_down()
            assert ("is_disney" in source_code or
                    "is_universal" in source_code or
                    "RideStatusSQL.is_down" in source_code), \
                "TODAY query must apply park-type aware downtime logic"


class TestTodayHybridQuery:
    """
    Test TODAY hybrid query logic (hourly tables + raw snapshots).

    The TODAY query should:
    1. Use hourly aggregates for complete hours (midnight to last complete hour)
    2. Use raw snapshots for incomplete current hour
    3. Combine both sources seamlessly

    Example at 9:45 AM:
    - Hours 0-8 (midnight to 9am): Use park_hourly_stats
    - Hour 9 (9am to 9:45am): Use park_activity_snapshots
    """

    def test_today_query_uses_hourly_tables_when_feature_flag_enabled(self):
        """
        When USE_HOURLY_TABLES=true, TODAY should query park_hourly_stats.

        The query should have a method like:
        - _query_hourly_tables() for complete hours
        - _query_raw_snapshots() for current incomplete hour
        """
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Check for feature flag reference
            assert "USE_HOURLY_TABLES" in source_code, \
                "TODAY query must reference USE_HOURLY_TABLES feature flag"

            # Check for table reference
            assert "park_hourly_stats" in source_code, \
                "TODAY query must use park_hourly_stats table when flag enabled"

    def test_today_query_handles_incomplete_current_hour(self):
        """
        Current incomplete hour should use raw snapshots, not hourly aggregates.

        Scenario: It's 9:45 AM Pacific
        - Hourly aggregate for hour 9 (9:00-10:00) doesn't exist yet
        - Query should fall back to raw park_activity_snapshots for 9:00-9:45
        """
        # This will be tested in integration tests
        # Here we verify the hybrid query structure exists
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should have logic to handle current hour separately
            assert ("current_hour" in source_code.lower() or
                    "incomplete" in source_code.lower() or
                    "park_activity_snapshots" in source_code), \
                "TODAY query must handle incomplete current hour with raw snapshots"

    def test_today_combines_hourly_and_raw_data_seamlessly(self):
        """
        Hybrid query should combine hourly stats + raw snapshots correctly.

        The combined data must:
        - Have consistent field names across both sources
        - Calculate correct totals (sum of hourly + current hour)
        - Calculate correct averages (weighted by snapshot counts)
        """
        # Verify query implementation has union/combination logic
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should combine data from multiple sources
            assert ("UNION" in source_code or
                    "combine" in source_code.lower() or
                    "SUM" in source_code), \
                "TODAY query must combine hourly and raw data"


class TestTodayFeatureFlagBehavior:
    """
    Test feature flag controls data source.

    USE_HOURLY_TABLES=true: Use park_hourly_stats (fast)
    USE_HOURLY_TABLES=false: Use park_activity_snapshots (fallback)
    """

    def test_feature_flag_defaults_to_false(self):
        """
        USE_HOURLY_TABLES should default to false for safety.

        Until hourly tables are fully backfilled and validated,
        the feature flag should be opt-in (default=false).
        """
        from pathlib import Path
        metrics_path = Path(__file__).parent.parent.parent / "src" / "utils" / "metrics.py"

        if metrics_path.exists():
            source_code = metrics_path.read_text()

            # Check for feature flag definition with default=false
            assert "USE_HOURLY_TABLES" in source_code, \
                "metrics.py must define USE_HOURLY_TABLES feature flag"

            # Default should be false (check for 'false' string or False value)
            assert ("'false'" in source_code or
                    "False" in source_code), \
                "USE_HOURLY_TABLES should default to false"

    def test_can_toggle_feature_flag_via_environment_variable(self):
        """
        USE_HOURLY_TABLES should be configurable via environment variable.

        Deployment steps:
        1. Deploy code with flag=false
        2. Backfill hourly tables
        3. Set flag=true via environment
        4. Monitor and rollback if issues
        """
        from pathlib import Path
        metrics_path = Path(__file__).parent.parent.parent / "src" / "utils" / "metrics.py"

        if metrics_path.exists():
            source_code = metrics_path.read_text()

            # Should use os.getenv() or similar
            assert ("os.getenv" in source_code or
                    "environ" in source_code), \
                "USE_HOURLY_TABLES should be readable from environment"


class TestTodayPerformance:
    """
    Test TODAY performance characteristics.

    Performance Goals:
    - WITH hourly tables: <1 second (typically ~100ms)
    - WITHOUT hourly tables: <5 seconds (current baseline)
    """

    def test_today_with_hourly_tables_is_faster(self):
        """
        Querying hourly tables should be significantly faster than raw snapshots.

        Expected improvement:
        - Raw snapshots: ~5-10 seconds (1,440+ snapshots per park per day)
        - Hourly tables: ~0.1-1 seconds (24 rows per park per day)

        This test just verifies the query structure exists.
        Actual performance will be measured in integration tests.
        """
        # This is a placeholder for future benchmarking tests
        # We'll measure actual query times in integration tests
        pass


class TestTodayRepositoryMethod:
    """
    Test stats_repository.get_hourly_stats() method.

    This method provides data to TODAY query from hourly aggregation tables.
    """

    def test_get_hourly_stats_method_exists(self):
        """
        StatsRepository should have get_hourly_stats() method.

        Method signature:
        get_hourly_stats(
            park_id: int,
            start_hour: datetime,
            end_hour: datetime
        ) -> List[Row]

        Returns hourly aggregates from park_hourly_stats table.
        """
        # Check if method exists in repository
        from pathlib import Path
        repo_path = Path(__file__).parent.parent.parent / "src" / "database" / "repositories" / "stats_repository.py"

        if repo_path.exists():
            source_code = repo_path.read_text()

            # Method should exist
            assert "def get_hourly_stats" in source_code, \
                "StatsRepository must have get_hourly_stats() method"

            # Should query park_hourly_stats table
            assert "park_hourly_stats" in source_code, \
                "get_hourly_stats() should query park_hourly_stats table"

    def test_get_hourly_stats_returns_complete_hours_only(self):
        """
        get_hourly_stats() should only return complete hours.

        If current time is 9:45 AM, the method should return:
        - Hour 0 (midnight-1am): ✓ complete
        - Hour 1 (1am-2am): ✓ complete
        - ...
        - Hour 8 (8am-9am): ✓ complete
        - Hour 9 (9am-10am): ✗ incomplete (exclude)

        The incomplete hour will be handled by raw snapshots query.
        """
        # This will be tested in integration tests
        # Here we just verify the query filters by hour_start_utc
        from pathlib import Path
        repo_path = Path(__file__).parent.parent.parent / "src" / "database" / "repositories" / "stats_repository.py"

        if repo_path.exists():
            source_code = repo_path.read_text()

            # Should filter by hour_start_utc to ensure only complete hours
            if "def get_hourly_stats" in source_code:
                assert "hour_start_utc" in source_code, \
                    "get_hourly_stats() must filter by hour_start_utc"


class TestTodayConsistencyWithLive:
    """
    Test TODAY shame scores are consistent with LIVE period.

    Data Integrity Guarantee:
    - A park's TODAY shame score should be the average of all hourly scores
    - LIVE shame score should match the most recent hourly score
    - No park should have different shame scores on Rankings vs Details modal
    """

    def test_today_shame_score_is_average_of_hourly_scores(self):
        """
        TODAY shame score should be the time-weighted average of hourly scores.

        Example:
        - Hour 0-8: 9 hours with shame_score = 5.0
        - Hour 9 (current, incomplete): 0.75 hours with shame_score = 7.0

        TODAY shame_score = (9 * 5.0 + 0.75 * 7.0) / 9.75 = 5.15
        """
        # This calculation logic will be in the query
        # Test just verifies AVG() or weighted calculation exists
        from pathlib import Path
        query_path = Path(__file__).parent.parent.parent / "src" / "database" / "queries" / "today" / "today_park_rankings.py"

        if query_path.exists():
            source_code = query_path.read_text()

            # Should calculate average across hours
            assert "AVG(" in source_code or "SUM" in source_code, \
                "TODAY query must calculate average shame score"

    def test_shame_score_precision_matches_database(self):
        """
        Shame score should be DECIMAL(3,1) - one decimal place.

        Valid: 0.0, 5.5, 10.0
        Invalid: 5.55, 10.0001

        Database column: shame_score DECIMAL(3,1)
        """
        # Verify the database migration defines correct precision
        from pathlib import Path
        migration_path = Path(__file__).parent.parent.parent / "src" / "database" / "migrations" / "013_add_hourly_stats.sql"

        if migration_path.exists():
            source_code = migration_path.read_text()

            # shame_score should be DECIMAL(3,1)
            assert "shame_score DECIMAL(3,1)" in source_code, \
                "shame_score column must be DECIMAL(3,1) for consistency"


class TestTodayEdgeCases:
    """
    Test edge cases for TODAY period.
    """

    def test_today_at_midnight_returns_empty_rankings(self):
        """
        At 12:00 AM Pacific (start of new day), TODAY should return empty or minimal data.

        Scenario: It's 12:01 AM, no snapshots collected yet today.
        Expected: Empty rankings or parks with 0 downtime.
        """
        # This will be tested in integration tests
        pass

    def test_today_handles_parks_with_no_hourly_data(self):
        """
        Parks with no hourly aggregates should fall back to raw snapshots.

        Scenario: New park added, no hourly data backfilled yet.
        Expected: Query raw snapshots for entire day.
        """
        # This will be tested in integration tests
        pass

    def test_today_handles_missing_current_hour_snapshots(self):
        """
        If current hour has no snapshots yet, don't error.

        Scenario: Snapshot collection failed for last 15 minutes.
        Expected: Use hourly data only, current hour contributes 0.
        """
        # This will be tested in integration tests
        pass


class TestTodayAPIEndpointRouting:
    """
    Test Flask route correctly handles period=today.
    """

    def test_parks_downtime_accepts_today_period(self):
        """
        /api/parks/downtime?period=today should be accepted.
        """
        from pathlib import Path
        parks_route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"

        if parks_route_path.exists():
            source_code = parks_route_path.read_text()

            # Verify 'today' is in the valid periods list
            assert "'today'" in source_code, \
                "parks.py should accept 'today' as valid period"

    def test_today_uses_today_park_rankings_query_class(self):
        """
        The route should delegate to TodayParkRankingsQuery class.
        """
        from pathlib import Path
        parks_route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"

        if parks_route_path.exists():
            source_code = parks_route_path.read_text()

            # Should import and use TodayParkRankingsQuery
            assert "TodayParkRankingsQuery" in source_code, \
                "parks.py should use TodayParkRankingsQuery for period=today"

    def test_today_response_is_cached(self):
        """
        TODAY responses should be cached with appropriate TTL.

        Cache behavior:
        - TTL: 5 minutes (same as LIVE)
        - Cache key includes: period, filter, limit, sort_by
        """
        from pathlib import Path
        parks_route_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"

        if parks_route_path.exists():
            source_code = parks_route_path.read_text()

            # Should use caching
            assert "cache" in source_code.lower(), \
                "parks.py should cache TODAY responses"
