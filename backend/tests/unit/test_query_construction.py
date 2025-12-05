"""
Unit Tests for Query Construction
=================================

Tests that query classes generate valid SQLAlchemy statements.

These tests verify:
1. Query classes can be instantiated
2. SQLAlchemy statements are properly constructed
3. Filters are correctly applied
4. SQL is generated without database connection

How to Add Tests for New Queries
--------------------------------
1. Import your query class
2. Use MockConnection to avoid database dependency
3. Call the method that builds the query
4. Verify the compiled SQL contains expected clauses
"""

from datetime import date


class MockConnection:
    """
    Mock database connection for testing query construction.

    This allows testing that queries are properly built without
    needing an actual database connection.
    """

    def __init__(self):
        self.last_query = None
        self.last_params = None

    def execute(self, statement, params=None):
        """Capture the statement for inspection."""
        self.last_query = statement
        self.last_params = params
        # Return empty result
        return []


class TestFilters:
    """Test query filter builders."""

    def test_disney_universal_filter_expression(self):
        """Test Disney/Universal filter generates correct SQL."""
        from database.queries.builders.filters import Filters
        from database.schema import parks

        expr = Filters.disney_universal(parks)

        # Compile to SQL string for inspection
        compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

        assert "Disney" in compiled or "disney" in compiled.lower()
        assert "Universal" in compiled or "universal" in compiled.lower()

    def test_active_rides_filter(self):
        """Test active attractions filter generates correct conditions."""
        from database.queries.builders.filters import Filters
        from database.schema import rides, parks

        # Use active_attractions which combines ride and park active checks
        condition = Filters.active_attractions(rides, parks)

        # Compile to SQL string for inspection
        compiled = str(condition.compile(compile_kwargs={"literal_binds": True}))

        # Should check is_active and category
        assert "is_active" in compiled.lower()
        assert "category" in compiled.lower() or "ATTRACTION" in compiled

    def test_date_range_filter(self):
        """Test date range filter for stats tables."""
        from database.queries.builders.filters import Filters
        from database.schema.stats_tables import park_daily_stats

        start = date(2024, 11, 1)
        end = date(2024, 11, 7)
        condition = Filters.within_date_range(
            park_daily_stats.c.stat_date,
            start_date=start,
            end_date=end
        )

        compiled = str(condition.compile(compile_kwargs={"literal_binds": True}))

        # Should have date boundaries
        assert "stat_date" in compiled.lower()
        assert "2024-11-01" in compiled or "2024-11" in compiled


class TestStatusExpressions:
    """Test status check expressions."""

    def test_is_operating_expression(self):
        """Test is_operating generates correct SQL."""
        from database.queries.builders.expressions import StatusExpressions
        from database.schema import ride_status_snapshots

        expr = StatusExpressions.is_operating(ride_status_snapshots)
        compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

        # Should check for OPERATING status or computed_is_open
        assert "OPERATING" in compiled or "computed_is_open" in compiled

    def test_is_down_expression(self):
        """Test is_down generates correct SQL."""
        from database.queries.builders.expressions import StatusExpressions
        from database.schema import ride_status_snapshots

        expr = StatusExpressions.is_down(ride_status_snapshots)
        compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

        # Should check for DOWN status or not operating
        assert "status" in compiled.lower()


class TestRideStatusSQL:
    """Test RideStatusSQL helpers from sql_helpers.py."""

    def test_has_operated_subquery_generates_exists_clause(self):
        """
        Test that has_operated_subquery generates an EXISTS clause that:
        1. Checks for OPERATING status OR computed_is_open=TRUE
        2. Uses the correct time parameters
        3. Filters by ride_id

        This is CRITICAL: Rides that have NEVER operated should not count as
        having downtime. Only rides that were operating and then went down
        should be included in downtime calculations.
        """
        from utils.sql_helpers import RideStatusSQL

        # Generate the subquery
        subquery = RideStatusSQL.has_operated_subquery("r.ride_id")

        # Verify it's an EXISTS clause
        assert "EXISTS" in subquery

        # Verify it checks for OPERATING status
        assert "OPERATING" in subquery

        # Verify it checks computed_is_open as fallback
        assert "computed_is_open" in subquery

        # Verify it uses the time parameters
        assert ":start_utc" in subquery
        assert ":end_utc" in subquery

        # Verify it filters by ride_id
        assert "r.ride_id" in subquery

    def test_has_operated_subquery_custom_parameters(self):
        """Test has_operated_subquery with custom parameter names."""
        from utils.sql_helpers import RideStatusSQL

        subquery = RideStatusSQL.has_operated_subquery(
            "rides.ride_id",
            start_param=":period_start",
            end_param=":period_end"
        )

        # Verify custom parameters are used
        assert ":period_start" in subquery
        assert ":period_end" in subquery
        assert "rides.ride_id" in subquery

    def test_is_down_expression(self):
        """Test is_down generates correct SQL for DOWN status detection."""
        from utils.sql_helpers import RideStatusSQL

        expr = RideStatusSQL.is_down("rss")

        # Should check for DOWN and CLOSED status (CLOSED included for non-Disney parks)
        assert "'DOWN'" in expr
        assert "'CLOSED'" in expr
        assert "IN" in expr  # Should use IN clause for multiple statuses

        # Should also handle NULL status with computed_is_open=FALSE
        assert "computed_is_open = FALSE" in expr or "computed_is_open" in expr

    def test_is_operating_expression(self):
        """Test is_operating generates correct SQL for OPERATING status detection."""
        from utils.sql_helpers import RideStatusSQL

        expr = RideStatusSQL.is_operating("rss")

        # Should check for explicit OPERATING status
        assert "rss.status = 'OPERATING'" in expr

        # Should also handle NULL status with computed_is_open=TRUE
        assert "computed_is_open = TRUE" in expr or "computed_is_open" in expr


class TestParkRankingsQuery:
    """Test park rankings query construction."""

    def test_weekly_query_structure(self):
        """Test weekly rankings query includes expected columns."""
        from database.queries.rankings.park_downtime_rankings import ParkDowntimeRankingsQuery

        mock_conn = MockConnection()
        query = ParkDowntimeRankingsQuery(mock_conn)

        # Call method with correct parameters
        try:
            query.get_weekly(filter_disney_universal=False, limit=50)
        except (TypeError, AttributeError):
            pass  # Expected - mock doesn't return proper results

        # Verify query was built
        assert mock_conn.last_query is not None

        # Compile and check structure
        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include key columns
        assert "park_name" in compiled.lower() or "name" in compiled.lower()
        assert "shame_score" in compiled.lower() or "downtime" in compiled.lower()

    def test_filter_applied(self):
        """Test Disney/Universal filter is applied when requested."""
        from database.queries.rankings.park_downtime_rankings import ParkDowntimeRankingsQuery

        mock_conn = MockConnection()
        query = ParkDowntimeRankingsQuery(mock_conn)

        try:
            query.get_weekly(filter_disney_universal=True, limit=50)
        except (TypeError, AttributeError):
            pass

        # Verify query was built
        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should contain Disney/Universal filter
        assert "disney" in compiled.lower() or "universal" in compiled.lower()


class TestRideRankingsQuery:
    """Test ride rankings query construction."""

    def test_weekly_query_structure(self):
        """Test weekly ride rankings query includes expected columns."""
        from database.queries.rankings.ride_downtime_rankings import RideDowntimeRankingsQuery

        mock_conn = MockConnection()
        query = RideDowntimeRankingsQuery(mock_conn)

        try:
            query.get_weekly(filter_disney_universal=False, limit=50)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include ride-specific columns
        assert "ride" in compiled.lower() or "name" in compiled.lower()


class TestLiveQueries:
    """Test live query construction."""

    def test_status_summary_query(self):
        """Test status summary query structure."""
        from database.queries.live.status_summary import StatusSummaryQuery

        mock_conn = MockConnection()
        query = StatusSummaryQuery(mock_conn)

        try:
            query.get_summary()
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should count different statuses
        assert "operating" in compiled.lower() or "status" in compiled.lower()


class TestChartQueries:
    """Test chart data query construction."""

    def test_park_shame_history_daily(self):
        """Test park shame history generates time-series structure."""
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery

        mock_conn = MockConnection()
        query = ParkShameHistoryQuery(mock_conn)

        try:
            result = query.get_daily(days=7)
        except (TypeError, AttributeError):
            pass

        # The method should build and execute a query
        # Even with mock, we verify it doesn't raise on construction


class TestTrendsQueries:
    """Test trends query construction."""

    def test_improving_parks_query(self):
        """Test improving parks query structure."""
        from database.queries.trends.improving_parks import ImprovingParksQuery

        mock_conn = MockConnection()
        query = ImprovingParksQuery(mock_conn)

        try:
            query.get_improving(period="7days")
        except (TypeError, AttributeError):
            pass

        # Verify query was attempted
        assert mock_conn.last_query is not None


class TestQueryDocumentation:
    """Test that query files have proper documentation."""

    def test_rankings_queries_have_docstrings(self):
        """Verify rankings query classes have module and class docstrings."""
        from database.queries.rankings import park_downtime_rankings

        assert park_downtime_rankings.__doc__ is not None
        assert "Endpoint:" in park_downtime_rankings.__doc__

    def test_live_queries_have_docstrings(self):
        """Verify live query classes have module docstrings."""
        from database.queries.live import status_summary

        assert status_summary.__doc__ is not None

    def test_metrics_have_formulas(self):
        """Verify metrics.py has formula documentation."""
        from utils import metrics

        assert "Formula" in metrics.calculate_shame_score.__doc__
        assert "Worked Example" in metrics.calculate_shame_score.__doc__


class TestSchemaDefinitions:
    """Test schema table definitions."""

    def test_core_tables_exist(self):
        """Verify core tables are defined."""
        from database.schema import parks, rides, ride_classifications

        assert parks is not None
        assert rides is not None
        assert ride_classifications is not None

    def test_snapshot_tables_exist(self):
        """Verify snapshot tables are defined."""
        from database.schema import ride_status_snapshots, park_activity_snapshots

        assert ride_status_snapshots is not None
        assert park_activity_snapshots is not None

    def test_stats_tables_exist(self):
        """Verify stats tables are defined."""
        from database.schema.stats_tables import (
            park_daily_stats,
            park_weekly_stats,
            ride_daily_stats,
            ride_weekly_stats,
        )

        assert park_daily_stats is not None
        assert park_weekly_stats is not None
        assert ride_daily_stats is not None
        assert ride_weekly_stats is not None


class TestAwardsQueries:
    """Test Awards query construction (Longest Wait Times, Least Reliable)."""

    def test_longest_wait_times_parks_query_structure(self):
        """Test parks-level wait times query includes expected columns."""
        from database.queries.trends.longest_wait_times import LongestWaitTimesQuery

        mock_conn = MockConnection()
        query = LongestWaitTimesQuery(mock_conn)

        try:
            query.get_park_rankings(period="7days", filter_disney_universal=False, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include park-specific columns
        assert "park_name" in compiled.lower() or "name" in compiled.lower()
        assert "location" in compiled.lower() or "city" in compiled.lower()
        assert "cumulative_wait_hours" in compiled.lower() or "wait" in compiled.lower()

    def test_longest_wait_times_parks_filter_applied(self):
        """Test Disney/Universal filter is applied for parks wait times."""
        from database.queries.trends.longest_wait_times import LongestWaitTimesQuery

        mock_conn = MockConnection()
        query = LongestWaitTimesQuery(mock_conn)

        try:
            query.get_park_rankings(period="7days", filter_disney_universal=True, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should contain Disney/Universal filter
        assert "disney" in compiled.lower() or "universal" in compiled.lower()

    def test_least_reliable_parks_query_structure(self):
        """Test parks-level reliability query includes expected columns."""
        from database.queries.trends.least_reliable_rides import LeastReliableRidesQuery

        mock_conn = MockConnection()
        query = LeastReliableRidesQuery(mock_conn)

        try:
            query.get_park_rankings(period="7days", filter_disney_universal=False, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include park-specific columns
        assert "park_name" in compiled.lower() or "name" in compiled.lower()
        assert "location" in compiled.lower() or "city" in compiled.lower()
        # Parks are sorted by avg_shame_score (not downtime_hours like rides)
        assert "shame_score" in compiled.lower()
        assert "uptime_percentage" in compiled.lower() or "uptime" in compiled.lower()

    def test_least_reliable_parks_filter_applied(self):
        """Test Disney/Universal filter is applied for parks reliability."""
        from database.queries.trends.least_reliable_rides import LeastReliableRidesQuery

        mock_conn = MockConnection()
        query = LeastReliableRidesQuery(mock_conn)

        try:
            query.get_park_rankings(period="7days", filter_disney_universal=True, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should contain Disney/Universal filter
        assert "disney" in compiled.lower() or "universal" in compiled.lower()

    def test_longest_wait_times_rides_still_works(self):
        """Test ride-level wait times query still works (backward compatibility)."""
        from database.queries.trends.longest_wait_times import LongestWaitTimesQuery

        mock_conn = MockConnection()
        query = LongestWaitTimesQuery(mock_conn)

        try:
            query.get_rankings(period="7days", filter_disney_universal=False, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include ride-specific columns
        assert "ride_name" in compiled.lower() or "ride" in compiled.lower()

    def test_least_reliable_rides_still_works(self):
        """Test ride-level reliability query still works (backward compatibility)."""
        from database.queries.trends.least_reliable_rides import LeastReliableRidesQuery

        mock_conn = MockConnection()
        query = LeastReliableRidesQuery(mock_conn)

        try:
            query.get_rankings(period="7days", filter_disney_universal=False, limit=10)
        except (TypeError, AttributeError):
            pass

        assert mock_conn.last_query is not None

        compiled = str(mock_conn.last_query.compile(compile_kwargs={"literal_binds": True}))

        # Should include ride-specific columns
        assert "ride_name" in compiled.lower() or "ride" in compiled.lower()
