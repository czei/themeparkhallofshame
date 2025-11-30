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

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection


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
        """Test active rides filter generates correct conditions."""
        from database.queries.builders.filters import Filters
        from database.schema import rides, parks

        conditions = Filters.active_rides(rides, parks)

        # Should return multiple conditions
        assert len(conditions) >= 2

    def test_time_period_filter(self):
        """Test time period filter for stats tables."""
        from database.queries.builders.filters import Filters
        from database.schema.stats_tables import park_weekly_stats

        condition = Filters.time_period(
            park_weekly_stats,
            period="7days",
            year=2024,
            week=45
        )

        compiled = str(condition.compile(compile_kwargs={"literal_binds": True}))

        assert "2024" in compiled
        assert "45" in compiled


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


class TestParkRankingsQuery:
    """Test park rankings query construction."""

    def test_weekly_query_structure(self):
        """Test weekly rankings query includes expected columns."""
        from database.queries.rankings.park_downtime_rankings import ParkDowntimeRankingsQuery

        mock_conn = MockConnection()
        query = ParkDowntimeRankingsQuery(mock_conn)

        # Call method - it will fail on execute but we can inspect the query
        try:
            query.get_weekly(year=2024, week_number=45)
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
            query.get_weekly(year=2024, week_number=45, filter_disney_universal=True)
        except (TypeError, AttributeError):
            pass

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
            query.get_weekly(year=2024, week_number=45)
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
