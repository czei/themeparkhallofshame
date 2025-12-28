"""
Unit tests for shame breakdown response structure.

These tests verify that the StatsRepository shame breakdown methods
return all required fields for the frontend to render correctly.

Tests use mocking to avoid database dependencies.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date
from decimal import Decimal


class TestShameBreakdownResponseStructure:
    """Test that shame breakdown methods return all required fields."""

    def test_yesterday_breakdown_returns_required_fields(self):
        """
        Verify get_park_yesterday_shame_breakdown returns all required fields.

        Required fields:
        - shame_score: float
        - total_downtime_hours: float
        - avg_uptime_percentage: float
        - rides: list (array of ride objects)
        - rides_with_downtime: int (count, for backwards compatibility)
        - weighted_downtime_hours: float
        - total_park_weight: float
        - rides_affected_count: int
        """
        from database.repositories.stats_repository import StatsRepository

        # Create mock session and stats
        mock_session = MagicMock()
        mock_daily_stat = MagicMock()
        mock_daily_stat.shame_score = Decimal('5.5')
        mock_daily_stat.total_downtime_hours = Decimal('10.0')
        mock_daily_stat.avg_uptime_percentage = Decimal('80.0')
        mock_daily_stat.rides_with_downtime = 5
        mock_daily_stat.total_rides_tracked = 20

        # Mock the query to return our stat
        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = mock_daily_stat
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)

        # Mock the rides helper method
        mock_rides = [
            {'ride_id': 1, 'ride_name': 'Test Ride', 'tier': 1, 'tier_weight': 3,
             'downtime_hours': 2.0, 'weighted_contribution': 6.0}
        ]
        repo._get_rides_with_downtime_for_date = MagicMock(return_value=mock_rides)

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # Verify all required fields are present
        assert 'shame_score' in breakdown, "Missing shame_score"
        assert 'total_downtime_hours' in breakdown, "Missing total_downtime_hours"
        assert 'avg_uptime_percentage' in breakdown, "Missing avg_uptime_percentage"
        assert 'rides' in breakdown, "Missing rides array"
        assert 'weighted_downtime_hours' in breakdown, "Missing weighted_downtime_hours"
        assert 'total_park_weight' in breakdown, "Missing total_park_weight"
        assert 'rides_affected_count' in breakdown, "Missing rides_affected_count"

        # Verify types are correct (numbers, not strings)
        assert isinstance(breakdown['shame_score'], (int, float))
        assert isinstance(breakdown['total_downtime_hours'], (int, float))
        assert isinstance(breakdown['weighted_downtime_hours'], (int, float))
        assert isinstance(breakdown['total_park_weight'], (int, float))
        assert isinstance(breakdown['rides'], list)

    def test_empty_stats_returns_zero_defaults(self):
        """
        Verify breakdown returns sensible defaults when no stats exist.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = None  # No stats
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # Verify defaults
        assert breakdown['shame_score'] == 0
        assert breakdown['total_downtime_hours'] == 0
        assert breakdown['rides'] == []
        assert breakdown['weighted_downtime_hours'] == 0
        assert breakdown['total_park_weight'] == 0
        assert breakdown['rides_affected_count'] == 0


class TestRidesArrayNotCount:
    """Test that 'rides' is an array, not a count."""

    def test_rides_field_is_array_not_integer(self):
        """
        CRITICAL: 'rides' must be an array of objects, not an integer.

        Frontend code does:
            const tier1Rides = ridesArray.filter(r => r.tier === 1);

        If 'rides' is an integer, .filter() will throw:
            "rides.filter is not a function"
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()
        mock_daily_stat = MagicMock()
        mock_daily_stat.shame_score = Decimal('5.0')
        mock_daily_stat.total_downtime_hours = Decimal('5.0')
        mock_daily_stat.avg_uptime_percentage = Decimal('75.0')
        mock_daily_stat.rides_with_downtime = 3  # This is the COUNT
        mock_daily_stat.total_rides_tracked = 10

        mock_query = MagicMock()
        mock_query.filter.return_value.first.return_value = mock_daily_stat
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)
        repo._get_rides_with_downtime_for_date = MagicMock(return_value=[
            {'ride_id': 1, 'ride_name': 'Ride A', 'tier': 1, 'downtime_hours': 1.0},
            {'ride_id': 2, 'ride_name': 'Ride B', 'tier': 2, 'downtime_hours': 2.0},
            {'ride_id': 3, 'ride_name': 'Ride C', 'tier': 3, 'downtime_hours': 1.5},
        ])

        with patch('utils.timezone.get_yesterday_date_range',
                   return_value=(date(2025, 12, 25), date(2025, 12, 25), None)):
            breakdown = repo.get_park_yesterday_shame_breakdown(park_id=194)

        # CRITICAL: rides must be a list, not an int
        assert isinstance(breakdown['rides'], list), \
            f"'rides' must be a list, got {type(breakdown['rides'])}"
        assert not isinstance(breakdown['rides'], int), \
            "'rides' must NOT be an integer (count)"

        # rides_with_downtime can be the count for backwards compatibility
        assert isinstance(breakdown['rides_with_downtime'], int), \
            "rides_with_downtime should be int count for backwards compat"


class TestNumericFieldTypes:
    """Test that numeric fields are numbers, not strings."""

    def test_weighted_contribution_is_float_not_string(self):
        """
        Frontend uses .toFixed() on weighted_contribution.
        If it's a string, we get: "weighted_contribution.toFixed is not a function"
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Create mock result with Decimal (as SQLAlchemy returns)
        mock_result = MagicMock()
        mock_result.ride_id = 1
        mock_result.ride_name = 'Test Ride'
        mock_result.tier = 2
        mock_result.tier_weight = 2
        mock_result.total_downtime_minutes = Decimal('120')  # Decimal from DB

        mock_query = MagicMock()
        mock_query.join.return_value.outerjoin.return_value.filter.return_value.\
            group_by.return_value.order_by.return_value.all.return_value = [mock_result]
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)
        rides = repo._get_rides_with_downtime_for_date_range(
            park_id=194,
            start_date=date(2025, 12, 19),
            end_date=date(2025, 12, 25)
        )

        # Verify numeric types
        assert len(rides) == 1
        ride = rides[0]
        assert isinstance(ride['downtime_hours'], float), \
            f"downtime_hours should be float, got {type(ride['downtime_hours'])}"
        assert isinstance(ride['weighted_contribution'], float), \
            f"weighted_contribution should be float, got {type(ride['weighted_contribution'])}"


class TestTierFromClassification:
    """Test that tier data comes from ride_classifications table."""

    def test_tier_from_classification_not_rides_table(self):
        """
        Tier should come from ride_classifications table, not rides.tier column.

        The rides.tier column is often NULL. The actual tier classification
        is stored in ride_classifications table.
        """
        from database.repositories.stats_repository import StatsRepository

        mock_session = MagicMock()

        # Mock result where tier comes from RideClassification
        mock_result = MagicMock()
        mock_result.ride_id = 1
        mock_result.ride_name = 'Flagship Ride'
        mock_result.tier = 1  # From RideClassification, not Ride
        mock_result.tier_weight = 3  # From RideClassification
        mock_result.total_downtime_minutes = Decimal('60')

        mock_query = MagicMock()
        mock_query.join.return_value.outerjoin.return_value.filter.return_value.\
            group_by.return_value.order_by.return_value.all.return_value = [mock_result]
        mock_session.query.return_value = mock_query

        repo = StatsRepository(mock_session)
        rides = repo._get_rides_with_downtime_for_date_range(
            park_id=194,
            start_date=date(2025, 12, 19),
            end_date=date(2025, 12, 25)
        )

        ride = rides[0]
        assert ride['tier'] == 1, "Tier should be 1 (from classification)"
        assert ride['tier_weight'] == 3, "Tier weight should be 3 for tier 1"
