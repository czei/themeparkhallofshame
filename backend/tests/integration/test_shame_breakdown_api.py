"""
Integration tests for shame breakdown API response structure.

These tests verify that the shame breakdown API returns all required fields
for the frontend to render correctly:
- rides: Array of ride objects (not just a count)
- weighted_downtime_hours: float
- total_park_weight: float
- rides_affected_count: int
- breakdown_type: str (live/today/yesterday/last_week/last_month)
- shame_score: float
- tier information from ride_classifications table

This prevents regressions like:
- rides_with_downtime being an int instead of array
- Missing weighted_downtime_hours causing toFixed() errors
- Tier data not being pulled from ride_classifications
"""

import pytest
from datetime import datetime, date, timedelta
from decimal import Decimal


@pytest.mark.integration
class TestShameBreakdownResponseStructure:
    """Test that shame breakdown API returns all required fields."""

    def test_yesterday_breakdown_has_rides_array(self, mysql_session):
        """
        Verify yesterday breakdown returns 'rides' as an array, not just a count.

        Frontend needs the rides array to:
        - Filter by tier (tier1Rides, tier2Rides, tier3Rides)
        - Display ride names and downtime hours
        """
        from models.orm_park import Park
        from models.orm_ride import Ride
        from models.orm_stats import ParkDailyStats, RideDailyStats
        from models.orm_classification import RideClassification
        from database.repositories.stats_repository import StatsRepository
        from utils.timezone import get_yesterday_date_range

        # Create test park
        park = Park(
            park_id=8001,
            name='Test Breakdown Park',
            queue_times_id=80001,
            city='Test City',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.merge(park)

        # Create test ride with classification
        ride = Ride(
            ride_id=80001,
            park_id=8001,
            name='Test Ride',
            queue_times_id=800001,
            is_active=True
        )
        mysql_session.merge(ride)

        classification = RideClassification(
            classification_id=80001,
            ride_id=80001,
            tier=1,
            tier_weight=3,
            classification_method='manual_override'
        )
        mysql_session.merge(classification)

        # Create daily stats with downtime
        start_date, _, _ = get_yesterday_date_range()
        park_stats = ParkDailyStats(
            stat_id=80001,
            park_id=8001,
            stat_date=start_date,
            total_rides_tracked=1,
            total_downtime_hours=Decimal('2.5'),
            rides_with_downtime=1,
            shame_score=Decimal('5.0'),
            avg_uptime_percentage=Decimal('75.0')
        )
        mysql_session.merge(park_stats)

        ride_stats = RideDailyStats(
            stat_id=80001,
            ride_id=80001,
            stat_date=start_date,
            downtime_minutes=150,
            uptime_percentage=Decimal('75.0')
        )
        mysql_session.merge(ride_stats)
        mysql_session.flush()

        # Test the repository method
        repo = StatsRepository(mysql_session)
        breakdown = repo.get_park_yesterday_shame_breakdown(8001)

        # Verify 'rides' is an array
        assert 'rides' in breakdown, "Response must have 'rides' key"
        assert isinstance(breakdown['rides'], list), "'rides' must be a list, not int"
        assert len(breakdown['rides']) > 0, "'rides' array should have data"

        # Verify rides have required fields
        ride_data = breakdown['rides'][0]
        assert 'ride_id' in ride_data
        assert 'ride_name' in ride_data
        assert 'tier' in ride_data
        assert 'downtime_hours' in ride_data
        assert 'weighted_contribution' in ride_data

    def test_yesterday_breakdown_has_numeric_fields(self, mysql_session):
        """
        Verify breakdown returns numeric fields as numbers, not strings.

        Frontend uses .toFixed() on these fields, which requires numbers.
        """
        from models.orm_park import Park
        from models.orm_stats import ParkDailyStats
        from database.repositories.stats_repository import StatsRepository
        from utils.timezone import get_yesterday_date_range

        # Create minimal test data
        park = Park(
            park_id=8002,
            name='Test Numeric Park',
            queue_times_id=80002,
            city='Test City',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.merge(park)

        start_date, _, _ = get_yesterday_date_range()
        park_stats = ParkDailyStats(
            stat_id=80002,
            park_id=8002,
            stat_date=start_date,
            total_rides_tracked=10,
            total_downtime_hours=Decimal('5.5'),
            rides_with_downtime=3,
            shame_score=Decimal('7.5'),
            avg_uptime_percentage=Decimal('80.0')
        )
        mysql_session.merge(park_stats)
        mysql_session.flush()

        repo = StatsRepository(mysql_session)
        breakdown = repo.get_park_yesterday_shame_breakdown(8002)

        # Verify numeric fields are numbers (not strings)
        assert isinstance(breakdown['shame_score'], (int, float)), \
            f"shame_score must be numeric, got {type(breakdown['shame_score'])}"
        assert isinstance(breakdown['total_downtime_hours'], (int, float)), \
            f"total_downtime_hours must be numeric, got {type(breakdown['total_downtime_hours'])}"
        assert isinstance(breakdown['weighted_downtime_hours'], (int, float)), \
            f"weighted_downtime_hours must be numeric, got {type(breakdown['weighted_downtime_hours'])}"
        assert isinstance(breakdown['total_park_weight'], (int, float)), \
            f"total_park_weight must be numeric, got {type(breakdown['total_park_weight'])}"

    def test_weekly_breakdown_has_rides_array(self, mysql_session):
        """
        Verify last_week breakdown returns 'rides' array, not just stats.
        """
        from models.orm_park import Park
        from models.orm_ride import Ride
        from models.orm_stats import ParkDailyStats, RideDailyStats
        from models.orm_classification import RideClassification
        from database.repositories.stats_repository import StatsRepository
        from utils.timezone import get_last_week_date_range

        # Create test park and ride
        park = Park(
            park_id=8003,
            name='Test Weekly Park',
            queue_times_id=80003,
            city='Test City',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.merge(park)

        ride = Ride(
            ride_id=80003,
            park_id=8003,
            name='Weekly Test Ride',
            queue_times_id=800003,
            is_active=True
        )
        mysql_session.merge(ride)

        classification = RideClassification(
            classification_id=80003,
            ride_id=80003,
            tier=2,
            tier_weight=2,
            classification_method='manual_override'
        )
        mysql_session.merge(classification)

        start_date, end_date, _ = get_last_week_date_range()

        # Create stats for each day of the week
        for i, d in enumerate(range(7)):
            stat_date = start_date + timedelta(days=d)
            park_stats = ParkDailyStats(
                stat_id=80010 + i,
                park_id=8003,
                stat_date=stat_date,
                total_rides_tracked=1,
                total_downtime_hours=Decimal('1.0'),
                rides_with_downtime=1,
                shame_score=Decimal('3.0')
            )
            mysql_session.merge(park_stats)

            ride_stats = RideDailyStats(
                stat_id=80010 + i,
                ride_id=80003,
                stat_date=stat_date,
                downtime_minutes=60
            )
            mysql_session.merge(ride_stats)

        mysql_session.flush()

        repo = StatsRepository(mysql_session)
        breakdown = repo.get_park_weekly_shame_breakdown(8003)

        # Verify response structure
        assert 'rides' in breakdown, "Weekly breakdown must have 'rides' key"
        assert isinstance(breakdown['rides'], list), "'rides' must be a list"
        assert 'days_tracked' in breakdown, "Must have days_tracked"
        assert 'period_label' in breakdown, "Must have period_label for display"


@pytest.mark.integration
class TestTierClassification:
    """Test that tier data comes from ride_classifications table."""

    def test_rides_have_correct_tier_from_classification(self, mysql_session):
        """
        Verify ride tier is pulled from ride_classifications, not rides table.

        The rides.tier column is often NULL; actual tier data is in
        ride_classifications table.
        """
        from models.orm_park import Park
        from models.orm_ride import Ride
        from models.orm_stats import ParkDailyStats, RideDailyStats
        from models.orm_classification import RideClassification
        from database.repositories.stats_repository import StatsRepository
        from utils.timezone import get_yesterday_date_range

        # Create park
        park = Park(
            park_id=8004,
            name='Test Tier Park',
            queue_times_id=80004,
            city='Test City',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.merge(park)

        # Create rides with different tiers in classification
        rides_data = [
            (80004, 'Flagship Ride', 1, 3),  # Tier 1
            (80005, 'Standard Ride', 2, 2),  # Tier 2
            (80006, 'Minor Ride', 3, 1),     # Tier 3
        ]

        for ride_id, name, tier, weight in rides_data:
            ride = Ride(
                ride_id=ride_id,
                park_id=8004,
                name=name,
                queue_times_id=ride_id * 10,
                is_active=True,
                tier=None  # rides.tier is NULL
            )
            mysql_session.merge(ride)

            classification = RideClassification(
                classification_id=ride_id,
                ride_id=ride_id,
                tier=tier,
                tier_weight=weight,
                classification_method='manual_override'
            )
            mysql_session.merge(classification)

        # Create daily stats
        start_date, _, _ = get_yesterday_date_range()
        park_stats = ParkDailyStats(
            stat_id=80004,
            park_id=8004,
            stat_date=start_date,
            total_rides_tracked=3,
            total_downtime_hours=Decimal('3.0'),
            rides_with_downtime=3,
            shame_score=Decimal('5.0')
        )
        mysql_session.merge(park_stats)

        for ride_id, _, _, _ in rides_data:
            ride_stats = RideDailyStats(
                stat_id=ride_id,
                ride_id=ride_id,
                stat_date=start_date,
                downtime_minutes=60
            )
            mysql_session.merge(ride_stats)

        mysql_session.flush()

        repo = StatsRepository(mysql_session)
        breakdown = repo.get_park_yesterday_shame_breakdown(8004)

        # Verify tier distribution
        rides = breakdown['rides']
        tier_counts = {}
        for r in rides:
            tier = r['tier']
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        assert 1 in tier_counts, "Should have Tier 1 rides"
        assert 2 in tier_counts, "Should have Tier 2 rides"
        assert 3 in tier_counts, "Should have Tier 3 rides"

        # Verify tier weights are correct
        for r in rides:
            if r['tier'] == 1:
                assert r['tier_weight'] == 3, "Tier 1 should have weight 3"
            elif r['tier'] == 2:
                assert r['tier_weight'] == 2, "Tier 2 should have weight 2"
            elif r['tier'] == 3:
                assert r['tier_weight'] == 1, "Tier 3 should have weight 1"


@pytest.mark.integration
class TestBreakdownTypeField:
    """Test that breakdown_type field is returned correctly."""

    def test_breakdown_type_added_by_api_route(self, mysql_session):
        """
        Verify that breakdown_type is added to API response.

        The breakdown_type field tells frontend which renderer to use:
        - 'live': Real-time shame score
        - 'today': Today's average
        - 'yesterday': Yesterday's data
        - 'last_week': Weekly average
        - 'last_month': Monthly average
        """
        # Note: This tests the API route behavior, not repository.
        # The repository doesn't add breakdown_type; the route does.
        # This is tested at the API level in test_today_api_contract.py
        pass
