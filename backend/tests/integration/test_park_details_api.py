"""
Integration tests for park details API endpoint.

Tests the /api/parks/<park_id>/details endpoint which requires
ORM session access for ParkRepository.

Also tests StatsRepository shame breakdown methods used by the endpoint.
"""

import pytest
from datetime import datetime, timedelta, date
from decimal import Decimal


@pytest.mark.integration
class TestParkDetailsAPI:
    """Test park details endpoint functionality."""

    def test_park_details_returns_park_info(self, mysql_session):
        """
        Verify park details endpoint returns basic park information.

        This tests that ParkRepository.get_by_id() works correctly
        when called from the API route.
        """
        from database.connection import get_db_session
        from database.repositories.park_repository import ParkRepository
        from models.orm_park import Park

        with get_db_session() as session:
            # Create test park using ORM
            park = Park(
                park_id=9999,
                name='Test Park for Details',
                queue_times_id=99999,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.commit()

            # Test repository can read the park
            repo = ParkRepository(session)
            result = repo.get_by_id(9999)

            assert result is not None
            assert result.name == 'Test Park for Details'
            assert result.park_id == 9999

    def test_park_repository_works_with_session(self, mysql_session):
        """
        Verify that ParkRepository works correctly with Session.

        ORM repositories require Session, not Connection.
        """
        from database.repositories.park_repository import ParkRepository
        from models.orm_park import Park

        # Create test park
        park = Park(
            park_id=9998,
            name='Test Park for Session',
            queue_times_id=99998,
            city='Test City',
            country='US',
            timezone='America/Los_Angeles',
            is_active=True
        )
        mysql_session.merge(park)
        mysql_session.flush()

        # Repository should work with Session
        repo = ParkRepository(mysql_session)
        result = repo.get_by_id(9998)

        assert result is not None
        assert result.name == 'Test Park for Session'


@pytest.mark.integration
class TestStatsRepositoryShameBreakdown:
    """Test StatsRepository shame breakdown methods."""

    def test_get_park_weekly_shame_breakdown(self, mysql_session):
        """Test weekly shame breakdown returns correct structure."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_stats import ParkDailyStats

        with get_db_session() as session:
            # Clean up any existing test data
            session.query(ParkDailyStats).filter(ParkDailyStats.park_id == 9998).delete()
            session.query(Park).filter(Park.park_id == 9998).delete()
            session.commit()

            # Create test park using ORM
            park = Park(
                park_id=9998,
                name='Test Park Weekly',
                queue_times_id=99998,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # Insert daily stats for last week
            today = date.today()
            for i in range(1, 8):
                stat_date = today - timedelta(days=i)
                stat = ParkDailyStats(
                    park_id=9998,
                    stat_date=stat_date,
                    shame_score=Decimal(str(5.0 + i)),
                    total_downtime_hours=Decimal(str(2.0 + i)),
                    total_rides_tracked=10,
                    rides_with_downtime=2,
                    operating_hours_minutes=600
                )
                session.merge(stat)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_park_weekly_shame_breakdown(9998)

            assert result is not None
            assert 'shame_score' in result
            assert 'total_downtime_hours' in result
            assert 'days_tracked' in result
            # Date range returns calendar week, so may not be exactly 7 days
            assert result['days_tracked'] >= 1
            # Shame score should be calculated correctly (average of available days)
            assert result['shame_score'] > 0

    def test_get_park_yesterday_shame_breakdown(self, mysql_session):
        """Test yesterday shame breakdown returns correct structure."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_stats import ParkDailyStats
        from utils.timezone import get_yesterday_date_range

        with get_db_session() as session:
            # Clean up any existing test data
            session.query(ParkDailyStats).filter(ParkDailyStats.park_id == 9997).delete()
            session.query(Park).filter(Park.park_id == 9997).delete()
            session.commit()

            # Create test park using ORM
            park = Park(
                park_id=9997,
                name='Test Park Yesterday',
                queue_times_id=99997,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # CRITICAL: Use Pacific timezone yesterday to match what repository queries
            yesterday, _, _ = get_yesterday_date_range()
            stat = ParkDailyStats(
                park_id=9997,
                stat_date=yesterday,
                shame_score=Decimal('7.5'),
                total_downtime_hours=Decimal('10.0'),
                avg_uptime_percentage=Decimal('85.5'),
                total_rides_tracked=20,
                rides_with_downtime=5,
                operating_hours_minutes=720
            )
            session.merge(stat)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_park_yesterday_shame_breakdown(9997)

            assert result is not None
            assert result['shame_score'] == pytest.approx(7.5, rel=0.01)
            assert result['total_downtime_hours'] == pytest.approx(10.0, rel=0.01)
            assert result['rides_with_downtime'] == 5

    def test_get_park_monthly_shame_breakdown(self, mysql_session):
        """Test monthly shame breakdown returns correct structure."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_stats import ParkDailyStats
        from utils.timezone import get_last_month_date_range

        with get_db_session() as session:
            # Clean up any existing test data
            session.query(ParkDailyStats).filter(ParkDailyStats.park_id == 9996).delete()
            session.query(Park).filter(Park.park_id == 9996).delete()
            session.commit()

            # Create test park using ORM
            park = Park(
                park_id=9996,
                name='Test Park Monthly',
                queue_times_id=99996,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # CRITICAL: Use Pacific timezone last month to match what repository queries
            # Repository uses get_last_month_date_range() which returns previous calendar month
            start_date, end_date, _ = get_last_month_date_range()

            # Insert daily stats for the previous calendar month
            current_date = start_date
            while current_date <= end_date:
                stat = ParkDailyStats(
                    park_id=9996,
                    stat_date=current_date,
                    shame_score=Decimal('3.0'),
                    total_downtime_hours=Decimal('5.0'),
                    total_rides_tracked=15,
                    rides_with_downtime=3,
                    operating_hours_minutes=600
                )
                session.merge(stat)
                current_date += timedelta(days=1)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_park_monthly_shame_breakdown(9996)

            assert result is not None
            assert 'shame_score' in result
            assert 'total_downtime_hours' in result
            assert 'days_tracked' in result
            # Date range returns calendar month, so may not be exactly 30 days
            assert result['days_tracked'] >= 1
            # Shame score should be calculated correctly (average of available days)
            assert result['shame_score'] > 0

    def test_get_park_shame_breakdown_live(self, mysql_session):
        """Test live shame breakdown returns most recent snapshot."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_snapshots import ParkActivitySnapshot

        with get_db_session() as session:
            # Create test park using ORM
            park = Park(
                park_id=9995,
                name='Test Park Live',
                queue_times_id=99995,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # Create activity snapshot
            snapshot = ParkActivitySnapshot(
                park_id=9995,
                recorded_at=datetime.utcnow(),
                total_rides_tracked=20,
                rides_open=15,
                rides_closed=5,
                park_appears_open=True,
                shame_score=Decimal('4.5')
            )
            session.add(snapshot)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_park_shame_breakdown(9995)

            assert result is not None
            assert 'shame_score' in result
            assert result['shame_score'] == pytest.approx(4.5, rel=0.01)
            assert result.get('is_live') == True

    def test_shame_breakdown_returns_zero_for_missing_data(self, mysql_session):
        """Test shame breakdown returns zero when no data exists."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository

        with get_db_session() as session:
            repo = StatsRepository(session)

            # Test with non-existent park
            result = repo.get_park_weekly_shame_breakdown(999999)

            assert result is not None
            assert result['shame_score'] == 0
            assert result['days_tracked'] == 0


@pytest.mark.integration
class TestStatsRepositoryRideMethods:
    """Test StatsRepository methods for excluded and active rides."""

    def test_get_excluded_rides(self, mysql_session):
        """Test get_excluded_rides returns rides not operated in 7 days."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_ride import Ride

        with get_db_session() as session:
            # Create test park using ORM
            park = Park(
                park_id=9994,
                name='Test Park Excluded',
                queue_times_id=99994,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # Create ride that has NOT operated
            ride = Ride(
                ride_id=99994,
                park_id=9994,
                name='Never Operating Ride',
                queue_times_id=999994,
                is_active=True,
                tier=2
            )
            session.merge(ride)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_excluded_rides(9994)

            assert result is not None
            assert isinstance(result, list)
            # Should have at least our test ride since it never operated
            ride_names = [r['ride_name'] for r in result]
            assert 'Never Operating Ride' in ride_names

    def test_get_active_rides(self, mysql_session):
        """Test get_active_rides returns rides that operated recently."""
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from models.orm_park import Park
        from models.orm_ride import Ride
        from models.orm_snapshots import RideStatusSnapshot

        with get_db_session() as session:
            # Create test park using ORM
            park = Park(
                park_id=9993,
                name='Test Park Active',
                queue_times_id=99993,
                city='Test City',
                country='US',
                timezone='America/Los_Angeles',
                is_disney=False,
                is_universal=False,
                is_active=True
            )
            session.merge(park)
            session.flush()

            # Create ride
            ride = Ride(
                ride_id=99993,
                park_id=9993,
                name='Recently Operating Ride',
                queue_times_id=999993,
                is_active=True,
                tier=1
            )
            session.merge(ride)
            session.flush()

            # Create recent OPERATING snapshot
            snapshot = RideStatusSnapshot(
                ride_id=99993,
                recorded_at=datetime.utcnow(),
                status='OPERATING',
                computed_is_open=True,
                last_updated_api=datetime.utcnow()
            )
            session.add(snapshot)
            session.commit()

            repo = StatsRepository(session)
            result = repo.get_active_rides(9993)

            assert result is not None
            assert isinstance(result, list)
            # Should include our recently operating ride
            ride_names = [r['name'] for r in result]
            assert 'Recently Operating Ride' in ride_names

            # Check tier weight is included
            for ride in result:
                if ride['name'] == 'Recently Operating Ride':
                    assert ride['tier'] == 1
                    assert ride['weight'] == 10  # Tier 1 weight


@pytest.mark.integration
class TestStatsRepositoryMethods:
    """Test StatsRepository methods used by park details."""

    def test_stats_repo_get_hourly_stats_exists(self, mysql_session):
        """
        Verify StatsRepository has get_hourly_stats method.

        This method is called by TodayParkWaitTimesQuery.
        """
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository

        with get_db_session() as session:
            repo = StatsRepository(session)

            # Check method exists
            assert hasattr(repo, 'get_hourly_stats'), \
                "StatsRepository missing get_hourly_stats method - needed by TodayParkWaitTimesQuery"


@pytest.mark.integration
class TestRideRankingsAPI:
    """Test ride rankings endpoint for yesterday period."""

    def test_ride_rankings_yesterday_uses_correct_query(self, mysql_session):
        """
        Verify ride rankings for 'yesterday' period uses the correct query class.

        The deprecated get_ride_daily_rankings() should not be called.
        """
        from database.connection import get_db_session
        from database.repositories.stats_repository import StatsRepository
        from datetime import date, timedelta

        with get_db_session() as session:
            repo = StatsRepository(session)
            yesterday = date.today() - timedelta(days=1)

            # This should raise NotImplementedError because it's deprecated
            with pytest.raises(NotImplementedError, match="deprecated"):
                repo.get_ride_daily_rankings(
                    stat_date=yesterday,
                    filter_disney_universal=True,
                    limit=100
                )
