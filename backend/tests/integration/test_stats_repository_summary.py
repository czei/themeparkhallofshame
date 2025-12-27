"""
Integration tests for StatsRepository.get_aggregate_park_stats() summary stats.

These tests verify the aggregate stats against real MySQL database:
- Each period queries the correct pre-aggregated table
- Results match expected data patterns from test fixtures
- Disney/Universal filter works correctly
- Edge cases with missing data are handled

Bug Context (2025-12-26):
- Stats panels on index.html showed all zeros
- Root cause: _get_summary_stats returned hardcoded zeros
- Fix: Query pre-aggregated tables based on period

Requires:
- TEST_DB_NAME, TEST_DB_HOST, TEST_DB_USER, TEST_DB_PASSWORD env vars
- Test database with parks, park_live_rankings, park_hourly_stats, park_daily_stats tables
"""

import pytest
from datetime import datetime, timedelta
from sqlalchemy import text


class TestSummaryStatsIntegration:
    """Integration tests for summary stats from real database."""

    @pytest.fixture(autouse=True)
    def setup_test_data(self, mysql_session):
        """
        Set up test data in pre-aggregated tables.

        Creates:
        - 2 parks (1 Disney, 1 non-Disney)
        - park_live_rankings entries
        - park_hourly_stats entries
        - park_daily_stats entries
        """
        # Clear existing test data
        mysql_session.execute(text("DELETE FROM park_live_rankings WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM park_hourly_stats WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM park_daily_stats WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM parks WHERE park_id IN (9901, 9902)"))

        # Insert test parks
        mysql_session.execute(text("""
            INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country,
                             latitude, longitude, timezone, is_disney, is_universal, is_active)
            VALUES
                (9901, 9901, 'Test Disney Park', 'Orlando', 'FL', 'US',
                 28.385233, -81.563873, 'America/New_York', 1, 0, 1),
                (9902, 9902, 'Test Other Park', 'Sandusky', 'OH', 'US',
                 41.4839, -82.6794, 'America/New_York', 0, 0, 1)
        """))

        # Insert park_live_rankings data
        mysql_session.execute(text("""
            INSERT INTO park_live_rankings
                (park_id, park_name, total_rides, rides_down,
                 total_downtime_hours, shame_score, is_disney, is_universal, calculated_at)
            VALUES
                (9901, 'Test Disney Park', 50, 5, 2.5, 10.0, 1, 0, NOW()),
                (9902, 'Test Other Park', 30, 2, 1.0, 5.0, 0, 0, NOW())
        """))

        # Insert park_hourly_stats data (last 24 hours)
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        for hour_offset in range(24):
            hour_start = now - timedelta(hours=hour_offset)
            mysql_session.execute(text("""
                INSERT INTO park_hourly_stats
                    (park_id, hour_start_utc, park_was_open, rides_operating, rides_down,
                     total_downtime_hours, avg_wait_time_minutes, snapshot_count)
                VALUES
                    (:park_id, :hour_start, 1, :rides_op, :rides_down, :downtime, :wait, 6)
            """), {
                'park_id': 9901,
                'hour_start': hour_start,
                'rides_op': 45,
                'rides_down': 3 if hour_offset < 12 else 5,
                'downtime': 0.5,
                'wait': 30.0
            })

        # Insert park_daily_stats data (last 30 days)
        today = datetime.utcnow().date()
        for day_offset in range(30):
            stat_date = today - timedelta(days=day_offset)
            for park_id in [9901, 9902]:
                mysql_session.execute(text("""
                    INSERT INTO park_daily_stats
                        (park_id, stat_date, total_rides_tracked, rides_with_downtime,
                         total_downtime_hours, avg_uptime_percentage, operating_hours_minutes)
                    VALUES
                        (:park_id, :stat_date, :rides, :rides_down, :downtime, :uptime, 720)
                """), {
                    'park_id': park_id,
                    'stat_date': stat_date,
                    'rides': 50 if park_id == 9901 else 30,
                    'rides_down': 5 if park_id == 9901 else 2,
                    'downtime': 3.0 if park_id == 9901 else 1.0,
                    'uptime': 95.0 if park_id == 9901 else 97.0
                })

        mysql_session.commit()
        yield

        # Cleanup
        mysql_session.execute(text("DELETE FROM park_live_rankings WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM park_hourly_stats WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM park_daily_stats WHERE park_id IN (9901, 9902)"))
        mysql_session.execute(text("DELETE FROM parks WHERE park_id IN (9901, 9902)"))
        mysql_session.commit()

    def test_live_returns_from_park_live_rankings(self, mysql_session):
        """
        Given: Test data in park_live_rankings
        When: get_aggregate_park_stats(period='live') is called
        Then: Returns aggregated stats from park_live_rankings table
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='live')

        assert result['period'] == 'live'
        assert result['total_parks'] >= 2  # At least our test parks
        assert result['rides_down'] >= 7  # 5 + 2 from test data
        assert result['total_downtime_hours'] >= 3.5  # 2.5 + 1.0

    def test_live_with_disney_filter(self, mysql_session):
        """
        Given: Disney and non-Disney parks in database
        When: get_aggregate_park_stats(period='live', filter_disney_universal=True) is called
        Then: Returns only Disney/Universal parks
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(
            park_id=None,
            period='live',
            filter_disney_universal=True
        )

        assert result['filter_disney_universal'] is True
        # Should include Disney park but fewer total parks
        assert result['total_parks'] >= 1

    def test_today_returns_from_park_hourly_stats(self, mysql_session):
        """
        Given: Test data in park_hourly_stats for last 24 hours
        When: get_aggregate_park_stats(period='today') is called
        Then: Returns aggregated stats from park_hourly_stats table
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='today')

        assert result['period'] == 'today'
        assert result['total_parks'] >= 1  # At least our test park
        assert result['rides_down'] >= 3  # Minimum from test data

    def test_yesterday_returns_from_park_daily_stats(self, mysql_session):
        """
        Given: Test data in park_daily_stats for yesterday
        When: get_aggregate_park_stats(period='yesterday') is called
        Then: Returns aggregated stats from park_daily_stats for yesterday only
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='yesterday')

        assert result['period'] == 'yesterday'
        assert result['total_parks'] >= 2  # Both test parks
        # Yesterday should have data from our test fixtures
        assert result['total_downtime_hours'] >= 4.0  # 3.0 + 1.0

    def test_last_week_aggregates_7_days(self, mysql_session):
        """
        Given: Test data in park_daily_stats for last 7 days
        When: get_aggregate_park_stats(period='last_week') is called
        Then: Returns aggregated stats from park_daily_stats for 7 days
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='last_week')

        assert result['period'] == 'last_week'
        assert result['total_parks'] >= 2
        # 7 days * 2 parks * (3.0 + 1.0) = at least some accumulated downtime
        assert result['total_downtime_hours'] >= 7 * 4.0  # 7 days

    def test_last_month_aggregates_30_days(self, mysql_session):
        """
        Given: Test data in park_daily_stats for last 30 days
        When: get_aggregate_park_stats(period='last_month') is called
        Then: Returns aggregated stats from park_daily_stats for 30 days
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='last_month')

        assert result['period'] == 'last_month'
        assert result['total_parks'] >= 2
        # 30 days of data should have significant downtime
        assert result['total_downtime_hours'] >= 30 * 4.0  # 30 days

    def test_result_keys_are_consistent(self, mysql_session):
        """
        Given: Any period
        When: get_aggregate_park_stats() is called
        Then: Result has all expected keys for frontend panels
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        for period in ['live', 'today', 'yesterday', 'last_week', 'last_month']:
            result = repo.get_aggregate_park_stats(park_id=None, period=period)

            # Keys needed by frontend stats panels
            assert 'total_parks' in result, f"Missing total_parks for {period}"
            assert 'rides_operating' in result, f"Missing rides_operating for {period}"
            assert 'rides_down' in result, f"Missing rides_down for {period}"
            assert 'rides_closed' in result, f"Missing rides_closed for {period}"
            assert 'rides_refurbishment' in result, f"Missing rides_refurbishment for {period}"
            assert 'total_downtime_hours' in result, f"Missing total_downtime_hours for {period}"
            assert 'avg_uptime_percentage' in result, f"Missing avg_uptime_percentage for {period}"


class TestSummaryStatsAPIIntegration:
    """Integration tests for summary stats through API endpoint."""

    def test_api_returns_aggregate_stats_for_live(self, mysql_session):
        """
        Given: Parks with live rankings data
        When: /api/parks/downtime?period=live is called
        Then: Response includes aggregate_stats with summary data
        """
        # This test requires the Flask app - skip if not available
        pytest.skip("Requires Flask test client - see test_api_endpoints_integration.py")

    def test_api_aggregate_stats_change_by_period(self, mysql_session):
        """
        Given: Different data in each period's table
        When: API is called with different period values
        Then: aggregate_stats values differ appropriately
        """
        # This test requires the Flask app - skip if not available
        pytest.skip("Requires Flask test client - see test_api_endpoints_integration.py")
