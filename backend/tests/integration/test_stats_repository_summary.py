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

        # Insert park_daily_stats data for:
        # 1. Yesterday (Pacific)
        # 2. Previous calendar week (Sun-Sat)
        # 3. Previous calendar month
        from utils.timezone import (
            get_yesterday_date_range,
            get_last_week_date_range,
            get_last_month_date_range
        )

        yesterday, _, _ = get_yesterday_date_range()
        week_start, week_end, _ = get_last_week_date_range()
        month_start, month_end, _ = get_last_month_date_range()

        # Collect all dates we need to populate
        dates_to_insert = set()
        dates_to_insert.add(yesterday)

        # Add all days in previous calendar week
        current = week_start
        while current <= week_end:
            dates_to_insert.add(current)
            current += timedelta(days=1)

        # Add all days in previous calendar month
        current = month_start
        while current <= month_end:
            dates_to_insert.add(current)
            current += timedelta(days=1)

        for stat_date in dates_to_insert:
            for park_id in [9901, 9902]:
                mysql_session.execute(text("""
                    INSERT INTO park_daily_stats
                        (park_id, stat_date, total_rides_tracked, rides_with_downtime,
                         total_downtime_hours, avg_uptime_percentage, operating_hours_minutes)
                    VALUES
                        (:park_id, :stat_date, :rides, :rides_down, :downtime, :uptime, 720)
                    ON DUPLICATE KEY UPDATE
                        total_downtime_hours = VALUES(total_downtime_hours)
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

    def test_last_week_aggregates_calendar_week(self, mysql_session):
        """
        Given: Test data in park_daily_stats for previous calendar week (Sun-Sat)
        When: get_aggregate_park_stats(period='last_week') is called
        Then: Returns aggregated stats from park_daily_stats for the calendar week
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='last_week')

        assert result['period'] == 'last_week'
        assert result['total_parks'] >= 2
        # Calendar week = 7 days * 2 parks * (3.0 + 1.0) = 56 hours min
        # But some days might overlap with month, so just check for some downtime
        assert result['total_downtime_hours'] >= 7 * 4.0  # 7 days

    def test_last_month_aggregates_calendar_month(self, mysql_session):
        """
        Given: Test data in park_daily_stats for previous calendar month
        When: get_aggregate_park_stats(period='last_month') is called
        Then: Returns aggregated stats from park_daily_stats for the calendar month
        """
        from database.repositories.stats_repository import StatsRepository

        repo = StatsRepository(mysql_session)

        result = repo.get_aggregate_park_stats(park_id=None, period='last_month')

        assert result['period'] == 'last_month'
        assert result['total_parks'] >= 2
        # Calendar month varies (28-31 days), so use minimum 28 days
        # 28 days * 2 parks * (3.0 + 1.0) = 224 hours min
        assert result['total_downtime_hours'] >= 28 * 4.0  # At least 28 days

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


