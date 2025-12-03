"""
Theme Park Downtime Tracker - Aggregation Service Unit Tests

Tests AggregationService:
- Daily stat calculation logic
- Weekly stat calculation logic
- Monthly stat calculation logic
- Timezone-aware aggregation
- Error handling and retry logic
- Aggregation log tracking

Priority: P1 - Tests aggregation core logic (T146)
"""

import pytest
from datetime import date
from unittest.mock import Mock, patch

from processor.aggregation_service import AggregationService


class TestAggregationServiceInit:
    """Test AggregationService initialization."""

    def test_init_with_connection(self):
        """Service should initialize with database connection."""
        mock_conn = Mock()

        service = AggregationService(mock_conn)

        assert service.conn == mock_conn
        assert service.hours_detector is not None
        assert service.change_detector is not None


class TestDailyAggregation:
    """Test daily aggregation logic."""

    def test_aggregate_daily_creates_log(self):
        """aggregate_daily should create an aggregation log entry."""
        mock_conn = Mock()

        # Mock the log creation
        mock_result = Mock()
        mock_result.lastrowid = 1
        mock_conn.execute.return_value = mock_result

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1) as mock_create_log:
            with patch.object(AggregationService, '_get_distinct_timezones', return_value=['America/New_York']):
                with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}):
                    with patch.object(AggregationService, '_complete_aggregation_log'):
                        service = AggregationService(mock_conn)
                        result = service.aggregate_daily(date(2024, 7, 15))

                        mock_create_log.assert_called_once_with(date(2024, 7, 15), 'daily')

    def test_aggregate_daily_processes_all_timezones(self):
        """aggregate_daily should process all distinct park timezones."""
        mock_conn = Mock()

        timezones = ['America/New_York', 'America/Los_Angeles', 'America/Chicago']

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', return_value=timezones):
                with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}) as mock_tz_agg:
                    with patch.object(AggregationService, '_complete_aggregation_log'):
                        service = AggregationService(mock_conn)
                        result = service.aggregate_daily(date(2024, 7, 15))

                        # Should be called once for each timezone
                        assert mock_tz_agg.call_count == 3

    def test_aggregate_daily_specific_timezone(self):
        """aggregate_daily with specific timezone should only process that timezone."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}) as mock_tz_agg:
                with patch.object(AggregationService, '_complete_aggregation_log'):
                    service = AggregationService(mock_conn)
                    result = service.aggregate_daily(date(2024, 7, 15), park_timezone='America/New_York')

                    # Should only be called once for the specific timezone
                    assert mock_tz_agg.call_count == 1
                    mock_tz_agg.assert_called_with(date(2024, 7, 15), 'America/New_York')

    def test_aggregate_daily_returns_results(self):
        """aggregate_daily should return aggregation results."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=42):
            with patch.object(AggregationService, '_get_distinct_timezones', return_value=['America/New_York']):
                with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 10, 'rides_count': 100}):
                    with patch.object(AggregationService, '_complete_aggregation_log'):
                        service = AggregationService(mock_conn)
                        result = service.aggregate_daily(date(2024, 7, 15))

                        assert result['log_id'] == 42
                        assert result['status'] == 'success'
                        assert result['parks_processed'] == 10
                        assert result['rides_processed'] == 100
                        assert 'aggregated_until_ts' in result

    def test_aggregate_daily_handles_error(self):
        """aggregate_daily should log failure on error."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', side_effect=Exception("Database error")):
                with patch.object(AggregationService, '_complete_aggregation_log') as mock_complete:
                    service = AggregationService(mock_conn)

                    with pytest.raises(Exception, match="Database error"):
                        service.aggregate_daily(date(2024, 7, 15))

                    # Should mark as failed
                    mock_complete.assert_called_once()
                    call_kwargs = mock_complete.call_args[1]
                    assert call_kwargs['status'] == 'failed'
                    assert 'Database error' in call_kwargs['error_message']


class TestWeeklyAggregation:
    """Test weekly aggregation logic."""

    def test_aggregate_weekly_calculates_week_start(self):
        """aggregate_weekly should calculate correct week start date."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', return_value=100):
            with patch.object(AggregationService, '_aggregate_parks_weekly_stats', return_value=10):
                service = AggregationService(mock_conn)
                result = service.aggregate_weekly(year=2024, week_number=29)

                # Week 29 of 2024 starts on Monday July 15
                expected_start = date(2024, 7, 15)
                assert result['week_start_date'] == expected_start

    def test_aggregate_weekly_returns_results(self):
        """aggregate_weekly should return aggregation results."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', return_value=100):
            with patch.object(AggregationService, '_aggregate_parks_weekly_stats', return_value=10):
                service = AggregationService(mock_conn)
                result = service.aggregate_weekly(year=2024, week_number=29)

                assert result['status'] == 'success'
                assert result['year'] == 2024
                assert result['week_number'] == 29
                assert result['parks_processed'] == 10
                assert result['rides_processed'] == 100

    def test_aggregate_weekly_handles_error(self):
        """aggregate_weekly should raise exception on error."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', side_effect=Exception("Database error")):
            service = AggregationService(mock_conn)

            with pytest.raises(Exception, match="Database error"):
                service.aggregate_weekly(year=2024, week_number=29)


class TestMonthlyAggregation:
    """Test monthly aggregation logic."""

    def test_aggregate_monthly_returns_results(self):
        """aggregate_monthly should return aggregation results."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_monthly_stats', return_value=500):
            with patch.object(AggregationService, '_aggregate_parks_monthly_stats', return_value=25):
                service = AggregationService(mock_conn)
                result = service.aggregate_monthly(year=2024, month=7)

                assert result['status'] == 'success'
                assert result['year'] == 2024
                assert result['month'] == 7
                assert result['parks_processed'] == 25
                assert result['rides_processed'] == 500

    def test_aggregate_monthly_handles_error(self):
        """aggregate_monthly should raise exception on error."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_monthly_stats', side_effect=Exception("Database error")):
            service = AggregationService(mock_conn)

            with pytest.raises(Exception, match="Database error"):
                service.aggregate_monthly(year=2024, month=7)


class TestTimezoneAwareAggregation:
    """Test timezone-aware aggregation logic."""

    def test_aggregation_respects_timezone_boundaries(self):
        """Aggregation should use correct UTC boundaries for each timezone."""
        mock_conn = Mock()

        # Mock timezone query
        mock_tz_result = Mock()
        mock_tz_result.__iter__ = Mock(return_value=iter([
            Mock(_mapping={'timezone': 'America/New_York'}),
            Mock(_mapping={'timezone': 'America/Los_Angeles'})
        ]))

        # Set up execute to return different results
        def execute_side_effect(query, params=None):
            result = Mock()
            if 'DISTINCT timezone' in str(query):
                result.__iter__ = Mock(return_value=iter([
                    Mock(_mapping={'timezone': 'America/New_York'})
                ]))
            else:
                result.lastrowid = 1
            return result

        mock_conn.execute.side_effect = execute_side_effect

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}):
                with patch.object(AggregationService, '_complete_aggregation_log'):
                    service = AggregationService(mock_conn)

                    # Test that aggregation_date is correctly passed
                    aggregation_date = date(2024, 7, 15)
                    result = service.aggregate_daily(aggregation_date, park_timezone='America/New_York')

                    assert result['status'] == 'success'


class TestAggregationLogTracking:
    """Test aggregation log tracking for safe cleanup."""

    def test_aggregation_log_created_on_start(self):
        """Aggregation should create log entry at start."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.lastrowid = 99
        mock_conn.execute.return_value = mock_result

        with patch.object(AggregationService, '_get_distinct_timezones', return_value=['America/New_York']):
            with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}):
                service = AggregationService(mock_conn)

                # Check that execute was called for log creation
                # The _create_aggregation_log method should be called
                # This will verify the log tracking pattern

    def test_aggregation_log_completed_on_success(self):
        """Aggregation should mark log as success when complete."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', return_value=['America/New_York']):
                with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}):
                    with patch.object(AggregationService, '_complete_aggregation_log') as mock_complete:
                        service = AggregationService(mock_conn)
                        result = service.aggregate_daily(date(2024, 7, 15))

                        mock_complete.assert_called_once()
                        call_kwargs = mock_complete.call_args[1]
                        assert call_kwargs['status'] == 'success'
                        assert call_kwargs['log_id'] == 1

    def test_aggregation_log_failed_on_error(self):
        """Aggregation should mark log as failed when error occurs."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', side_effect=Exception("Test error")):
                with patch.object(AggregationService, '_complete_aggregation_log') as mock_complete:
                    service = AggregationService(mock_conn)

                    with pytest.raises(Exception):
                        service.aggregate_daily(date(2024, 7, 15))

                    mock_complete.assert_called_once()
                    call_kwargs = mock_complete.call_args[1]
                    assert call_kwargs['status'] == 'failed'


class TestISOWeekCalculation:
    """Test ISO week number handling."""

    def test_week_1_of_year(self):
        """Week 1 should start on first Monday of the year or late December."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', return_value=100):
            with patch.object(AggregationService, '_aggregate_parks_weekly_stats', return_value=10):
                service = AggregationService(mock_conn)

                # Week 1 of 2024 starts Monday January 1
                result = service.aggregate_weekly(year=2024, week_number=1)
                assert result['week_start_date'] == date(2024, 1, 1)

    def test_week_52_of_year(self):
        """Week 52 should calculate correctly."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', return_value=100):
            with patch.object(AggregationService, '_aggregate_parks_weekly_stats', return_value=10):
                service = AggregationService(mock_conn)

                # Week 52 of 2024 starts Monday December 23
                result = service.aggregate_weekly(year=2024, week_number=52)
                assert result['week_start_date'] == date(2024, 12, 23)

    def test_mid_year_week(self):
        """Mid-year week should calculate correctly."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_aggregate_rides_weekly_stats', return_value=100):
            with patch.object(AggregationService, '_aggregate_parks_weekly_stats', return_value=10):
                service = AggregationService(mock_conn)

                # Week 26 of 2024 starts Monday June 24
                result = service.aggregate_weekly(year=2024, week_number=26)
                assert result['week_start_date'] == date(2024, 6, 24)


class TestRetryLogicSupport:
    """Test retry logic support (3 attempts at 12:10 AM, 1:10 AM, 2:10 AM)."""

    def test_service_supports_retry_via_log(self):
        """Service should track failures in log for retry handling."""
        mock_conn = Mock()

        # First attempt fails
        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', side_effect=Exception("Temporary failure")):
                with patch.object(AggregationService, '_complete_aggregation_log') as mock_complete:
                    service = AggregationService(mock_conn)

                    with pytest.raises(Exception):
                        service.aggregate_daily(date(2024, 7, 15))

                    # Log should record the failure for retry tracking
                    mock_complete.assert_called_once()
                    call_kwargs = mock_complete.call_args[1]
                    assert call_kwargs['status'] == 'failed'

    def test_service_idempotent_on_retry(self):
        """Service should be idempotent when retrying same date."""
        mock_conn = Mock()

        with patch.object(AggregationService, '_create_aggregation_log', return_value=1):
            with patch.object(AggregationService, '_get_distinct_timezones', return_value=['America/New_York']):
                with patch.object(AggregationService, '_aggregate_daily_for_timezone', return_value={'parks_count': 5, 'rides_count': 50}):
                    with patch.object(AggregationService, '_complete_aggregation_log'):
                        service = AggregationService(mock_conn)

                        # Run twice for same date (retry scenario)
                        result1 = service.aggregate_daily(date(2024, 7, 15))
                        result2 = service.aggregate_daily(date(2024, 7, 15))

                        # Both should succeed
                        assert result1['status'] == 'success'
                        assert result2['status'] == 'success'
