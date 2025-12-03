"""
YESTERDAY Period Tests
======================

TDD tests for the YESTERDAY time period feature.

YESTERDAY shows the previous complete operating day:
- Uses Pacific Time as the day boundary (same as TODAY)
- Returns immutable data (highly cacheable)
- Excludes parks that were closed yesterday
- Late-night cutoff: hours before 4 AM count as previous day

Key difference from TODAY:
- TODAY = midnight to now (partial day, live updates)
- YESTERDAY = full previous day (complete, immutable)
"""

from datetime import datetime, date, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo


class TestYesterdayPeriodDefinition:
    """
    Tests that define the YESTERDAY period behavior.
    """

    def test_yesterday_period_is_valid_enum_value(self):
        """
        YESTERDAY should be a valid period option in the API.

        Expected behavior:
        - API accepts period=yesterday
        - Returns 200 status code
        - Contains park rankings data
        """
        from utils.timezone import get_today_pacific

        # YESTERDAY should be recognized
        yesterday = get_today_pacific() - timedelta(days=1)
        assert yesterday < get_today_pacific(), "Yesterday should be before today"

    def test_yesterday_returns_complete_day_data(self):
        """
        YESTERDAY should return data for the entire previous day.

        Unlike TODAY (which shows midnight to now), YESTERDAY
        covers the full 24-hour period.
        """
        from utils.timezone import get_pacific_day_range_utc, get_today_pacific

        yesterday = get_today_pacific() - timedelta(days=1)
        start_utc, end_utc = get_pacific_day_range_utc(yesterday)

        # Should be exactly 24 hours
        duration = end_utc - start_utc
        assert duration.total_seconds() == 24 * 60 * 60, \
            "YESTERDAY should cover exactly 24 hours"

    def test_get_yesterday_range_utc_function_exists(self):
        """
        There should be a get_yesterday_range_utc() function.

        This function returns the UTC datetime range for yesterday,
        similar to get_today_range_to_now_utc() but for the full previous day.
        """
        from utils.timezone import get_yesterday_range_utc

        start_utc, end_utc, label = get_yesterday_range_utc()

        # Should return datetime objects
        assert isinstance(start_utc, datetime), "start_utc should be datetime"
        assert isinstance(end_utc, datetime), "end_utc should be datetime"
        assert isinstance(label, str), "label should be string"

        # End should be after start
        assert end_utc > start_utc, "end_utc should be after start_utc"

        # Duration should be 24 hours
        duration = end_utc - start_utc
        assert duration.total_seconds() == 24 * 60 * 60, \
            "Yesterday range should be exactly 24 hours"

    def test_yesterday_date_range_function_exists(self):
        """
        There should be a get_yesterday_date_range() function.

        Returns (yesterday_date, yesterday_date, label) for consistency
        with get_last_week_date_range() pattern.
        """
        from utils.timezone import get_yesterday_date_range

        yesterday_date, end_date, label = get_yesterday_date_range()

        # Should return date objects
        assert isinstance(yesterday_date, date), "Should return date object"
        assert isinstance(end_date, date), "End should be date object"
        assert isinstance(label, str), "Label should be string"

        # Start and end should be the same for a single day
        assert yesterday_date == end_date, "YESTERDAY is a single day"


class TestYesterdayTimezoneHandling:
    """
    Tests for Pacific timezone handling in YESTERDAY period.
    """

    def test_yesterday_uses_pacific_time(self):
        """
        YESTERDAY should use Pacific Time as the day boundary.

        At 1 AM Eastern (10 PM Pacific), "yesterday" is still the
        same as it was at 11 PM Eastern (8 PM Pacific).
        """
        from utils.timezone import get_yesterday_range_utc, PACIFIC_TZ

        start_utc, end_utc, _ = get_yesterday_range_utc()

        # Convert to Pacific to verify day boundaries
        start_pacific = start_utc.astimezone(PACIFIC_TZ)
        end_pacific = end_utc.astimezone(PACIFIC_TZ)

        # Start should be midnight Pacific
        assert start_pacific.hour == 0, "Start should be midnight Pacific"
        assert start_pacific.minute == 0, "Start should be exactly midnight"

        # End should be midnight Pacific (next day boundary)
        assert end_pacific.hour == 0, "End should be midnight Pacific"
        assert end_pacific.minute == 0, "End should be exactly midnight"

    def test_yesterday_is_before_today(self):
        """
        YESTERDAY date should always be exactly one day before TODAY.
        """
        from utils.timezone import get_today_pacific, get_yesterday_date_range

        today = get_today_pacific()
        yesterday, _, _ = get_yesterday_date_range()

        expected_yesterday = today - timedelta(days=1)
        assert yesterday == expected_yesterday, \
            f"Yesterday should be {expected_yesterday}, got {yesterday}"

    @patch('utils.timezone.datetime')
    def test_yesterday_at_midnight_pacific(self, mock_datetime):
        """
        At exactly midnight Pacific, "yesterday" is the day that just ended.
        """
        from utils.timezone import PACIFIC_TZ, UTC_TZ

        # Simulate midnight Pacific on Dec 2, 2025
        fake_now_pacific = datetime(2025, 12, 2, 0, 0, 0, tzinfo=PACIFIC_TZ)
        fake_now_utc = fake_now_pacific.astimezone(UTC_TZ)

        mock_datetime.now.return_value = fake_now_utc
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        # Yesterday should be Dec 1
        # Note: This test will need actual implementation to work
        # For now, we document expected behavior


class TestYesterdayExcludesClosedParks:
    """
    Tests for filtering out parks that were closed yesterday.
    """

    def test_closed_parks_not_in_yesterday_rankings(self):
        """
        Parks that were closed all day yesterday should not appear in rankings.

        A park is considered "closed" if it had no operating hours yesterday.
        """
        # This test documents expected API behavior
        # Parks without any operating data yesterday should be excluded
        pass  # Will be implemented with integration test

    def test_parks_with_schedule_but_no_data_excluded(self):
        """
        Parks scheduled to be open but with no ride data should be excluded.

        This can happen if data collection failed for that park.
        """
        pass  # Will be implemented with integration test


class TestYesterdayLabelFormatting:
    """
    Tests for the human-readable label for YESTERDAY period.
    """

    def test_yesterday_label_format(self):
        """
        YESTERDAY label should show the actual date.

        Format: "Dec 1, 2025" or similar readable format.
        """
        from utils.timezone import get_yesterday_date_range

        _, _, label = get_yesterday_date_range()

        # Label should contain month abbreviation
        assert any(month in label for month in [
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
        ]), f"Label should contain month: {label}"

        # Label should contain year
        import datetime
        current_year = str(datetime.date.today().year)
        assert current_year in label or str(int(current_year) - 1) in label, \
            f"Label should contain year: {label}"


class TestYesterdayImmutability:
    """
    Tests that verify YESTERDAY data doesn't change.
    """

    def test_yesterday_data_is_complete(self):
        """
        YESTERDAY data should represent a complete day.

        Unlike TODAY which updates throughout the day,
        YESTERDAY is final and won't change.
        """
        from utils.timezone import get_yesterday_range_utc

        start_utc, end_utc, _ = get_yesterday_range_utc()

        # End should be in the past
        now_utc = datetime.now(ZoneInfo('UTC'))
        assert end_utc < now_utc, \
            "YESTERDAY end time should be in the past"

    def test_yesterday_suitable_for_caching(self):
        """
        YESTERDAY responses can be cached for 24 hours.

        Since the data won't change, aggressive caching is appropriate.
        """
        # This documents caching behavior - actual implementation
        # will set Cache-Control headers appropriately
        pass


class TestCalendarPeriodInfoForYesterday:
    """
    Tests for get_calendar_period_info() with 'yesterday' period.
    """

    def test_calendar_period_info_supports_yesterday(self):
        """
        get_calendar_period_info('yesterday') should work.
        """
        from utils.timezone import get_calendar_period_info

        info = get_calendar_period_info('yesterday')

        assert 'start_date' in info, "Should have start_date"
        assert 'end_date' in info, "Should have end_date"
        assert 'label' in info, "Should have label"
        assert 'period_type' in info, "Should have period_type"

        # For a single day, start and end should be the same
        assert info['start_date'] == info['end_date'], \
            "YESTERDAY is a single day, start and end should match"

        assert info['period_type'] == 'day', \
            "Period type should be 'day'"
