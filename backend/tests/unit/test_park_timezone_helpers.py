"""
Unit Tests: Park-Specific Timezone Helpers (Feature 004)

Tests for park-specific timezone functions that support parks
with different timezones (European, Asian, etc.).

Feature: 004-themeparks-data-collection
Task: T045
"""

import pytest
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from freezegun import freeze_time

from utils.timezone import (
    get_park_timezone,
    get_park_today,
    get_park_now,
    get_park_day_range_utc,
    get_park_yesterday_range_utc,
    utc_to_park_date,
    UTC_TZ,
    PACIFIC_TZ,
    _timezone_cache,
)


class TestGetParkTimezone:
    """Tests for get_park_timezone()."""

    def test_returns_zoneinfo_for_valid_timezone(self):
        """Should return ZoneInfo for valid IANA timezone."""
        tz = get_park_timezone('America/New_York')
        assert isinstance(tz, ZoneInfo)
        assert str(tz) == 'America/New_York'

    def test_caches_timezone_objects(self):
        """Should cache ZoneInfo objects to avoid repeated parsing."""
        # Clear cache except defaults
        _timezone_cache.clear()
        _timezone_cache['America/Los_Angeles'] = PACIFIC_TZ
        _timezone_cache['UTC'] = UTC_TZ

        # First call should add to cache
        tz1 = get_park_timezone('Europe/Paris')
        assert 'Europe/Paris' in _timezone_cache

        # Second call should return cached object
        tz2 = get_park_timezone('Europe/Paris')
        assert tz1 is tz2

    def test_returns_pacific_from_cache(self):
        """Should return Pacific timezone from cache."""
        tz = get_park_timezone('America/Los_Angeles')
        assert tz is PACIFIC_TZ

    def test_returns_utc_from_cache(self):
        """Should return UTC timezone from cache."""
        tz = get_park_timezone('UTC')
        assert tz is UTC_TZ

    def test_handles_european_timezones(self):
        """Should handle European timezone strings."""
        paris = get_park_timezone('Europe/Paris')
        london = get_park_timezone('Europe/London')

        # Paris should be CET/CEST, London should be GMT/BST
        assert str(paris) == 'Europe/Paris'
        assert str(london) == 'Europe/London'

    def test_handles_asian_timezones(self):
        """Should handle Asian timezone strings."""
        tokyo = get_park_timezone('Asia/Tokyo')
        hong_kong = get_park_timezone('Asia/Hong_Kong')

        assert str(tokyo) == 'Asia/Tokyo'
        assert str(hong_kong) == 'Asia/Hong_Kong'


class TestGetParkToday:
    """Tests for get_park_today()."""

    @freeze_time("2025-12-15 10:00:00", tz_offset=0)  # 10 AM UTC
    def test_returns_correct_date_for_eastern_timezone(self):
        """Eastern Time: 10 AM UTC = 5 AM ET = Dec 15."""
        today = get_park_today('America/New_York')
        assert today == date(2025, 12, 15)

    @freeze_time("2025-12-15 03:00:00", tz_offset=0)  # 3 AM UTC
    def test_returns_previous_date_for_pacific_late_night(self):
        """Pacific Time: 3 AM UTC = 7 PM PT previous day (Dec 14)."""
        today = get_park_today('America/Los_Angeles')
        assert today == date(2025, 12, 14)

    @freeze_time("2025-12-15 10:00:00", tz_offset=0)  # 10 AM UTC
    def test_returns_correct_date_for_paris(self):
        """Paris: 10 AM UTC = 11 AM CET = Dec 15."""
        today = get_park_today('Europe/Paris')
        assert today == date(2025, 12, 15)

    @freeze_time("2025-12-15 22:00:00", tz_offset=0)  # 10 PM UTC
    def test_returns_next_date_for_tokyo_late_utc(self):
        """Tokyo: 10 PM UTC = 7 AM JST next day (Dec 16)."""
        today = get_park_today('Asia/Tokyo')
        assert today == date(2025, 12, 16)


class TestGetParkNow:
    """Tests for get_park_now()."""

    @freeze_time("2025-12-15 14:30:00", tz_offset=0)
    def test_returns_timezone_aware_datetime(self):
        """Should return datetime with park timezone attached."""
        now = get_park_now('America/New_York')
        assert now.tzinfo is not None

    @freeze_time("2025-12-15 14:30:00", tz_offset=0)  # 2:30 PM UTC
    def test_eastern_time_offset(self):
        """Eastern Time: 2:30 PM UTC = 9:30 AM ET."""
        now = get_park_now('America/New_York')
        assert now.hour == 9
        assert now.minute == 30


class TestGetParkDayRangeUtc:
    """Tests for get_park_day_range_utc()."""

    def test_pacific_winter_time(self):
        """Pacific Standard Time (PST = UTC-8)."""
        # Dec 15, 2025 is in winter (PST)
        start, end = get_park_day_range_utc(
            date(2025, 12, 15),
            'America/Los_Angeles'
        )

        # Midnight PST = 8 AM UTC
        assert start == datetime(2025, 12, 15, 8, 0, 0, tzinfo=UTC_TZ)
        # Next midnight PST = next day 8 AM UTC
        assert end == datetime(2025, 12, 16, 8, 0, 0, tzinfo=UTC_TZ)

    def test_pacific_summer_time(self):
        """Pacific Daylight Time (PDT = UTC-7)."""
        # July 15, 2025 is in summer (PDT)
        start, end = get_park_day_range_utc(
            date(2025, 7, 15),
            'America/Los_Angeles'
        )

        # Midnight PDT = 7 AM UTC
        assert start == datetime(2025, 7, 15, 7, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 7, 16, 7, 0, 0, tzinfo=UTC_TZ)

    def test_eastern_winter_time(self):
        """Eastern Standard Time (EST = UTC-5)."""
        start, end = get_park_day_range_utc(
            date(2025, 12, 15),
            'America/New_York'
        )

        # Midnight EST = 5 AM UTC
        assert start == datetime(2025, 12, 15, 5, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 12, 16, 5, 0, 0, tzinfo=UTC_TZ)

    def test_paris_winter_time(self):
        """Central European Time (CET = UTC+1)."""
        start, end = get_park_day_range_utc(
            date(2025, 12, 15),
            'Europe/Paris'
        )

        # Midnight CET = 11 PM UTC previous day
        assert start == datetime(2025, 12, 14, 23, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 12, 15, 23, 0, 0, tzinfo=UTC_TZ)

    def test_paris_summer_time(self):
        """Central European Summer Time (CEST = UTC+2)."""
        start, end = get_park_day_range_utc(
            date(2025, 7, 15),
            'Europe/Paris'
        )

        # Midnight CEST = 10 PM UTC previous day
        assert start == datetime(2025, 7, 14, 22, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 7, 15, 22, 0, 0, tzinfo=UTC_TZ)

    def test_tokyo_time(self):
        """Japan Standard Time (JST = UTC+9, no DST)."""
        start, end = get_park_day_range_utc(
            date(2025, 12, 15),
            'Asia/Tokyo'
        )

        # Midnight JST = 3 PM UTC previous day
        assert start == datetime(2025, 12, 14, 15, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 12, 15, 15, 0, 0, tzinfo=UTC_TZ)

    def test_day_span_is_24_hours(self):
        """Day range should span exactly 24 hours."""
        start, end = get_park_day_range_utc(
            date(2025, 6, 15),
            'America/New_York'
        )

        duration = end - start
        assert duration == timedelta(hours=24)


class TestGetParkYesterdayRangeUtc:
    """Tests for get_park_yesterday_range_utc()."""

    @freeze_time("2025-12-15 14:00:00", tz_offset=0)  # 2 PM UTC
    def test_returns_yesterday_for_eastern(self):
        """Eastern: 2 PM UTC = 9 AM ET Dec 15, so yesterday = Dec 14."""
        start, end, label = get_park_yesterday_range_utc('America/New_York')

        # Yesterday in EST = Dec 14
        # Midnight EST = 5 AM UTC
        assert start == datetime(2025, 12, 14, 5, 0, 0, tzinfo=UTC_TZ)
        assert end == datetime(2025, 12, 15, 5, 0, 0, tzinfo=UTC_TZ)
        assert "Dec 14" in label

    @freeze_time("2025-12-15 02:00:00", tz_offset=0)  # 2 AM UTC
    def test_pacific_late_night_yesterday(self):
        """Pacific: 2 AM UTC = 6 PM PT Dec 14, so yesterday = Dec 13."""
        start, end, label = get_park_yesterday_range_utc('America/Los_Angeles')

        # At 2 AM UTC, it's still Dec 14 in Pacific
        # So "yesterday" is Dec 13
        assert "Dec 13" in label

    @freeze_time("2025-12-15 14:00:00", tz_offset=0)
    def test_returns_label_format(self):
        """Label should be human-readable format."""
        _, _, label = get_park_yesterday_range_utc('America/New_York')

        # Should contain month and day
        assert "Dec" in label
        assert "2025" in label


class TestUtcToParkDate:
    """Tests for utc_to_park_date()."""

    def test_converts_utc_to_pacific_date(self):
        """Should convert UTC datetime to Pacific date."""
        # 3 AM UTC on Dec 15 = 7 PM Pacific on Dec 14
        utc_dt = datetime(2025, 12, 15, 3, 0, 0, tzinfo=UTC_TZ)
        park_date = utc_to_park_date(utc_dt, 'America/Los_Angeles')

        assert park_date == date(2025, 12, 14)

    def test_converts_utc_to_eastern_date(self):
        """Should convert UTC datetime to Eastern date."""
        # 3 AM UTC on Dec 15 = 10 PM Eastern on Dec 14
        utc_dt = datetime(2025, 12, 15, 3, 0, 0, tzinfo=UTC_TZ)
        park_date = utc_to_park_date(utc_dt, 'America/New_York')

        assert park_date == date(2025, 12, 14)

    def test_converts_utc_to_tokyo_date(self):
        """Should convert UTC datetime to Tokyo date."""
        # 3 AM UTC on Dec 15 = 12 PM (noon) JST on Dec 15
        utc_dt = datetime(2025, 12, 15, 3, 0, 0, tzinfo=UTC_TZ)
        park_date = utc_to_park_date(utc_dt, 'Asia/Tokyo')

        assert park_date == date(2025, 12, 15)

    def test_handles_naive_datetime_as_utc(self):
        """Should treat naive datetime as UTC."""
        naive_dt = datetime(2025, 12, 15, 3, 0, 0)  # No tzinfo
        park_date = utc_to_park_date(naive_dt, 'America/Los_Angeles')

        # 3 AM UTC = 7 PM Pacific on Dec 14
        assert park_date == date(2025, 12, 14)

    def test_converts_paris_date_correctly(self):
        """Should convert UTC to Paris date correctly."""
        # 11 PM UTC on Dec 14 = 12 AM (midnight) CET on Dec 15
        utc_dt = datetime(2025, 12, 14, 23, 0, 0, tzinfo=UTC_TZ)
        park_date = utc_to_park_date(utc_dt, 'Europe/Paris')

        assert park_date == date(2025, 12, 15)


class TestDaylightSavingTransitions:
    """Tests for DST transition edge cases."""

    def test_spring_forward_pacific(self):
        """Pacific spring forward: March 9, 2025 (2 AM -> 3 AM)."""
        # March 9 spans the transition
        start, end = get_park_day_range_utc(
            date(2025, 3, 9),
            'America/Los_Angeles'
        )

        # March 9 is the transition day - only 23 hours
        duration = end - start
        assert duration == timedelta(hours=23)

    def test_fall_back_pacific(self):
        """Pacific fall back: November 2, 2025 (2 AM -> 1 AM)."""
        start, end = get_park_day_range_utc(
            date(2025, 11, 2),
            'America/Los_Angeles'
        )

        # November 2 is the transition day - 25 hours
        duration = end - start
        assert duration == timedelta(hours=25)

    def test_spring_forward_paris(self):
        """Paris spring forward: March 30, 2025 (2 AM -> 3 AM)."""
        start, end = get_park_day_range_utc(
            date(2025, 3, 30),
            'Europe/Paris'
        )

        # March 30 is the transition day - only 23 hours
        duration = end - start
        assert duration == timedelta(hours=23)

    def test_no_dst_tokyo(self):
        """Tokyo has no DST - always 24 hours."""
        # Check a date that would be DST transition elsewhere
        start, end = get_park_day_range_utc(
            date(2025, 3, 9),
            'Asia/Tokyo'
        )

        duration = end - start
        assert duration == timedelta(hours=24)
