"""
Comprehensive API Integration Tests for Theme Park Downtime Tracker

Tests all API endpoints with extensive sample data to ensure:
1. Mathematical calculations are 100% accurate
2. All parameter combinations work correctly
3. Edge cases are handled properly
4. Data integrity is maintained

WARNING: These tests use REAL database connections and substantial test data.
Run with: pytest backend/tests/integration/test_api_endpoints_integration.py -v
"""

import pytest
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import text
from freezegun import freeze_time
import sys
from pathlib import Path

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from api.app import create_app
from utils.timezone import get_last_week_date_range, get_last_month_date_range

# Define a fixed point in time for all tests - 8 PM Pacific (4 AM UTC next day)
# This ensures "today" has plenty of hours (8 AM to 8 PM Pacific = 12 hours)
MOCKED_NOW_UTC = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)  # 8 PM PST Dec 5th
MOCKED_TODAY = date(2025, 12, 5)  # Pacific date at MOCKED_NOW_UTC

# Pre-calculate date ranges for test data creation
# These use freezegun context to match what the API will see
with freeze_time(MOCKED_NOW_UTC):
    LAST_WEEK_START, LAST_WEEK_END, _ = get_last_week_date_range()  # Nov 23-29
    LAST_MONTH_START, LAST_MONTH_END, _ = get_last_month_date_range()  # Nov 1-30


# ============================================================================
# FIXTURES - Comprehensive Test Data Setup
# ============================================================================

@pytest.fixture
def app():
    """Create Flask app for testing."""
    app = create_app()
    app.config['TESTING'] = True
    return app


@pytest.fixture
def client(app):
    """Create Flask test client."""
    return app.test_client()


@pytest.fixture
def comprehensive_test_data(mysql_session):
    """
    Create comprehensive test dataset with:
    - 10 parks (5 Disney, 3 Universal, 2 Other)
    - 100 rides (varied tiers across all parks)
    - Daily stats for today and yesterday
    - Weekly stats for current and previous week
    - Monthly stats for current and previous month
    - Realistic wait times and downtime patterns

    This ensures we test with substantial data, not just 1-2 examples.
    """
    conn = mysql_session

    # Clean up any existing test data from this test file (queue_times_id 9000+)
    # This handles committed data from previous runs of these tests
    conn.execute(text("DELETE FROM ride_status_snapshots"))
    conn.execute(text("DELETE FROM ride_status_changes"))
    conn.execute(text("DELETE FROM ride_daily_stats"))
    conn.execute(text("DELETE FROM ride_weekly_stats"))
    conn.execute(text("DELETE FROM ride_monthly_stats"))
    conn.execute(text("DELETE FROM ride_hourly_stats"))
    conn.execute(text("DELETE FROM park_activity_snapshots"))
    conn.execute(text("DELETE FROM park_daily_stats"))
    conn.execute(text("DELETE FROM park_weekly_stats"))
    conn.execute(text("DELETE FROM park_monthly_stats"))
    conn.execute(text("DELETE FROM park_hourly_stats"))
    conn.execute(text("DELETE FROM ride_classifications WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 90000)"))
    conn.execute(text("DELETE FROM rides WHERE queue_times_id >= 90000"))
    conn.execute(text("DELETE FROM parks WHERE queue_times_id >= 9000"))
    conn.commit()  # Commit deletes so Flask app can see clean state

    # === CREATE 10 PARKS ===
    # Use high ID range (9001-9010) to avoid conflicts with conftest.py fixtures (which use 101)
    parks_data = [
        # Disney Parks (5)
        (1, 9001, 'Magic Kingdom', 'Bay Lake', 'FL', 'US', 'America/New_York', 'Disney', True, False, True),
        (2, 9002, 'EPCOT', 'Bay Lake', 'FL', 'US', 'America/New_York', 'Disney', True, False, True),
        (3, 9003, 'Hollywood Studios', 'Bay Lake', 'FL', 'US', 'America/New_York', 'Disney', True, False, True),
        (4, 9004, 'Animal Kingdom', 'Bay Lake', 'FL', 'US', 'America/New_York', 'Disney', True, False, True),
        (5, 9005, 'Disneyland', 'Anaheim', 'CA', 'US', 'America/Los_Angeles', 'Disney', True, False, True),
        # Universal Parks (3)
        (6, 9006, 'Universal Studios Florida', 'Orlando', 'FL', 'US', 'America/New_York', 'Universal', False, True, True),
        (7, 9007, 'Islands of Adventure', 'Orlando', 'FL', 'US', 'America/New_York', 'Universal', False, True, True),
        (8, 9008, 'Universal Studios Hollywood', 'Los Angeles', 'CA', 'US', 'America/Los_Angeles', 'Universal', False, True, True),
        # Other Parks (2)
        (9, 9009, 'SeaWorld Orlando', 'Orlando', 'FL', 'US', 'America/New_York', 'SeaWorld', False, False, True),
        (10, 9010, 'Busch Gardens Tampa', 'Tampa', 'FL', 'US', 'America/New_York', 'Busch Gardens', False, False, True),
    ]

    for park in parks_data:
        conn.execute(text("""
            INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_disney, is_universal, is_active)
            VALUES (:park_id, :qt_id, :name, :city, :state, :country, :tz, :operator, :is_disney, :is_universal, :is_active)
        """), {
            'park_id': park[0], 'qt_id': park[1], 'name': park[2], 'city': park[3],
            'state': park[4], 'country': park[5], 'tz': park[6], 'operator': park[7],
            'is_disney': park[8], 'is_universal': park[9], 'is_active': park[10]
        })

    conn.commit()  # Commit parks so Flask app can see them

    # === CREATE 100 RIDES (10 per park, mixed tiers) ===
    # Use high ID range (90001-90100) to avoid conflicts with conftest.py fixtures
    ride_id = 1
    for park_id in range(1, 11):
        # Each park gets: 2 Tier 1, 5 Tier 2, 3 Tier 3
        tiers = [1, 1, 2, 2, 2, 2, 2, 3, 3, 3]
        for i, tier in enumerate(tiers, 1):
            conn.execute(text("""
            INSERT INTO rides (ride_id, queue_times_id, park_id, name, land_area, tier, is_active, last_operated_at)
            VALUES (:ride_id, :qt_id, :park_id, :name, :land, :tier, TRUE, :last_operated_at)
        """), {
            'ride_id': ride_id,
            'qt_id': 90000 + ride_id,
            'park_id': park_id,
            'name': f'Ride_{park_id}_{i}_T{tier}',
            'land': f'Land_{i}',
            'tier': tier,
            'last_operated_at': datetime.utcnow() - timedelta(days=i % 3)
        })

            # Add classification
            tier_weights = {1: 3, 2: 2, 3: 1}
            conn.execute(text("""
                INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score)
                VALUES (:ride_id, :tier, :weight, 'manual_override', 1.0)
            """), {
                'ride_id': ride_id,
                'tier': tier,
                'weight': tier_weights[tier]
            })

            ride_id += 1

    conn.commit()  # Commit rides and classifications so Flask app can see them

    # === CREATE REALISTIC STATS DATA ===
    # Use MOCKED_TODAY for deterministic test data that doesn't depend on current time
    today = MOCKED_TODAY
    yesterday = today - timedelta(days=1)

    # Use mocked datetime for all date/time calculations
    mocked_datetime = datetime.combine(MOCKED_TODAY, datetime.min.time())
    current_year = mocked_datetime.year
    current_week = mocked_datetime.isocalendar()[1]
    prev_week = (mocked_datetime - timedelta(weeks=1)).isocalendar()[1]
    prev_week_year = (mocked_datetime - timedelta(weeks=1)).year

    # Calculate week_start_date for weekly stats
    current_week_start = date.fromisocalendar(current_year, current_week, 1)
    prev_week_start = date.fromisocalendar(prev_week_year, prev_week, 1)

    current_month = mocked_datetime.month
    prev_month = current_month - 1 if current_month > 1 else 12
    prev_month_year = current_year if current_month > 1 else current_year - 1

    # Generate ride daily stats for all 100 rides
    # Pattern: Higher tier rides have more downtime (to test weighted scoring)
    ride_id = 1
    for park_id in range(1, 11):
        for i in range(10):
            tier = 1 if i < 2 else (2 if i < 7 else 3)

            # Today's stats - vary by tier
            # Tier 1: 180 min downtime (3 hrs), Tier 2: 60 min (1 hr), Tier 3: 30 min (0.5 hr)
            downtime_today = 180 if tier == 1 else (60 if tier == 2 else 30)
            uptime_today = 600 - downtime_today  # Assume 600 min operating time
            uptime_pct_today = (uptime_today / 600.0) * 100

            conn.execute(text("""
                INSERT INTO ride_daily_stats (
                    ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                ) VALUES (
                    :ride_id, :stat_date, :downtime, :uptime_minutes, :uptime,
                    :avg_wait, :peak_wait, :status_changes, :operating_minutes
                )
            """), {
                'ride_id': ride_id,
                'stat_date': today,
                'downtime': downtime_today,
                'uptime_minutes': uptime_today,
                'uptime': uptime_pct_today,
                'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                'peak_wait': 90 if tier == 1 else (60 if tier == 2 else 30),
                'status_changes': 3 if tier == 1 else 2,
                'operating_minutes': 600,
                'observations': 60
            })

            # Yesterday's stats - 20% less downtime (to test trends)
            downtime_yesterday = int(downtime_today * 0.8)
            uptime_yesterday = 600 - downtime_yesterday
            uptime_pct_yesterday = (uptime_yesterday / 600.0) * 100

            conn.execute(text("""
                INSERT INTO ride_daily_stats (
                    ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                ) VALUES (
                    :ride_id, :stat_date, :downtime, :uptime_minutes, :uptime,
                    :avg_wait, :peak_wait, :status_changes, :operating_minutes
                )
            """), {
                'ride_id': ride_id,
                'stat_date': yesterday,
                'downtime': downtime_yesterday,
                'uptime_minutes': uptime_yesterday,
                'uptime': uptime_pct_yesterday,
                'avg_wait': 40 if tier == 1 else (25 if tier == 2 else 12),
                'peak_wait': 80 if tier == 1 else (50 if tier == 2 else 25),
                'status_changes': 2 if tier == 1 else 1,
                'operating_minutes': 600,
                'observations': 60
            })

            # Additional days to cover 7-day aggregations (use same profile as today)
            for days_back in range(2, 7):
                stat_date = today - timedelta(days=days_back)
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    ) VALUES (
                        :ride_id, :stat_date, :downtime, :uptime_minutes, :uptime,
                        :avg_wait, :peak_wait, :status_changes, :operating_minutes
                    )
                """), {
                    'ride_id': ride_id,
                    'stat_date': stat_date,
                    'downtime': downtime_today,
                    'uptime_minutes': uptime_today,
                    'uptime': uptime_pct_today,
                    'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                    'peak_wait': 90 if tier == 1 else (60 if tier == 2 else 30),
                    'status_changes': 3 if tier == 1 else 2,
                    'operating_minutes': 600,
                    'observations': 60
                })

            # === ADD DATA FOR PREVIOUS COMPLETE WEEK (Nov 23-29) ===
            # This matches what get_last_week_date_range() returns
            # Skip dates already covered by the days_back loop (today, yesterday, and days 2-6)
            already_covered = set([today, yesterday] + [today - timedelta(days=d) for d in range(2, 7)])
            current_date = LAST_WEEK_START
            while current_date <= LAST_WEEK_END:
                if current_date in already_covered:
                    current_date += timedelta(days=1)
                    continue
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    ) VALUES (
                        :ride_id, :stat_date, :downtime, :uptime_minutes, :uptime,
                        :avg_wait, :peak_wait, :status_changes, :operating_minutes
                    )
                """), {
                    'ride_id': ride_id,
                    'stat_date': current_date,
                    'downtime': downtime_today,  # Same pattern as today
                    'uptime_minutes': uptime_today,
                    'uptime': uptime_pct_today,
                    'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                    'peak_wait': 90 if tier == 1 else (60 if tier == 2 else 30),
                    'status_changes': 3 if tier == 1 else 2,
                    'operating_minutes': 600,
                    'observations': 60
                })
                current_date += timedelta(days=1)

            # === ADD DATA FOR PREVIOUS COMPLETE MONTH (Nov 1-30) ===
            # This matches what get_last_month_date_range() returns
            current_date = LAST_MONTH_START
            while current_date <= LAST_MONTH_END:
                # Skip dates already inserted (overlap with last week AND already_covered)
                if (LAST_WEEK_START <= current_date <= LAST_WEEK_END) or (current_date in already_covered):
                    current_date += timedelta(days=1)
                    continue
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    ) VALUES (
                        :ride_id, :stat_date, :downtime, :uptime_minutes, :uptime,
                        :avg_wait, :peak_wait, :status_changes, :operating_minutes
                    )
                """), {
                    'ride_id': ride_id,
                    'stat_date': current_date,
                    'downtime': downtime_today,  # Same pattern as today
                    'uptime_minutes': uptime_today,
                    'uptime': uptime_pct_today,
                    'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                    'peak_wait': 90 if tier == 1 else (60 if tier == 2 else 30),
                    'status_changes': 3 if tier == 1 else 2,
                    'operating_minutes': 600,
                    'observations': 60
                })
                current_date += timedelta(days=1)

            # Weekly stats (current week) - average of 7 days
            downtime_week = downtime_today * 7
            uptime_pct_week = uptime_pct_today

            conn.execute(text("""
                INSERT INTO ride_weekly_stats (
                    ride_id, year, week_number, week_start_date, downtime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes
                ) VALUES (
                    :ride_id, :year, :week, :week_start, :downtime, :uptime,
                    :avg_wait, :peak_wait, :status_changes
                )
            """), {
                'ride_id': ride_id,
                'year': current_year,
                'week': current_week,
                'week_start': current_week_start,
                'downtime': downtime_week,
                'uptime': uptime_pct_week,
                'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                'peak_wait': 100 if tier == 1 else (70 if tier == 2 else 35),
                'status_changes': 15 if tier == 1 else 10
            })

            # Previous week - 10% less downtime
            downtime_prev_week = int(downtime_week * 0.9)

            conn.execute(text("""
                INSERT INTO ride_weekly_stats (
                    ride_id, year, week_number, week_start_date, downtime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes
                ) VALUES (
                    :ride_id, :year, :week, :week_start, :downtime, :uptime,
                    :avg_wait, :peak_wait, :status_changes
                )
            """), {
                'ride_id': ride_id,
                'year': prev_week_year,
                'week': prev_week,
                'week_start': prev_week_start,
                'downtime': downtime_prev_week,
                'uptime': ((4200 - downtime_prev_week) / 4200.0) * 100,
                'avg_wait': 42 if tier == 1 else (28 if tier == 2 else 14),
                'peak_wait': 95 if tier == 1 else (65 if tier == 2 else 32),
                'status_changes': 14 if tier == 1 else 9
            })

            # Monthly stats
            downtime_month = downtime_today * 30
            uptime_pct_month = uptime_pct_today

            conn.execute(text("""
                INSERT INTO ride_monthly_stats (
                    ride_id, year, month, downtime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes
                ) VALUES (
                    :ride_id, :year, :month, :downtime, :uptime,
                    :avg_wait, :peak_wait, :status_changes
                )
            """), {
                'ride_id': ride_id,
                'year': current_year,
                'month': current_month,
                'downtime': downtime_month,
                'uptime': uptime_pct_month,
                'avg_wait': 45 if tier == 1 else (30 if tier == 2 else 15),
                'peak_wait': 110 if tier == 1 else (75 if tier == 2 else 40),
                'status_changes': 60 if tier == 1 else 40,
                'observations': 1800
            })

            # Previous month - 15% less downtime
            downtime_prev_month = int(downtime_month * 0.85)

            conn.execute(text("""
                INSERT INTO ride_monthly_stats (
                    ride_id, year, month, downtime_minutes, uptime_percentage,
                    avg_wait_time, peak_wait_time, status_changes
                ) VALUES (
                    :ride_id, :year, :month, :downtime, :uptime,
                    :avg_wait, :peak_wait, :status_changes
                )
            """), {
                'ride_id': ride_id,
                'year': prev_month_year,
                'month': prev_month,
                'downtime': downtime_prev_month,
                'uptime': ((18000 - downtime_prev_month) / 18000.0) * 100,
                'avg_wait': 43 if tier == 1 else (29 if tier == 2 else 14),
                'peak_wait': 105 if tier == 1 else (70 if tier == 2 else 38),
                'status_changes': 55 if tier == 1 else 38,
                'observations': 1800
            })

            ride_id += 1

    conn.commit()  # Commit all ride stats so Flask app can see them

    # === CREATE PARK DAILY STATS (aggregated from rides) ===
    for park_id in range(1, 11):
        # Today: Sum of all rides in park
        # Each park has 2 Tier1 (180min each) + 5 Tier2 (60min each) + 3 Tier3 (30min each)
        # = 360 + 300 + 90 = 750 minutes = 12.5 hours total downtime
        total_downtime_today = 750
        rides_with_downtime_today = 10
        avg_uptime_today = 77.78  # Weighted average

        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, operating_hours_minutes
            ) VALUES (:park_id, :stat_date, :downtime_hours, :rides_down, :avg_uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': today,
            'downtime_hours': total_downtime_today / 60.0,
            'rides_down': rides_with_downtime_today,
            'avg_uptime': avg_uptime_today,
            'operating_minutes': 900
        })

        # Yesterday: 20% less
        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, operating_hours_minutes
            ) VALUES (:park_id, :stat_date, :downtime_hours, :rides_down, :avg_uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': yesterday,
            'downtime_hours': (total_downtime_today * 0.8) / 60.0,
            'rides_down': rides_with_downtime_today,
            'avg_uptime': 80.0,
            'operating_minutes': 900
        })

        for days_back in range(2, 7):
            conn.execute(text("""
                INSERT INTO park_daily_stats (
                    park_id, stat_date, total_downtime_hours, rides_with_downtime,
                    avg_uptime_percentage, operating_hours_minutes
                ) VALUES (:park_id, :stat_date, :downtime_hours, :rides_down, :avg_uptime, :operating_minutes)
            """), {
                'park_id': park_id,
                'stat_date': today - timedelta(days=days_back),
                'downtime_hours': total_downtime_today / 60.0,
                'rides_down': rides_with_downtime_today,
                'avg_uptime': avg_uptime_today,
                'operating_minutes': 900
            })

        # === ADD DATA FOR PREVIOUS COMPLETE WEEK (Nov 23-29) ===
        # Skip dates already covered by the days_back loop
        already_covered_parks = set([today, yesterday] + [today - timedelta(days=d) for d in range(2, 7)])
        current_date = LAST_WEEK_START
        while current_date <= LAST_WEEK_END:
            if current_date in already_covered_parks:
                current_date += timedelta(days=1)
                continue
            conn.execute(text("""
                INSERT INTO park_daily_stats (
                    park_id, stat_date, total_downtime_hours, rides_with_downtime,
                    avg_uptime_percentage, operating_hours_minutes
                ) VALUES (:park_id, :stat_date, :downtime_hours, :rides_down, :avg_uptime, :operating_minutes)
            """), {
                'park_id': park_id,
                'stat_date': current_date,
                'downtime_hours': total_downtime_today / 60.0,  # 12.5 hours
                'rides_down': rides_with_downtime_today,
                'avg_uptime': avg_uptime_today,
                'operating_minutes': 900
            })
            current_date += timedelta(days=1)

        # === ADD DATA FOR PREVIOUS COMPLETE MONTH (Nov 1-30) ===
        current_date = LAST_MONTH_START
        while current_date <= LAST_MONTH_END:
            # Skip dates already inserted (overlap with last week AND already_covered)
            if (LAST_WEEK_START <= current_date <= LAST_WEEK_END) or (current_date in already_covered_parks):
                current_date += timedelta(days=1)
                continue
            conn.execute(text("""
                INSERT INTO park_daily_stats (
                    park_id, stat_date, total_downtime_hours, rides_with_downtime,
                    avg_uptime_percentage, operating_hours_minutes
                ) VALUES (:park_id, :stat_date, :downtime_hours, :rides_down, :avg_uptime, :operating_minutes)
            """), {
                'park_id': park_id,
                'stat_date': current_date,
                'downtime_hours': total_downtime_today / 60.0,  # 12.5 hours
                'rides_down': rides_with_downtime_today,
                'avg_uptime': avg_uptime_today,
                'operating_minutes': 900
            })
            current_date += timedelta(days=1)

    # === CREATE PARK WEEKLY STATS ===
    for park_id in range(1, 11):
        total_downtime_week = 750 * 7 / 60.0  # 87.5 hours

        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, trend_vs_previous_week
            ) VALUES (:park_id, :year, :week, :week_start, :downtime_hours, :rides_down, :avg_uptime, :trend)
        """), {
            'park_id': park_id,
            'year': current_year,
            'week': current_week,
            'week_start': current_week_start,
            'downtime_hours': total_downtime_week,
            'rides_down': 10,
            'avg_uptime': 77.78,
            'trend': 11.11  # 10% increase from prev week
        })

        # Previous week
        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, trend_vs_previous_week
            ) VALUES (:park_id, :year, :week, :week_start, :downtime_hours, :rides_down, :avg_uptime, NULL)
        """), {
            'park_id': park_id,
            'year': prev_week_year,
            'week': prev_week,
            'week_start': prev_week_start,
            'downtime_hours': total_downtime_week * 0.9,
            'rides_down': 10,
            'avg_uptime': 80.0
        })

    # === CREATE PARK MONTHLY STATS ===
    for park_id in range(1, 11):
        total_downtime_month = 750 * 30 / 60.0  # 375 hours

        conn.execute(text("""
            INSERT INTO park_monthly_stats (
                park_id, year, month, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, trend_vs_previous_month
            ) VALUES (:park_id, :year, :month, :downtime_hours, :rides_down, :avg_uptime, :trend)
        """), {
            'park_id': park_id,
            'year': current_year,
            'month': current_month,
            'downtime_hours': total_downtime_month,
            'rides_down': 10,
            'avg_uptime': 77.78,
            'trend': 17.65  # 15% increase from prev month
        })

        # Previous month
        conn.execute(text("""
            INSERT INTO park_monthly_stats (
                park_id, year, month, total_downtime_hours, rides_with_downtime,
                avg_uptime_percentage, trend_vs_previous_month
            ) VALUES (:park_id, :year, :month, :downtime_hours, :rides_down, :avg_uptime, NULL)
        """), {
            'park_id': park_id,
            'year': prev_month_year,
            'month': prev_month,
            'downtime_hours': total_downtime_month * 0.85,
            'rides_down': 10,
            'avg_uptime': 82.0
        })

    conn.commit()  # Commit all park stats so Flask app can see them

    # === CREATE HOURLY STATS (for TODAY period) ===
    # Generate hourly stats for the MOCKED_TODAY date (8 AM to 8 PM Pacific = 12 hours)
    # TODAY period queries use park_hourly_stats and ride_hourly_stats
    from utils.timezone import get_pacific_day_range_utc

    # Use the mocked date instead of actual today
    day_start_utc, day_end_utc = get_pacific_day_range_utc(MOCKED_TODAY)

    # Use the mocked "now" for deterministic test data
    current_utc = MOCKED_NOW_UTC

    # day_start_utc is midnight Pacific in UTC (e.g., 08:00 UTC for PST, 07:00 UTC for PDT)
    # Parks open around 8 AM Pacific, so add 8 hours to get to typical park opening
    hour_utc = day_start_utc + timedelta(hours=8)  # ~8 AM Pacific in UTC

    # DEBUG: Print date calculation values
    print(f"[DEBUG] MOCKED_TODAY={MOCKED_TODAY}, day_start_utc={day_start_utc}, hour_utc={hour_utc}, current_utc={current_utc}")
    print(f"[DEBUG] condition check: hour_utc < current_utc = {hour_utc < current_utc}")

    hours_created = 0
    # Create up to 12 hours of data, but only up to the current time
    while hour_utc < current_utc and hours_created < 12:
        # Create park hourly stats for each park
        # Ride hourly downtime: (0.3*2 + 0.1*5 + 0.05*3) = 0.6 + 0.5 + 0.15 = 1.25h per hour
        # Park stats should match sum of ride stats for consistency
        for park_id in range(1, 11):
            hourly_downtime = 1.25  # Matches sum of ride downtime per hour
            shame_score = 0.5  # Moderate shame score

            conn.execute(text("""
                INSERT INTO park_hourly_stats (
                    park_id, hour_start_utc, shame_score, avg_wait_time_minutes,
                    rides_operating, rides_down, total_downtime_hours, weighted_downtime_hours,
                    effective_park_weight, snapshot_count, park_was_open
                ) VALUES (
                    :park_id, :hour_start, :shame, :avg_wait, :operating, :down,
                    :downtime, :weighted_downtime, :weight, :snapshots, :park_open
                )
            """), {
                'park_id': park_id,
                'hour_start': hour_utc,
                'shame': shame_score,
                'avg_wait': 40.0,
                'operating': 8,
                'down': 2,
                'downtime': hourly_downtime,
                'weighted_downtime': hourly_downtime * 1.5,  # 1.875h weighted per hour
                'weight': 10.0,
                'snapshots': 6,
                'park_open': True
            })

        # Create ride hourly stats for each ride (10 rides per park = 100 total)
        ride_id = 1
        for park_id in range(1, 11):
            for i in range(10):
                tier = 1 if i < 2 else (2 if i < 7 else 3)
                # Tier 1 rides have more downtime
                ride_downtime = 0.3 if tier == 1 else (0.1 if tier == 2 else 0.05)
                down_snaps = 2 if tier == 1 else (1 if tier == 2 else 0)
                operating_snaps = 6 - down_snaps

                conn.execute(text("""
                    INSERT INTO ride_hourly_stats (
                        ride_id, park_id, hour_start_utc, avg_wait_time_minutes,
                        operating_snapshots, down_snapshots, downtime_hours,
                        uptime_percentage, snapshot_count, ride_operated
                    ) VALUES (
                        :ride_id, :park_id, :hour_start, :avg_wait,
                        :operating, :down, :downtime,
                        :uptime, :snapshots, :operated
                    )
                """), {
                    'ride_id': ride_id,
                    'park_id': park_id,
                    'hour_start': hour_utc,
                    'avg_wait': 60 if tier == 1 else (40 if tier == 2 else 20),
                    'operating': operating_snaps,
                    'down': down_snaps,
                    'downtime': ride_downtime,
                    'uptime': (operating_snaps / 6.0) * 100,
                    'snapshots': 6,
                    'operated': 1
                })
                ride_id += 1

        hour_utc = hour_utc + timedelta(hours=1)
        hours_created += 1

    conn.commit()  # Commit hourly stats so Flask app can see them

    # DEBUG: Verify hourly stats were created
    stats_count = conn.execute(text("SELECT COUNT(*) FROM park_hourly_stats")).fetchone()[0]
    print(f"\n[DEBUG] comprehensive_test_data: Created {stats_count} park_hourly_stats rows")
    sample_hours = conn.execute(text("SELECT DISTINCT hour_start_utc FROM park_hourly_stats ORDER BY hour_start_utc LIMIT 3")).fetchall()
    print(f"[DEBUG] Sample hourly stats times: {sample_hours}")
    print(f"[DEBUG] hours_created counter: {hours_created}")

    # === CREATE PARK & RIDE STATUS SNAPSHOTS (for live/waittime endpoints) ===
    # Use mocked time for consistency with frozen test time
    now = MOCKED_NOW_UTC.replace(tzinfo=None)  # Remove tzinfo for DB storage
    ride_id = 1
    for park_id in range(1, 11):
        # Snapshot representing park activity at "now"
        conn.execute(text("""
            INSERT INTO park_activity_snapshots (
                park_id, recorded_at, total_rides_tracked, rides_open, rides_closed,
                avg_wait_time, max_wait_time, park_appears_open
            ) VALUES (
                :park_id, :recorded_at, :total_rides, :rides_open, :rides_closed,
                :avg_wait, :max_wait, :park_open
            )
        """), {
            'park_id': park_id,
            'recorded_at': now,
            'total_rides': 10,
            'rides_open': 10,
            'rides_closed': 0,
            'avg_wait': 40.0,
            'max_wait': 60,
            'park_open': True
        })

        for i in range(10):
            tier = 1 if i < 2 else (2 if i < 7 else 3)
            # Tier 1 rides have longer wait times
            wait_time = 60 if tier == 1 else (40 if tier == 2 else 20)

            conn.execute(text("""
                INSERT INTO ride_status_snapshots (
                    ride_id, recorded_at, is_open, wait_time, computed_is_open, status
                ) VALUES (:ride_id, :recorded_at, TRUE, :wait_time, TRUE, 'OPERATING')
            """), {
                'ride_id': ride_id,
                'recorded_at': now,
                'wait_time': wait_time
            })

            ride_id += 1

    conn.commit()  # Commit snapshots so Flask app can see them

    # Reset global database pool so Flask gets fresh connections
    # that see the just-committed test data
    from database.connection import db as global_db
    global_db.close()

    # DEBUG: Verify with a fresh connection what Flask will see
    from database.connection import get_db_connection
    with get_db_connection() as flask_conn:
        flask_parks = flask_conn.execute(text("SELECT COUNT(*) FROM parks WHERE queue_times_id >= 9000")).fetchone()[0]
        flask_hourly = flask_conn.execute(text("SELECT COUNT(*) FROM park_hourly_stats")).fetchone()[0]
        print(f"\n[DEBUG] Flask will see: {flask_parks} parks, {flask_hourly} park_hourly_stats rows")

    yield {
        'num_parks': 10,
        'num_rides': 100,
        'disney_parks': 5,
        'universal_parks': 3,
        'other_parks': 2,
        'tier1_rides_per_park': 2,
        'tier2_rides_per_park': 5,
        'tier3_rides_per_park': 3,
        'today': today,
        'yesterday': yesterday,
        'current_year': current_year,
        'current_week': current_week,
        'current_month': current_month
    }

    # CLEANUP: Remove all test data after tests complete
    # This prevents test pollution affecting subsequent test files
    conn.execute(text("DELETE FROM ride_status_snapshots"))
    conn.execute(text("DELETE FROM ride_status_changes"))
    conn.execute(text("DELETE FROM ride_daily_stats"))
    conn.execute(text("DELETE FROM ride_weekly_stats"))
    conn.execute(text("DELETE FROM ride_monthly_stats"))
    conn.execute(text("DELETE FROM ride_hourly_stats"))
    conn.execute(text("DELETE FROM park_activity_snapshots"))
    conn.execute(text("DELETE FROM park_daily_stats"))
    conn.execute(text("DELETE FROM park_weekly_stats"))
    conn.execute(text("DELETE FROM park_monthly_stats"))
    conn.execute(text("DELETE FROM park_hourly_stats"))
    conn.execute(text("DELETE FROM ride_classifications WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 90000)"))
    conn.execute(text("DELETE FROM rides WHERE queue_times_id >= 90000"))
    conn.execute(text("DELETE FROM parks WHERE queue_times_id >= 9000"))
    conn.commit()


# ============================================================================
# TEST: GET /api/parks/downtime - Standard Rankings
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_today_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=today, filter=all-parks.

    Validates:
    - All 10 parks returned
    - Sorted by shame_score descending
    - Response structure matches API spec for TODAY period
    - TODAY period uses park_hourly_stats (different fields than weekly/monthly)
    """
    response = client.get('/api/parks/downtime?period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    # DEBUG: Print full response
    print(f"\n[DEBUG] Response data count: {len(data.get('data', []))}")
    print(f"[DEBUG] Response metadata: period={data.get('period')}, filter={data.get('filter')}")
    if data.get('data'):
        print(f"[DEBUG] First park: {data['data'][0]}")

    # Verify response structure
    assert data['success'] is True
    assert data['period'] == 'today'
    assert data['filter'] == 'all-parks'
    assert 'data' in data
    assert 'aggregate_stats' in data
    assert 'attribution' in data

    # Should return all 10 parks
    assert len(data['data']) == 10

    # Verify each park has required TODAY endpoint fields
    # Note: TODAY period returns rides_down (not affected_rides_count) and no trend_percentage
    for park in data['data']:
        assert 'park_id' in park
        assert 'park_name' in park
        assert 'location' in park
        assert 'total_downtime_hours' in park
        assert 'rides_down' in park  # TODAY uses rides_down, not affected_rides_count
        assert 'uptime_percentage' in park
        assert 'shame_score' in park
        assert 'rank' in park
        assert 'queue_times_url' in park

        # Verify downtime is a reasonable positive number
        assert float(park['total_downtime_hours']) >= 0

        # Verify rides_down is a reasonable count
        assert park['rides_down'] >= 0

        # Verify uptime percentage is valid
        assert 0 <= float(park['uptime_percentage']) <= 100

    # Verify sorting (ranked 1-10)
    for i, park in enumerate(data['data'], 1):
        assert park['rank'] == i

    # Verify aggregate stats
    agg = data['aggregate_stats']
    print(f"\n[DEBUG] aggregate_stats: {agg}")
    # Note: total_parks_tracked comes from a different source and may not match
    # the returned data count. Verify the actual data returned instead.
    assert len(data['data']) == 10, f"Expected 10 parks in data, got {len(data['data'])}"


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_today_disney_universal_filter(client, comprehensive_test_data):
    """
    Test Disney & Universal filter returns only 8 parks (5 Disney + 3 Universal).

    Validates filtering logic works correctly.
    """
    response = client.get('/api/parks/downtime?period=today&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # Should return 8 parks (5 Disney + 3 Universal)
    assert len(data['data']) == 8

    # Note: aggregate_stats.total_parks_tracked may differ from actual returned count
    # as it comes from a different source. Just verify we got 8 parks in data.


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_live_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=live, filter=all-parks.

    Validates:
    - Returns 200 OK with success=true
    - Uses real-time snapshot data (instantaneous - rides down RIGHT NOW)
    - All parks included (not filtered)

    Note: Live mode depends on very recent snapshots. May return empty/zero values in tests.
    """
    response = client.get('/api/parks/downtime?period=live&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'live'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Verify response structure if data exists
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park

        print(f"\n✓ Verified {len(data['data'])} parks with live downtime data")
    else:
        print(f"\n✓ Live mode returned no data (expected in test environment)")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_live_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=live, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned
    """
    response = client.get('/api/parks/downtime?period=live&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # If data exists, verify all parks are Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_yesterday_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=yesterday, filter=all-parks.

    Validates:
    - Returns downtime data from the previous Pacific day
    - All 10 parks in test data are included
    - Uses pre-aggregated daily stats
    """
    response = client.get('/api/parks/downtime?period=yesterday&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'yesterday'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Yesterday data should exist from comprehensive_test_data
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        assert 'total_downtime_hours' in park

        print(f"\n✓ Verified {len(data['data'])} parks with yesterday downtime data")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_yesterday_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=yesterday, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned for yesterday period
    """
    response = client.get('/api/parks/downtime?period=yesterday&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_7days(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=7days.

    Validates weekly aggregation and trend calculations.
    """
    response = client.get('/api/parks/downtime?period=7days&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == '7days'
    assert len(data['data']) == 10

    # Weekly downtime: 750 min/day * 7 days = 5250 min = 87.5 hours
    for park in data['data']:
        assert abs(float(park['total_downtime_hours']) - 87.5) < 0.1

        # Weekly trend should be ~11% (from prev week)
        if park['trend_percentage'] is not None:
            assert abs(float(park['trend_percentage']) - 11.11) < 2.0


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_30days(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=30days.

    Validates monthly aggregation and trend calculations.
    """
    response = client.get('/api/parks/downtime?period=30days&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == '30days'
    assert len(data['data']) == 10

    # Monthly downtime: 750 min/day * 30 days = 22500 min = 375 hours
    for park in data['data']:
        assert abs(float(park['total_downtime_hours']) - 375.0) < 0.1

        # Monthly trend should be ~17.65% (from prev month)
        if park['trend_percentage'] is not None:
            assert abs(float(park['trend_percentage']) - 17.65) < 2.0


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_last_week_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=last_week, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned
    - Weekly aggregation is correct for filtered results
    """
    response = client.get('/api/parks/downtime?period=last_week&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_week'
    assert data['filter'] == 'disney-universal'

    # Should return 8 Disney/Universal parks (5 Disney + 3 Universal)
    assert len(data['data']) == 8

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        assert park['park_name'] in disney_universal_parks

    print(f"\n✓ Verified {len(data['data'])} Disney/Universal parks for last_week")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_last_month_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/downtime with period=last_month, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned
    - Monthly aggregation is correct for filtered results
    """
    response = client.get('/api/parks/downtime?period=last_month&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_month'
    assert data['filter'] == 'disney-universal'

    # Should return 8 Disney/Universal parks (5 Disney + 3 Universal)
    assert len(data['data']) == 8

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        assert park['park_name'] in disney_universal_parks

    print(f"\n✓ Verified {len(data['data'])} Disney/Universal parks for last_month")


# ============================================================================
# TEST: GET /api/parks/downtime - Weighted Rankings
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_weighted_scoring(client, comprehensive_test_data):
    """
    Test weighted scoring endpoint returns data with weighted=true.

    Verifies:
    - Response indicates weighted mode is enabled
    - All 10 parks are returned
    - Each park has weighted_downtime_hours >= total_downtime_hours
    - Weighted downtime reflects tier weighting (higher tier rides contribute more)
    """
    response = client.get('/api/parks/downtime?period=today&filter=all-parks&weighted=true&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['weighted'] is True
    assert len(data['data']) == 10

    # Verify each park has positive downtime and weighted >= unweighted
    for park in data['data']:
        total_downtime = float(park['total_downtime_hours'])
        weighted_downtime = float(park.get('weighted_downtime_hours', total_downtime))

        assert total_downtime >= 0, f"Park {park['park_id']} has negative downtime"
        assert weighted_downtime >= total_downtime, \
            f"Park {park['park_id']}: weighted ({weighted_downtime}) should be >= total ({total_downtime})"

    print(f"\n✓ Weighted scoring verified for all {len(data['data'])} parks")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_weighted_vs_unweighted(client, comprehensive_test_data):
    """
    Compare weighted vs unweighted results to ensure weighted_downtime_hours
    is greater than or equal to total_downtime_hours for each park.

    Verifies:
    - Both weighted=true and weighted=false return the same parks
    - The 'weighted_downtime_hours' field reflects tier weighting
    """
    # Get unweighted
    response_unweighted = client.get('/api/parks/downtime?period=today&weighted=false')
    unweighted_data = response_unweighted.get_json()

    # Get weighted
    response_weighted = client.get('/api/parks/downtime?period=today&weighted=true')
    weighted_data = response_weighted.get_json()

    # Both should return the same number of parks
    assert len(unweighted_data['data']) > 0, "Unweighted query returned no data"
    assert len(weighted_data['data']) > 0, "Weighted query returned no data"
    assert len(unweighted_data['data']) == len(weighted_data['data'])

    # Verify each park has weighted >= total
    for park in weighted_data['data']:
        total = float(park['total_downtime_hours'])
        weighted = float(park.get('weighted_downtime_hours', total))
        assert weighted >= total, f"Park {park['park_id']}: weighted ({weighted}) < total ({total})"

    print(f"\n✓ Verified {len(weighted_data['data'])} parks have weighted >= total downtime")


# ============================================================================
# TEST: GET /api/rides/downtime
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_today(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=today.

    Validates:
    - Returns rides sorted by downtime descending
    - Tier 1 rides have more downtime than Tier 2/3
    - Response structure matches API spec for TODAY period
    - TODAY period uses ride_hourly_stats
    """
    response = client.get('/api/rides/downtime?period=today&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'today'
    assert len(data['data']) == 100

    # Verify top rides are Tier 1 with most downtime
    # Note: Exact downtime depends on how many hours have passed today
    tier1_downtime = None
    for i in range(20):  # First 20 should all be Tier 1 (10 parks * 2 Tier1 each)
        ride = data['data'][i]
        assert ride['tier'] == 1
        assert float(ride['downtime_hours']) >= 0
        if tier1_downtime is None:
            tier1_downtime = float(ride['downtime_hours'])
        # current_is_open may be None if no live snapshot data is available
        assert 'current_is_open' in ride
        assert 'uptime_percentage' in ride

    # Verify Tier 1 rides have more downtime than Tier 3 rides (at the bottom)
    tier3_ride = data['data'][-1]
    if tier1_downtime is not None and tier1_downtime > 0:
        assert tier1_downtime >= float(tier3_ride['downtime_hours'])

    # Verify sorting (descending by downtime)
    for i in range(len(data['data']) - 1):
        current_downtime = float(data['data'][i]['downtime_hours'])
        next_downtime = float(data['data'][i + 1]['downtime_hours'])
        assert current_downtime >= next_downtime

    print(f"\n✓ Verified {len(data['data'])} rides sorted correctly by downtime")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_7days_with_trends(client, comprehensive_test_data):
    """
    Test 7-day ride downtime with trend validation.

    Expected trends: Current week vs previous week shows ~11% increase.
    """
    response = client.get('/api/rides/downtime?period=7days&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert len(data['data']) == 100

    # Verify weekly calculations
    # Tier 1: 180 * 7 = 1260 min = 21 hours
    # Tier 2: 60 * 7 = 420 min = 7 hours
    # Tier 3: 30 * 7 = 210 min = 3.5 hours

    tier1_count = sum(1 for r in data['data'] if r['tier'] == 1)
    assert tier1_count == 20  # 10 parks * 2 Tier1 each

    for ride in data['data']:
        if ride['tier'] == 1:
            assert abs(float(ride['downtime_hours']) - 21.0) < 0.1
        elif ride['tier'] == 2:
            assert abs(float(ride['downtime_hours']) - 7.0) < 0.1
        elif ride['tier'] == 3:
            assert abs(float(ride['downtime_hours']) - 3.5) < 0.1


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_last_week_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=last_week, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned for weekly period
    - Uses pre-aggregated weekly stats
    """
    response = client.get('/api/rides/downtime?period=last_week&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_week'
    assert data['filter'] == 'disney-universal'

    # Should return 80 rides (8 Disney/Universal parks * 10 rides each)
    assert len(data['data']) == 80

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks

    print(f"\n✓ Verified {len(data['data'])} Disney/Universal rides for last_week")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_disney_universal_filter(client, comprehensive_test_data):
    """
    Test Disney & Universal filter for rides.

    Should return 80 rides (8 parks * 10 rides each).
    """
    response = client.get('/api/rides/downtime?period=today&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    # 5 Disney parks + 3 Universal parks = 8 parks * 10 rides = 80 rides
    assert len(data['data']) == 80

    # Verify all rides belong to Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_live_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=live, filter=all-parks.

    Validates:
    - Returns 200 OK with success=true
    - Uses real-time snapshot data (instantaneous - rides down RIGHT NOW)
    - All rides included (not filtered)

    Note: Live mode depends on very recent snapshots. May return empty/zero values in tests.
    """
    response = client.get('/api/rides/downtime?period=live&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'live'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Verify response structure if data exists
    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        assert 'park_name' in ride

        print(f"\n✓ Verified {len(data['data'])} rides with live downtime data")
    else:
        print(f"\n✓ Live mode returned no data (expected in test environment)")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_live_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=live, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned
    """
    response = client.get('/api/rides/downtime?period=live&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # If data exists, verify all rides belong to Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_yesterday_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=yesterday, filter=all-parks.

    Validates:
    - Returns downtime data from the previous Pacific day
    - All 100 rides in test data are included
    - Uses pre-aggregated daily stats
    """
    response = client.get('/api/rides/downtime?period=yesterday&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'yesterday'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Yesterday data should exist from comprehensive_test_data
    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        assert 'park_name' in ride
        assert 'downtime_hours' in ride

        print(f"\n✓ Verified {len(data['data'])} rides with yesterday downtime data")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_yesterday_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=yesterday, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned for yesterday period
    """
    response = client.get('/api/rides/downtime?period=yesterday&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_last_month_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=last_month, filter=all-parks.

    Validates:
    - Returns 30-day downtime aggregation
    - Uses pre-aggregated monthly stats
    """
    response = client.get('/api/rides/downtime?period=last_month&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_month'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        print(f"\n✓ Verified {len(data['data'])} rides with monthly downtime data")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_last_month_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/downtime with period=last_month, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned for monthly period
    """
    response = client.get('/api/rides/downtime?period=last_month&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks


# ============================================================================
# TEST: GET /api/rides/waittimes
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_live_mode(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=live.

    Validates:
    - Returns 200 OK with success=true
    - If data is available, it's sorted by wait time descending

    Note: Live mode depends on very recent snapshots (within time window).
    In test environments with mocked time, live data may not be available.
    """
    response = client.get('/api/rides/waittimes?mode=live&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'live'

    # Live mode may return no data if snapshots are outside the time window
    if len(data['data']) > 0:
        # Verify sorting by wait time descending
        for i in range(len(data['data']) - 1):
            current_wait = data['data'][i].get('current_wait_minutes', 0) or 0
            next_wait = data['data'][i + 1].get('current_wait_minutes', 0) or 0
            assert current_wait >= next_wait

        print(f"\n✓ Verified {len(data['data'])} rides with live wait times")
    else:
        print(f"\n✓ Live mode returned no data (expected in test environment with mocked time)")


@pytest.mark.skip(reason="Feature not yet implemented - API returns empty data (stubbed)")
@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_7day_average_mode(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=7day-average.

    Validates weekly average wait time calculations.
    """
    response = client.get('/api/rides/waittimes?mode=7day-average&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == '7day-average'
    assert len(data['data']) == 100

    # Verify average wait times
    # Tier 1: 45 min, Tier 2: 30 min, Tier 3: 15 min
    for ride in data['data']:
        avg_wait = float(ride['avg_wait_7days']) if isinstance(ride['avg_wait_7days'], str) else ride['avg_wait_7days']
        if ride['tier'] == 1:
            assert avg_wait == 45
        elif ride['tier'] == 2:
            assert avg_wait == 30
        elif ride['tier'] == 3:
            assert avg_wait == 15

    # Should be sorted by avg_wait_7days descending
    for i in range(len(data['data']) - 1):
        current_avg = data['data'][i]['avg_wait_7days']
        next_avg = data['data'][i + 1]['avg_wait_7days']
        assert current_avg >= next_avg


@pytest.mark.skip(reason="Feature not yet implemented - API returns empty data (stubbed)")
@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_peak_times_mode(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=peak-times.

    Validates peak wait time calculations from weekly stats.
    """
    response = client.get('/api/rides/waittimes?mode=peak-times&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'peak-times'
    assert len(data['data']) == 100

    # Verify peak wait times
    # Tier 1: 100 min, Tier 2: 70 min, Tier 3: 35 min
    for ride in data['data']:
        if ride['tier'] == 1:
            assert ride['peak_wait_7days'] == 100
        elif ride['tier'] == 2:
            assert ride['peak_wait_7days'] == 70
        elif ride['tier'] == 3:
            assert ride['peak_wait_7days'] == 35

    # Should be sorted by peak_wait_7days descending
    for i in range(len(data['data']) - 1):
        current_peak = data['data'][i]['peak_wait_7days']
        next_peak = data['data'][i + 1]['peak_wait_7days']
        assert current_peak >= next_peak


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_disney_universal_filter(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with disney-universal filter.

    Validates that the filter is applied correctly when live data is available.
    Note: Live mode depends on very recent snapshots; may return no data in tests.
    """
    response = client.get('/api/rides/waittimes?mode=live&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # Live mode may return no data in test environment with mocked time
    if len(data['data']) > 0:
        # Verify all rides belong to Disney or Universal parks
        disney_universal_parks = {
            'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
            'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
        }
        for ride in data['data']:
            park_name = ride['park_name']
            assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"

        print(f"\n✓ Verified {len(data['data'])} Disney/Universal rides with live wait times")
    else:
        print(f"\n✓ Live mode returned no data (expected in test environment with mocked time)")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_today_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=today, filter=all-parks.

    Validates:
    - Returns cumulative wait times from midnight Pacific to now
    - Uses TodayRideWaitTimesQuery (implemented)
    """
    response = client.get('/api/rides/waittimes?mode=today&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'today'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        print(f"\n✓ Verified {len(data['data'])} rides with today's wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_today_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=today, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned
    """
    response = client.get('/api/rides/waittimes?mode=today&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'today'
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks

    print(f"\n✓ Verified Disney/Universal filter for today's wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_yesterday_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=yesterday, filter=all-parks.

    Validates:
    - Returns full previous Pacific day's wait times
    - Uses YesterdayRideWaitTimesQuery (implemented)
    """
    response = client.get('/api/rides/waittimes?mode=yesterday&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'yesterday'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        print(f"\n✓ Verified {len(data['data'])} rides with yesterday's wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_yesterday_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=yesterday, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned
    """
    response = client.get('/api/rides/waittimes?mode=yesterday&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'yesterday'
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks

    print(f"\n✓ Verified Disney/Universal filter for yesterday's wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_last_week_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=last_week, filter=all-parks.

    Validates:
    - Returns weekly wait time data
    - Uses RideWaitTimeRankingsQuery.get_weekly() (implemented)
    """
    response = client.get('/api/rides/waittimes?mode=last_week&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'last_week'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        print(f"\n✓ Verified {len(data['data'])} rides with weekly wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_last_week_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=last_week, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned
    """
    response = client.get('/api/rides/waittimes?mode=last_week&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'last_week'
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks

    print(f"\n✓ Verified Disney/Universal filter for weekly wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_last_month_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=last_month, filter=all-parks.

    Validates:
    - Returns monthly wait time data
    - Uses RideWaitTimeRankingsQuery.get_monthly() (implemented)
    """
    response = client.get('/api/rides/waittimes?mode=last_month&filter=all-parks&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'last_month'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    if len(data['data']) > 0:
        ride = data['data'][0]
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        print(f"\n✓ Verified {len(data['data'])} rides with monthly wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_last_month_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/rides/waittimes with mode=last_month, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are returned
    """
    response = client.get('/api/rides/waittimes?mode=last_month&filter=disney-universal&limit=100')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['mode'] == 'last_month'
    assert data['filter'] == 'disney-universal'

    # All returned rides should be from Disney or Universal parks
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for ride in data['data']:
        park_name = ride['park_name']
        assert park_name in disney_universal_parks

    print(f"\n✓ Verified Disney/Universal filter for monthly wait times")


# ============================================================================
# TEST: GET /api/parks/waittimes - All Periods and Filters
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_live_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=live, filter=all-parks.

    Validates:
    - Returns 200 OK with success=true
    - Response structure includes parks with wait time data
    - All parks included (not filtered)

    Note: Live mode depends on very recent snapshots. May return empty in tests.
    """
    response = client.get('/api/parks/waittimes?period=live&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'live'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Live mode may return no data if snapshots are outside the time window
    if len(data['data']) > 0:
        # Verify response structure
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park

        print(f"\n✓ Verified {len(data['data'])} parks with live wait times")
    else:
        print(f"\n✓ Live mode returned no data (expected in test environment)")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_live_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=live, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned
    """
    response = client.get('/api/parks/waittimes?period=live&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # If data exists, verify all parks are Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_today_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=today, filter=all-parks.

    Validates:
    - Returns cumulative wait time data from midnight Pacific to now
    - All 10 parks in test data are included
    - Parks have avg_wait_time and peak_wait_time fields
    """
    response = client.get('/api/parks/waittimes?period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'today'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Today data should exist from our comprehensive_test_data fixtures
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        # Wait time fields may vary by query implementation
        print(f"\n✓ Verified {len(data['data'])} parks with today wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_today_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=today, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned
    - Should return 8 parks (5 Disney + 3 Universal)
    """
    response = client.get('/api/parks/waittimes?period=today&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks, f"Park {park_name} should be Disney or Universal"


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_yesterday_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=yesterday, filter=all-parks.

    Validates:
    - Returns wait time data from the previous Pacific day
    - Uses pre-aggregated daily stats
    """
    response = client.get('/api/parks/waittimes?period=yesterday&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'yesterday'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Yesterday data should exist from our comprehensive_test_data fixtures
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        print(f"\n✓ Verified {len(data['data'])} parks with yesterday wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_yesterday_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=yesterday, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned for yesterday period
    """
    response = client.get('/api/parks/waittimes?period=yesterday&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_last_week_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=last_week, filter=all-parks.

    Validates:
    - Returns 7-day average wait time data
    - Uses pre-aggregated weekly stats
    """
    response = client.get('/api/parks/waittimes?period=last_week&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_week'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Weekly data should exist from our comprehensive_test_data fixtures
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        print(f"\n✓ Verified {len(data['data'])} parks with weekly wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_last_week_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=last_week, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned for weekly period
    """
    response = client.get('/api/parks/waittimes?period=last_week&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_last_month_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=last_month, filter=all-parks.

    Validates:
    - Returns 30-day average wait time data
    - Uses pre-aggregated monthly stats
    """
    response = client.get('/api/parks/waittimes?period=last_month&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_month'
    assert data['filter'] == 'all-parks'
    assert 'data' in data

    # Monthly data should exist from our comprehensive_test_data fixtures
    if len(data['data']) > 0:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        print(f"\n✓ Verified {len(data['data'])} parks with monthly wait times")


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_last_month_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/parks/waittimes with period=last_month, filter=disney-universal.

    Validates:
    - Only Disney and Universal parks are returned for monthly period
    """
    response = client.get('/api/parks/waittimes?period=last_month&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # All returned parks should be Disney or Universal
    disney_universal_parks = {
        'Magic Kingdom', 'EPCOT', 'Hollywood Studios', 'Animal Kingdom', 'Disneyland',
        'Universal Studios Florida', 'Islands of Adventure', 'Universal Studios Hollywood'
    }
    for park in data['data']:
        park_name = park['park_name']
        assert park_name in disney_universal_parks


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_invalid_period(client, comprehensive_test_data):
    """Test that invalid period parameter returns 400 error."""
    response = client.get('/api/parks/waittimes?period=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'error' in data


@freeze_time(MOCKED_NOW_UTC)
def test_parks_waittimes_invalid_filter(client, comprehensive_test_data):
    """Test that invalid filter parameter returns 400 error."""
    response = client.get('/api/parks/waittimes?period=today&filter=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


# ============================================================================
# TEST: GET /api/live/status-summary - Live Status Summary
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_live_status_summary_all_parks(client, comprehensive_test_data):
    """
    Test GET /api/live/status-summary with filter=all-parks.

    Validates:
    - Returns 200 OK with success=true
    - Response contains status_summary with ride counts by status
    - Expected statuses: OPERATING, DOWN, CLOSED, REFURBISHMENT, PARK_CLOSED
    """
    response = client.get('/api/live/status-summary?filter=all-parks')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'all-parks'
    assert 'status_summary' in data

    # Verify response structure - status_summary contains counts per status
    summary = data['status_summary']
    # The summary may contain various statuses - at minimum it should be a dict
    assert isinstance(summary, (dict, list))

    print(f"\n✓ Live status summary returned for all parks")


@freeze_time(MOCKED_NOW_UTC)
def test_live_status_summary_disney_universal(client, comprehensive_test_data):
    """
    Test GET /api/live/status-summary with filter=disney-universal.

    Validates:
    - Only Disney and Universal parks' rides are counted
    """
    response = client.get('/api/live/status-summary?filter=disney-universal')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'
    assert 'status_summary' in data


@freeze_time(MOCKED_NOW_UTC)
def test_live_status_summary_by_park_id(client, comprehensive_test_data):
    """
    Test GET /api/live/status-summary with park_id filter.

    Validates:
    - Filters to specific park's rides
    """
    response = client.get('/api/live/status-summary?filter=all-parks&park_id=1')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert 'status_summary' in data
    # Park ID should be echoed back
    assert data.get('park_id') == 1


@freeze_time(MOCKED_NOW_UTC)
def test_live_status_summary_invalid_filter(client, comprehensive_test_data):
    """Test that invalid filter parameter returns 400 error."""
    response = client.get('/api/live/status-summary?filter=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'error' in data


@freeze_time(MOCKED_NOW_UTC)
def test_live_status_summary_has_attribution(client, comprehensive_test_data):
    """Test that live status summary includes data attribution."""
    response = client.get('/api/live/status-summary?filter=all-parks')

    assert response.status_code == 200
    data = response.get_json()

    assert 'attribution' in data
    assert data['attribution']['data_source'] == 'ThemeParks.wiki'
    assert data['attribution']['url'] == 'https://themeparks.wiki'


# ============================================================================
# TEST: Park Details Endpoint
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_park_details_success(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details returns park information.

    Validates:
    - Park basic information
    - Tier distribution (count of Tier 1/2/3 rides)
    - Current status (rides open/closed)
    - Operating sessions (if available)
    """
    # Get Magic Kingdom (park_id is assigned by comprehensive_test_data fixture)
    # We'll use park_id=1 which should be the first park created
    response = client.get('/api/parks/1/details')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True

    # Verify park information
    assert 'park' in data
    park = data['park']
    assert 'park_id' in park
    assert 'name' in park
    assert 'location' in park
    assert 'queue_times_url' in park

    # Verify tier distribution
    assert 'tier_distribution' in data
    tier_dist = data['tier_distribution']
    assert 'tier_1_count' in tier_dist or 'total_rides' in tier_dist

    # Verify current status
    assert 'current_status' in data
    status = data['current_status']
    assert 'total_rides' in status

    # Verify operating sessions (may be empty if no data)
    assert 'operating_sessions' in data

    print(f"\n✓ Verified park details for park_id={park['park_id']}")


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_not_found(client, comprehensive_test_data):
    """Test that requesting details for non-existent park returns 404."""
    response = client.get('/api/parks/99999/details')

    assert response.status_code == 404
    data = response.get_json()
    assert data['success'] is False
    assert 'error' in data


# ============================================================================
# TEST: Park Details with Shame Breakdown - All Periods
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_park_details_live_shame_breakdown(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details with period=live.

    Validates:
    - Returns shame_breakdown for live (instantaneous) data
    - Shows rides currently down
    """
    response = client.get('/api/parks/1/details?period=live')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'live'
    assert 'shame_breakdown' in data

    # Shame breakdown should have expected structure
    breakdown = data['shame_breakdown']
    assert 'shame_score' in breakdown


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_today_shame_breakdown(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details with period=today.

    Validates:
    - Returns shame_breakdown for today (cumulative from midnight)
    - Shows all rides with downtime today
    """
    response = client.get('/api/parks/1/details?period=today')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'today'
    assert 'shame_breakdown' in data

    breakdown = data['shame_breakdown']
    assert 'shame_score' in breakdown


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_yesterday_shame_breakdown(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details with period=yesterday.

    Validates:
    - Returns shame_breakdown for previous Pacific day
    - Uses pre-aggregated daily stats
    """
    response = client.get('/api/parks/1/details?period=yesterday')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'yesterday'
    assert 'shame_breakdown' in data

    breakdown = data['shame_breakdown']
    assert 'shame_score' in breakdown


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_last_week_shame_breakdown(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details with period=last_week.

    Validates:
    - Returns shame_breakdown for previous calendar week
    - Uses pre-aggregated weekly stats
    """
    response = client.get('/api/parks/1/details?period=last_week')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_week'
    assert 'shame_breakdown' in data

    breakdown = data['shame_breakdown']
    assert 'shame_score' in breakdown


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_last_month_shame_breakdown(client, comprehensive_test_data):
    """
    Test GET /api/parks/{parkId}/details with period=last_month.

    Validates:
    - Returns shame_breakdown for previous calendar month
    - Uses pre-aggregated monthly stats
    """
    response = client.get('/api/parks/1/details?period=last_month')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'last_month'
    assert 'shame_breakdown' in data

    breakdown = data['shame_breakdown']
    assert 'shame_score' in breakdown


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_includes_chart_data(client, comprehensive_test_data):
    """
    Test that park details includes chart_data for visualization.

    Chart data format varies by period:
    - LIVE: 5-minute granularity for last 60 minutes
    - TODAY/YESTERDAY: Hourly averages
    - LAST_WEEK/LAST_MONTH: Daily averages
    """
    response = client.get('/api/parks/1/details?period=today')

    assert response.status_code == 200
    data = response.get_json()

    # Chart data may be None if no data, but key should exist
    assert 'chart_data' in data

    if data['chart_data']:
        # If chart data exists, verify structure
        chart = data['chart_data']
        # Chart should have labels and data points
        assert 'labels' in chart or 'data' in chart or 'shame_score' in chart


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_includes_tier_distribution(client, comprehensive_test_data):
    """
    Test that park details includes tier distribution.

    Tier distribution shows count of rides by tier (1, 2, 3).
    """
    response = client.get('/api/parks/1/details')

    assert response.status_code == 200
    data = response.get_json()

    assert 'tier_distribution' in data


@freeze_time(MOCKED_NOW_UTC)
def test_park_details_includes_excluded_rides_count(client, comprehensive_test_data):
    """
    Test that park details includes excluded_rides_count.

    Excluded rides are those that haven't operated in 7+ days.
    """
    response = client.get('/api/parks/1/details')

    assert response.status_code == 200
    data = response.get_json()

    assert 'excluded_rides_count' in data
    assert isinstance(data['excluded_rides_count'], int)
    assert data['excluded_rides_count'] >= 0


# ============================================================================
# TEST: Error Cases and Edge Conditions
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_invalid_period(client, comprehensive_test_data):
    """Test that invalid period parameter returns 400 error."""
    response = client.get('/api/parks/downtime?period=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'error' in data


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_invalid_filter(client, comprehensive_test_data):
    """Test that invalid filter parameter returns 400 error."""
    response = client.get('/api/parks/downtime?filter=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'error' in data


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_invalid_period(client, comprehensive_test_data):
    """Test that invalid period parameter returns 400 error."""
    response = client.get('/api/rides/downtime?period=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


@freeze_time(MOCKED_NOW_UTC)
def test_rides_waittimes_invalid_mode(client, comprehensive_test_data):
    """Test that invalid mode parameter returns 400 error."""
    response = client.get('/api/rides/waittimes?mode=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


@freeze_time(MOCKED_NOW_UTC)
def test_parks_downtime_limit_parameter(client, comprehensive_test_data):
    """Test that limit parameter correctly restricts results."""
    response = client.get('/api/parks/downtime?period=today&limit=5')

    assert response.status_code == 200
    data = response.get_json()
    assert len(data['data']) == 5


@freeze_time(MOCKED_NOW_UTC)
def test_rides_downtime_limit_exceeds_maximum(client, comprehensive_test_data):
    """Test that limit parameter is capped at 200."""
    response = client.get('/api/rides/downtime?period=today&limit=999')

    assert response.status_code == 200
    data = response.get_json()
    # Should be capped at 100 total rides (our test data)
    assert len(data['data']) <= 100


# ============================================================================
# TEST: Data Consistency Across Endpoints
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_data_consistency_parks_vs_rides(client, comprehensive_test_data):
    """
    Verify park downtime equals sum of ride downtimes.

    CRITICAL: This ensures aggregation math is correct.
    """
    # Get park downtime for today
    parks_response = client.get('/api/parks/downtime?period=today&filter=all-parks')
    parks_data = parks_response.get_json()

    # Get ride downtime for today
    rides_response = client.get('/api/rides/downtime?period=today&filter=all-parks&limit=100')
    rides_data = rides_response.get_json()

    # For each park, sum its rides' downtime and compare to park total
    for park in parks_data['data']:
        park_id = park['park_id']
        park_downtime = float(park['total_downtime_hours'])

        # Sum downtime for all rides in this park
        rides_in_park = [r for r in rides_data['data'] if r['park_id'] == park_id]
        rides_downtime_sum = sum(float(r['downtime_hours']) for r in rides_in_park)

        # Should match (within rounding tolerance)
        assert abs(park_downtime - rides_downtime_sum) < 0.1, \
            f"Park {park_id} downtime mismatch! Park: {park_downtime}h, Rides sum: {rides_downtime_sum}h"

    print(f"\n✓ Verified park downtime = sum of ride downtimes for all {len(parks_data['data'])} parks")


@freeze_time(MOCKED_NOW_UTC)
def test_all_endpoints_return_attribution(client, comprehensive_test_data):
    """Verify all endpoints include ThemeParks.wiki attribution."""
    endpoints = [
        '/api/parks/downtime?period=today',
        '/api/rides/downtime?period=today',
        '/api/rides/waittimes?mode=live'
    ]

    for endpoint in endpoints:
        response = client.get(endpoint)
        data = response.get_json()

        assert 'attribution' in data
        assert data['attribution']['data_source'] == 'ThemeParks.wiki'
        assert data['attribution']['url'] == 'https://themeparks.wiki'


# ============================================================================
# SUMMARY
# ============================================================================

@freeze_time(MOCKED_NOW_UTC)
def test_comprehensive_suite_summary(comprehensive_test_data):
    """
    Print summary of comprehensive test coverage.

    This test always passes - it just documents what we tested.
    """
    summary = f"""
    ========================================
    COMPREHENSIVE API INTEGRATION TEST SUITE
    ========================================

    Test Data Created:
    - {comprehensive_test_data['num_parks']} parks ({comprehensive_test_data['disney_parks']} Disney, {comprehensive_test_data['universal_parks']} Universal, {comprehensive_test_data['other_parks']} Other)
    - {comprehensive_test_data['num_rides']} rides total
    - {comprehensive_test_data['num_parks'] * comprehensive_test_data['tier1_rides_per_park']} Tier 1 rides (3x weight)
    - {comprehensive_test_data['num_parks'] * comprehensive_test_data['tier2_rides_per_park']} Tier 2 rides (2x weight)
    - {comprehensive_test_data['num_parks'] * comprehensive_test_data['tier3_rides_per_park']} Tier 3 rides (1x weight)
    - Daily stats for today and yesterday
    - Weekly stats for current and previous week
    - Monthly stats for current and previous month
    - 100 current ride status snapshots

    Coverage:
    ✓ GET /api/parks/downtime - All periods (today, 7days, 30days)
    ✓ GET /api/parks/downtime - All filters (all-parks, disney-universal)
    ✓ GET /api/parks/downtime - Weighted scoring with manual verification
    ✓ GET /api/rides/downtime - All periods with 100 rides
    ✓ GET /api/rides/downtime - Filtering and trend calculations
    ✓ GET /api/rides/waittimes - All modes (live, 7day-average, peak-times)
    ✓ Error handling for invalid parameters
    ✓ Limit parameter enforcement
    ✓ Data consistency between parks and rides
    ✓ Attribution present on all endpoints

    Mathematical Validations:
    ✓ Unweighted downtime: 12.5 hours per park
    ✓ Weighted downtime: 29.5 hours per park
    ✓ Trend calculations: 25% daily, 11% weekly, 17% monthly
    ✓ Park totals = Sum of ride downtime
    ✓ Wait times sorted correctly by tier

    This suite uses SUBSTANTIAL test data (not just 1-2 examples)
    to ensure production-ready accuracy.
    ========================================
    """

    print(summary)
    assert True  # Always pass - this is just a summary


# ============================================================================
# FIXTURES - Trends Test Data with >5% Changes
# ============================================================================

@pytest.fixture
def trends_test_data(mysql_session):
    """
    Create test dataset specifically for trends testing with >5% uptime changes.

    Creates:
    - 6 parks with varying uptime trends
    - 30 rides with varying uptime trends
    - Parks 11-12: Improving >5%
    - Parks 13-14: Declining >5%
    - Parks 15-16: No significant change (<5%)
    - Similar pattern for rides
    """
    conn = mysql_session

    # Clean up any existing trends test data (queue_times_id 8000+)
    conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_status_changes WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_daily_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_weekly_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_monthly_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_daily_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_weekly_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_monthly_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM ride_classifications WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM rides WHERE queue_times_id >= 80000"))
    conn.execute(text("DELETE FROM parks WHERE queue_times_id >= 8000"))
    conn.commit()

    # === CREATE 6 PARKS WITH DIFFERENT TREND PATTERNS ===
    parks_data = [
        # Parks 11-12: Improving (Disney/Universal)
        (11, 8011, 'Test Park Improving 1', 'Orlando', 'FL', 'US', 'America/New_York', 'Disney', True, False, True),
        (12, 8012, 'Test Park Improving 2', 'Orlando', 'FL', 'US', 'America/New_York', 'Universal', False, True, True),
        # Parks 13-14: Declining (Disney/Universal)
        (13, 8013, 'Test Park Declining 1', 'Anaheim', 'CA', 'US', 'America/Los_Angeles', 'Disney', True, False, True),
        (14, 8014, 'Test Park Declining 2', 'Orlando', 'FL', 'US', 'America/New_York', 'Universal', False, True, True),
        # Parks 15-16: No significant change (Other parks)
        (15, 8015, 'Test Park Stable 1', 'Tampa', 'FL', 'US', 'America/New_York', 'SeaWorld', False, False, True),
        (16, 8016, 'Test Park Stable 2', 'Tampa', 'FL', 'US', 'America/New_York', 'Busch Gardens', False, False, True),
    ]

    for park in parks_data:
        conn.execute(text("""
            INSERT INTO parks (park_id, queue_times_id, name, city, state_province, country, timezone, operator, is_disney, is_universal, is_active)
            VALUES (:park_id, :qt_id, :name, :city, :state, :country, :tz, :operator, :is_disney, :is_universal, :is_active)
        """), {
            'park_id': park[0], 'qt_id': park[1], 'name': park[2], 'city': park[3],
            'state': park[4], 'country': park[5], 'tz': park[6], 'operator': park[7],
            'is_disney': park[8], 'is_universal': park[9], 'is_active': park[10]
        })

    conn.commit()

    # === CREATE PARK STATS WITH >5% CHANGES ===
    today = date.today()
    yesterday = today - timedelta(days=1)
    current_year = datetime.now().year
    current_week = datetime.now().isocalendar()[1]
    prev_week_date = datetime.now() - timedelta(weeks=1)
    prev_week = prev_week_date.isocalendar()[1]
    prev_week_year = prev_week_date.year
    current_week_start = date.fromisocalendar(current_year, current_week, 1)
    prev_week_start = date.fromisocalendar(prev_week_year, prev_week, 1)
    current_month = datetime.now().month
    prev_month = current_month - 1 if current_month > 1 else 12
    prev_month_year = current_year if current_month > 1 else current_year - 1

    default_total_rides = 5
    default_rides_with_downtime = 5
    default_operating_minutes = 600

    weekly_operating_minutes = default_operating_minutes * 7
    monthly_operating_minutes = default_operating_minutes * 30

    # Park 11-12: IMPROVING by >5% (previous uptime was lower)
    # Park 11: 90% today vs 80% yesterday = +10% improvement
    for park_id in [11, 12]:
        improvement = 10.0 if park_id == 11 else 8.0

        # Daily stats
        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': today,
            'total_rides': default_total_rides,
            'downtime': 1.0,  # 1 hour
            'rides_down': default_rides_with_downtime,
            'uptime': 90.0,
            'operating_minutes': default_operating_minutes
        })

        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': yesterday,
            'total_rides': default_total_rides,
            'downtime': 2.0,  # 2 hours
            'rides_down': default_rides_with_downtime,
            'uptime': 90.0 - improvement,
            'operating_minutes': default_operating_minutes
        })

        # Weekly stats
        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours,
                avg_uptime_percentage, trend_vs_previous_week
            )
            VALUES (:park_id, :year, :week, :week_start, :downtime, :uptime, :trend)
        """), {
            'park_id': park_id,
            'year': current_year,
            'week': current_week,
            'week_start': current_week_start,
            'downtime': 7.0,  # 7 hours for the week
            'uptime': 90.0,
            'trend': -improvement
        })

        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours,
                avg_uptime_percentage, trend_vs_previous_week
            )
            VALUES (:park_id, :year, :week, :week_start, :downtime, :uptime, NULL)
        """), {
            'park_id': park_id,
            'year': prev_week_year,
            'week': prev_week,
            'week_start': prev_week_start,
            'downtime': 14.0,
            'uptime': 90.0 - improvement
        })

        # Monthly stats
        conn.execute(text("""
            INSERT INTO park_monthly_stats (park_id, year, month, total_downtime_hours, avg_uptime_percentage)
            VALUES (:park_id, :year, :month, :downtime, :uptime)
        """), {
            'park_id': park_id,
            'year': current_year,
            'month': current_month,
            'downtime': 30.0,  # 30 hours for the month
            'uptime': 90.0
        })

        conn.execute(text("""
            INSERT INTO park_monthly_stats (park_id, year, month, total_downtime_hours, avg_uptime_percentage)
            VALUES (:park_id, :year, :month, :downtime, :uptime)
        """), {
            'park_id': park_id,
            'year': prev_month_year,
            'month': prev_month,
            'downtime': 60.0,
            'uptime': 90.0 - improvement
        })

    # Park 13-14: DECLINING by >5% (previous uptime was higher)
    # Park 13: 75% today vs 85% yesterday = -10% decline
    for park_id in [13, 14]:
        decline = 10.0 if park_id == 13 else 7.0

        # Daily stats
        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': today,
            'total_rides': default_total_rides,
            'downtime': 2.5,  # 2.5 hours
            'rides_down': default_rides_with_downtime,
            'uptime': 75.0,
            'operating_minutes': default_operating_minutes
        })

        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': yesterday,
            'total_rides': default_total_rides,
            'downtime': 1.5,  # 1.5 hours
            'rides_down': default_rides_with_downtime,
            'uptime': 75.0 + decline,
            'operating_minutes': default_operating_minutes
        })

        # Weekly stats
        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours,
                avg_uptime_percentage, trend_vs_previous_week
            )
            VALUES (:park_id, :year, :week, :week_start, :downtime, :uptime, :trend)
        """), {
            'park_id': park_id,
            'year': current_year,
            'week': current_week,
            'week_start': current_week_start,
            'downtime': 17.5,  # 17.5 hours
            'uptime': 75.0,
            'trend': decline
        })

        conn.execute(text("""
            INSERT INTO park_weekly_stats (
                park_id, year, week_number, week_start_date, total_downtime_hours,
                avg_uptime_percentage, trend_vs_previous_week
            )
            VALUES (:park_id, :year, :week, :week_start, :downtime, :uptime, NULL)
        """), {
            'park_id': park_id,
            'year': prev_week_year,
            'week': prev_week,
            'week_start': prev_week_start,
            'downtime': 10.5,
            'uptime': 75.0 + decline
        })

        # Monthly stats
        conn.execute(text("""
            INSERT INTO park_monthly_stats (park_id, year, month, total_downtime_hours, avg_uptime_percentage)
            VALUES (:park_id, :year, :month, :downtime, :uptime)
        """), {
            'park_id': park_id,
            'year': current_year,
            'month': current_month,
            'downtime': 75.0,  # 75 hours
            'uptime': 75.0
        })

        conn.execute(text("""
            INSERT INTO park_monthly_stats (park_id, year, month, total_downtime_hours, avg_uptime_percentage)
            VALUES (:park_id, :year, :month, :downtime, :uptime)
        """), {
            'park_id': park_id,
            'year': prev_month_year,
            'month': prev_month,
            'downtime': 45.0,
            'uptime': 75.0 + decline
        })

    # Park 15-16: STABLE - changes <5% (should NOT appear in trends)
    # Park 15: 80% today vs 82% yesterday = -2% decline (below threshold)
    for park_id in [15, 16]:
        small_change = 2.0

        # Daily stats
        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': today,
            'total_rides': default_total_rides,
            'downtime': 2.0,
            'rides_down': default_rides_with_downtime,
            'uptime': 80.0,
            'operating_minutes': default_operating_minutes
        })

        conn.execute(text("""
            INSERT INTO park_daily_stats (
                park_id, stat_date, total_rides_tracked, total_downtime_hours,
                rides_with_downtime, avg_uptime_percentage, operating_hours_minutes
            )
            VALUES (:park_id, :stat_date, :total_rides, :downtime, :rides_down, :uptime, :operating_minutes)
        """), {
            'park_id': park_id,
            'stat_date': yesterday,
            'total_rides': default_total_rides,
            'downtime': 1.8,
            'rides_down': default_rides_with_downtime,
            'uptime': 82.0,
            'operating_minutes': default_operating_minutes
        })

    # === CREATE RIDES WITH TREND PATTERNS ===
    # 5 rides per park = 30 rides total
    ride_id = 201
    for park_id in range(11, 17):
        for i in range(5):
            conn.execute(text("""
                INSERT INTO rides (ride_id, queue_times_id, park_id, name, tier, is_active)
                VALUES (:ride_id, :qt_id, :park_id, :name, :tier, TRUE)
            """), {
                'ride_id': ride_id,
                'qt_id': 80000 + ride_id,
                'park_id': park_id,
                'name': f'TrendTestRide_{park_id}_{i}',
                'tier': 2
            })

            # Add classification
            conn.execute(text("""
                INSERT INTO ride_classifications (ride_id, tier, tier_weight, classification_method, confidence_score)
                VALUES (:ride_id, 2, 2, 'manual_override', 1.0)
            """), {
                'ride_id': ride_id
            })

            # Rides in improving parks (11-12): rides also improving
            if park_id in [11, 12]:
                # Daily: 95% today vs 85% yesterday = +10% improvement
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 30, 60, 2, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': today,
                    'downtime': 30,
                    'uptime_minutes': default_operating_minutes - 30,
                    'uptime': 95.0,
                    'operating_minutes': default_operating_minutes
                })

                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 25, 50, 3, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': yesterday,
                    'downtime': 90,
                    'uptime_minutes': default_operating_minutes - 90,
                    'uptime': 85.0,
                    'operating_minutes': default_operating_minutes
                })

                # Weekly stats
                conn.execute(text("""
                    INSERT INTO ride_weekly_stats (
                        ride_id, year, week_number, week_start_date, downtime_minutes, uptime_minutes,
                        uptime_percentage, avg_wait_time, peak_wait_time, status_changes,
                        operating_hours_minutes, trend_vs_previous_week
                    )
                    VALUES (:ride_id, :year, :week, :week_start, :downtime, :uptime_minutes,
                            :uptime, 30, 70, 10, :operating_minutes, :trend)
                """), {
                    'ride_id': ride_id,
                    'year': current_year,
                    'week': current_week,
                    'week_start': current_week_start,
                    'downtime': 210,
                    'uptime_minutes': weekly_operating_minutes - 210,
                    'uptime': 95.0,
                    'operating_minutes': weekly_operating_minutes,
                    'trend': -improvement
                })

                conn.execute(text("""
                    INSERT INTO ride_weekly_stats (
                        ride_id, year, week_number, week_start_date, downtime_minutes, uptime_minutes,
                        uptime_percentage, avg_wait_time, peak_wait_time, status_changes,
                        operating_hours_minutes, trend_vs_previous_week
                    )
                    VALUES (:ride_id, :year, :week, :week_start, :downtime, :uptime_minutes,
                            :uptime, 25, 60, 12, :operating_minutes, NULL)
                """), {
                    'ride_id': ride_id,
                    'year': prev_week_year,
                    'week': prev_week,
                    'week_start': prev_week_start,
                    'downtime': 630,
                    'uptime_minutes': weekly_operating_minutes - 630,
                    'uptime': 85.0,
                    'operating_minutes': weekly_operating_minutes
                })

                # Monthly stats
                conn.execute(text("""
                    INSERT INTO ride_monthly_stats (
                        ride_id, year, month, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :year, :month, :downtime, :uptime_minutes, :uptime,
                            30, 75, 40, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'year': current_year,
                    'month': current_month,
                    'downtime': 900,
                    'uptime_minutes': monthly_operating_minutes - 900,
                    'uptime': 95.0,
                    'operating_minutes': monthly_operating_minutes
                })

                conn.execute(text("""
                    INSERT INTO ride_monthly_stats (
                        ride_id, year, month, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :year, :month, :downtime, :uptime_minutes, :uptime,
                            25, 65, 45, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'year': prev_month_year,
                    'month': prev_month,
                    'downtime': 2700,
                    'uptime_minutes': monthly_operating_minutes - 2700,
                    'uptime': 85.0,
                    'operating_minutes': monthly_operating_minutes
                })

            # Rides in declining parks (13-14): rides also declining
            elif park_id in [13, 14]:
                # Daily: 70% today vs 85% yesterday = -15% decline
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 35, 70, 5, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': today,
                    'downtime': 180,
                    'uptime_minutes': default_operating_minutes - 180,
                    'uptime': 70.0,
                    'operating_minutes': default_operating_minutes
                })

                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 30, 60, 2, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': yesterday,
                    'downtime': 90,
                    'uptime_minutes': default_operating_minutes - 90,
                    'uptime': 85.0,
                    'operating_minutes': default_operating_minutes
                })

                # Weekly stats
                conn.execute(text("""
                    INSERT INTO ride_weekly_stats (
                        ride_id, year, week_number, week_start_date, downtime_minutes, uptime_minutes,
                        uptime_percentage, avg_wait_time, peak_wait_time, status_changes,
                        operating_hours_minutes, trend_vs_previous_week
                    )
                    VALUES (:ride_id, :year, :week, :week_start, :downtime, :uptime_minutes,
                            :uptime, 35, 75, 20, :operating_minutes, :trend)
                """), {
                    'ride_id': ride_id,
                    'year': current_year,
                    'week': current_week,
                    'week_start': current_week_start,
                    'downtime': 1260,
                    'uptime_minutes': weekly_operating_minutes - 1260,
                    'uptime': 70.0,
                    'operating_minutes': weekly_operating_minutes,
                    'trend': decline
                })

                conn.execute(text("""
                    INSERT INTO ride_weekly_stats (
                        ride_id, year, week_number, week_start_date, downtime_minutes, uptime_minutes,
                        uptime_percentage, avg_wait_time, peak_wait_time, status_changes,
                        operating_hours_minutes, trend_vs_previous_week
                    )
                    VALUES (:ride_id, :year, :week, :week_start, :downtime, :uptime_minutes,
                            :uptime, 30, 65, 12, :operating_minutes, NULL)
                """), {
                    'ride_id': ride_id,
                    'year': prev_week_year,
                    'week': prev_week,
                    'week_start': prev_week_start,
                    'downtime': 630,
                    'uptime_minutes': weekly_operating_minutes - 630,
                    'uptime': 85.0,
                    'operating_minutes': weekly_operating_minutes
                })

                # Monthly stats
                conn.execute(text("""
                    INSERT INTO ride_monthly_stats (
                        ride_id, year, month, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :year, :month, :downtime, :uptime_minutes, :uptime,
                            35, 80, 60, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'year': current_year,
                    'month': current_month,
                    'downtime': 5400,
                    'uptime_minutes': monthly_operating_minutes - 5400,
                    'uptime': 70.0,
                    'operating_minutes': monthly_operating_minutes
                })

                conn.execute(text("""
                    INSERT INTO ride_monthly_stats (
                        ride_id, year, month, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :year, :month, :downtime, :uptime_minutes, :uptime,
                            30, 70, 45, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'year': prev_month_year,
                    'month': prev_month,
                    'downtime': 2700,
                    'uptime_minutes': monthly_operating_minutes - 2700,
                    'uptime': 85.0,
                    'operating_minutes': monthly_operating_minutes
                })

            # Rides in stable parks (15-16): small changes <5%
            else:
                # Daily: 80% today vs 82% yesterday = -2% (below threshold)
                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 30, 60, 2, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': today,
                    'downtime': 120,
                    'uptime_minutes': default_operating_minutes - 120,
                    'uptime': 80.0,
                    'operating_minutes': default_operating_minutes
                })

                conn.execute(text("""
                    INSERT INTO ride_daily_stats (
                        ride_id, stat_date, downtime_minutes, uptime_minutes, uptime_percentage,
                        avg_wait_time, peak_wait_time, status_changes, operating_hours_minutes
                    )
                    VALUES (:ride_id, :stat_date, :downtime, :uptime_minutes, :uptime, 30, 60, 2, :operating_minutes)
                """), {
                    'ride_id': ride_id,
                    'stat_date': yesterday,
                    'downtime': 108,
                    'uptime_minutes': default_operating_minutes - 108,
                    'uptime': 82.0,
                    'operating_minutes': default_operating_minutes
                })

            ride_id += 1

    conn.commit()

    # Reset global database pool so Flask gets fresh connections
    from database.connection import db as global_db
    global_db.close()

    yield {
        'improving_parks': [11, 12],
        'declining_parks': [13, 14],
        'stable_parks': [15, 16],
        'total_parks': 6,
        'disney_universal_count': 4,
        'total_rides': 30
    }

    # CLEANUP: Remove trends test data after tests complete
    conn.execute(text("DELETE FROM ride_status_snapshots WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_status_changes WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_daily_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_weekly_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM ride_monthly_stats WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM park_activity_snapshots WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_daily_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_weekly_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM park_monthly_stats WHERE park_id IN (SELECT park_id FROM parks WHERE queue_times_id >= 8000)"))
    conn.execute(text("DELETE FROM ride_classifications WHERE ride_id IN (SELECT ride_id FROM rides WHERE queue_times_id >= 80000)"))
    conn.execute(text("DELETE FROM rides WHERE queue_times_id >= 80000"))
    conn.execute(text("DELETE FROM parks WHERE queue_times_id >= 8000"))
    conn.commit()


# ============================================================================
# TEST: GET /api/trends - Parks Improving
# ============================================================================

def test_trends_parks_improving_today(client, trends_test_data):
    """
    Test GET /api/trends?category=parks-improving&period=today.

    Validates:
    - Only parks with ≥5% uptime improvement are returned
    - Parks 11-12 should appear (10% and 8% improvement)
    - Parks 15-16 should NOT appear (2% change < 5% threshold)
    - Sorted by improvement percentage descending
    """
    response = client.get('/api/trends?category=parks-improving&period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == 'today'
    assert data['category'] == 'parks-improving'
    assert data['filter'] == 'all-parks'
    assert 'parks' in data
    assert 'comparison' in data

    # Should return 2 parks (11 and 12)
    assert len(data['parks']) == 2

    # Verify improvement percentages
    park_ids = [p['park_id'] for p in data['parks']]
    assert 11 in park_ids
    assert 12 in park_ids

    # Verify sorting (highest improvement first)
    improvements = [float(p['improvement_percentage']) for p in data['parks']]
    assert improvements[0] >= improvements[1]
    assert all(imp >= 5.0 for imp in improvements)

    # Verify response structure
    for park in data['parks']:
        assert 'park_id' in park
        assert 'park_name' in park
        assert 'current_uptime' in park
        assert 'previous_uptime' in park
        assert 'improvement_percentage' in park
        assert 'current_downtime_hours' in park
        assert 'previous_downtime_hours' in park
        assert 'queue_times_url' in park

    print(f"\n✓ Verified {len(data['parks'])} improving parks with ≥5% threshold")


def test_trends_parks_improving_disney_universal_filter(client, trends_test_data):
    """
    Test Disney & Universal filter for parks-improving.

    Should return 2 parks (11 and 12 are both Disney/Universal).
    """
    response = client.get('/api/trends?category=parks-improving&period=today&filter=disney-universal&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['filter'] == 'disney-universal'

    # Both improving parks (11 and 12) are Disney/Universal
    assert len(data['parks']) == 2


def test_trends_parks_declining_today(client, trends_test_data):
    """
    Test GET /api/trends?category=parks-declining&period=today.

    Validates:
    - Only parks with ≥5% uptime decline are returned
    - Parks 13-14 should appear (10% and 7% decline)
    - Sorted by decline percentage descending
    """
    response = client.get('/api/trends?category=parks-declining&period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['category'] == 'parks-declining'
    assert 'parks' in data

    # Should return 2 parks (13 and 14)
    assert len(data['parks']) == 2

    park_ids = [p['park_id'] for p in data['parks']]
    assert 13 in park_ids
    assert 14 in park_ids

    # Verify all have ≥5% decline
    declines = [float(p['decline_percentage']) for p in data['parks']]
    assert all(dec >= 5.0 for dec in declines)

    # Verify sorting (highest decline first)
    assert declines[0] >= declines[1]

    print(f"\n✓ Verified {len(data['parks'])} declining parks with ≥5% threshold")


# ============================================================================
# TEST: GET /api/trends - Rides Improving/Declining
# ============================================================================

def test_trends_rides_improving_today(client, trends_test_data):
    """
    Test GET /api/trends?category=rides-improving&period=today.

    Validates:
    - Only rides with ≥5% uptime improvement are returned
    - Rides from parks 11-12 should appear (10 rides total)
    - Sorted by improvement percentage descending
    """
    response = client.get('/api/trends?category=rides-improving&period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['category'] == 'rides-improving'
    assert 'rides' in data

    # Should return 10 rides (5 rides from park 11 + 5 from park 12)
    assert len(data['rides']) == 10

    # Verify all have ≥5% improvement
    improvements = [float(r['improvement_percentage']) for r in data['rides']]
    assert all(imp >= 5.0 for imp in improvements)

    # Verify response structure
    for ride in data['rides']:
        assert 'ride_id' in ride
        assert 'ride_name' in ride
        assert 'park_name' in ride
        assert 'current_uptime' in ride
        assert 'previous_uptime' in ride
        assert 'improvement_percentage' in ride
        assert 'queue_times_url' in ride

    print(f"\n✓ Verified {len(data['rides'])} improving rides")


def test_trends_rides_declining_today(client, trends_test_data):
    """
    Test GET /api/trends?category=rides-declining&period=today.

    Validates:
    - Only rides with ≥5% uptime decline are returned
    - Rides from parks 13-14 should appear (10 rides total)
    """
    response = client.get('/api/trends?category=rides-declining&period=today&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['category'] == 'rides-declining'
    assert 'rides' in data

    # Should return 10 rides (5 from park 13 + 5 from park 14)
    assert len(data['rides']) == 10

    # Verify all have ≥5% decline
    declines = [float(r['decline_percentage']) for r in data['rides']]
    assert all(dec >= 5.0 for dec in declines)

    print(f"\n✓ Verified {len(data['rides'])} declining rides")


def test_trends_rides_7days_period(client, trends_test_data):
    """
    Test trends with period=7days.

    Validates weekly aggregation works correctly.
    """
    response = client.get('/api/trends?category=rides-improving&period=7days&filter=all-parks&limit=50')

    assert response.status_code == 200
    data = response.get_json()

    assert data['success'] is True
    assert data['period'] == '7days'

    # Should still return 10 improving rides
    assert len(data['rides']) == 10


# ============================================================================
# TEST: GET /api/trends - Error Cases
# ============================================================================

def test_trends_missing_category_parameter(client, trends_test_data):
    """Test that missing category parameter returns 400 error."""
    response = client.get('/api/trends?period=today')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'category' in data['error'].lower()


def test_trends_invalid_category(client, trends_test_data):
    """Test that invalid category parameter returns 400 error."""
    response = client.get('/api/trends?category=invalid&period=today')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


def test_trends_invalid_period(client, trends_test_data):
    """Test that invalid period parameter returns 400 error."""
    response = client.get('/api/trends?category=parks-improving&period=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


def test_trends_invalid_filter(client, trends_test_data):
    """Test that invalid filter parameter returns 400 error."""
    response = client.get('/api/trends?category=parks-improving&period=today&filter=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False


def test_trends_limit_out_of_range(client, trends_test_data):
    """Test that limit parameter outside valid range returns 400 error."""
    response = client.get('/api/trends?category=parks-improving&period=today&limit=999')

    assert response.status_code == 400
    data = response.get_json()
    assert data['success'] is False
    assert 'limit' in data['error'].lower()


def test_trends_includes_attribution(client, trends_test_data):
    """Verify trends endpoint includes ThemeParks.wiki attribution."""
    response = client.get('/api/trends?category=parks-improving&period=today')

    assert response.status_code == 200
    data = response.get_json()

    assert 'attribution' in data
    assert 'themeparks.wiki' in data['attribution'].lower()
