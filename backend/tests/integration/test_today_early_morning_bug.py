"""
TDD Test for TODAY Early Morning Bug

BUG: At 5:41 AM PST (early morning), the TODAY ranking shows:
- High shame scores (e.g., 50-99)
- But total_downtime_hours = 0
- And rides_down = 0

ROOT CAUSE:
- shame_score uses park_activity_snapshots.shame_score (7-day hybrid denominator)
- total_downtime_hours uses ride_hourly_stats WHERE ride_operated = 1 (operated TODAY)
- Early in the day, NO rides have operated TODAY yet
- So total_downtime_hours = 0, but shame_score > 0 (from 7-day window)

EXPECTED:
- shame_score and total_downtime_hours should be CONSISTENT
- If no rides operated TODAY, shame_score should be 0 (or park not in rankings)
- If rides operated TODAY and have downtime, both should reflect that downtime

This test will FAIL initially (RED), documenting the bug.
"""
import pytest
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time

from database.connection import get_db_connection
from database.queries.today.today_park_rankings import TodayParkRankingsQuery


# Early morning PST: 5:41 AM PST Dec 21, 2025 = 13:41 UTC Dec 21, 2025
EARLY_MORNING_UTC = datetime(2025, 12, 21, 13, 41, 0, tzinfo=timezone.utc)


@freeze_time(EARLY_MORNING_UTC)
class TestTodayEarlyMorningBug:
    """
    TDD test for investigating TODAY shame score bug at early morning.

    This test will FAIL initially (RED), showing the bug exists.
    Then we'll fix the code to make it pass (GREEN).
    """

    def test_today_shame_score_matches_downtime_hours(self, mysql_connection):
        """
        TODAY rankings should have CONSISTENT shame_score and total_downtime_hours.

        If total_downtime_hours = 0, then shame_score should also be 0 (or very low).
        If shame_score > 0, then total_downtime_hours should also be > 0.

        BUG (Production at 5:41 AM PST):
        - Kennywood shows shame_score = 99, total_downtime_hours = 0
        - Other parks show high shame scores but zero downtime
        - Last Updated timestamp shows 8:41 AM PST (3 hours in the future)

        ROOT CAUSE:
        - shame_score from park_activity_snapshots uses 7-day hybrid denominator
        - total_downtime_hours from ride_hourly_stats uses "operated TODAY" filter
        - Mismatch creates inconsistent display

        Expected behavior:
        - Both metrics should use the same denominator logic
        - Either BOTH use "operated TODAY" or BOTH use "7-day hybrid"
        """
        # Arrange: Get TODAY rankings
        query = TodayParkRankingsQuery(mysql_connection)

        # Act: Get rankings for today (early morning)
        rankings = query.get_rankings(limit=50)

        # Assert: For each park, shame_score and downtime should be consistent
        print(f"\n🔍 TODAY Rankings at {EARLY_MORNING_UTC} (5:41 AM PST):")

        inconsistent_parks = []
        for park in rankings:
            park_name = park['park_name']
            shame_score = park['shame_score']
            downtime_hours = park.get('total_downtime_hours', 0)
            rides_down = park.get('rides_down', 0)

            print(f"\n  {park_name}:")
            print(f"    Shame score: {shame_score}")
            print(f"    Downtime hours: {downtime_hours}")
            print(f"    Rides down: {rides_down}")

            # Check for inconsistency: high shame score but zero downtime
            if shame_score > 5 and downtime_hours == 0:
                inconsistent_parks.append({
                    'park_name': park_name,
                    'shame_score': shame_score,
                    'downtime_hours': downtime_hours,
                    'rides_down': rides_down
                })

        # RED: This will FAIL initially with inconsistent parks
        assert len(inconsistent_parks) == 0, \
            f"Found {len(inconsistent_parks)} parks with inconsistent shame_score and downtime:\n" + \
            "\n".join([
                f"  - {p['park_name']}: shame_score={p['shame_score']}, "
                f"downtime_hours={p['downtime_hours']}, rides_down={p['rides_down']}"
                for p in inconsistent_parks
            ])


    def test_today_early_morning_should_have_zero_or_consistent_data(self, mysql_connection):
        """
        At 5:41 AM PST, most parks haven't opened yet.

        Expected outcomes:
        1. Parks that haven't opened TODAY: Not in rankings (or shame_score = 0)
        2. Parks that HAVE opened TODAY: Consistent shame_score and downtime

        NOT acceptable:
        - Parks with high shame_score but zero downtime (current bug)
        """
        query = TodayParkRankingsQuery(mysql_connection)
        rankings = query.get_rankings(limit=50)

        for park in rankings:
            shame_score = park['shame_score']
            downtime_hours = park.get('total_downtime_hours', 0)

            # If shame_score > 0, downtime should also be > 0 (proportionally)
            # Rough heuristic: shame_score of 10 ≈ 1 hour of downtime per ride
            if shame_score > 5:
                assert downtime_hours > 0, \
                    f"{park['park_name']} has shame_score={shame_score} but downtime_hours={downtime_hours}"


    def test_timestamp_bug_investigation(self, mysql_connection):
        """
        User reports: "Last Updated says 8:41:33 AM PST when it's around 5:41 AM PST"

        This is a 3-hour difference, suggesting a timezone or calculation error.

        This test investigates where the "Last Updated" timestamp comes from
        and why it might be showing a future time.
        """
        # TODO: Find where "Last Updated" timestamp is generated
        # Possible sources:
        # 1. park_activity_snapshots.recorded_at
        # 2. park_hourly_stats.created_at or updated_at
        # 3. API response metadata (last_updated field)

        # For now, let's check the most recent snapshot times
        from sqlalchemy import text

        result = mysql_connection.execute(text("""
            SELECT MAX(recorded_at) as last_snapshot
            FROM park_activity_snapshots
        """))
        row = result.fetchone()

        if row and row[0]:
            last_snapshot_utc = row[0]
            print(f"\n🔍 Last snapshot recorded_at: {last_snapshot_utc}")
            print(f"   Current time (frozen): {EARLY_MORNING_UTC}")
            print(f"   Difference: {(EARLY_MORNING_UTC - last_snapshot_utc).total_seconds()} seconds")
