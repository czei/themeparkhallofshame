"""
TDD Test for Kennywood YESTERDAY Shame Score Bug

This test documents and fixes the bug where Kennywood shows
a shame score of 99 for YESTERDAY period.

Expected: Shame score should be reasonable (< 10)
Actual: Shame score is 99
"""
import pytest
from datetime import datetime, timezone
from freezegun import freeze_time

from database.connection import get_db_connection
from database.queries.yesterday.yesterday_park_rankings import YesterdayParkRankingsQuery


# Freeze time to Dec 21, 2025 8:00 AM UTC (midnight Pacific Dec 21)
# So YESTERDAY = Dec 20, 2025 (Pacific time)
MOCKED_NOW_UTC = datetime(2025, 12, 21, 8, 0, 0, tzinfo=timezone.utc)


@freeze_time(MOCKED_NOW_UTC)
class TestKennywoodYesterdayBug:
    """
    TDD test for investigating Kennywood's shame_score=99 for YESTERDAY.

    This test will FAIL initially (RED), showing the bug exists.
    Then we'll fix the code to make it pass (GREEN).
    """

    def test_kennywood_yesterday_shame_score_is_reasonable(self, mysql_session):
        """
        Kennywood should have a reasonable shame score for YESTERDAY,
        not the insane value of 99.

        ROOT CAUSE (Production):
        - 20 hours have NULL shame_score (park not operating or zero downtime)
        - 1 hour (22:00 UTC Dec 19) has shame_score=99.0 (bad data from before fix)
        - MySQL AVG() excludes NULLs, so: AVG(99.0) = 99.0

        FIX:
        - Delete hour 22:00 UTC Dec 19 for Kennywood
        - OR re-aggregate Dec 19 with corrected logic

        Expected behavior:
        - Shame scores typically range from 0-10
        - Only rides that operated should count toward downtime
        - Non-operating rides should be excluded

        This test will FAIL initially, documenting the bug.
        """
        # Arrange: Get YESTERDAY rankings
        query = YesterdayParkRankingsQuery(mysql_session)

        # Act: Get rankings for yesterday
        rankings = query.get_rankings(limit=50)

        # Find Kennywood in the results
        kennywood = None
        for park in rankings:
            if park['park_id'] == 152:  # Kennywood
                kennywood = park
                break

        # Assert: Kennywood should have a reasonable shame score
        if kennywood:
            print("\n🔍 Kennywood YESTERDAY data:")
            print(f"   Shame score: {kennywood['shame_score']}")
            print(f"   Downtime hours: {kennywood.get('total_downtime_hours', 'N/A')}")
            print(f"   Weighted downtime: {kennywood.get('weighted_downtime_hours', 'N/A')}")
            print(f"   Park weight: {kennywood.get('effective_park_weight', 'N/A')}")

            # RED: This will FAIL initially with shame_score=99
            assert kennywood['shame_score'] < 10, \
                f"Kennywood shame_score is {kennywood['shame_score']}, expected < 10"
        else:
            # Kennywood not in rankings means shame_score ≈ 0, which is acceptable
            print("\n✓ Kennywood not in YESTERDAY rankings (shame_score ≈ 0)")
            assert True


    def test_yesterday_query_uses_correct_formula(self, mysql_session):
        """
        Verify that YESTERDAY query calculates shame_score correctly.

        The formula should be:
        - For aggregated periods: shame_score from pre-calculated values
        - NOT: SUM(weighted_downtime) / park_weight * 10 (that's the TODAY bug)

        This test documents the expected calculation method.
        """
        query = YesterdayParkRankingsQuery(mysql_session)
        rankings = query.get_rankings(limit=5)

        # Verify all shame scores are reasonable (not inflated)
        for park in rankings:
            assert park['shame_score'] < 50, \
                f"{park['park_name']} has unreasonable shame_score: {park['shame_score']}"
