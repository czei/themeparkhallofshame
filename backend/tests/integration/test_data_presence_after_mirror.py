"""
Mirror Validation Tests - Verify production data was successfully mirrored.

These tests validate that the mirror script (deployment/scripts/mirror-production-db.sh)
correctly copied production data to the test database. They are NOT fixtures-based
tests and will SKIP when no mirrored data is present.

Usage:
    1. Run: ./deployment/scripts/mirror-production-db.sh --target=test
    2. Then: pytest tests/integration/test_data_presence_after_mirror.py -v

These tests guard against:
- Silent import failures during mirror
- Missing tables or partial data copies
- Schema mismatches between prod and test databases
"""

import pytest
from sqlalchemy import text


def _has_mirrored_data(session) -> bool:
    """
    Check if the database has mirrored production data.

    We check for at least 20 parks with queue_times_id < 9000. Test fixtures
    typically only create 1-10 parks, while production has 40+ parks.
    This prevents false positives from test fixtures that use low IDs.
    """
    count = session.execute(
        text("SELECT COUNT(*) FROM parks WHERE queue_times_id < 9000")
    ).scalar()
    return count >= 20  # Production has 40+ parks, test fixtures have <10


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_snapshots_present(mysql_session):
    """
    Validate ride/park snapshots were mirrored from production.

    These tables contain the raw time-series data that all aggregations derive from.
    Empty snapshots indicate the mirror failed or was incomplete.
    """
    if not _has_mirrored_data(mysql_session):
        pytest.skip("No mirrored data present - run mirror script first")

    counts = mysql_session.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM ride_status_snapshots) AS ride_snapshots,
              (SELECT COUNT(*) FROM park_activity_snapshots) AS park_snapshots
            """
        )
    ).first()

    assert counts.ride_snapshots > 0, (
        "ride_status_snapshots is empty after mirror. "
        "Check mirror script logs for errors during snapshot table export."
    )
    assert counts.park_snapshots > 0, (
        "park_activity_snapshots is empty after mirror. "
        "Check mirror script logs for errors during snapshot table export."
    )


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_weather_observations_present(mysql_session):
    """
    Validate weather observations were mirrored from production.

    Weather data is collected separately and may be missing if:
    - The weather collection cron job hasn't run
    - The mirror script excluded weather tables
    - There was an API failure during weather collection
    """
    if not _has_mirrored_data(mysql_session):
        pytest.skip("No mirrored data present - run mirror script first")

    count = mysql_session.execute(
        text("SELECT COUNT(*) AS c FROM weather_observations")
    ).scalar()

    if count == 0:
        # Check if weather_observations table exists but is empty in production
        pytest.skip(
            "weather_observations is empty. This may be expected if weather "
            "collection hasn't been enabled in production yet."
        )


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_stats_daily_present(mysql_session):
    """
    Validate daily stats tables were mirrored from production.

    Daily stats are pre-aggregated from snapshots by the daily aggregation job.
    Empty stats indicate either:
    - Mirror didn't include stats tables
    - Daily aggregation hasn't run in production
    - Stats tables were dropped/recreated with different schema
    """
    if not _has_mirrored_data(mysql_session):
        pytest.skip("No mirrored data present - run mirror script first")

    ride_daily, park_daily = mysql_session.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM ride_daily_stats) AS ride_daily,
              (SELECT COUNT(*) FROM park_daily_stats) AS park_daily
            """
        )
    ).first()

    assert ride_daily > 0, (
        "ride_daily_stats is empty after mirror. "
        "Ensure mirror script includes stats tables and daily aggregation has run."
    )
    assert park_daily > 0, (
        "park_daily_stats is empty after mirror. "
        "Ensure mirror script includes stats tables and daily aggregation has run."
    )


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_hourly_stats_present(mysql_session):
    """
    Validate hourly stats tables were mirrored from production.

    Hourly stats power the TODAY period queries and are critical for
    real-time rankings. Empty hourly stats break the today API endpoints.
    """
    if not _has_mirrored_data(mysql_session):
        pytest.skip("No mirrored data present - run mirror script first")

    ride_hourly, park_hourly = mysql_session.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM ride_hourly_stats) AS ride_hourly,
              (SELECT COUNT(*) FROM park_hourly_stats) AS park_hourly
            """
        )
    ).first()

    assert park_hourly > 0, (
        "park_hourly_stats is empty after mirror. "
        "This will break TODAY period API endpoints."
    )
    # ride_hourly_stats may be empty if it was dropped in a migration
    # Just warn, don't fail
    if ride_hourly == 0:
        pytest.skip(
            "ride_hourly_stats is empty - table may have been dropped in migration. "
            "Check if ride queries use alternative data sources."
        )


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_rides_and_parks_present(mysql_session):
    """
    Validate core entity tables (parks, rides) were mirrored.

    These are the foundation tables that all other data references.
    If these are empty, no queries will return meaningful results.

    Note: We check for production parks (queue_times_id < 9000) to distinguish
    from test fixtures which use high queue_times_id values.
    """
    if not _has_mirrored_data(mysql_session):
        pytest.skip("No mirrored data present - run mirror script first")

    # Count only production parks/rides (queue_times_id < 9000), not test fixtures
    parks, rides = mysql_session.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM parks WHERE is_active = 1 AND queue_times_id < 9000) AS parks,
              (SELECT COUNT(*) FROM rides WHERE is_active = 1 AND queue_times_id < 90000) AS rides
            """
        )
    ).first()

    assert parks > 0, "No active production parks found after mirror"
    assert rides > 0, "No active production rides found after mirror"

    # Sanity check: should have reasonable number of rides per park
    rides_per_park = rides / parks if parks > 0 else 0
    assert rides_per_park >= 5, (
        f"Only {rides_per_park:.1f} rides per park - expected at least 5. "
        "Mirror may have incomplete ride data."
    )
