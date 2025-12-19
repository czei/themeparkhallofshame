"""
Sanity checks to ensure critical raw and weather tables are populated after a mirror.

These tests are intentionally lightweight and rely solely on the real MySQL
database (no mocks). They guard against silent import failures that can lead
to cross-report metric drift (e.g., empty snapshots or weather data).
"""

import pytest
from sqlalchemy import text


@pytest.mark.integration
def test_snapshots_present(mysql_connection):
    """Ride/park snapshots should not be empty after a full mirror."""
    counts = mysql_connection.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM ride_status_snapshots) AS ride_snapshots,
              (SELECT COUNT(*) FROM park_activity_snapshots) AS park_snapshots
            """
        )
    ).first()

    assert counts.ride_snapshots > 0, "ride_status_snapshots is empty"
    assert counts.park_snapshots > 0, "park_activity_snapshots is empty"


@pytest.mark.integration
def test_weather_observations_present(mysql_connection):
    """Weather observations should mirror production (non-empty in prod)."""
    count = mysql_connection.execute(
        text("SELECT COUNT(*) AS c FROM weather_observations")
    ).scalar()
    assert count > 0, "weather_observations is empty; mirror likely missed weather data"


@pytest.mark.integration
def test_stats_daily_present(mysql_connection):
    """Daily stats should exist for at least one park and one ride."""
    ride_daily, park_daily = mysql_connection.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM ride_daily_stats) AS ride_daily,
              (SELECT COUNT(*) FROM park_daily_stats) AS park_daily
            """
        )
    ).first()

    assert ride_daily > 0, "ride_daily_stats is empty"
    assert park_daily > 0, "park_daily_stats is empty"
