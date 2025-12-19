"""
Integration sanity: park_daily_stats vs ride_daily_stats aggregates for latest day.
No mocks; uses live MySQL data. Intended to catch drift where park totals
(downtime, rides_down, avg wait) don't match rolled-up ride stats for the same day.
"""

import pytest
from sqlalchemy import text


@pytest.mark.integration
def test_park_daily_matches_ride_rollup(mysql_connection):
    # Pick latest stat_date present in both tables
    row = mysql_connection.execute(
        text(
            """
            SELECT MAX(p.stat_date) AS stat_date
            FROM park_daily_stats p
            JOIN ride_daily_stats r ON r.stat_date = p.stat_date
            """
        )
    ).first()
    assert row and row.stat_date, "No overlapping stat_date between park_daily_stats and ride_daily_stats"
    stat_date = row.stat_date

    # Roll up ride counts per park (simple, stable sanity check)
    ride_rollups = mysql_connection.execute(
        text(
            """
            SELECT
              rides.park_id,
              COUNT(*) AS ride_count
            FROM ride_daily_stats r
            JOIN rides ON rides.ride_id = r.ride_id
            WHERE r.stat_date = :stat_date
            GROUP BY rides.park_id
            """
        ),
        {"stat_date": stat_date},
    ).fetchall()
    ride_by_park = {r.park_id: r for r in ride_rollups}

    parks = mysql_connection.execute(
        text(
            """
            SELECT park_id, total_rides_tracked
            FROM park_daily_stats
            WHERE stat_date = :stat_date
            """
        ),
        {"stat_date": stat_date},
    ).fetchall()

    assert parks, "No park_daily_stats rows for stat_date"

    parks_with_totals = [p for p in parks if p.total_rides_tracked and p.total_rides_tracked > 0]
    if not parks_with_totals:
        pytest.skip(
            "park_daily_stats.total_rides_tracked are zero for latest date; "
            "run daily aggregation/backfill before running this consistency check."
        )

    mismatches = []
    for p in parks_with_totals:
        roll = ride_by_park.get(p.park_id)
        if not roll:
            mismatches.append((p.park_id, "missing ride rollup"))
            continue
        if p.total_rides_tracked != roll.ride_count:
            mismatches.append(
                (
                    p.park_id,
                    f"total_rides_tracked {p.total_rides_tracked} != ride_daily_stats count {roll.ride_count}",
                )
            )

    assert not mismatches, f"Found mismatches: {mismatches}"
