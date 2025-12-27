"""
Mirror Validation: Park vs Ride Daily Stats Consistency

Validates that park_daily_stats totals match the rolled-up ride_daily_stats
for the same date. This catches drift where aggregation jobs produce
inconsistent data between park and ride level stats.

This test requires mirrored production data and will SKIP when the test
database is empty or lacks daily stats.

Usage:
    1. Run: ./deployment/scripts/mirror-production-db.sh --target=test
    2. Then: pytest tests/integration/test_daily_consistency_park_vs_rides.py -v
"""

import pytest
from sqlalchemy import text


def _has_daily_stats(session) -> bool:
    """Check if daily stats tables have data."""
    count = session.execute(
        text(
            """
            SELECT COUNT(*) FROM park_daily_stats p
            JOIN ride_daily_stats r ON r.stat_date = p.stat_date
            LIMIT 1
            """
        )
    ).scalar()
    return count > 0


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_park_daily_matches_ride_rollup(mysql_session):
    """
    Validate park_daily_stats.total_rides_tracked matches ride_daily_stats count.

    This is a sanity check that catches:
    - Aggregation bugs where park stats don't reflect actual ride data
    - Missing ride records that weren't included in park rollups
    - Schema changes that broke the aggregation pipeline

    The test uses the latest overlapping date to ensure we're checking
    the most recent aggregation run.
    """
    if not _has_daily_stats(mysql_session):
        pytest.skip(
            "No overlapping daily stats data - run mirror script with stats tables, "
            "or ensure daily aggregation has run in production"
        )

    # Pick latest stat_date present in both tables
    row = mysql_session.execute(
        text(
            """
            SELECT MAX(p.stat_date) AS stat_date
            FROM park_daily_stats p
            JOIN ride_daily_stats r ON r.stat_date = p.stat_date
            """
        )
    ).first()

    stat_date = row.stat_date

    # Roll up ride counts per park (simple, stable sanity check)
    ride_rollups = mysql_session.execute(
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

    parks = mysql_session.execute(
        text(
            """
            SELECT park_id, total_rides_tracked
            FROM park_daily_stats
            WHERE stat_date = :stat_date
            """
        ),
        {"stat_date": stat_date},
    ).fetchall()

    assert parks, f"No park_daily_stats rows for stat_date {stat_date}"

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

    assert not mismatches, (
        f"Found {len(mismatches)} park(s) with ride count mismatches on {stat_date}: "
        f"{mismatches[:5]}{'...' if len(mismatches) > 5 else ''}"
    )


@pytest.mark.integration
@pytest.mark.mirror_validation
def test_park_downtime_vs_ride_rollup(mysql_session):
    """
    Validate park total_downtime_hours approximately matches sum of ride downtime.

    Park-level downtime should be close to the sum of individual ride downtime,
    though small differences are acceptable due to rounding and weighted calculations.
    """
    if not _has_daily_stats(mysql_session):
        pytest.skip("No overlapping daily stats data")

    # Get latest date with both park and ride stats
    row = mysql_session.execute(
        text(
            """
            SELECT MAX(p.stat_date) AS stat_date
            FROM park_daily_stats p
            JOIN ride_daily_stats r ON r.stat_date = p.stat_date
            """
        )
    ).first()

    stat_date = row.stat_date

    # Compare park downtime vs sum of ride downtime
    comparison = mysql_session.execute(
        text(
            """
            SELECT
              p.park_id,
              p.total_downtime_hours AS park_downtime,
              COALESCE(SUM(r.downtime_hours), 0) AS ride_sum
            FROM park_daily_stats p
            LEFT JOIN rides ON rides.park_id = p.park_id
            LEFT JOIN ride_daily_stats r ON r.ride_id = rides.ride_id AND r.stat_date = p.stat_date
            WHERE p.stat_date = :stat_date
              AND p.total_downtime_hours > 0
            GROUP BY p.park_id, p.total_downtime_hours
            """
        ),
        {"stat_date": stat_date},
    ).fetchall()

    if not comparison:
        pytest.skip("No parks with downtime data for consistency check")

    large_mismatches = []
    for row in comparison:
        if row.park_downtime and row.ride_sum:
            # Allow 20% variance for weighted calculations
            diff_pct = abs(row.park_downtime - row.ride_sum) / row.park_downtime * 100
            if diff_pct > 20:
                large_mismatches.append(
                    (row.park_id, f"park={row.park_downtime:.2f}h vs rides={row.ride_sum:.2f}h ({diff_pct:.0f}% diff)")
                )

    assert not large_mismatches, (
        f"Found {len(large_mismatches)} park(s) with >20% downtime mismatch: {large_mismatches[:3]}"
    )
