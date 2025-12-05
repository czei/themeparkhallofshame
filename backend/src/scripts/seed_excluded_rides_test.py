#!/usr/bin/env python3
"""
Excluded Rides Feature - Local Test Data Seed Script
=====================================================

Creates minimal test data specifically for testing the 7-day hybrid denominator
and excluded rides feature locally.

Creates:
- 2 parks (one with excluded rides, one without)
- 10-15 rides per park with varying last_operated_at dates
- ride_classifications with tier_weight values
- park_activity_snapshots with shame_score values
- ride_status_snapshots for live status

Usage:
    cd backend
    PYTHONPATH=src python -m scripts.seed_excluded_rides_test
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from sqlalchemy import text
from database.connection import get_db_connection


def clear_test_data(conn):
    """Clear existing test data from relevant tables."""
    print("Clearing existing data...")
    tables = [
        "ride_status_snapshots",
        "park_activity_snapshots",
        "ride_classifications",
        "rides",
        "parks",
    ]
    for table in tables:
        try:
            conn.execute(text(f"DELETE FROM {table}"))
            print(f"  Cleared {table}")
        except Exception as e:
            print(f"  Warning: Could not clear {table}: {e}")


def seed_parks(conn) -> Dict[str, int]:
    """Insert test parks and return name->id mapping."""
    print("\nSeeding parks...")
    parks = [
        {
            "name": "Test Park Alpha",
            "city": "Orlando",
            "state": "FL",
            "country": "US",
            "tz": "America/New_York",
            "operator": "Test Corp",
            "queue_times_id": 99901,
        },
        {
            "name": "Test Park Beta",
            "city": "Anaheim",
            "state": "CA",
            "country": "US",
            "tz": "America/Los_Angeles",
            "operator": "Test Corp",
            "queue_times_id": 99902,
        },
    ]

    park_ids = {}
    for park in parks:
        result = conn.execute(text("""
            INSERT INTO parks (queue_times_id, name, city, state_province, country, timezone, operator, is_active)
            VALUES (:qt_id, :name, :city, :state, :country, :tz, :operator, TRUE)
        """), {
            "qt_id": park["queue_times_id"],
            "name": park["name"],
            "city": park["city"],
            "state": park["state"],
            "country": park["country"],
            "tz": park["tz"],
            "operator": park["operator"],
        })
        park_ids[park["name"]] = result.lastrowid
        print(f"  Created {park['name']} (ID: {result.lastrowid})")

    return park_ids


def seed_rides(conn, park_ids: Dict[str, int]) -> Dict[str, List[int]]:
    """
    Insert test rides with varying last_operated_at dates.

    Test Park Alpha: Has 3 excluded rides (>7 days since operation)
    Test Park Beta: All rides operated recently (no excluded rides)
    """
    print("\nSeeding rides...")
    now = datetime.utcnow()

    # Rides for Test Park Alpha (with excluded rides)
    alpha_rides = [
        # Active rides (operated recently)
        {"name": "Alpha Coaster", "tier": 1, "days_ago": 0},       # Operating now
        {"name": "Alpha Splash", "tier": 1, "days_ago": 1},        # Yesterday
        {"name": "Alpha Dark Ride", "tier": 2, "days_ago": 2},     # 2 days ago
        {"name": "Alpha Log Flume", "tier": 2, "days_ago": 5},     # 5 days ago
        {"name": "Alpha Carousel", "tier": 3, "days_ago": 6},      # 6 days ago (edge case)
        {"name": "Alpha Train", "tier": 3, "days_ago": 7},         # Exactly 7 days (included)
        # Excluded rides (>7 days since operation)
        {"name": "Alpha Ferris Wheel", "tier": 2, "days_ago": 8},  # EXCLUDED - 8 days
        {"name": "Alpha Bumper Cars", "tier": 3, "days_ago": 15},  # EXCLUDED - 15 days
        {"name": "Alpha Vintage Ride", "tier": 1, "days_ago": 30}, # EXCLUDED - 30 days (refurb)
    ]

    # Rides for Test Park Beta (no excluded rides)
    beta_rides = [
        {"name": "Beta Hyper", "tier": 1, "days_ago": 0},
        {"name": "Beta Launch", "tier": 1, "days_ago": 0},
        {"name": "Beta Rapids", "tier": 2, "days_ago": 1},
        {"name": "Beta Simulator", "tier": 2, "days_ago": 2},
        {"name": "Beta Swings", "tier": 3, "days_ago": 3},
        {"name": "Beta Teacups", "tier": 3, "days_ago": 4},
    ]

    ride_ids = {"Test Park Alpha": [], "Test Park Beta": []}
    qt_counter = 99001

    for park_name, rides in [("Test Park Alpha", alpha_rides), ("Test Park Beta", beta_rides)]:
        park_id = park_ids[park_name]
        for ride in rides:
            qt_counter += 1
            last_op = now - timedelta(days=ride["days_ago"]) if ride["days_ago"] is not None else None

            result = conn.execute(text("""
                INSERT INTO rides (queue_times_id, park_id, name, tier, is_active, category, last_operated_at)
                VALUES (:qt_id, :park_id, :name, :tier, TRUE, 'ATTRACTION', :last_op)
            """), {
                "qt_id": qt_counter,
                "park_id": park_id,
                "name": ride["name"],
                "tier": ride["tier"],
                "last_op": last_op,
            })
            ride_ids[park_name].append(result.lastrowid)

            excluded = ride["days_ago"] > 7 if ride["days_ago"] is not None else True
            status = "EXCLUDED" if excluded else "active"
            print(f"  {park_name}: {ride['name']} (Tier {ride['tier']}, {ride['days_ago']} days ago) [{status}]")

    return ride_ids


def seed_ride_classifications(conn, ride_ids: Dict[str, List[int]]):
    """Insert ride_classifications with tier_weight values."""
    print("\nSeeding ride_classifications...")

    # Get ride details
    all_ride_ids = ride_ids["Test Park Alpha"] + ride_ids["Test Park Beta"]

    for ride_id in all_ride_ids:
        result = conn.execute(text("SELECT tier FROM rides WHERE ride_id = :rid"), {"rid": ride_id})
        row = result.fetchone()
        if row:
            tier = row[0]
            # Tier weights: Tier 1 = 3, Tier 2 = 2, Tier 3 = 1
            tier_weight = {1: 3, 2: 2, 3: 1}.get(tier, 2)

            conn.execute(text("""
                INSERT INTO ride_classifications (ride_id, tier, tier_weight)
                VALUES (:rid, :tier, :weight)
            """), {"rid": ride_id, "tier": tier, "weight": tier_weight})

    print(f"  Created {len(all_ride_ids)} ride classifications")


def seed_ride_status_snapshots(conn, ride_ids: Dict[str, List[int]]):
    """Insert current ride status snapshots (some rides down)."""
    print("\nSeeding ride_status_snapshots (live status)...")
    now = datetime.utcnow()

    # Test Park Alpha: 2 rides currently down
    alpha_down = set([ride_ids["Test Park Alpha"][0], ride_ids["Test Park Alpha"][2]])  # Coaster and Dark Ride

    # Test Park Beta: 1 ride currently down
    beta_down = set([ride_ids["Test Park Beta"][1]])  # Launch coaster

    all_down = alpha_down | beta_down

    for park_name, rids in ride_ids.items():
        for rid in rids:
            is_open = rid not in all_down
            wait_time = 45 if is_open else 0

            conn.execute(text("""
                INSERT INTO ride_status_snapshots (ride_id, recorded_at, is_open, wait_time, computed_is_open, status)
                VALUES (:rid, :rec, :is_open, :wait, :computed, :status)
            """), {
                "rid": rid,
                "rec": now,
                "is_open": is_open,
                "wait": wait_time,
                "computed": is_open,
                "status": "OPERATING" if is_open else "CLOSED",
            })

    print(f"  Created snapshots for {len(ride_ids['Test Park Alpha']) + len(ride_ids['Test Park Beta'])} rides")
    print(f"  Rides currently DOWN: {len(all_down)}")


def seed_park_activity_snapshots(conn, park_ids: Dict[str, int]):
    """Insert park_activity_snapshots with pre-calculated shame_score."""
    print("\nSeeding park_activity_snapshots...")
    now = datetime.utcnow()

    # Test Park Alpha:
    # - 6 active rides (operated in last 7 days): weights = 3+3+2+2+1+1 = 12
    # - 2 rides currently down (Coaster=3, Dark Ride=2) = 5 down weight
    # - Shame score = (5/12) * 10 = 4.2

    # Test Park Beta:
    # - 6 active rides: weights = 3+3+2+2+1+1 = 12
    # - 1 ride currently down (Launch=3) = 3 down weight
    # - Shame score = (3/12) * 10 = 2.5

    snapshots = [
        {"park_id": park_ids["Test Park Alpha"], "shame_score": 4.2, "total_rides": 9, "rides_open": 4, "rides_closed": 2},
        {"park_id": park_ids["Test Park Beta"], "shame_score": 2.5, "total_rides": 6, "rides_open": 5, "rides_closed": 1},
    ]

    for snap in snapshots:
        conn.execute(text("""
            INSERT INTO park_activity_snapshots
            (park_id, recorded_at, total_rides_tracked, rides_open, rides_closed, park_appears_open, shame_score)
            VALUES (:pid, :rec, :total, :rides_open, :rides_closed, TRUE, :shame)
        """), {
            "pid": snap["park_id"],
            "rec": now,
            "total": snap["total_rides"],
            "rides_open": snap["rides_open"],
            "rides_closed": snap["rides_closed"],
            "shame": snap["shame_score"],
        })
        print(f"  Park ID {snap['park_id']}: shame_score={snap['shame_score']}, rides_closed={snap['rides_closed']}")


def print_summary(park_ids: Dict[str, int]):
    """Print summary of seeded data."""
    print("\n" + "=" * 60)
    print("LOCAL TEST DATA SEEDING COMPLETE")
    print("=" * 60)
    print("\nParks created:")
    for name, pid in park_ids.items():
        print(f"  - {name} (ID: {pid})")

    print("\nTest scenarios:")
    print(f"  Test Park Alpha (ID {park_ids['Test Park Alpha']}):")
    print("    - 9 total rides, 6 included in shame score")
    print("    - 3 EXCLUDED rides (>7 days since operation)")
    print("    - 2 rides currently DOWN")
    print("    - Expected shame_score: 4.2")
    print("    - Expected excluded_rides_count: 3")

    print(f"\n  Test Park Beta (ID {park_ids['Test Park Beta']}):")
    print("    - 6 total rides, all included in shame score")
    print("    - 0 excluded rides")
    print("    - 1 ride currently DOWN")
    print("    - Expected shame_score: 2.5")
    print("    - Expected excluded_rides_count: 0")

    print("\nTo test:")
    print("  1. Start backend: PYTHONPATH=src python -m flask run --port 5001")
    print("  2. Test rankings: curl http://localhost:5001/api/parks/downtime?period=live")
    print(f"  3. Test details (with excluded): curl http://localhost:5001/api/parks/{park_ids['Test Park Alpha']}/details?period=live")
    print(f"  4. Test details (no excluded): curl http://localhost:5001/api/parks/{park_ids['Test Park Beta']}/details?period=live")


def main():
    """Main entry point."""
    print("=" * 60)
    print("EXCLUDED RIDES FEATURE - LOCAL TEST DATA SEED")
    print("=" * 60)

    with get_db_connection() as conn:
        clear_test_data(conn)
        park_ids = seed_parks(conn)
        ride_ids = seed_rides(conn, park_ids)
        seed_ride_classifications(conn, ride_ids)
        seed_ride_status_snapshots(conn, ride_ids)
        seed_park_activity_snapshots(conn, park_ids)

    print_summary(park_ids)


if __name__ == '__main__':
    main()
