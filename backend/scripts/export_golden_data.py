#!/usr/bin/env python3
"""
Export Golden Data from Development Database

Captures snapshot data for a specific date along with reference data
(parks, rides) needed to run golden data tests.

Usage:
    python scripts/export_golden_data.py --date=2025-12-21
    python scripts/export_golden_data.py --date=2025-12-21 --generate-expected
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pymysql
from pymysql import Error


def get_db_connection():
    """Get database connection from environment."""
    return pymysql.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', 3306)),
        database=os.environ.get('DB_NAME', 'themepark_tracker_dev'),
        user=os.environ.get('DB_USER', 'root'),
        password=os.environ.get('DB_PASSWORD', '294e043ww'),
        cursorclass=pymysql.cursors.DictCursor
    )


def export_parks(conn, output_path: Path):
    """Export parks table to SQL INSERT statements."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT park_id, queue_times_id, name, city, state_province, country,
               latitude, longitude, timezone, operator, is_disney, is_universal,
               is_active, created_at
        FROM parks
        ORDER BY park_id
    """)
    parks = cursor.fetchall()
    cursor.close()

    columns = ['park_id', 'queue_times_id', 'name', 'city', 'state_province', 'country',
               'latitude', 'longitude', 'timezone', 'operator', 'is_disney', 'is_universal',
               'is_active', 'created_at']

    with open(output_path, 'w') as f:
        f.write("-- Parks reference data for golden dataset\n")
        f.write("-- Auto-generated, do not edit manually\n\n")

        for park in parks:
            values = []
            for key in columns:
                val = park[key]
                if val is None:
                    values.append('NULL')
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, datetime):
                    values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(val, bool):
                    values.append('1' if val else '0')
                else:
                    values.append(str(val))

            f.write(f"INSERT INTO parks ({', '.join(columns)}) VALUES "
                   f"({', '.join(values)}) ON DUPLICATE KEY UPDATE name=VALUES(name);\n")

    print(f"  Exported {len(parks)} parks to {output_path}")


def export_rides(conn, output_path: Path):
    """Export rides table to SQL INSERT statements."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ride_id, queue_times_id, park_id, name, entity_type, land_area,
               tier, category, is_active, created_at, last_operated_at
        FROM rides
        ORDER BY ride_id
    """)
    rides = cursor.fetchall()
    cursor.close()

    columns = ['ride_id', 'queue_times_id', 'park_id', 'name', 'entity_type', 'land_area',
               'tier', 'category', 'is_active', 'created_at', 'last_operated_at']

    with open(output_path, 'w') as f:
        f.write("-- Rides reference data for golden dataset\n")
        f.write("-- Auto-generated, do not edit manually\n\n")

        for ride in rides:
            values = []
            for key in columns:
                val = ride[key]
                if val is None:
                    values.append('NULL')
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, datetime):
                    values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(val, bool):
                    values.append('1' if val else '0')
                else:
                    values.append(str(val))

            f.write(f"INSERT INTO rides ({', '.join(columns)}) VALUES "
                   f"({', '.join(values)}) ON DUPLICATE KEY UPDATE name=VALUES(name);\n")

    print(f"  Exported {len(rides)} rides to {output_path}")


def export_snapshots(conn, target_date: str, output_path: Path):
    """Export snapshot data for a specific date."""
    cursor = conn.cursor()

    # Export park_activity_snapshots
    cursor.execute("""
        SELECT snapshot_id, park_id, recorded_at, park_appears_open, rides_open,
               rides_closed, avg_wait_time, max_wait_time, shame_score
        FROM park_activity_snapshots
        WHERE DATE(recorded_at) = %s
        ORDER BY recorded_at, park_id
    """, (target_date,))
    park_snapshots = cursor.fetchall()

    # Export ride_status_snapshots
    cursor.execute("""
        SELECT snapshot_id, ride_id, recorded_at, status, computed_is_open,
               wait_time, last_updated_api
        FROM ride_status_snapshots
        WHERE DATE(recorded_at) = %s
        ORDER BY recorded_at, ride_id
    """, (target_date,))
    ride_snapshots = cursor.fetchall()
    cursor.close()

    park_cols = ['snapshot_id', 'park_id', 'recorded_at', 'park_appears_open',
                 'rides_open', 'rides_closed', 'avg_wait_time', 'max_wait_time', 'shame_score']
    ride_cols = ['snapshot_id', 'ride_id', 'recorded_at', 'status', 'computed_is_open',
                 'wait_time', 'last_updated_api']

    with open(output_path, 'w') as f:
        f.write(f"-- Snapshot data for {target_date}\n")
        f.write("-- Auto-generated, do not edit manually\n\n")

        f.write("-- Park activity snapshots\n")
        for snap in park_snapshots:
            values = []
            for key in park_cols:
                val = snap[key]
                if val is None:
                    values.append('NULL')
                elif isinstance(val, datetime):
                    values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(val, bool):
                    values.append('1' if val else '0')
                else:
                    values.append(str(val))

            f.write(f"INSERT INTO park_activity_snapshots ({', '.join(park_cols)}) "
                   f"VALUES ({', '.join(values)}) ON DUPLICATE KEY UPDATE recorded_at=VALUES(recorded_at);\n")

        f.write("\n-- Ride status snapshots\n")
        for snap in ride_snapshots:
            values = []
            for key in ride_cols:
                val = snap[key]
                if val is None:
                    values.append('NULL')
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, datetime):
                    values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(val, bool):
                    values.append('1' if val else '0')
                else:
                    values.append(str(val))

            f.write(f"INSERT INTO ride_status_snapshots ({', '.join(ride_cols)}) "
                   f"VALUES ({', '.join(values)}) ON DUPLICATE KEY UPDATE recorded_at=VALUES(recorded_at);\n")

    print(f"  Exported {len(park_snapshots)} park snapshots and {len(ride_snapshots)} ride snapshots to {output_path}")


def generate_expected_results(conn, target_date: str, output_dir: Path):
    """Generate expected results for golden data tests by running actual queries."""
    cursor = conn.cursor()

    # The "end of day" in UTC for the target date (11:59:59 PM PST = 7:59:59 AM UTC next day)
    # For "yesterday" queries, we need to be at end of next day looking back
    next_day = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

    # Generate expected parks downtime rankings for "yesterday" period
    # This is what the API would return at end of next day asking for "yesterday"
    cursor.execute("""
        SELECT
            p.park_id,
            p.name as park_name,
            p.is_disney,
            p.is_universal,
            COALESCE(AVG(CASE WHEN pas.park_appears_open = 1 THEN pas.shame_score END), 0) as avg_shame_score,
            COUNT(DISTINCT CASE WHEN pas.park_appears_open = 1 THEN DATE(pas.recorded_at) END) as days_with_data
        FROM parks p
        LEFT JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            AND DATE(pas.recorded_at) = %s
        WHERE EXISTS (
            SELECT 1 FROM park_activity_snapshots pas2
            WHERE pas2.park_id = p.park_id
            AND DATE(pas2.recorded_at) = %s
            AND pas2.park_appears_open = 1
        )
        GROUP BY p.park_id
        HAVING avg_shame_score > 0
        ORDER BY avg_shame_score DESC
        LIMIT 50
    """, (target_date, target_date))

    parks_yesterday = cursor.fetchall()

    # Convert to serializable format
    parks_yesterday_results = {
        "query_date": target_date,
        "period": "yesterday",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parks": [
            {
                "park_id": p['park_id'],
                "park_name": p['park_name'],
                "is_disney": bool(p['is_disney']),
                "is_universal": bool(p['is_universal']),
                "shame_score": round(float(p['avg_shame_score']), 1),
            }
            for p in parks_yesterday
        ]
    }

    expected_path = output_dir / "parks_downtime_yesterday.json"
    with open(expected_path, 'w') as f:
        json.dump(parks_yesterday_results, f, indent=2)
    print(f"  Generated expected results: {expected_path}")

    # Generate ride downtime rankings for yesterday
    cursor.execute("""
        SELECT
            r.ride_id,
            r.name as ride_name,
            r.tier,
            p.name as park_name,
            p.park_id,
            COUNT(*) as total_snapshots,
            SUM(CASE
                WHEN pas.park_appears_open = 1 AND (
                    (p.is_disney = 1 OR p.is_universal = 1) AND rss.status = 'DOWN'
                    OR (p.is_disney = 0 AND p.is_universal = 0) AND rss.status IN ('DOWN', 'CLOSED')
                ) THEN 1 ELSE 0
            END) as down_snapshots
        FROM rides r
        JOIN parks p ON r.park_id = p.park_id
        JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
        JOIN park_activity_snapshots pas ON p.park_id = pas.park_id
            AND pas.recorded_at = rss.recorded_at
        WHERE DATE(rss.recorded_at) = %s
          AND r.is_active = 1
          AND EXISTS (
              SELECT 1 FROM ride_status_snapshots rss2
              JOIN park_activity_snapshots pas2 ON pas2.park_id = r.park_id
                  AND pas2.recorded_at = rss2.recorded_at
              WHERE rss2.ride_id = r.ride_id
                AND DATE(rss2.recorded_at) = %s
                AND pas2.park_appears_open = 1
                AND rss2.computed_is_open = 1
          )
        GROUP BY r.ride_id
        HAVING down_snapshots > 0
        ORDER BY down_snapshots DESC, r.tier ASC
        LIMIT 50
    """, (target_date, target_date))

    rides_yesterday = cursor.fetchall()

    rides_yesterday_results = {
        "query_date": target_date,
        "period": "yesterday",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rides": [
            {
                "ride_id": r['ride_id'],
                "ride_name": r['ride_name'],
                "park_name": r['park_name'],
                "park_id": r['park_id'],
                "tier": int(r['tier']) if r['tier'] else None,
                "down_snapshots": int(r['down_snapshots']),
                "total_snapshots": int(r['total_snapshots']),
            }
            for r in rides_yesterday
        ]
    }

    expected_path = output_dir / "rides_downtime_yesterday.json"
    with open(expected_path, 'w') as f:
        json.dump(rides_yesterday_results, f, indent=2)
    print(f"  Generated expected results: {expected_path}")

    cursor.close()


def main():
    parser = argparse.ArgumentParser(description='Export golden data from development database')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--generate-expected', action='store_true',
                       help='Generate expected results JSON files')
    args = parser.parse_args()

    target_date = args.date

    # Validate date format
    try:
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format '{target_date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    # Setup output directory
    output_dir = Path(__file__).parent.parent / "tests" / "golden_data" / "datasets" / target_date
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_dir = output_dir / "expected"
    expected_dir.mkdir(exist_ok=True)

    print(f"Exporting golden data for {target_date}...")

    try:
        conn = get_db_connection()

        # Export reference data
        export_parks(conn, output_dir / "parks.sql")
        export_rides(conn, output_dir / "rides.sql")

        # Export snapshot data
        export_snapshots(conn, target_date, output_dir / "snapshots.sql")

        # Generate expected results if requested
        if args.generate_expected:
            print("\nGenerating expected results...")
            generate_expected_results(conn, target_date, expected_dir)

        conn.close()
        print(f"\nGolden data exported to: {output_dir}")

    except Error as e:
        print(f"Database error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
