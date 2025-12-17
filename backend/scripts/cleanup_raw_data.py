#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Raw Data Cleanup Script
Safely deletes raw snapshots older than 24 hours after successful aggregation.
Also cleans up weather observations older than 30 days.

Usage:
    python cleanup_raw_data.py [--dry-run] [--force]

Safety features:
- Only deletes data AFTER successful aggregation (checks aggregation_log)
- Uses aggregated_until_ts to determine safe deletion threshold
- Defaults to keeping 48 hours if no successful aggregation found
- Weather observations retained for 30 days
- Provides detailed summary before deletion
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add backend/src to Python path
backend_src = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from database.connection import get_db_connection
from utils.logger import logger
from sqlalchemy import text


def get_safe_deletion_threshold(conn) -> datetime:
    """
    Determine safe timestamp threshold for deletion of ride/park snapshots.

    Returns:
        UTC timestamp - safe to delete snapshots older than this
    """
    # Query most recent successful daily aggregation
    query = text("""
        SELECT MAX(aggregated_until_ts) AS safe_threshold
        FROM aggregation_log
        WHERE aggregation_type = 'daily'
            AND status = 'success'
    """)

    result = conn.execute(query)
    row = result.fetchone()

    if row.safe_threshold:
        # Found successful aggregation - safe to delete up to that point
        logger.info(f"Most recent successful aggregation: {row.safe_threshold}")
        return row.safe_threshold
    else:
        # No successful aggregation found - keep 48 hours as safety buffer
        threshold = datetime.utcnow() - timedelta(hours=48)
        logger.warning(f"No successful aggregation found - using 48-hour safety buffer: {threshold}")
        return threshold


def get_weather_retention_threshold() -> datetime:
    """
    Determine weather data retention threshold.

    Weather observations are retained for 30 days.

    Returns:
        UTC timestamp - safe to delete weather observations older than this
    """
    threshold = datetime.utcnow() - timedelta(days=30)
    logger.info(f"Weather retention threshold: {threshold} (30 days)")
    return threshold


def preview_deletion(conn, threshold: datetime, weather_threshold: datetime) -> dict:
    """
    Preview what will be deleted.

    Args:
        conn: Database connection
        threshold: Deletion threshold for ride/park data
        weather_threshold: Deletion threshold for weather data

    Returns:
        Dictionary with counts
    """
    # Ride status snapshots
    snapshots_query = text("""
        SELECT COUNT(*) AS count
        FROM ride_status_snapshots
        WHERE recorded_at < :threshold
    """)

    result = conn.execute(snapshots_query, {"threshold": threshold})
    snapshots_count = result.fetchone().count

    # Ride status changes
    changes_query = text("""
        SELECT COUNT(*) AS count
        FROM ride_status_changes
        WHERE changed_at < :threshold
    """)

    result = conn.execute(changes_query, {"threshold": threshold})
    changes_count = result.fetchone().count

    # Park activity snapshots
    park_activity_query = text("""
        SELECT COUNT(*) AS count
        FROM park_activity_snapshots
        WHERE recorded_at < :threshold
    """)

    result = conn.execute(park_activity_query, {"threshold": threshold})
    park_activity_count = result.fetchone().count

    # Weather observations
    weather_query = text("""
        SELECT COUNT(*) AS count
        FROM weather_observations
        WHERE observation_time < :threshold
    """)

    result = conn.execute(weather_query, {"threshold": weather_threshold})
    weather_count = result.fetchone().count

    return {
        "ride_status_snapshots": snapshots_count,
        "ride_status_changes": changes_count,
        "park_activity_snapshots": park_activity_count,
        "weather_observations": weather_count,
        "total": snapshots_count + changes_count + park_activity_count + weather_count
    }


def execute_cleanup(conn, threshold: datetime, weather_threshold: datetime) -> dict:
    """
    Execute cleanup of raw data.

    Args:
        conn: Database connection
        threshold: Deletion threshold for ride/park data
        weather_threshold: Deletion threshold for weather data

    Returns:
        Dictionary with deleted counts
    """
    deleted = {}

    # Delete ride status snapshots
    snapshots_delete = text("""
        DELETE FROM ride_status_snapshots
        WHERE recorded_at < :threshold
    """)

    result = conn.execute(snapshots_delete, {"threshold": threshold})
    deleted["ride_status_snapshots"] = result.rowcount
    logger.info(f"Deleted {result.rowcount} ride status snapshots")

    # Delete ride status changes
    changes_delete = text("""
        DELETE FROM ride_status_changes
        WHERE changed_at < :threshold
    """)

    result = conn.execute(changes_delete, {"threshold": threshold})
    deleted["ride_status_changes"] = result.rowcount
    logger.info(f"Deleted {result.rowcount} ride status changes")

    # Delete park activity snapshots
    park_activity_delete = text("""
        DELETE FROM park_activity_snapshots
        WHERE recorded_at < :threshold
    """)

    result = conn.execute(park_activity_delete, {"threshold": threshold})
    deleted["park_activity_snapshots"] = result.rowcount
    logger.info(f"Deleted {result.rowcount} park activity snapshots")

    # Delete weather observations
    weather_delete = text("""
        DELETE FROM weather_observations
        WHERE observation_time < :threshold
    """)

    result = conn.execute(weather_delete, {"threshold": weather_threshold})
    deleted["weather_observations"] = result.rowcount
    logger.info(f"Deleted {result.rowcount} weather observations")

    deleted["total"] = sum(deleted.values())

    return deleted


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Safely cleanup raw snapshot data after aggregation"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt (use with caution)'
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("RAW DATA CLEANUP SCRIPT")
    logger.info("=" * 60)

    with get_db_connection() as conn:
        # Determine safe deletion thresholds
        threshold = get_safe_deletion_threshold(conn)
        weather_threshold = get_weather_retention_threshold()

        logger.info(f"Ride/park data threshold: {threshold}")
        logger.info(f"Weather data threshold: {weather_threshold}")
        logger.info(f"Will delete ride/park data older than: {threshold.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info(f"Will delete weather data older than: {weather_threshold.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        # Preview deletion
        logger.info("")
        logger.info("Previewing deletion...")
        preview = preview_deletion(conn, threshold, weather_threshold)

        logger.info("=" * 60)
        logger.info("DELETION PREVIEW")
        logger.info("=" * 60)
        logger.info(f"Ride status snapshots: {preview['ride_status_snapshots']:,}")
        logger.info(f"Ride status changes: {preview['ride_status_changes']:,}")
        logger.info(f"Park activity snapshots: {preview['park_activity_snapshots']:,}")
        logger.info(f"Weather observations: {preview['weather_observations']:,}")
        logger.info(f"TOTAL records to delete: {preview['total']:,}")
        logger.info("=" * 60)

        if preview['total'] == 0:
            logger.info("No records to delete")
            return 0

        if args.dry_run:
            logger.info("")
            logger.info("DRY RUN - No records deleted")
            return 0

        # Confirm deletion
        if not args.force:
            logger.info("")
            response = input(f"Delete {preview['total']:,} records? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Cleanup cancelled")
                return 0

        # Execute cleanup
        logger.info("")
        logger.info("Executing cleanup...")
        deleted = execute_cleanup(conn, threshold, weather_threshold)

        logger.info("=" * 60)
        logger.info("CLEANUP COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Ride status snapshots deleted: {deleted['ride_status_snapshots']:,}")
        logger.info(f"Ride status changes deleted: {deleted['ride_status_changes']:,}")
        logger.info(f"Park activity snapshots deleted: {deleted['park_activity_snapshots']:,}")
        logger.info(f"Weather observations deleted: {deleted['weather_observations']:,}")
        logger.info(f"TOTAL records deleted: {deleted['total']:,}")
        logger.info("=" * 60)

        return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"Cleanup script failed: {e}", exc_info=True)
        sys.exit(1)
