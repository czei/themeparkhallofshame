#!/usr/bin/env python3
"""
Backfill Shame Scores for Historical park_activity_snapshots
============================================================

Calculates and populates shame_score for existing park_activity_snapshots
that have NULL values. This is needed after adding the shame_score column
to ensure historical data has the correct scores.

Formula (same as collect_snapshots.py):
    shame_score = (down_weight / effective_park_weight) * 10

Where:
- down_weight: Sum of tier_weights for rides that were DOWN at snapshot time
- effective_park_weight: Sum of tier_weights for rides that operated in 7 days
  PRIOR to the snapshot time

Usage:
    cd backend
    PYTHONPATH=src python -m scripts.backfill_shame_scores [--dry-run] [--batch-size=1000]
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

from sqlalchemy import text

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from database.connection import get_db_connection
from utils.logger import logger


def get_snapshots_to_backfill(conn, batch_size: int = 1000):
    """
    Get park_activity_snapshots that need shame_score backfilled.

    Returns snapshots with shame_score IS NULL, ordered by recorded_at DESC
    (most recent first for faster verification of results).
    """
    result = conn.execute(text("""
        SELECT
            pas.snapshot_id,
            pas.park_id,
            pas.recorded_at,
            pas.park_appears_open,
            p.name as park_name
        FROM park_activity_snapshots pas
        JOIN parks p ON pas.park_id = p.park_id
        WHERE pas.shame_score IS NULL
        ORDER BY pas.recorded_at DESC
        LIMIT :batch_size
    """), {"batch_size": batch_size})

    return [dict(row._mapping) for row in result]


def calculate_historical_shame_score(conn, park_id: int, snapshot_time: datetime) -> float:
    """
    Calculate shame score for a historical snapshot.

    This mimics the formula in collect_snapshots.py:calculate_shame_score()
    but works with historical data by looking at the ride_status_snapshots
    recorded at the same time.

    Args:
        conn: Database connection
        park_id: Park ID
        snapshot_time: The recorded_at timestamp of the park_activity_snapshot

    Returns:
        Calculated shame score (0.0-10.0), or 0.0 if no eligible rides
    """
    # Step 1: Get effective park weight (rides that operated in 7 days PRIOR to snapshot)
    seven_days_before = snapshot_time - timedelta(days=7)

    effective_result = conn.execute(text("""
        SELECT COALESCE(SUM(COALESCE(rc.tier_weight, 2)), 0) AS effective_weight
        FROM rides r
        LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        WHERE r.park_id = :park_id
          AND r.is_active = TRUE
          AND r.category = 'ATTRACTION'
          AND r.last_operated_at IS NOT NULL
          AND r.last_operated_at >= :seven_days_before
          AND r.last_operated_at <= :snapshot_time
    """), {
        "park_id": park_id,
        "seven_days_before": seven_days_before,
        "snapshot_time": snapshot_time
    })
    effective_weight = effective_result.scalar() or 0

    # Zero-denominator protection
    if not effective_weight:
        # Fallback: use full roster weight if no rides have last_operated_at set
        # This handles historical data before last_operated_at was populated
        fallback_result = conn.execute(text("""
            SELECT COALESCE(SUM(COALESCE(rc.tier_weight, 2)), 0) AS total_weight
            FROM rides r
            LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
            WHERE r.park_id = :park_id
              AND r.is_active = TRUE
              AND r.category = 'ATTRACTION'
        """), {"park_id": park_id})
        effective_weight = fallback_result.scalar() or 0

        if not effective_weight:
            return 0.0

    # Step 2: Get down weight (rides that were DOWN at that exact snapshot time)
    # For ThemeParks.wiki data: status = 'DOWN'
    # For Queue-Times data: computed_is_open = FALSE (they don't distinguish DOWN vs CLOSED)
    # We need to use the snapshot from the same recorded_at timestamp
    down_result = conn.execute(text("""
        SELECT COALESCE(SUM(COALESCE(rc.tier_weight, 2)), 0) AS down_weight
        FROM rides r
        LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        INNER JOIN ride_status_snapshots rss ON r.ride_id = rss.ride_id
        WHERE r.park_id = :park_id
          AND r.is_active = TRUE
          AND r.category = 'ATTRACTION'
          AND rss.recorded_at = :snapshot_time
          AND (
              rss.status = 'DOWN'
              OR (rss.status IS NULL AND rss.computed_is_open = FALSE)
          )
    """), {
        "park_id": park_id,
        "snapshot_time": snapshot_time
    })
    down_weight = down_result.scalar() or 0

    # Step 3: Calculate shame score
    if down_weight == 0:
        return 0.0

    shame_score = round((down_weight / effective_weight) * 10, 1)
    return min(shame_score, 10.0)  # Cap at 10.0


def backfill_batch(conn, snapshots: list, dry_run: bool = False) -> dict:
    """
    Backfill shame scores for a batch of snapshots.

    Args:
        conn: Database connection
        snapshots: List of snapshot dicts to process
        dry_run: If True, don't actually update the database

    Returns:
        Stats dict with counts
    """
    stats = {
        "processed": 0,
        "updated": 0,
        "skipped_closed": 0,
        "errors": 0
    }

    for snap in snapshots:
        try:
            snapshot_id = snap["snapshot_id"]
            park_id = snap["park_id"]
            recorded_at = snap["recorded_at"]
            park_name = snap["park_name"]
            park_appears_open = snap["park_appears_open"]

            stats["processed"] += 1

            # Skip closed parks - they should have shame_score = 0 or NULL
            if not park_appears_open:
                if not dry_run:
                    conn.execute(text("""
                        UPDATE park_activity_snapshots
                        SET shame_score = 0.0
                        WHERE snapshot_id = :snapshot_id
                    """), {"snapshot_id": snapshot_id})
                stats["skipped_closed"] += 1
                continue

            # Calculate shame score for this snapshot
            shame_score = calculate_historical_shame_score(conn, park_id, recorded_at)

            if not dry_run:
                conn.execute(text("""
                    UPDATE park_activity_snapshots
                    SET shame_score = :shame_score
                    WHERE snapshot_id = :snapshot_id
                """), {"snapshot_id": snapshot_id, "shame_score": shame_score})

            stats["updated"] += 1

            if stats["processed"] % 100 == 0:
                logger.info(f"  Processed {stats['processed']} snapshots...")

        except Exception as e:
            logger.error(f"Error processing snapshot {snap.get('id')}: {e}")
            stats["errors"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill shame scores for historical snapshots")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually update database")
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of snapshots per batch")
    parser.add_argument("--max-batches", type=int, default=0, help="Max batches to process (0=all)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("SHAME SCORE BACKFILL")
    logger.info("=" * 60)

    if args.dry_run:
        logger.info("DRY RUN MODE - No database changes will be made")

    total_stats = {
        "processed": 0,
        "updated": 0,
        "skipped_closed": 0,
        "errors": 0,
        "batches": 0
    }

    with get_db_connection() as conn:
        # Check how many snapshots need backfilling
        count_result = conn.execute(text("""
            SELECT COUNT(*) FROM park_activity_snapshots WHERE shame_score IS NULL
        """))
        total_to_process = count_result.scalar()
        logger.info(f"Total snapshots to backfill: {total_to_process}")

        if total_to_process == 0:
            logger.info("No snapshots need backfilling. Done!")
            return

        # Process in batches
        while True:
            snapshots = get_snapshots_to_backfill(conn, args.batch_size)

            if not snapshots:
                break

            total_stats["batches"] += 1
            logger.info(f"\nProcessing batch {total_stats['batches']} ({len(snapshots)} snapshots)...")

            batch_stats = backfill_batch(conn, snapshots, args.dry_run)

            for key in ["processed", "updated", "skipped_closed", "errors"]:
                total_stats[key] += batch_stats[key]

            logger.info(f"  Batch complete: {batch_stats['updated']} updated, "
                       f"{batch_stats['skipped_closed']} skipped (closed), "
                       f"{batch_stats['errors']} errors")

            # Check if we've hit max batches
            if args.max_batches > 0 and total_stats["batches"] >= args.max_batches:
                logger.info(f"Reached max batches limit ({args.max_batches})")
                break

            # If we got fewer than batch_size, we're done
            if len(snapshots) < args.batch_size:
                break

    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total batches:        {total_stats['batches']}")
    logger.info(f"Total processed:      {total_stats['processed']}")
    logger.info(f"Total updated:        {total_stats['updated']}")
    logger.info(f"Total skipped:        {total_stats['skipped_closed']}")
    logger.info(f"Total errors:         {total_stats['errors']}")


if __name__ == "__main__":
    main()
