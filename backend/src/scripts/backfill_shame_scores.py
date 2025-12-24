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

from sqlalchemy import select, update, func, or_

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from database.connection import get_db_session
from utils.logger import logger
from models import Park, Ride, RideClassification, RideStatusSnapshot, ParkActivitySnapshot


def get_snapshots_to_backfill(session, batch_size: int = 1000):
    """
    Get park_activity_snapshots that need shame_score backfilled.

    Returns snapshots with shame_score IS NULL, ordered by recorded_at DESC
    (most recent first for faster verification of results).
    """
    stmt = (
        select(
            ParkActivitySnapshot.snapshot_id,
            ParkActivitySnapshot.park_id,
            ParkActivitySnapshot.recorded_at,
            ParkActivitySnapshot.park_appears_open,
            Park.name.label('park_name')
        )
        .join(Park, ParkActivitySnapshot.park_id == Park.park_id)
        .where(ParkActivitySnapshot.shame_score.is_(None))
        .order_by(ParkActivitySnapshot.recorded_at.desc())
        .limit(batch_size)
    )

    result = session.execute(stmt)
    return [dict(row._mapping) for row in result]


def calculate_historical_shame_score(session, park_id: int, snapshot_time: datetime) -> float:
    """
    Calculate shame score for a historical snapshot.

    This mimics the formula in collect_snapshots.py:calculate_shame_score()
    but works with historical data by looking at the ride_status_snapshots
    recorded at the same time.

    Args:
        session: Database session
        park_id: Park ID
        snapshot_time: The recorded_at timestamp of the park_activity_snapshot

    Returns:
        Calculated shame score (0.0-10.0), or 0.0 if no eligible rides
    """
    # Step 1: Get effective park weight (rides that operated in 7 days PRIOR to snapshot)
    seven_days_before = snapshot_time - timedelta(days=7)

    effective_stmt = (
        select(
            func.coalesce(
                func.sum(
                    func.coalesce(RideClassification.tier_weight, 2)
                ),
                0
            ).label('effective_weight')
        )
        .select_from(Ride)
        .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
        .where(Ride.park_id == park_id)
        .where(Ride.is_active == True)
        .where(Ride.category == 'ATTRACTION')
        .where(Ride.last_operated_at.isnot(None))
        .where(Ride.last_operated_at >= seven_days_before)
        .where(Ride.last_operated_at <= snapshot_time)
    )
    effective_result = session.execute(effective_stmt)
    effective_weight = effective_result.scalar() or 0

    # Zero-denominator protection
    if not effective_weight:
        # Fallback: use full roster weight if no rides have last_operated_at set
        # This handles historical data before last_operated_at was populated
        fallback_stmt = (
            select(
                func.coalesce(
                    func.sum(
                        func.coalesce(RideClassification.tier_weight, 2)
                    ),
                    0
                ).label('total_weight')
            )
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
        )
        fallback_result = session.execute(fallback_stmt)
        effective_weight = fallback_result.scalar() or 0

        if not effective_weight:
            return 0.0

    # Step 2: Get down weight (rides that were DOWN at that exact snapshot time)
    # For ThemeParks.wiki data: status = 'DOWN'
    # For Queue-Times data: computed_is_open = FALSE (they don't distinguish DOWN vs CLOSED)
    # We need to use the snapshot from the same recorded_at timestamp
    down_stmt = (
        select(
            func.coalesce(
                func.sum(
                    func.coalesce(RideClassification.tier_weight, 2)
                ),
                0
            ).label('down_weight')
        )
        .select_from(Ride)
        .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
        .join(RideStatusSnapshot, Ride.ride_id == RideStatusSnapshot.ride_id)
        .where(Ride.park_id == park_id)
        .where(Ride.is_active == True)
        .where(Ride.category == 'ATTRACTION')
        .where(RideStatusSnapshot.recorded_at == snapshot_time)
        .where(
            or_(
                RideStatusSnapshot.status == 'DOWN',
                (RideStatusSnapshot.status.is_(None) & (RideStatusSnapshot.computed_is_open == False))
            )
        )
    )
    down_result = session.execute(down_stmt)
    down_weight = down_result.scalar() or 0

    # Step 3: Calculate shame score
    if down_weight == 0:
        return 0.0

    shame_score = round((down_weight / effective_weight) * 10, 1)
    return min(shame_score, 10.0)  # Cap at 10.0


def backfill_batch(session, snapshots: list, dry_run: bool = False) -> dict:
    """
    Backfill shame scores for a batch of snapshots.

    Args:
        session: Database session
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
                    stmt = (
                        update(ParkActivitySnapshot)
                        .where(ParkActivitySnapshot.snapshot_id == snapshot_id)
                        .values(shame_score=0.0)
                    )
                    session.execute(stmt)
                stats["skipped_closed"] += 1
                continue

            # Calculate shame score for this snapshot
            shame_score = calculate_historical_shame_score(session, park_id, recorded_at)

            if not dry_run:
                stmt = (
                    update(ParkActivitySnapshot)
                    .where(ParkActivitySnapshot.snapshot_id == snapshot_id)
                    .values(shame_score=shame_score)
                )
                session.execute(stmt)

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

    with get_db_session() as session:
        # Check how many snapshots need backfilling
        count_stmt = (
            select(func.count())
            .select_from(ParkActivitySnapshot)
            .where(ParkActivitySnapshot.shame_score.is_(None))
        )
        count_result = session.execute(count_stmt)
        total_to_process = count_result.scalar()
        logger.info(f"Total snapshots to backfill: {total_to_process}")

        if total_to_process == 0:
            logger.info("No snapshots need backfilling. Done!")
            return

        # Process in batches
        while True:
            snapshots = get_snapshots_to_backfill(session, args.batch_size)

            if not snapshots:
                break

            total_stats["batches"] += 1
            logger.info(f"\nProcessing batch {total_stats['batches']} ({len(snapshots)} snapshots)...")

            batch_stats = backfill_batch(session, snapshots, args.dry_run)

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
