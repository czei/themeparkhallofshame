#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Ride Classification CLI
Runs ride classification on all unclassified rides.

Usage:
    python classify_rides.py [--all] [--park-id PARK_ID] [--dry-run] [--max-concurrent N]

Options:
    --all                Re-classify ALL rides (ignores existing classifications)
    --park-id PARK_ID    Only classify rides for specific park
    --dry-run            Show what would be classified without saving
    --max-concurrent N   Maximum concurrent AI requests (default: 5)
"""

import sys
import os
import argparse
from pathlib import Path
from typing import Optional

# Add backend/src to Python path
backend_src = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from classifier.classification_service import ClassificationService
from database.connection import get_db_connection
from utils.logger import logger
from sqlalchemy import text


def get_unclassified_rides(park_id: Optional[int] = None, classify_all: bool = False):
    """
    Fetch rides that need classification.

    Args:
        park_id: Optional park ID to filter
        classify_all: If True, get all rides regardless of classification status

    Returns:
        List of ride dictionaries
    """
    park_filter = "AND r.park_id = :park_id" if park_id else ""
    classification_filter = "" if classify_all else "AND rc.classification_id IS NULL"

    query = text(f"""
        SELECT
            r.ride_id,
            r.name AS ride_name,
            r.park_id,
            p.name AS park_name,
            CONCAT(p.city, ', ', p.state_province) AS park_location,
            p.country
        FROM rides r
        INNER JOIN parks p ON r.park_id = p.park_id
        LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
        WHERE r.is_active = TRUE
            AND p.is_active = TRUE
            {classification_filter}
            {park_filter}
        ORDER BY p.park_id, r.name
    """)

    params = {}
    if park_id:
        params['park_id'] = park_id

    with get_db_connection() as conn:
        result = conn.execute(query, params)
        return [dict(row._mapping) for row in result]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Classify rides into tiers (1=major, 2=standard, 3=minor)"
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Re-classify all rides (ignores existing classifications)'
    )
    parser.add_argument(
        '--park-id',
        type=int,
        help='Only classify rides for specific park ID'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be classified without saving to database'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent AI requests (default: 5)'
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("RIDE CLASSIFICATION SCRIPT")
    logger.info("=" * 60)

    # Fetch rides to classify
    logger.info(f"Fetching rides to classify (all={args.all}, park_id={args.park_id})...")
    rides = get_unclassified_rides(park_id=args.park_id, classify_all=args.all)

    if not rides:
        logger.info("No rides to classify!")
        return 0

    logger.info(f"Found {len(rides)} rides to classify")

    # Initialize classification service
    base_dir = Path(__file__).parent.parent.parent  # Project root
    manual_overrides_path = base_dir / 'data' / 'manual_overrides.csv'
    exact_matches_path = base_dir / 'data' / 'exact_matches.json'
    working_directory = str(base_dir.absolute())

    classifier = ClassificationService(
        manual_overrides_path=str(manual_overrides_path),
        exact_matches_path=str(exact_matches_path),
        working_directory=working_directory
    )

    # Classify rides
    logger.info(f"Classifying {len(rides)} rides (max_concurrent={args.max_concurrent})...")
    results = classifier.classify_batch(rides, max_concurrent_ai=args.max_concurrent)

    # Display results
    logger.info("=" * 60)
    logger.info("CLASSIFICATION RESULTS")
    logger.info("=" * 60)

    tier_counts = {1: 0, 2: 0, 3: 0}
    method_counts = {}
    flagged_count = 0

    for result in results:
        tier_counts[result.tier] += 1
        method_counts[result.classification_method] = method_counts.get(result.classification_method, 0) + 1

        if result.flagged_for_review:
            flagged_count += 1

        logger.info(
            f"  {result.ride_name} ({result.park_name})\n"
            f"    → Tier {result.tier} ({result.tier_weight}x weight)\n"
            f"    → Method: {result.classification_method}\n"
            f"    → Confidence: {result.confidence_score:.2f}\n"
            f"    → Reasoning: {result.reasoning_text}\n"
            f"    → Flagged: {result.flagged_for_review}"
        )

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total classified: {len(results)}")
    logger.info(f"  Tier 1 (major, 3x weight): {tier_counts[1]}")
    logger.info(f"  Tier 2 (standard, 2x weight): {tier_counts[2]}")
    logger.info(f"  Tier 3 (minor, 1x weight): {tier_counts[3]}")
    logger.info("")
    logger.info("Classification methods:")
    for method, count in method_counts.items():
        logger.info(f"  {method}: {count}")
    logger.info("")
    logger.info(f"Flagged for review (confidence < 0.50): {flagged_count}")

    # Save to database (unless dry-run)
    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN - No changes saved to database")
    else:
        logger.info("")
        logger.info("Saving classifications to database...")
        for result in results:
            classifier.save_classification(result)
        logger.info(f"✓ Saved {len(results)} classifications")

    logger.info("=" * 60)
    logger.info("CLASSIFICATION COMPLETE")
    logger.info("=" * 60)

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"Classification failed: {e}", exc_info=True)
        sys.exit(1)
