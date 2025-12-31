#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Park Shame Score Backfill Script

Simple, focused script to backfill park_daily_stats.shame_score values
using the corrected formula:

    shame_score = COALESCE(
        CASE WHEN effective_park_weight > 0
             THEN LEAST((weighted_downtime_hours / effective_park_weight) * 10, 10)
             ELSE 0
        END,
        0
    )

This script:
1. Recalculates weighted_downtime_hours, effective_park_weight, and shame_score
2. Updates park_daily_stats with the corrected values
3. Uses COALESCE to ensure 0 instead of NULL

Usage:
    python -m scripts.backfill_park_shame_scores --days 30
    python -m scripts.backfill_park_shame_scores --all
    python -m scripts.backfill_park_shame_scores --dry-run

Feature 003-orm-refactoring
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from utils.timezone import get_today_pacific
from database.connection import get_db_session
from sqlalchemy import text


def backfill_shame_scores(days: int = None, dry_run: bool = False) -> dict:
    """
    Backfill park_daily_stats.shame_score using corrected formula.

    Args:
        days: Number of days to backfill (None for all)
        dry_run: If True, preview changes without committing

    Returns:
        Dictionary with stats about the backfill
    """
    stats = {
        'total_records': 0,
        'updated_records': 0,
        'null_before': 0,
        'null_after': 0,
        'errors': 0
    }

    with get_db_session() as session:
        # Build date filter
        date_filter = ""
        if days:
            start_date = get_today_pacific() - timedelta(days=days)
            date_filter = f"AND pds.stat_date >= '{start_date}'"

        # Count records before
        count_query = text(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN shame_score IS NULL THEN 1 ELSE 0 END) as null_count
            FROM park_daily_stats pds
            WHERE 1=1 {date_filter}
        """)

        result = session.execute(count_query).first()
        stats['total_records'] = result.total
        stats['null_before'] = result.null_count

        logger.info(f"Found {stats['total_records']} records, {stats['null_before']} with NULL shame_score")

        if dry_run:
            # Show what would be updated
            preview_query = text(f"""
                SELECT
                    p.name as park_name,
                    pds.stat_date,
                    pds.shame_score as old_shame_score,
                    pds.weighted_downtime_hours as old_weighted,
                    pds.effective_park_weight as old_weight,
                    -- Calculate new values
                    COALESCE(
                        SUM((rds.downtime_minutes / 60.0) * COALESCE(rc.tier_weight, 2)),
                        0
                    ) as new_weighted,
                    COALESCE(
                        SUM(
                            CASE WHEN rds.uptime_percentage > 0
                                 THEN COALESCE(rc.tier_weight, 2)
                                 ELSE 0
                            END
                        ),
                        0
                    ) as new_weight,
                    COALESCE(
                        CASE
                            WHEN COALESCE(SUM(CASE WHEN rds.uptime_percentage > 0 THEN COALESCE(rc.tier_weight, 2) ELSE 0 END), 0) > 0
                            THEN LEAST(
                                ROUND(
                                    (COALESCE(SUM((rds.downtime_minutes / 60.0) * COALESCE(rc.tier_weight, 2)), 0) /
                                     COALESCE(SUM(CASE WHEN rds.uptime_percentage > 0 THEN COALESCE(rc.tier_weight, 2) ELSE 0 END), 0)) * 10,
                                    1
                                ),
                                10.0
                            )
                            ELSE 0
                        END,
                        0
                    ) as new_shame_score
                FROM park_daily_stats pds
                JOIN parks p ON pds.park_id = p.park_id
                LEFT JOIN rides r ON r.park_id = pds.park_id
                LEFT JOIN ride_daily_stats rds ON rds.ride_id = r.ride_id AND rds.stat_date = pds.stat_date
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE 1=1 {date_filter}
                GROUP BY pds.stat_id, p.name, pds.stat_date, pds.shame_score,
                         pds.weighted_downtime_hours, pds.effective_park_weight
                HAVING new_shame_score != COALESCE(pds.shame_score, -1)
                ORDER BY pds.stat_date DESC, new_shame_score DESC
                LIMIT 20
            """)

            logger.info("\n=== DRY RUN: Sample of records that would be updated ===")
            results = session.execute(preview_query).fetchall()
            for row in results:
                logger.info(
                    f"  {row.park_name} ({row.stat_date}): "
                    f"shame {row.old_shame_score} -> {row.new_shame_score}, "
                    f"weighted {row.old_weighted} -> {row.new_weighted:.2f}, "
                    f"weight {row.old_weight} -> {row.new_weight:.2f}"
                )

            logger.info(f"\n*** DRY RUN - No changes made. Would update up to {stats['null_before']} records ***")
            return stats

        # Actually update the records using a single UPDATE with subquery
        update_query = text(f"""
            UPDATE park_daily_stats pds
            JOIN (
                SELECT
                    pds2.stat_id,
                    COALESCE(
                        ROUND(SUM((rds.downtime_minutes / 60.0) * COALESCE(rc.tier_weight, 2)), 2),
                        0
                    ) as weighted_downtime_hours,
                    COALESCE(
                        ROUND(SUM(
                            CASE WHEN rds.uptime_percentage > 0
                                 THEN COALESCE(rc.tier_weight, 2)
                                 ELSE 0
                            END
                        ), 2),
                        0
                    ) as effective_park_weight
                FROM park_daily_stats pds2
                JOIN parks p ON pds2.park_id = p.park_id
                LEFT JOIN rides r ON r.park_id = pds2.park_id
                LEFT JOIN ride_daily_stats rds ON rds.ride_id = r.ride_id AND rds.stat_date = pds2.stat_date
                LEFT JOIN ride_classifications rc ON r.ride_id = rc.ride_id
                WHERE 1=1 {date_filter.replace('pds.', 'pds2.')}
                GROUP BY pds2.stat_id
            ) calc ON pds.stat_id = calc.stat_id
            SET
                pds.weighted_downtime_hours = calc.weighted_downtime_hours,
                pds.effective_park_weight = calc.effective_park_weight,
                pds.shame_score = COALESCE(
                    CASE
                        WHEN calc.effective_park_weight > 0
                        THEN LEAST(
                            ROUND((calc.weighted_downtime_hours / calc.effective_park_weight) * 10, 1),
                            10.0
                        )
                        ELSE 0
                    END,
                    0
                )
        """)

        result = session.execute(update_query)
        stats['updated_records'] = result.rowcount

        # Commit the changes
        session.commit()

        # Count NULL records after
        result = session.execute(count_query).first()
        stats['null_after'] = result.null_count

        logger.info(f"Updated {stats['updated_records']} records")
        logger.info(f"NULL shame_scores: {stats['null_before']} -> {stats['null_after']}")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Backfill park_daily_stats.shame_score with corrected formula',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --days 30            # Backfill last 30 days
  %(prog)s --all                # Backfill all historical data
  %(prog)s --days 30 --dry-run  # Preview changes without committing
        """
    )

    parser.add_argument(
        '--days',
        type=int,
        help='Number of days to backfill (from yesterday backwards)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Backfill all historical data'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without committing to database'
    )

    args = parser.parse_args()

    if not args.days and not args.all:
        parser.error("Must specify --days N or --all")

    days = None if args.all else args.days

    logger.info("=" * 60)
    logger.info("PARK SHAME SCORE BACKFILL")
    logger.info("=" * 60)
    logger.info(f"Days to backfill: {'ALL' if days is None else days}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 60)

    try:
        stats = backfill_shame_scores(days=days, dry_run=args.dry_run)

        logger.info("")
        logger.info("=" * 60)
        logger.info("BACKFILL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records: {stats['total_records']}")
        logger.info(f"Updated: {stats['updated_records']}")
        logger.info(f"NULL before: {stats['null_before']}")
        logger.info(f"NULL after: {stats['null_after']}")
        logger.info("=" * 60)

        if stats['null_after'] > 0:
            logger.warning(f"WARNING: {stats['null_after']} records still have NULL shame_score")
        else:
            logger.info("SUCCESS: All shame_scores are now populated")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
