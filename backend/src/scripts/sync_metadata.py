#!/usr/bin/env python3
"""
Sync Metadata Script

Syncs entity metadata from ThemeParks.wiki API to the local database.
Collects coordinates, indoor/outdoor classification, height requirements, and tags.

Feature: 004-themeparks-data-collection
Task: T052

Usage:
    # Sync all rides with themeparks_wiki_id
    python -m scripts.sync_metadata

    # Sync a specific park's rides
    python -m scripts.sync_metadata --park-uuid <uuid>

    # Show coverage statistics only
    python -m scripts.sync_metadata --coverage

    # Dry run (don't save changes)
    python -m scripts.sync_metadata --dry-run
"""

import argparse
import sys

from database.connection import get_db_session
from collector.metadata_collector import MetadataCollector
from utils.logger import setup_logger

logger = setup_logger(__name__)


def sync_metadata(
    session,
    park_uuid: str = None,
    dry_run: bool = False
) -> dict:
    """
    Sync metadata from ThemeParks.wiki API.

    Args:
        session: Database session
        park_uuid: Optional park UUID to sync (syncs all if None)
        dry_run: If True, don't commit changes

    Returns:
        Dict with sync statistics
    """
    collector = MetadataCollector(session)

    if park_uuid:
        logger.info(f"Syncing metadata for park {park_uuid}")
        stats = collector.sync_park_metadata(park_uuid)
    else:
        logger.info("Syncing metadata for all rides")
        stats = collector.sync_all_metadata()

    if not dry_run:
        session.commit()
        logger.info(f"Committed {stats['synced']} metadata records")
    else:
        session.rollback()
        logger.info(f"Dry run: would have synced {stats['synced']} records")

    return stats


def show_coverage(session) -> dict:
    """
    Show metadata coverage statistics.

    Args:
        session: Database session

    Returns:
        Coverage statistics dict
    """
    collector = MetadataCollector(session)
    return collector.get_coverage_stats()


def main():
    parser = argparse.ArgumentParser(
        description="Sync entity metadata from ThemeParks.wiki API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--park-uuid',
        type=str,
        metavar='UUID',
        help='Sync metadata for a specific park (ThemeParks.wiki UUID)'
    )
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Show coverage statistics only (no sync)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without committing changes'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    try:
        with get_db_session() as session:
            if args.coverage:
                # Just show coverage
                stats = show_coverage(session)

                print("\n=== Metadata Coverage ===")
                print(f"Total rides with themeparks_wiki_id: {stats['total_rides']}")
                print(f"Rides with metadata: {stats['with_metadata']} ({stats['metadata_coverage_pct']:.1f}%)")
                print(f"Rides with coordinates: {stats['with_coordinates']} ({stats['coordinate_coverage_pct']:.1f}%)")
                print(f"Rides with height requirements: {stats['with_height_requirement']}")
                print(f"Rides with indoor/outdoor: {stats['with_indoor_outdoor']}")

                return 0

            # Run sync
            stats = sync_metadata(
                session,
                park_uuid=args.park_uuid,
                dry_run=args.dry_run
            )

            print("\n=== Sync Results ===")
            print(f"Synced: {stats['synced']}")
            print(f"Skipped: {stats['skipped']}")
            print(f"Failed: {stats['failed']}")

            if args.dry_run:
                print("\n(Dry run - no changes committed)")

            # Show coverage after sync
            coverage = show_coverage(session)
            print(f"\nMetadata coverage: {coverage['metadata_coverage_pct']:.1f}%")
            print(f"Coordinate coverage: {coverage['coordinate_coverage_pct']:.1f}%")

            return 0 if stats['failed'] == 0 else 1

    except Exception as e:
        logger.exception(f"Metadata sync failed: {e}")
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
