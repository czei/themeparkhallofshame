#!/usr/bin/env python3
"""
Theme Park Data Warehouse - Historical Import CLI
Imports historical wait time data from archive.themeparks.wiki S3 bucket.

Usage:
    # Import all parks
    python -m scripts.import_historical --all-parks

    # Import specific destination
    python -m scripts.import_historical --destination <uuid>

    # Import with date range
    python -m scripts.import_historical --destination <uuid> --start-date 2020-01-01 --end-date 2025-12-31

    # Resume interrupted import
    python -m scripts.import_historical --resume

    # List available destinations
    python -m scripts.import_historical --list-destinations

    # Check import status
    python -m scripts.import_historical --status <import_id>

Feature: 004-themeparks-data-collection
Task: T027
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from database.connection import get_db_session
from importer import ArchiveImporter, ImportProgress, ImportResult
from database.repositories.import_repository import ImportRepository


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def print_progress(progress: ImportProgress) -> None:
    """Print progress update to console."""
    pct = progress.percent_complete
    bar_width = 40
    filled = int(bar_width * pct / 100)
    bar = '=' * filled + '-' * (bar_width - filled)

    print(f"\r[{bar}] {pct:.1f}% | Files: {progress.files_processed}/{progress.files_total} | "
          f"Records: {progress.records_imported:,} | Errors: {progress.errors_encountered} | "
          f"Date: {progress.current_date}", end='', flush=True)


def list_destinations(importer: ArchiveImporter) -> None:
    """List all available destinations in the archive."""
    print("Listing destinations from archive.themeparks.wiki...")
    destinations = importer.list_destinations()

    print(f"\nFound {len(destinations)} destinations:\n")
    for dest in sorted(destinations):
        print(f"  {dest}")

    print(f"\nTotal: {len(destinations)} destinations")


def check_status(import_id: str, session) -> None:
    """Check status of an import."""
    repo = ImportRepository(session)
    checkpoint = repo.get_by_import_id(import_id)

    if not checkpoint:
        print(f"Import not found: {import_id}")
        return

    print(f"\n{'='*60}")
    print(f"Import ID:     {checkpoint.import_id}")
    print(f"Destination:   {checkpoint.destination_uuid}")
    print(f"Status:        {checkpoint.status}")
    print(f"Records:       {checkpoint.records_imported:,}")
    print(f"Errors:        {checkpoint.errors_encountered}")
    print(f"Last Date:     {checkpoint.last_processed_date}")
    print(f"Last File:     {checkpoint.last_processed_file}")
    print(f"Started:       {checkpoint.started_at}")
    print(f"Completed:     {checkpoint.completed_at}")
    print(f"{'='*60}")


def list_active_imports(session) -> None:
    """List all active imports."""
    repo = ImportRepository(session)
    active = repo.get_active_imports()

    if not active:
        print("No active imports found.")
        return

    print(f"\nActive imports ({len(active)}):\n")
    print(f"{'Import ID':<20} {'Destination':<40} {'Status':<12} {'Records':>12}")
    print("-" * 90)

    for cp in active:
        dest = cp.destination_uuid[:36] + '...' if len(cp.destination_uuid) > 36 else cp.destination_uuid
        print(f"{cp.import_id:<20} {dest:<40} {cp.status:<12} {cp.records_imported:>12,}")


def import_destination(
    importer: ArchiveImporter,
    destination_uuid: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    resume: bool = True
) -> ImportResult:
    """Import a single destination."""
    print(f"\nImporting destination: {destination_uuid}")
    if start_date:
        print(f"Start date: {start_date}")
    if end_date:
        print(f"End date: {end_date}")
    print()

    result = importer.import_destination(
        destination_uuid=destination_uuid,
        start_date=start_date,
        end_date=end_date,
        resume=resume
    )

    print()  # New line after progress bar
    return result


def import_all_destinations(
    importer: ArchiveImporter,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> None:
    """Import all destinations."""
    destinations = importer.list_destinations()
    print(f"\nImporting {len(destinations)} destinations...")

    total_records = 0
    total_errors = 0
    failed = []

    for i, dest_uuid in enumerate(destinations, 1):
        print(f"\n[{i}/{len(destinations)}] Processing {dest_uuid}")

        try:
            result = importer.import_destination(
                destination_uuid=dest_uuid,
                start_date=start_date,
                end_date=end_date,
                resume=True
            )
            print()  # New line after progress bar
            total_records += result.records_imported
            total_errors += result.errors_encountered

            print(f"  Imported: {result.records_imported:,} records, {result.errors_encountered} errors")

        except Exception as e:
            logger.exception(f"Failed to import {dest_uuid}")
            failed.append((dest_uuid, str(e)))
            print(f"  FAILED: {e}")

    print(f"\n{'='*60}")
    print("Import Complete")
    print(f"{'='*60}")
    print(f"Total records: {total_records:,}")
    print(f"Total errors: {total_errors}")
    print(f"Failed destinations: {len(failed)}")

    if failed:
        print("\nFailed destinations:")
        for dest, error in failed:
            print(f"  {dest}: {error}")


def main():
    parser = argparse.ArgumentParser(
        description="Import historical wait time data from archive.themeparks.wiki"
    )

    # Mode selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--all-parks',
        action='store_true',
        help='Import all destinations'
    )
    group.add_argument(
        '--destination',
        type=str,
        help='Import specific destination UUID'
    )
    group.add_argument(
        '--resume',
        type=str,
        nargs='?',
        const='auto',
        help='Resume import (optionally specify import_id)'
    )
    group.add_argument(
        '--list-destinations',
        action='store_true',
        help='List available destinations'
    )
    group.add_argument(
        '--list-active',
        action='store_true',
        help='List active imports'
    )
    group.add_argument(
        '--status',
        type=str,
        help='Check status of import by import_id'
    )
    group.add_argument(
        '--pause',
        type=str,
        help='Pause import by import_id'
    )
    group.add_argument(
        '--cancel',
        type=str,
        help='Cancel import by import_id'
    )

    # Date filters
    parser.add_argument(
        '--start-date',
        type=parse_date,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=parse_date,
        help='End date (YYYY-MM-DD)'
    )

    # Options
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Records per batch (default: 10000)'
    )
    parser.add_argument(
        '--auto-create',
        action='store_true',
        help='Automatically create new entities for unmatched UUIDs'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh import (ignore existing checkpoints)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without importing'
    )

    args = parser.parse_args()

    with get_db_session() as session:
        importer = ArchiveImporter(
            session=session,
            batch_size=args.batch_size,
            auto_create_entities=args.auto_create,
            progress_callback=print_progress
        )

        if args.list_destinations:
            list_destinations(importer)
            return

        if args.list_active:
            list_active_imports(session)
            return

        if args.status:
            check_status(args.status, session)
            return

        if args.pause:
            if importer.pause_import(args.pause):
                print(f"Import {args.pause} paused")
            else:
                print(f"Could not pause import {args.pause} (not in progress)")
            return

        if args.cancel:
            if importer.cancel_import(args.cancel):
                print(f"Import {args.cancel} cancelled")
            else:
                print(f"Could not cancel import {args.cancel}")
            return

        if args.resume:
            if args.resume == 'auto':
                # Find any resumable import
                repo = ImportRepository(session)
                active = repo.get_active_imports()
                if not active:
                    print("No active imports to resume")
                    return
                for cp in active:
                    if cp.can_resume:
                        result = importer.resume_import(cp.import_id)
                        if result:
                            print(f"\nResumed import: {result.records_imported:,} records")
                            return
                print("No resumable imports found")
            else:
                result = importer.resume_import(args.resume)
                if result:
                    print(f"\nResumed import: {result.records_imported:,} records")
                else:
                    print(f"Could not resume import {args.resume}")
            return

        if args.dry_run:
            if args.destination:
                files = importer.list_files_for_destination(
                    args.destination,
                    args.start_date,
                    args.end_date
                )
                print(f"\nFound {len(files)} files for {args.destination}:")
                for f in files[:20]:
                    print(f"  {f}")
                if len(files) > 20:
                    print(f"  ... and {len(files) - 20} more")
            return

        # Start import
        start_time = datetime.now()

        if args.all_parks:
            import_all_destinations(
                importer,
                start_date=args.start_date,
                end_date=args.end_date
            )
        elif args.destination:
            result = import_destination(
                importer,
                destination_uuid=args.destination,
                start_date=args.start_date,
                end_date=args.end_date,
                resume=not args.no_resume
            )

            print(f"\n{'='*60}")
            print(f"Import Complete: {result.import_id}")
            print(f"{'='*60}")
            print(f"Status:        {result.status}")
            print(f"Records:       {result.records_imported:,}")
            print(f"Errors:        {result.errors_encountered}")
            print(f"Files:         {result.files_processed}")
            print(f"Duration:      {result.duration_seconds:.1f}s")

            if result.quality_summary:
                print("\nQuality Issues:")
                for issue_type, count in result.quality_summary.items():
                    print(f"  {issue_type}: {count}")

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\nTotal time: {elapsed:.1f}s")


if __name__ == '__main__':
    main()
