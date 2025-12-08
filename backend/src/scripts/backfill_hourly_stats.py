#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Hourly Stats Backfill Script
Populates historical hourly aggregation tables from existing snapshot data.

This script processes hours in reverse chronological order (newest first) to
quickly populate recent data for immediate use while continuing to backfill
older historical data in the background.

Usage:
    python -m scripts.backfill_hourly_stats [--days N] [--start YYYY-MM-DD] [--end YYYY-MM-DD]

Options:
    --days N         Backfill last N days (default: 7)
    --start DATE     Start date (inclusive, default: 7 days ago)
    --end DATE       End date (exclusive, default: now)
    --batch-size N   Number of hours to process per batch (default: 24)

Examples:
    # Backfill last 7 days
    python -m scripts.backfill_hourly_stats

    # Backfill last 30 days
    python -m scripts.backfill_hourly_stats --days 30

    # Backfill specific date range
    python -m scripts.backfill_hourly_stats --start 2025-11-01 --end 2025-12-01

Performance:
    - Processes newest hours first for immediate value
    - Uses same logic as aggregate_hourly.py
    - Skips hours already aggregated (idempotent)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from database.connection import get_db_connection
from sqlalchemy import text

# Import the HourlyAggregator class from aggregate_hourly
try:
    from scripts.aggregate_hourly import HourlyAggregator
except ImportError:
    # Alternative import path if running from different location
    from aggregate_hourly import HourlyAggregator


class HourlyBackfiller:
    """
    Backfills hourly statistics for historical data.

    Strategy:
    =========
    1. Process hours in REVERSE chronological order (newest first)
    2. Skip hours already aggregated (check park_hourly_stats table)
    3. Use HourlyAggregator for consistency with real-time aggregation
    4. Process in batches to show progress and enable resumption
    """

    def __init__(self, start_date: datetime, end_date: datetime, batch_size: int = 24):
        """
        Initialize backfiller.

        Args:
            start_date: Start of backfill range (inclusive)
            end_date: End of backfill range (exclusive)
            batch_size: Number of hours to process per batch
        """
        self.start_date = start_date.replace(minute=0, second=0, microsecond=0)
        self.end_date = end_date.replace(minute=0, second=0, microsecond=0)
        self.batch_size = batch_size

        # Calculate total hours to process
        self.total_hours = int((self.end_date - self.start_date).total_seconds() / 3600)

        self.stats = {
            'hours_processed': 0,
            'hours_skipped': 0,
            'hours_failed': 0,
            'parks_aggregated': 0,
            'rides_aggregated': 0
        }

    def run(self):
        """Main execution method."""
        logger.info("=" * 70)
        logger.info("HOURLY STATS BACKFILL")
        logger.info("=" * 70)
        logger.info(f"Date range: {self.start_date} to {self.end_date}")
        logger.info(f"Total hours: {self.total_hours}")
        logger.info(f"Batch size: {self.batch_size} hours")
        logger.info(f"Strategy: Newest first (reverse chronological)")
        logger.info("=" * 70)

        # Generate list of hours to process (newest first)
        hours_to_process = self._generate_hour_list()

        # Process in batches
        batch_num = 0
        for i in range(0, len(hours_to_process), self.batch_size):
            batch_num += 1
            batch = hours_to_process[i:i + self.batch_size]

            logger.info("")
            logger.info(f"Batch {batch_num}: Processing {len(batch)} hours...")
            logger.info(f"  Range: {batch[-1]} to {batch[0]}")  # Reversed, so [-1] is oldest

            self._process_batch(batch)

            # Print progress
            progress_pct = (self.stats['hours_processed'] + self.stats['hours_skipped']) / self.total_hours * 100
            logger.info(f"  Progress: {self.stats['hours_processed']} processed, "
                       f"{self.stats['hours_skipped']} skipped, "
                       f"{self.stats['hours_failed']} failed ({progress_pct:.1f}%)")

        # Final summary
        self._print_summary()

        logger.info("=" * 70)
        logger.info("BACKFILL COMPLETE ✓")
        logger.info("=" * 70)

        # Exit with error code if any hours failed
        if self.stats['hours_failed'] > 0:
            sys.exit(1)

    def _generate_hour_list(self) -> List[datetime]:
        """
        Generate list of hours to backfill, newest first.

        Returns:
            List of datetime objects for each hour (descending order)
        """
        hours = []
        current_hour = self.end_date - timedelta(hours=1)  # Start with most recent complete hour

        while current_hour >= self.start_date:
            hours.append(current_hour)
            current_hour -= timedelta(hours=1)

        return hours

    def _process_batch(self, batch: List[datetime]):
        """
        Process a batch of hours.

        Args:
            batch: List of hour timestamps to process
        """
        for hour in batch:
            try:
                # Check if already aggregated
                if self._is_already_aggregated(hour):
                    logger.debug(f"  ↷ {hour} - already aggregated, skipping")
                    self.stats['hours_skipped'] += 1
                    continue

                # Check if hour has any data
                if not self._has_snapshot_data(hour):
                    logger.debug(f"  ∅ {hour} - no snapshot data, skipping")
                    self.stats['hours_skipped'] += 1
                    continue

                # Aggregate this hour using HourlyAggregator
                logger.info(f"  ▶ {hour} - aggregating...")
                aggregator = HourlyAggregator(target_hour=hour)
                aggregator.run()

                self.stats['hours_processed'] += 1
                self.stats['parks_aggregated'] += aggregator.stats['parks_processed']
                self.stats['rides_aggregated'] += aggregator.stats['rides_processed']

            except Exception as e:
                logger.error(f"  ✗ {hour} - failed: {e}")
                self.stats['hours_failed'] += 1

    def _is_already_aggregated(self, hour: datetime) -> bool:
        """
        Check if hour already has aggregated data.

        Args:
            hour: Hour to check

        Returns:
            True if already aggregated
        """
        try:
            with get_db_connection() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM park_hourly_stats
                    WHERE hour_start_utc = :hour
                """), {'hour': hour})
                row = result.fetchone()
                return row is not None and row[0] > 0
        except:
            return False

    def _has_snapshot_data(self, hour: datetime) -> bool:
        """
        Check if hour has any snapshot data to aggregate.

        Args:
            hour: Hour to check

        Returns:
            True if snapshot data exists
        """
        hour_end = hour + timedelta(hours=1)
        try:
            with get_db_connection() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM park_activity_snapshots
                    WHERE recorded_at >= :hour_start
                      AND recorded_at < :hour_end
                """), {'hour_start': hour, 'hour_end': hour_end})
                row = result.fetchone()
                return row is not None and row[0] > 0
        except:
            return False

    def _print_summary(self):
        """Print backfill summary statistics."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("BACKFILL SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Date range:       {self.start_date} to {self.end_date}")
        logger.info(f"Total hours:      {self.total_hours}")
        logger.info(f"Hours processed:  {self.stats['hours_processed']}")
        logger.info(f"Hours skipped:    {self.stats['hours_skipped']}")
        logger.info(f"Hours failed:     {self.stats['hours_failed']}")
        logger.info(f"Parks aggregated: {self.stats['parks_aggregated']}")
        logger.info(f"Rides aggregated: {self.stats['rides_aggregated']}")
        logger.info("=" * 70)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Backfill hourly aggregation tables from historical snapshot data'
    )

    # Date range options
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to backfill (default: 7)'
    )
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD, inclusive)'
    )
    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD, exclusive)'
    )

    # Performance options
    parser.add_argument(
        '--batch-size',
        type=int,
        default=24,
        help='Number of hours to process per batch (default: 24)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Determine date range
    if args.start and args.end:
        # Explicit date range
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    elif args.start:
        # Start date only, end = now
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.utcnow()
    elif args.end:
        # End date only, start = end - days
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        start_date = end_date - timedelta(days=args.days)
    else:
        # Default: last N days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=args.days)

    # Run backfill
    backfiller = HourlyBackfiller(
        start_date=start_date,
        end_date=end_date,
        batch_size=args.batch_size
    )
    backfiller.run()


if __name__ == '__main__':
    main()
