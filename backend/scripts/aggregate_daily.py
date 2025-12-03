#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Daily Aggregation Script
Runs daily aggregation job with retry logic.

Usage:
    python aggregate_daily.py [--date YYYY-MM-DD] [--timezone TZ] [--dry-run]

Scheduled execution (cron) - Run at 5 AM UTC (1 AM Pacific, after PT day ends):
    10 5 * * * /path/to/aggregate_daily.py  # 5:10 AM UTC = 1:10 AM Pacific
    10 6 * * * /path/to/aggregate_daily.py --retry 1  # 6:10 AM UTC retry
    10 7 * * * /path/to/aggregate_daily.py --retry 2  # 7:10 AM UTC retry
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add backend/src to Python path
backend_src = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from processor.aggregation_service import AggregationService
from database.connection import get_db_connection
from utils.logger import logger
from utils.timezone import get_today_pacific


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run daily aggregation for theme park downtime data"
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date to aggregate (YYYY-MM-DD), defaults to yesterday'
    )
    parser.add_argument(
        '--timezone',
        type=str,
        help='Only aggregate parks in specific timezone (optional)'
    )
    parser.add_argument(
        '--retry',
        type=int,
        choices=[0, 1, 2],
        default=0,
        help='Retry attempt number (0=first run at 12:10 AM, 1=1:10 AM, 2=2:10 AM)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be aggregated without saving'
    )

    args = parser.parse_args()

    # Determine aggregation date
    if args.date:
        aggregation_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        # Default to yesterday in Pacific Time (US parks day boundary)
        aggregation_date = get_today_pacific() - timedelta(days=1)

    logger.info("=" * 60)
    logger.info("DAILY AGGREGATION SCRIPT")
    logger.info("=" * 60)
    logger.info(f"Aggregation date: {aggregation_date}")
    logger.info(f"Timezone filter: {args.timezone or 'all'}")
    logger.info(f"Retry attempt: {args.retry}")

    # Check if aggregation already succeeded
    with get_db_connection() as conn:
        aggregation_service = AggregationService(conn)

        last_success = aggregation_service.get_last_successful_aggregation('daily')

        if last_success and last_success['aggregation_date'] == aggregation_date:
            logger.info(f"✓ Aggregation for {aggregation_date} already completed successfully")
            logger.info(f"  Completed at: {last_success['completed_at']}")
            logger.info(f"  Parks processed: {last_success['parks_processed']}")
            logger.info(f"  Rides processed: {last_success['rides_processed']}")

            if args.retry > 0:
                logger.info("Skipping retry - aggregation already successful")
                return 0
            else:
                logger.info("Re-running aggregation (--retry not specified)")

        # Run aggregation
        if args.dry_run:
            logger.info("")
            logger.info("DRY RUN - Would aggregate the following:")
            logger.info(f"  Date: {aggregation_date}")
            logger.info(f"  Timezone: {args.timezone or 'all'}")
            logger.info("No changes saved")
            return 0

        try:
            logger.info("")
            logger.info(f"Starting daily aggregation for {aggregation_date}...")

            result = aggregation_service.aggregate_daily(
                aggregation_date=aggregation_date,
                park_timezone=args.timezone
            )

            logger.info("=" * 60)
            logger.info("AGGREGATION COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Status: {result['status']}")
            logger.info(f"Parks processed: {result['parks_processed']}")
            logger.info(f"Rides processed: {result['rides_processed']}")
            logger.info(f"Aggregated until: {result['aggregated_until_ts']}")
            logger.info("=" * 60)

            return 0

        except Exception as e:
            logger.error("=" * 60)
            logger.error("AGGREGATION FAILED")
            logger.error("=" * 60)
            logger.error(f"Error: {e}", exc_info=True)

            if args.retry < 2:
                logger.info(f"Will retry at attempt {args.retry + 1}")
            else:
                logger.error("Maximum retry attempts reached (3)")

            return 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"Aggregation script failed: {e}", exc_info=True)
        sys.exit(1)
