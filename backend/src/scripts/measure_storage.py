#!/usr/bin/env python3
"""
Storage Measurement Script

Measures database storage from information_schema and populates
the storage_metrics table for capacity planning and alerting.

Feature: 004-themeparks-data-collection
Task: T046

Usage:
    # Run daily measurement
    python -m scripts.measure_storage

    # Show current storage summary
    python -m scripts.measure_storage --summary

    # Check for alerts
    python -m scripts.measure_storage --check-alerts

    # Show growth projections
    python -m scripts.measure_storage --project-growth --days 365
"""

import argparse
import sys
from datetime import date
from decimal import Decimal
from typing import Optional

from database.connection import get_db_session
from database.repositories.storage_repository import StorageRepository
from utils.logger import get_logger

logger = get_logger(__name__)


def measure_storage(session) -> int:
    """
    Measure current database storage and record metrics.

    Returns:
        Number of tables measured
    """
    repo = StorageRepository(session)
    measurements = repo.measure_from_database()

    logger.info(f"Measured storage for {len(measurements)} tables")

    for m in measurements:
        logger.info(
            f"  {m.table_name}: {m.total_size_mb:.2f} MB "
            f"({m.row_count:,} rows)"
            f"{f', {m.partition_count} partitions' if m.partition_count else ''}"
        )

    return len(measurements)


def show_summary(session) -> None:
    """Display current storage summary."""
    repo = StorageRepository(session)
    total = repo.get_total_storage()
    latest = repo.get_all_tables_latest()

    print("\n=== Storage Summary ===")
    print(f"Total Size: {total['total_size_mb']:.2f} MB ({total['total_size_mb'] / Decimal('1024'):.2f} GB)")
    print(f"  Data: {total['total_data_mb']:.2f} MB")
    print(f"  Index: {total['total_index_mb']:.2f} MB")
    print(f"Total Rows: {total['total_rows']:,}")
    print(f"Tables: {total['table_count']}")
    print(f"Growth Rate: {total['total_growth_mb_per_day']:.2f} MB/day")

    print("\n=== Table Breakdown (Top 10) ===")
    print(f"{'Table':<40} {'Size (MB)':>12} {'Rows':>15} {'Growth/Day':>12}")
    print("-" * 80)

    for m in latest[:10]:
        growth = f"{m.growth_rate_mb_per_day:.2f}" if m.growth_rate_mb_per_day else "N/A"
        print(f"{m.table_name:<40} {m.total_size_mb:>12.2f} {m.row_count:>15,} {growth:>12}")


def check_alerts(
    session,
    warning_gb: Decimal = Decimal('50'),
    critical_gb: Decimal = Decimal('80')
) -> int:
    """
    Check for storage alerts.

    Returns:
        Number of alerts (0 = healthy)
    """
    repo = StorageRepository(session)
    alerts = repo.check_alerts(
        warning_threshold_gb=warning_gb,
        critical_threshold_gb=critical_gb
    )

    if not alerts:
        print("No storage alerts.")
        logger.info("Storage check: OK")
        return 0

    for alert in alerts:
        level = alert['level']
        message = alert['message']
        if level == 'CRITICAL':
            logger.error(f"ALERT [{level}]: {message}")
            print(f"CRITICAL: {message}")
        else:
            logger.warning(f"ALERT [{level}]: {message}")
            print(f"WARNING: {message}")

    return len(alerts)


def project_growth(session, days: int = 365) -> None:
    """Display storage growth projections."""
    repo = StorageRepository(session)
    projection = repo.project_storage(days)

    print("\n=== Storage Projections ===")
    print(f"Current Size: {projection['current_size_gb']:.2f} GB")
    print(f"Growth Rate: {projection['growth_rate_gb_per_month']:.2f} GB/month")
    print(f"\nProjected size in {days} days: {projection['projected_size_gb']:.2f} GB")

    # Calculate days until key thresholds
    current = projection['current_size_mb']
    rate = projection['growth_rate_mb_per_day']

    if rate > 0:
        thresholds = [
            (Decimal('51200'), '50 GB'),  # 50 GB
            (Decimal('81920'), '80 GB'),  # 80 GB
            (Decimal('102400'), '100 GB'),  # 100 GB
        ]

        print("\n=== Days Until Threshold ===")
        for threshold_mb, label in thresholds:
            if current >= threshold_mb:
                print(f"{label}: Already exceeded")
            else:
                days_until = int((threshold_mb - current) / rate)
                print(f"{label}: {days_until} days")
    else:
        print("\nNo growth rate data - unable to project thresholds")


def show_table_analysis(session, table_name: str, days: int = 30) -> None:
    """Show detailed analysis for a specific table."""
    repo = StorageRepository(session)
    analysis = repo.get_growth_analysis(table_name, days)

    print(f"\n=== Analysis: {table_name} ===")

    if 'analysis' in analysis and analysis['data_points'] < 2:
        print(analysis['analysis'])
        return

    print(f"Period: {analysis['period_days']} days ({analysis['data_points']} data points)")
    print(f"Current Size: {analysis['current_size_mb']:.2f} MB")
    print(f"Current Rows: {analysis['current_rows']:,}")

    if analysis.get('partition_count'):
        print(f"Partitions: {analysis['partition_count']}")

    print("\nGrowth:")
    print(f"  Size Change: {analysis['size_change_mb']:.2f} MB")
    print(f"  Row Change: {analysis['row_change']:,}")
    print(f"  Avg Growth: {analysis['avg_growth_mb_per_day']:.2f} MB/day")
    print(f"  Avg Rows: {analysis['avg_growth_rows_per_day']:.0f} rows/day")


def main():
    parser = argparse.ArgumentParser(
        description="Measure and analyze database storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show current storage summary'
    )
    parser.add_argument(
        '--check-alerts',
        action='store_true',
        help='Check for storage alerts'
    )
    parser.add_argument(
        '--project-growth',
        action='store_true',
        help='Show storage growth projections'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=365,
        help='Days for projection (default: 365)'
    )
    parser.add_argument(
        '--analyze-table',
        type=str,
        metavar='TABLE',
        help='Show detailed analysis for a specific table'
    )
    parser.add_argument(
        '--warning-gb',
        type=float,
        default=50.0,
        help='Warning threshold in GB (default: 50)'
    )
    parser.add_argument(
        '--critical-gb',
        type=float,
        default=80.0,
        help='Critical threshold in GB (default: 80)'
    )
    parser.add_argument(
        '--skip-measure',
        action='store_true',
        help='Skip measurement (only analyze existing data)'
    )

    args = parser.parse_args()

    try:
        with get_db_session() as session:
            exit_code = 0

            # Measure storage unless skipped
            if not args.skip_measure and not (args.summary or args.project_growth or args.analyze_table):
                count = measure_storage(session)
                if count == 0:
                    print("No new measurements (already measured today)")
                else:
                    print(f"Measured {count} tables")
                session.commit()

            # Show summary
            if args.summary:
                show_summary(session)

            # Check alerts
            if args.check_alerts:
                alert_count = check_alerts(
                    session,
                    warning_gb=Decimal(str(args.warning_gb)),
                    critical_gb=Decimal(str(args.critical_gb))
                )
                if alert_count > 0:
                    exit_code = 1

            # Project growth
            if args.project_growth:
                project_growth(session, args.days)

            # Analyze specific table
            if args.analyze_table:
                show_table_analysis(session, args.analyze_table, args.days)

            return exit_code

    except Exception as e:
        logger.exception(f"Storage measurement failed: {e}")
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
