#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Aggregate Verification Script
Verifies that aggregate table values match raw snapshot calculations.

This script compares stored aggregate values against recalculated values
from raw snapshots to catch bugs like timezone issues or interval mismatches.

Usage:
    python -m scripts.verify_aggregates --date 2025-12-17
    python -m scripts.verify_aggregates --date 2025-12-17 --table ride_daily
    python -m scripts.verify_aggregates --date 2025-12-17 --hourly
    python -m scripts.verify_aggregates --date 2025-12-17 --full
    python -m scripts.verify_aggregates --backfill --days 7
    python -m scripts.verify_aggregates --yesterday

Options:
    --date YYYY-MM-DD    Specific date to verify (Pacific timezone)
    --yesterday          Verify yesterday's aggregations (default if no date)
    --backfill           Verify multiple days
    --days N             Number of days to backfill (default: 7)
    --table TABLE        Specific table to verify (ride_daily, park_daily, ride_hourly, park_hourly)
    --hourly             Verify hourly stats only (including Disney DOWN check)
    --full               Run full audit (daily + hourly + special checks)
    --verbose            Show detailed mismatch information
    --json               Output results as JSON

Special Checks (included in --hourly and --full):
    - Disney/Universal DOWN status: Verifies DOWN status is counted correctly
    - Interval consistency: Verifies SNAPSHOT_INTERVAL_MINUTES matches reality

Exit codes:
    0 = All verifications passed
    1 = Critical failures found
    2 = Warnings found (but no critical failures)
"""

import sys
import argparse
import json
from pathlib import Path
from datetime import date, timedelta
from typing import Optional

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from utils.timezone import get_today_pacific
from database.connection import get_db_connection
from database.audit import AggregateVerifier, AuditSummary


def verify_date(
    target_date: date,
    table: Optional[str] = None,
    hourly: bool = False,
    full: bool = False,
    verbose: bool = False
) -> AuditSummary:
    """
    Verify aggregations for a specific date.

    Args:
        target_date: Pacific date to verify
        table: Optional specific table to verify
        hourly: Run hourly verification only
        full: Run full audit (daily + hourly + special checks)
        verbose: Show detailed output

    Returns:
        AuditSummary with verification results
    """
    from datetime import datetime

    with get_db_connection() as conn:
        verifier = AggregateVerifier(conn)

        if full:
            # Run complete verification (daily + hourly + special checks)
            summary = verifier.full_audit(target_date)
        elif hourly:
            # Run hourly verification only (includes Disney DOWN check)
            summary = verifier.audit_hourly(target_date)
        elif table == 'ride_daily':
            result = verifier.verify_ride_daily_stats(target_date)
            summary = AuditSummary(
                audit_timestamp=datetime.utcnow(),
                target_date=target_date,
                ride_daily_result=result,
                overall_passed=result.passed,
                critical_failures=1 if result.severity == "CRITICAL" else 0,
                warnings=1 if result.severity == "WARNING" else 0,
                issues_found=[result.message] if not result.passed else []
            )
        elif table == 'park_daily':
            result = verifier.verify_park_daily_stats(target_date)
            summary = AuditSummary(
                audit_timestamp=datetime.utcnow(),
                target_date=target_date,
                park_daily_result=result,
                overall_passed=result.passed,
                critical_failures=1 if result.severity == "CRITICAL" else 0,
                warnings=1 if result.severity == "WARNING" else 0,
                issues_found=[result.message] if not result.passed else []
            )
        elif table == 'ride_hourly' or table == 'park_hourly':
            # For specific hourly tables, run hourly audit
            summary = verifier.audit_hourly(target_date)
        else:
            # Default: daily stats only (original behavior)
            summary = verifier.audit_date(target_date)

        if verbose:
            print(verifier.get_summary_report(summary))

        return summary


def main():
    parser = argparse.ArgumentParser(
        description="Verify aggregate table data against raw snapshots"
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Date to verify (YYYY-MM-DD, Pacific timezone)'
    )
    parser.add_argument(
        '--yesterday',
        action='store_true',
        help='Verify yesterday (default if no date specified)'
    )
    parser.add_argument(
        '--backfill',
        action='store_true',
        help='Verify multiple days'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to backfill (default: 7)'
    )
    parser.add_argument(
        '--table',
        type=str,
        choices=['ride_daily', 'park_daily', 'ride_hourly', 'park_hourly'],
        help='Specific table to verify'
    )
    parser.add_argument(
        '--hourly',
        action='store_true',
        help='Verify hourly stats only (includes Disney DOWN check and interval check)'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run full audit: daily + hourly + special checks'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed output'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )

    args = parser.parse_args()

    # Determine target date(s)
    today = get_today_pacific()

    if args.backfill:
        # Verify multiple days
        dates_to_verify = [today - timedelta(days=i) for i in range(1, args.days + 1)]
    elif args.date:
        # Parse specific date
        try:
            year, month, day = args.date.split('-')
            dates_to_verify = [date(int(year), int(month), int(day))]
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Default to yesterday
        dates_to_verify = [today - timedelta(days=1)]

    # Run verification
    all_summaries = []
    total_critical = 0
    total_warnings = 0

    for target_date in dates_to_verify:
        if not args.json:
            logger.info(f"Verifying aggregations for {target_date}...")

        summary = verify_date(
            target_date,
            table=args.table,
            hourly=args.hourly,
            full=args.full,
            verbose=args.verbose
        )
        all_summaries.append(summary)
        total_critical += summary.critical_failures
        total_warnings += summary.warnings

        if not args.json and not args.verbose:
            # Print brief status
            status = "PASS" if summary.overall_passed else "FAIL"
            print(f"{target_date}: {status} (critical={summary.critical_failures}, warnings={summary.warnings})")

    # Output results
    if args.json:
        results = {
            'dates_verified': len(dates_to_verify),
            'total_critical_failures': total_critical,
            'total_warnings': total_warnings,
            'overall_passed': total_critical == 0,
            'summaries': [
                {
                    'date': str(s.target_date),
                    'passed': s.overall_passed,
                    'critical_failures': s.critical_failures,
                    'warnings': s.warnings,
                    'issues': s.issues_found
                }
                for s in all_summaries
            ]
        }
        print(json.dumps(results, indent=2))
    else:
        # Print summary
        print()
        print("=" * 60)
        print("VERIFICATION SUMMARY")
        print("=" * 60)
        print(f"Dates verified: {len(dates_to_verify)}")
        print(f"Total critical failures: {total_critical}")
        print(f"Total warnings: {total_warnings}")

        if total_critical > 0:
            print()
            print("FAILED - Critical issues require attention")
            print("Recommended: Re-run daily aggregation for affected dates")
        elif total_warnings > 0:
            print()
            print("PASSED with warnings - Review may be needed")
        else:
            print()
            print("PASSED - All aggregations verified")

    # Exit code based on results
    if total_critical > 0:
        sys.exit(1)
    elif total_warnings > 0:
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
