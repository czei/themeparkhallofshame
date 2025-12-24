#!/usr/bin/env python3
"""
Data Quality Alert Script
=========================

Sends daily email alerts about data quality issues including:
1. Stale data from ThemeParks.wiki
2. Awards validation (checking yesterday's awards for anomalies)

Run via cron once daily (e.g., 8am Pacific).

Usage:
    python -m scripts.send_data_quality_alert

Environment Variables:
    SENDGRID_API_KEY: SendGrid API key for sending emails
"""

import os
import sys
from datetime import datetime  # noqa: F401 - used in f-string
from typing import List, Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from database.connection import get_db_connection, get_db_session
from database.repositories.data_quality_repository import DataQualityRepository
from database.queries.trends.least_reliable_rides import LeastReliableRidesQuery
from database.queries.trends.longest_wait_times import LongestWaitTimesQuery
from database.queries.today import TodayParkWaitTimesQuery, TodayRideWaitTimesQuery
from database.queries.yesterday import YesterdayParkWaitTimesQuery, YesterdayRideWaitTimesQuery
from database.queries.rankings import ParkWaitTimeRankingsQuery, RideWaitTimeRankingsQuery
from utils.logger import logger

# Configuration
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = "alerts@webperformance.com"
TO_EMAIL = "michael@czei.org"

# Alert thresholds
MIN_STALENESS_HOURS = 24  # Only alert on data older than 24 hours
MIN_ISSUES_TO_ALERT = 5   # Only send email if at least this many issues

# Awards validation bounds
MAX_DOWNTIME_HOURS_YESTERDAY = 24  # A single day has max 24 hours
MAX_DOWNTIME_HOURS_LAST_WEEK = 168  # 7 days × 24 hours
MAX_DOWNTIME_HOURS_LAST_MONTH = 744  # 31 days × 24 hours
MAX_AVG_WAIT_TIME_MINUTES = 300  # 5 hours - extremely high but possible on peak days
MIN_WAIT_TIME_MINUTES = 0  # Can't be negative


def get_significant_issues(hours: int = 24) -> list:
    """Get data quality issues worth alerting about."""
    with get_db_session() as session:
        repo = DataQualityRepository(session)
        issues = repo.get_recent_issues(
            hours=hours,
            data_source="themeparks_wiki",
            unresolved_only=True,
            limit=100,
        )

        # Filter to only significant staleness (>24 hours)
        min_staleness_minutes = MIN_STALENESS_HOURS * 60
        significant = [
            i for i in issues
            if i.get("data_age_minutes", 0) >= min_staleness_minutes
        ]

        return significant


def get_summary_stats() -> dict:
    """Get summary statistics for the email."""
    with get_db_session() as session:
        repo = DataQualityRepository(session)
        summary = repo.get_summary_for_reporting(days=7, data_source="themeparks_wiki")

        # Filter out CLOSED status - those are expected for seasonal parks
        # Only show entities that report OPERATING but have stale data
        actionable = [
            s for s in summary
            if s.get("statuses_seen") and "CLOSED" not in s.get("statuses_seen", "")
        ]

        # Calculate stats
        total_entities = len(summary)
        if actionable:
            max_staleness = max(s.get("max_staleness_minutes", 0) for s in actionable)
            max_staleness_days = max_staleness / (60 * 24)
        else:
            max_staleness_days = 0

        return {
            "total_entities_with_issues": total_entities,
            "actionable_count": len(actionable),
            "max_staleness_days": round(max_staleness_days, 1),
            "top_offenders": actionable[:10],  # Top 10 worst (excluding CLOSED)
        }


# ============================================================================
# Awards Validation Functions
# ============================================================================

def validate_downtime_bounds(
    rides: List[Dict[str, Any]],
    period: str = "yesterday"
) -> List[Dict[str, Any]]:
    """
    Validate that downtime values are within physical bounds for the period.

    Args:
        rides: List of ride dicts with 'ride_name' and 'downtime_hours'
        period: 'yesterday', 'last_week', or 'last_month'

    Returns:
        List of issue dicts with category, severity, message, details
    """
    issues = []

    # Determine max allowed hours based on period
    if period in ("yesterday", "today"):
        max_hours = MAX_DOWNTIME_HOURS_YESTERDAY
    elif period in ("last_week", "7days"):
        max_hours = MAX_DOWNTIME_HOURS_LAST_WEEK
    elif period in ("last_month", "30days"):
        max_hours = MAX_DOWNTIME_HOURS_LAST_MONTH
    else:
        max_hours = MAX_DOWNTIME_HOURS_YESTERDAY  # Default to strictest

    for ride in rides:
        downtime = ride.get("downtime_hours", 0)
        ride_name = ride.get("ride_name", "Unknown")

        if downtime is None:
            continue

        if downtime > max_hours:
            issues.append({
                "category": "awards_downtime",
                "severity": "critical",
                "message": f"Downtime exceeds {max_hours}h for {period} period (impossible)",
                "details": f"{ride_name}: {downtime:.2f}h downtime (max possible: {max_hours}h)"
            })
        elif downtime < 0:
            issues.append({
                "category": "awards_downtime",
                "severity": "critical",
                "message": "Negative downtime detected (data error)",
                "details": f"{ride_name}: {downtime:.2f}h"
            })

    return issues


def validate_wait_time_bounds(rides: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate that wait time values are within reasonable bounds.

    Args:
        rides: List of ride dicts with 'ride_name' and 'avg_wait_time'

    Returns:
        List of issue dicts with category, severity, message, details
    """
    issues = []

    for ride in rides:
        avg_wait = ride.get("avg_wait_time", 0)
        ride_name = ride.get("ride_name", "Unknown")

        if avg_wait is None:
            continue

        if avg_wait > MAX_AVG_WAIT_TIME_MINUTES:
            issues.append({
                "category": "awards_wait_time",
                "severity": "high",
                "message": f"Average wait time exceeds {MAX_AVG_WAIT_TIME_MINUTES} minutes",
                "details": f"{ride_name}: {avg_wait} min average wait (suspiciously high)"
            })
        elif avg_wait < MIN_WAIT_TIME_MINUTES:
            issues.append({
                "category": "awards_wait_time",
                "severity": "critical",
                "message": "Negative wait time detected (data error)",
                "details": f"{ride_name}: {avg_wait} min"
            })

    return issues


def validate_awards_existence(
    rides: List[Dict[str, Any]],
    parks: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Validate that awards data exists (not empty).

    Args:
        rides: List of ride award data
        parks: List of park award data

    Returns:
        List of issue dicts if data is missing
    """
    issues = []

    if not rides and not parks:
        issues.append({
            "category": "awards_existence",
            "severity": "medium",
            "message": "No awards data found for yesterday",
            "details": "Both ride and park awards are empty (may indicate all parks closed)"
        })
    elif not rides:
        issues.append({
            "category": "awards_existence",
            "severity": "low",
            "message": "No ride awards data found",
            "details": "Ride awards list is empty"
        })
    elif not parks:
        issues.append({
            "category": "awards_existence",
            "severity": "low",
            "message": "No park awards data found",
            "details": "Park awards list is empty"
        })

    return issues


def validate_wait_times_not_all_zeros(conn) -> List[Dict[str, Any]]:
    """
    Validate that wait times are not ALL zeros for each period.

    This catches bugs where the API route falls through to wrong query
    (e.g., 'yesterday' routing to 30-day aggregate returning zeros).

    Periods checked:
    - today: TodayParkWaitTimesQuery, TodayRideWaitTimesQuery
    - yesterday: YesterdayParkWaitTimesQuery, YesterdayRideWaitTimesQuery
    - last_week: ParkWaitTimeRankingsQuery.get_weekly()
    - last_month: ParkWaitTimeRankingsQuery.get_monthly()

    Returns:
        List of issue dicts if any period returns all-zero data
    """
    issues = []

    # Define period configurations with their query classes
    period_checks = [
        {
            "period": "today",
            "park_query": lambda: TodayParkWaitTimesQuery(conn).get_rankings(limit=20),
            "ride_query": lambda: TodayRideWaitTimesQuery(conn).get_rankings(limit=20),
        },
        {
            "period": "yesterday",
            "park_query": lambda: YesterdayParkWaitTimesQuery(conn).get_rankings(limit=20),
            "ride_query": lambda: YesterdayRideWaitTimesQuery(conn).get_rankings(limit=20),
        },
        {
            "period": "last_week",
            "park_query": lambda: ParkWaitTimeRankingsQuery(conn).get_weekly(limit=20),
            "ride_query": lambda: RideWaitTimeRankingsQuery(conn).get_weekly(limit=20),
        },
        {
            "period": "last_month",
            "park_query": lambda: ParkWaitTimeRankingsQuery(conn).get_monthly(limit=20),
            "ride_query": lambda: RideWaitTimeRankingsQuery(conn).get_monthly(limit=20),
        },
    ]

    for check in period_checks:
        period = check["period"]

        try:
            # Check park wait times
            park_data = check["park_query"]()
            if park_data:
                # Get avg wait time field (could be avg_wait_minutes or avg_wait_time)
                avg_waits = [
                    p.get("avg_wait_minutes") or p.get("avg_wait_time") or 0
                    for p in park_data
                ]
                non_zero_count = sum(1 for w in avg_waits if w and w > 0)

                if non_zero_count == 0 and len(park_data) > 0:
                    issues.append({
                        "category": "wait_times_all_zeros",
                        "severity": "high",
                        "message": f"Park wait times ALL ZEROS for period={period}",
                        "details": f"{len(park_data)} parks returned, all with 0 avg wait time (likely routing bug)"
                    })

            # Check ride wait times
            ride_data = check["ride_query"]()
            if ride_data:
                avg_waits = [
                    r.get("avg_wait_minutes") or r.get("avg_wait_time") or 0
                    for r in ride_data
                ]
                non_zero_count = sum(1 for w in avg_waits if w and w > 0)

                if non_zero_count == 0 and len(ride_data) > 0:
                    issues.append({
                        "category": "wait_times_all_zeros",
                        "severity": "high",
                        "message": f"Ride wait times ALL ZEROS for period={period}",
                        "details": f"{len(ride_data)} rides returned, all with 0 avg wait time (likely routing bug)"
                    })

        except Exception as e:
            logger.warning(f"Error checking wait times for period={period}: {e}")
            # Don't add as issue - query errors are handled elsewhere

    return issues


def get_awards_issues() -> List[Dict[str, Any]]:
    """
    Main function to collect all awards validation issues.

    Queries yesterday's awards data and validates:
    1. Downtime values are within physical bounds
    2. Wait times are reasonable
    3. Data exists
    4. Wait times are not ALL zeros for any period

    Returns:
        List of all issues found
    """
    all_issues = []

    try:
        with get_db_connection() as conn:
            # Query yesterday's awards data
            downtime_query = LeastReliableRidesQuery(conn)
            wait_time_query = LongestWaitTimesQuery(conn)

            # Get ride-level awards for yesterday
            downtime_rides = downtime_query.get_rankings(period="yesterday", limit=20)
            wait_time_rides = wait_time_query.get_rankings(period="yesterday", limit=20)

            # Get park-level awards for yesterday
            downtime_parks = downtime_query.get_park_rankings(period="yesterday", limit=20)
            wait_time_parks = wait_time_query.get_park_rankings(period="yesterday", limit=20)

            # Validate downtime bounds
            all_issues.extend(validate_downtime_bounds(downtime_rides, period="yesterday"))
            all_issues.extend(validate_downtime_bounds(downtime_parks, period="yesterday"))

            # Validate wait time bounds
            all_issues.extend(validate_wait_time_bounds(wait_time_rides))
            all_issues.extend(validate_wait_time_bounds(wait_time_parks))

            # Validate data existence
            all_issues.extend(validate_awards_existence(
                rides=downtime_rides + wait_time_rides,
                parks=downtime_parks + wait_time_parks
            ))

            # Validate wait times are not ALL zeros for any period
            # This catches routing bugs where queries fall through to wrong data source
            all_issues.extend(validate_wait_times_not_all_zeros(conn))

    except Exception as e:
        logger.error(f"Error validating awards: {e}")
        all_issues.append({
            "category": "awards_error",
            "severity": "high",
            "message": "Failed to validate awards data",
            "details": str(e)
        })

    return all_issues


def format_email_html(issues: list, stats: dict, awards_issues: list = None) -> str:
    """Format the alert email as HTML."""

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; }}
            h1 {{ color: #d32f2f; }}
            h2 {{ color: #1976d2; margin-top: 30px; }}
            .stat {{ font-size: 24px; font-weight: bold; color: #333; }}
            .stat-label {{ color: #666; font-size: 14px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            .stale {{ color: #d32f2f; font-weight: bold; }}
            .warning {{ background-color: #fff3e0; }}
        </style>
    </head>
    <body>
        <h1>ThemeParks.wiki Data Quality Alert</h1>
        <p>Detected stale data that may need to be reported to ThemeParks.wiki maintainers.</p>

        <div style="display: flex; gap: 40px; margin: 20px 0;">
            <div>
                <div class="stat">{stats.get('actionable_count', 0)}</div>
                <div class="stat-label">Actionable issues</div>
            </div>
            <div>
                <div class="stat">{stats['max_staleness_days']} days</div>
                <div class="stat-label">Oldest stale data</div>
            </div>
            <div>
                <div class="stat">{stats['total_entities_with_issues']}</div>
                <div class="stat-label">Total (incl. CLOSED)</div>
            </div>
        </div>

        <h2>Actionable Issues (Last 7 Days)</h2>
        <p style="color: #666; font-size: 13px;">
            Showing only entities with non-CLOSED status that have stale data.
            CLOSED rides at seasonal parks are filtered out.
        </p>
        <table>
            <tr>
                <th>Park</th>
                <th>Entity Name</th>
                <th>Reported Status</th>
                <th>Staleness</th>
                <th>First Seen</th>
                <th>ThemeParks.wiki</th>
            </tr>
    """

    for item in stats.get("top_offenders", [])[:10]:
        staleness_days = round(item.get("max_staleness_minutes", 0) / (60 * 24), 1)
        staleness_class = "stale" if staleness_days > 30 else ""
        wiki_id = item.get('themeparks_wiki_id', '')
        wiki_link = f'<a href="https://api.themeparks.wiki/v1/entity/{wiki_id}/live" target="_blank">View Live</a>' if wiki_id else 'N/A'
        park_name = item.get('park_name') or 'Unknown Park'
        first_detected = item.get('first_detected')
        first_detected_str = first_detected.strftime('%Y-%m-%d') if first_detected else 'N/A'

        html += f"""
            <tr>
                <td>{park_name}</td>
                <td>{item.get('entity_name', 'Unknown')}</td>
                <td><code>{item.get('statuses_seen', 'N/A')}</code></td>
                <td class="{staleness_class}">{staleness_days} days</td>
                <td>{first_detected_str}</td>
                <td>{wiki_link}</td>
            </tr>
        """

    html += """
        </table>
    """

    # Add Awards Validation section if there are any issues
    if awards_issues:
        html += """
        <h2 style="color: #d32f2f;">Awards Validation Issues</h2>
        <p style="color: #666; font-size: 13px;">
            Issues detected in yesterday's awards data. These may indicate bugs in
            the awards calculation or data anomalies.
        </p>
        <table>
            <tr>
                <th>Severity</th>
                <th>Category</th>
                <th>Message</th>
                <th>Details</th>
            </tr>
        """
        for issue in awards_issues:
            severity = issue.get('severity', 'unknown')
            severity_color = {
                'critical': '#d32f2f',
                'high': '#f57c00',
                'medium': '#fbc02d',
                'low': '#388e3c'
            }.get(severity, '#666')

            html += f"""
            <tr>
                <td style="color: {severity_color}; font-weight: bold;">{severity.upper()}</td>
                <td>{issue.get('category', 'unknown')}</td>
                <td>{issue.get('message', '')}</td>
                <td style="font-size: 12px;">{issue.get('details', '')}</td>
            </tr>
            """

        html += "</table>"
    else:
        html += """
        <h2 style="color: #388e3c;">Awards Validation</h2>
        <p style="color: #388e3c;">All awards data for yesterday passed validation checks.</p>
        """

    html += """
        <h2>How to Report</h2>
        <p>You can report stale data issues to the ThemeParks.wiki project:</p>
        <ul>
            <li>GitHub: <a href="https://github.com/ThemeParks/parksapi/issues">ThemeParks/parksapi Issues</a></li>
            <li>Include the ThemeParks.wiki ID when reporting</li>
        </ul>

        <h2>API Endpoints</h2>
        <p>For full details, query these endpoints:</p>
        <ul>
            <li><code>GET /api/audit/data-quality?hours=24</code> - Recent issues</li>
            <li><code>GET /api/audit/data-quality/summary?days=7</code> - Summary by entity</li>
        </ul>

        <hr style="margin-top: 40px;">
        <p style="color: #999; font-size: 12px;">
            This alert was generated automatically by Theme Park Hall of Shame.<br>
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC
        </p>
    </body>
    </html>
    """

    return html


def send_alert(html_content: str, issue_count: int) -> bool:
    """Send the alert email via SendGrid."""
    if not SENDGRID_API_KEY:
        logger.error("SENDGRID_API_KEY not set")
        return False

    try:
        message = Mail(
            from_email=Email(FROM_EMAIL, "Theme Park Hall of Shame"),
            to_emails=To(TO_EMAIL),
            subject=f"Data Quality Alert: {issue_count} stale data issues detected",
            html_content=Content("text/html", html_content),
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        if response.status_code in (200, 201, 202):
            logger.info(f"Alert email sent successfully to {TO_EMAIL}")
            return True
        else:
            logger.error(f"SendGrid returned status {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        return False


def main():
    """Main entry point."""
    logger.info("Starting data quality alert check...")

    # Get stale data issues and stats
    issues = get_significant_issues(hours=24)
    stats = get_summary_stats()

    logger.info(f"Found {len(issues)} significant stale data issues in last 24h")
    logger.info(f"Total entities with issues (7 days): {stats['total_entities_with_issues']}")

    # Get awards validation issues
    awards_issues = get_awards_issues()
    logger.info(f"Found {len(awards_issues)} awards validation issues")

    # Count critical awards issues
    critical_awards = [i for i in awards_issues if i.get('severity') == 'critical']

    # Send if there are enough stale data issues OR any critical awards issues
    should_send = (
        len(issues) >= MIN_ISSUES_TO_ALERT or
        stats['total_entities_with_issues'] >= 10 or
        len(critical_awards) > 0
    )

    if not should_send:
        logger.info("Not enough issues to warrant an alert, skipping email")
        return

    # Format and send email
    html = format_email_html(issues, stats, awards_issues=awards_issues)
    success = send_alert(html, len(issues))

    if success:
        print(f"Alert sent: {len(issues)} issues reported to {TO_EMAIL}")
    else:
        print("Failed to send alert email", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
