#!/usr/bin/env python3
"""
Data Quality Alert Script
=========================

Sends daily email alerts about stale data issues from ThemeParks.wiki.
Run via cron once daily (e.g., 8am Pacific).

Usage:
    python -m scripts.send_data_quality_alert

Environment Variables:
    SENDGRID_API_KEY: SendGrid API key for sending emails
"""

import os
import sys
from datetime import datetime  # noqa: F401 - used in f-string

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from database.connection import get_db_connection
from database.repositories.data_quality_repository import DataQualityRepository
from utils.logger import logger

# Configuration
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = "alerts@webperformance.com"
TO_EMAIL = "michael@czei.org"

# Alert thresholds
MIN_STALENESS_HOURS = 24  # Only alert on data older than 24 hours
MIN_ISSUES_TO_ALERT = 5   # Only send email if at least this many issues


def get_significant_issues(hours: int = 24) -> list:
    """Get data quality issues worth alerting about."""
    with get_db_connection() as conn:
        repo = DataQualityRepository(conn)
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
    with get_db_connection() as conn:
        repo = DataQualityRepository(conn)
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


def format_email_html(issues: list, stats: dict) -> str:
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

        <h2>How to Report</h2>
        <p>You can report these issues to the ThemeParks.wiki project:</p>
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

    # Get issues and stats
    issues = get_significant_issues(hours=24)
    stats = get_summary_stats()

    logger.info(f"Found {len(issues)} significant issues in last 24h")
    logger.info(f"Total entities with issues (7 days): {stats['total_entities_with_issues']}")

    # Only send if there are enough issues
    if len(issues) < MIN_ISSUES_TO_ALERT and stats['total_entities_with_issues'] < 10:
        logger.info("Not enough issues to warrant an alert, skipping email")
        return

    # Format and send email
    html = format_email_html(issues, stats)
    success = send_alert(html, len(issues))

    if success:
        print(f"Alert sent: {len(issues)} issues reported to {TO_EMAIL}")
    else:
        print("Failed to send alert email", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
