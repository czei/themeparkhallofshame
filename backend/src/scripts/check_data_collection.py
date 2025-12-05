#!/usr/bin/env python3
"""
Data Collection Health Check Script
====================================

Monitors that data collection is running and sends immediate alerts if it stops.

This script checks if ride_status_snapshots and park_activity_snapshots have
recent data (within the last 30 minutes). If no data is found, sends an alert
email with diagnostic details.

Run via cron every hour:
    0 * * * * cd /opt/themeparkhallofshame/backend && python -m src.scripts.check_data_collection

Environment Variables:
    SENDGRID_API_KEY: SendGrid API key for sending emails
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from database.connection import get_db_connection
from utils.logger import logger

# Configuration
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = "alerts@webperformance.com"
TO_EMAIL = "michael@czei.org"

# Alert thresholds
MAX_DATA_AGE_MINUTES = 30  # Alert if no data in last 30 minutes
MIN_PARKS_EXPECTED = 5     # Alert if fewer than this many parks have recent data
MIN_RIDES_EXPECTED = 50    # Alert if fewer than this many rides have recent data


def check_recent_snapshots() -> Dict[str, Any]:
    """
    Check if we have recent ride status and park activity snapshots.

    Returns:
        Dict with diagnostic information:
        - has_recent_ride_data: bool
        - has_recent_park_data: bool
        - most_recent_ride_snapshot: datetime or None
        - most_recent_park_snapshot: datetime or None
        - parks_with_recent_data: int
        - rides_with_recent_data: int
        - minutes_since_last_ride_snapshot: int or None
        - minutes_since_last_park_snapshot: int or None
    """
    with get_db_connection() as conn:
        now = datetime.now()
        cutoff = now - timedelta(minutes=MAX_DATA_AGE_MINUTES)

        # Check ride status snapshots
        ride_result = conn.execute("""
            SELECT
                MAX(recorded_at) as most_recent,
                COUNT(DISTINCT ride_id) as ride_count,
                COUNT(*) as snapshot_count
            FROM ride_status_snapshots
            WHERE recorded_at >= %s
        """, (cutoff,))
        ride_row = ride_result.fetchone()

        # Check park activity snapshots
        park_result = conn.execute("""
            SELECT
                MAX(recorded_at) as most_recent,
                COUNT(DISTINCT park_id) as park_count,
                COUNT(*) as snapshot_count
            FROM park_activity_snapshots
            WHERE recorded_at >= %s
        """, (cutoff,))
        park_row = park_result.fetchone()

        # Get overall most recent snapshots (even if older than cutoff)
        overall_ride_result = conn.execute("""
            SELECT MAX(recorded_at) as most_recent
            FROM ride_status_snapshots
        """)
        overall_ride_row = overall_ride_result.fetchone()

        overall_park_result = conn.execute("""
            SELECT MAX(recorded_at) as most_recent
            FROM park_activity_snapshots
        """)
        overall_park_row = overall_park_result.fetchone()

        # Calculate results
        most_recent_ride = ride_row[0] if ride_row else None
        most_recent_park = park_row[0] if park_row else None

        overall_most_recent_ride = overall_ride_row[0] if overall_ride_row else None
        overall_most_recent_park = overall_park_row[0] if overall_park_row else None

        rides_with_recent_data = ride_row[1] if ride_row else 0
        parks_with_recent_data = park_row[1] if park_row else 0

        ride_snapshot_count = ride_row[2] if ride_row else 0
        park_snapshot_count = park_row[2] if park_row else 0

        # Calculate minutes since last snapshot
        minutes_since_ride = None
        if overall_most_recent_ride:
            minutes_since_ride = int((now - overall_most_recent_ride).total_seconds() / 60)

        minutes_since_park = None
        if overall_most_recent_park:
            minutes_since_park = int((now - overall_most_recent_park).total_seconds() / 60)

        has_recent_ride_data = (
            most_recent_ride is not None and
            rides_with_recent_data >= MIN_RIDES_EXPECTED and
            ride_snapshot_count > 0
        )

        has_recent_park_data = (
            most_recent_park is not None and
            parks_with_recent_data >= MIN_PARKS_EXPECTED and
            park_snapshot_count > 0
        )

        return {
            "has_recent_ride_data": has_recent_ride_data,
            "has_recent_park_data": has_recent_park_data,
            "most_recent_ride_snapshot": most_recent_ride,
            "most_recent_park_snapshot": most_recent_park,
            "overall_most_recent_ride_snapshot": overall_most_recent_ride,
            "overall_most_recent_park_snapshot": overall_most_recent_park,
            "parks_with_recent_data": parks_with_recent_data,
            "rides_with_recent_data": rides_with_recent_data,
            "ride_snapshot_count": ride_snapshot_count,
            "park_snapshot_count": park_snapshot_count,
            "minutes_since_last_ride_snapshot": minutes_since_ride,
            "minutes_since_last_park_snapshot": minutes_since_park,
        }


def get_collection_process_status() -> Dict[str, Any]:
    """
    Check if the data collection process is running.

    Returns:
        Dict with:
        - is_running: bool (whether collect_snapshots process found)
        - process_count: int (number of matching processes)
    """
    import subprocess

    try:
        # Check for running collect_snapshots process
        result = subprocess.run(
            ["pgrep", "-f", "collect_snapshots"],
            capture_output=True,
            text=True
        )

        process_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
        is_running = process_count > 0

        return {
            "is_running": is_running,
            "process_count": process_count,
        }
    except Exception as e:
        logger.warning(f"Could not check process status: {e}")
        return {
            "is_running": None,
            "process_count": None,
        }


def get_cron_log_tail(lines: int = 20) -> Optional[str]:
    """
    Get the last N lines from the collect_snapshots log.

    Args:
        lines: Number of lines to retrieve

    Returns:
        Log content or None if not available
    """
    log_path = "/opt/themeparkhallofshame/logs/collect_snapshots.log"

    try:
        with open(log_path, 'r') as f:
            # Read last N lines
            all_lines = f.readlines()
            return ''.join(all_lines[-lines:])
    except Exception as e:
        logger.warning(f"Could not read log file: {e}")
        return None


def format_alert_email(diagnostics: Dict[str, Any], process_status: Dict[str, Any]) -> str:
    """
    Format the alert email as HTML.

    Args:
        diagnostics: Output from check_recent_snapshots()
        process_status: Output from get_collection_process_status()

    Returns:
        HTML email content
    """
    now = datetime.now()

    # Determine severity and color
    severity = "CRITICAL"
    severity_color = "#d32f2f"

    if not diagnostics["has_recent_ride_data"] or not diagnostics["has_recent_park_data"]:
        severity = "CRITICAL"
        severity_color = "#d32f2f"

    # Format timestamps
    def format_timestamp(dt: Optional[datetime]) -> str:
        if dt is None:
            return "Never"
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    most_recent_ride = diagnostics.get("overall_most_recent_ride_snapshot")
    most_recent_park = diagnostics.get("overall_most_recent_park_snapshot")

    minutes_since_ride = diagnostics.get("minutes_since_last_ride_snapshot")
    minutes_since_park = diagnostics.get("minutes_since_last_park_snapshot")

    # Get recent log content
    log_tail = get_cron_log_tail(lines=30)
    log_section = ""
    if log_tail:
        log_section = f"""
        <h2>Recent Collection Log (last 30 lines)</h2>
        <pre style="background-color: #f5f5f5; padding: 10px; overflow-x: auto; font-size: 11px;">{log_tail}</pre>
        """

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto; }}
            h1 {{ color: {severity_color}; }}
            h2 {{ color: #1976d2; margin-top: 30px; }}
            .severity {{ font-size: 28px; font-weight: bold; color: {severity_color}; }}
            .stat {{ font-size: 20px; font-weight: bold; color: #333; margin: 10px 0; }}
            .stat-label {{ color: #666; font-size: 14px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            .error {{ color: #d32f2f; font-weight: bold; }}
            .ok {{ color: #388e3c; }}
            .warning {{ color: #f57c00; }}
        </style>
    </head>
    <body>
        <h1>Data Collection Alert: {severity}</h1>
        <p class="severity">Data collection appears to have stopped!</p>

        <h2>Current Status</h2>
        <table>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Status</th>
            </tr>
            <tr>
                <td>Last Ride Snapshot</td>
                <td>{format_timestamp(most_recent_ride)}</td>
                <td class="{'ok' if diagnostics['has_recent_ride_data'] else 'error'}">
                    {minutes_since_ride if minutes_since_ride is not None else 'N/A'} minutes ago
                </td>
            </tr>
            <tr>
                <td>Last Park Snapshot</td>
                <td>{format_timestamp(most_recent_park)}</td>
                <td class="{'ok' if diagnostics['has_recent_park_data'] else 'error'}">
                    {minutes_since_park if minutes_since_park is not None else 'N/A'} minutes ago
                </td>
            </tr>
            <tr>
                <td>Rides with Recent Data (last {MAX_DATA_AGE_MINUTES}m)</td>
                <td>{diagnostics['rides_with_recent_data']}</td>
                <td class="{'ok' if diagnostics['rides_with_recent_data'] >= MIN_RIDES_EXPECTED else 'error'}">
                    Expected: ≥{MIN_RIDES_EXPECTED}
                </td>
            </tr>
            <tr>
                <td>Parks with Recent Data (last {MAX_DATA_AGE_MINUTES}m)</td>
                <td>{diagnostics['parks_with_recent_data']}</td>
                <td class="{'ok' if diagnostics['parks_with_recent_data'] >= MIN_PARKS_EXPECTED else 'error'}">
                    Expected: ≥{MIN_PARKS_EXPECTED}
                </td>
            </tr>
            <tr>
                <td>Ride Snapshots (last {MAX_DATA_AGE_MINUTES}m)</td>
                <td>{diagnostics['ride_snapshot_count']}</td>
                <td class="{'ok' if diagnostics['ride_snapshot_count'] > 0 else 'error'}"></td>
            </tr>
            <tr>
                <td>Park Snapshots (last {MAX_DATA_AGE_MINUTES}m)</td>
                <td>{diagnostics['park_snapshot_count']}</td>
                <td class="{'ok' if diagnostics['park_snapshot_count'] > 0 else 'error'}"></td>
            </tr>
        </table>

        <h2>Process Status</h2>
        <table>
            <tr>
                <th>Check</th>
                <th>Result</th>
            </tr>
            <tr>
                <td>collect_snapshots Process Running</td>
                <td class="{'ok' if process_status.get('is_running') else 'error'}">
                    {'Yes' if process_status.get('is_running') else 'No'}
                    {f" ({process_status.get('process_count')} processes)" if process_status.get('process_count') else ''}
                </td>
            </tr>
        </table>

        {log_section}

        <h2>Recommended Actions</h2>
        <ol>
            <li>SSH into production server: <code>ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com</code></li>
            <li>Check if cron job is running: <code>crontab -l | grep collect_snapshots</code></li>
            <li>Check full log: <code>tail -100 /opt/themeparkhallofshame/logs/collect_snapshots.log</code></li>
            <li>Manually run collection: <code>cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.collect_snapshots</code></li>
            <li>Check disk space: <code>df -h</code></li>
            <li>Check database connectivity: <code>mysql -u admin -p themepark_tracker -e "SELECT 1;"</code></li>
        </ol>

        <hr style="margin-top: 40px;">
        <p style="color: #999; font-size: 12px;">
            This alert was generated automatically by Theme Park Hall of Shame.<br>
            Generated: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Alert Threshold: No data in last {MAX_DATA_AGE_MINUTES} minutes
        </p>
    </body>
    </html>
    """

    return html


def send_alert(html_content: str) -> bool:
    """
    Send the alert email via SendGrid.

    Args:
        html_content: HTML email body

    Returns:
        True if sent successfully, False otherwise
    """
    if not SENDGRID_API_KEY:
        logger.error("SENDGRID_API_KEY not set")
        return False

    try:
        message = Mail(
            from_email=Email(FROM_EMAIL, "Theme Park Hall of Shame"),
            to_emails=To(TO_EMAIL),
            subject="CRITICAL: Data Collection Stopped",
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
    logger.info("=" * 60)
    logger.info(f"DATA COLLECTION HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # Check recent snapshots
    diagnostics = check_recent_snapshots()

    logger.info(f"Has recent ride data: {diagnostics['has_recent_ride_data']}")
    logger.info(f"Has recent park data: {diagnostics['has_recent_park_data']}")
    logger.info(f"Rides with recent data: {diagnostics['rides_with_recent_data']}")
    logger.info(f"Parks with recent data: {diagnostics['parks_with_recent_data']}")
    logger.info(f"Minutes since last ride snapshot: {diagnostics['minutes_since_last_ride_snapshot']}")
    logger.info(f"Minutes since last park snapshot: {diagnostics['minutes_since_last_park_snapshot']}")

    # Check process status
    process_status = get_collection_process_status()
    logger.info(f"Collection process running: {process_status.get('is_running')}")

    # Determine if we need to alert
    should_alert = (
        not diagnostics["has_recent_ride_data"] or
        not diagnostics["has_recent_park_data"]
    )

    if should_alert:
        logger.warning("DATA COLLECTION ISSUE DETECTED - Sending alert email")
        html = format_alert_email(diagnostics, process_status)
        success = send_alert(html)

        if success:
            print(f"Alert sent to {TO_EMAIL}")
            logger.info("Alert email sent successfully")
        else:
            print("Failed to send alert email", file=sys.stderr)
            logger.error("Failed to send alert email")
            sys.exit(1)
    else:
        logger.info("Data collection healthy - no alert needed")
        print("Data collection is healthy")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
