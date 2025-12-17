#!/usr/bin/env python3
"""
Cron Job Wrapper with Failure Alerts

Purpose: Wrap all cron jobs to capture failures and send immediate alerts.

This wrapper:
- Executes scripts via subprocess with configurable timeout
- Captures stdout/stderr and exit code
- Sends immediate alert email on non-zero exit
- Logs to structured JSON format
- Includes last 50 lines of output in alert

Usage:
    python -m src.scripts.cron_wrapper <script_name> --timeout=<seconds>

Examples:
    # Wrap collect_snapshots with 5-minute timeout
    python -m src.scripts.cron_wrapper collect_snapshots --timeout=300

    # Wrap aggregate_daily with 30-minute timeout
    python -m src.scripts.cron_wrapper aggregate_daily --timeout=1800

Integration with crontab:
    # Before:
    */10 * * * * cd /opt/themeparkhallofshame/backend && python -m src.scripts.collect_snapshots

    # After:
    */10 * * * * cd /opt/themeparkhallofshame/backend && python -m src.scripts.cron_wrapper collect_snapshots --timeout=300
"""

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.email_utils import send_alert_email


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/themeparkhallofshame/logs/cron_wrapper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CronJobWrapper:
    """Wraps cron job execution with failure alerts."""

    def __init__(self, script_name: str, timeout: int):
        """
        Initialize cron wrapper.

        Args:
            script_name: Name of the script to run (e.g., 'collect_snapshots')
            timeout: Timeout in seconds
        """
        self.script_name = script_name
        self.timeout = timeout
        self.module_path = f"src.scripts.{script_name}"
        self.start_time = None
        self.end_time = None
        self.exit_code = None
        self.output = []

    def run(self) -> int:
        """
        Execute the wrapped script and handle failures.

        Returns:
            Exit code from the wrapped script
        """
        self.start_time = datetime.now(timezone.utc)
        logger.info(f"Starting cron job: {self.script_name} (timeout: {self.timeout}s)")

        try:
            # Execute the script as a Python module
            result = subprocess.run(
                [sys.executable, "-m", self.module_path],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                cwd=Path(__file__).parent.parent.parent  # backend directory
            )

            self.exit_code = result.returncode
            self.output = result.stdout.splitlines() + result.stderr.splitlines()
            self.end_time = datetime.now(timezone.utc)

            if self.exit_code == 0:
                self._log_success()
            else:
                self._log_failure()
                self._send_failure_alert()

            return self.exit_code

        except subprocess.TimeoutExpired as e:
            self.exit_code = 124  # Standard timeout exit code
            self.output = [
                f"ERROR: Script timed out after {self.timeout} seconds",
                f"Stdout: {e.stdout}" if e.stdout else "",
                f"Stderr: {e.stderr}" if e.stderr else "",
            ]
            self.end_time = datetime.now(timezone.utc)
            self._log_failure()
            self._send_failure_alert()
            return self.exit_code

        except Exception as e:
            self.exit_code = 1
            self.output = [f"ERROR: Unexpected wrapper error: {e}"]
            self.end_time = datetime.now(timezone.utc)
            self._log_failure()
            self._send_failure_alert()
            return self.exit_code

    def _log_success(self):
        """Log successful execution."""
        duration = (self.end_time - self.start_time).total_seconds()
        log_entry = {
            "timestamp": self.end_time.isoformat(),
            "script": self.script_name,
            "status": "success",
            "exit_code": self.exit_code,
            "duration_seconds": duration,
            "output_lines": len(self.output),
        }
        logger.info(json.dumps(log_entry))

    def _log_failure(self):
        """Log failed execution."""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        log_entry = {
            "timestamp": self.end_time.isoformat() if self.end_time else datetime.now(timezone.utc).isoformat(),
            "script": self.script_name,
            "status": "failure",
            "exit_code": self.exit_code,
            "duration_seconds": duration,
            "output_lines": len(self.output),
            "last_50_lines": self.output[-50:] if len(self.output) > 50 else self.output,
        }
        logger.error(json.dumps(log_entry))

    def _send_failure_alert(self):
        """Send immediate failure alert via email."""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        hostname = socket.gethostname()

        # Get last 50 lines of output
        output_tail = "\n".join(self.output[-50:]) if self.output else "(no output)"

        subject = f"🚨 Cron Job Failure: {self.script_name} on {hostname}"

        body = f"""
CRON JOB FAILURE ALERT

Script:      {self.script_name}
Exit Code:   {self.exit_code}
Duration:    {duration:.1f} seconds
Timeout:     {self.timeout} seconds
Server:      {hostname}
Timestamp:   {self.end_time.isoformat() if self.end_time else 'unknown'}

{'=' * 80}
LAST 50 LINES OF OUTPUT:
{'=' * 80}

{output_tail}

{'=' * 80}
DEBUGGING
{'=' * 80}

SSH to server:
    ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com

Check logs:
    tail -f /opt/themeparkhallofshame/logs/cron_wrapper.log
    tail -f /opt/themeparkhallofshame/logs/error.log

Check cron job status:
    systemctl status crond
    journalctl -u crond -n 100

Check aggregation status:
    mysql -u themepark_app -p themepark_tracker -e "SELECT * FROM aggregation_log ORDER BY started_at DESC LIMIT 10"

{'=' * 80}
RECOMMENDED ACTIONS
{'=' * 80}

1. Check if the database is accessible
2. Check if required environment variables are set
3. Check disk space: df -h
4. Review recent code changes
5. Check if dependencies are installed correctly

This alert was generated by: {__file__}
"""

        try:
            send_alert_email(
                subject=subject,
                body=body,
                alert_type="cron_failure",
            )
            logger.info(f"Failure alert sent for {self.script_name}")
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")


def main():
    """Main entry point for cron wrapper."""
    parser = argparse.ArgumentParser(description="Wrap cron jobs with failure alerts")
    parser.add_argument(
        "script_name",
        help="Name of the script to run (e.g., 'collect_snapshots')"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Timeout in seconds (default: 600)"
    )
    args = parser.parse_args()

    wrapper = CronJobWrapper(
        script_name=args.script_name,
        timeout=args.timeout
    )

    exit_code = wrapper.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
