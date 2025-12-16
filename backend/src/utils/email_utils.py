"""
Email Utilities for Theme Park Hall of Shame

Provides centralized email sending functionality using SendGrid.
"""

import logging
import os
from typing import Optional

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

logger = logging.getLogger(__name__)

# Email configuration from environment
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("ALERT_EMAIL_FROM", "alerts@webperformance.com")
TO_EMAIL = os.getenv("ALERT_EMAIL_TO", "michael@czei.org")


def send_alert_email(
    subject: str,
    body: str,
    alert_type: str = "general",
    to_email: Optional[str] = None,
    from_email: Optional[str] = None,
) -> bool:
    """
    Send an alert email via SendGrid.

    Args:
        subject: Email subject line
        body: Email body content (plain text)
        alert_type: Type of alert (for logging/categorization)
        to_email: Optional recipient email (defaults to configured TO_EMAIL)
        from_email: Optional sender email (defaults to configured FROM_EMAIL)

    Returns:
        True if email sent successfully, False otherwise

    Example:
        send_alert_email(
            subject="🚨 Cron Job Failure",
            body="The collect_snapshots job failed...",
            alert_type="cron_failure"
        )
    """
    if not SENDGRID_API_KEY:
        logger.error("SENDGRID_API_KEY not set - cannot send email")
        return False

    to_email = to_email or TO_EMAIL
    from_email = from_email or FROM_EMAIL

    if not to_email or not from_email:
        logger.error("Email addresses not configured")
        return False

    try:
        message = Mail(
            from_email=Email(from_email, "Theme Park Hall of Shame"),
            to_emails=To(to_email),
            subject=subject,
            plain_text_content=Content("text/plain", body),
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Alert email sent successfully: {alert_type} (status: {response.status_code})")
            return True
        else:
            logger.error(f"SendGrid returned non-2xx status: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        return False


def send_html_alert_email(
    subject: str,
    html_content: str,
    alert_type: str = "general",
    to_email: Optional[str] = None,
    from_email: Optional[str] = None,
) -> bool:
    """
    Send an HTML alert email via SendGrid.

    Args:
        subject: Email subject line
        html_content: Email body content (HTML)
        alert_type: Type of alert (for logging/categorization)
        to_email: Optional recipient email (defaults to configured TO_EMAIL)
        from_email: Optional sender email (defaults to configured FROM_EMAIL)

    Returns:
        True if email sent successfully, False otherwise
    """
    if not SENDGRID_API_KEY:
        logger.error("SENDGRID_API_KEY not set - cannot send email")
        return False

    to_email = to_email or TO_EMAIL
    from_email = from_email or FROM_EMAIL

    if not to_email or not from_email:
        logger.error("Email addresses not configured")
        return False

    try:
        message = Mail(
            from_email=Email(from_email, "Theme Park Hall of Shame"),
            to_emails=To(to_email),
            subject=subject,
            html_content=Content("text/html", html_content),
        )

        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"HTML alert email sent successfully: {alert_type} (status: {response.status_code})")
            return True
        else:
            logger.error(f"SendGrid returned non-2xx status: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Failed to send HTML alert email: {e}")
        return False
