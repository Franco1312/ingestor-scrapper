"""
Health notifications - Email, Slack webhook and stdout fallback.

This module handles sending notifications when health check issues are detected.
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def notify(
    slack_webhook_env: Optional[str] = None,
    email_env: Optional[str] = None,
    title: str = "",
    summary: Dict[str, Any] = None,
    level: str = "INFO",
) -> int:
    """
    Send notification via email, Slack webhook or stdout.

    Args:
        slack_webhook_env: Environment variable name containing Slack webhook URL
        email_env: Environment variable name containing email address
        title: Notification title
        summary: Dict with check results and context
        level: Severity level ("INFO", "WARN", "FAIL")

    Returns:
        Exit code: 0 (INFO), 2 (WARN), 3 (FAIL)
    """
    if summary is None:
        summary = {}

    # Determine webhook URL
    webhook_url = None
    if slack_webhook_env:
        webhook_url = os.environ.get(slack_webhook_env)

    # Determine email address
    email_address = None
    if email_env:
        email_address = os.environ.get(email_env)

    # Map level to exit code
    exit_codes = {"INFO": 0, "WARN": 2, "FAIL": 3}
    exit_code = exit_codes.get(level, 0)

    # Try email first
    if email_address:
        email_sent = _send_email(email_address, title, summary, level)
        if email_sent:
            logger.info("Sent email notification to %s", email_address)
            return exit_code

    # Try Slack webhook second
    if webhook_url:
        slack_sent = _send_slack_webhook(webhook_url, title, summary, level)
        if slack_sent:
            logger.info("Sent Slack notification for %s", title)
            return exit_code

    # Fall back to stdout
    _print_stdout(title, summary, level)

    return exit_code


def _send_email(
    email_address: str, title: str, summary: Dict[str, Any], level: str
) -> bool:
    """
    Send notification email using SMTP.

    Args:
        email_address: Recipient email address
        title: Notification title
        summary: Check results dict
        level: Severity level

    Returns:
        True if sent successfully, False otherwise
    """
    emoji_map = {"INFO": "✅", "WARN": "⚠️", "FAIL": "❌"}
    emoji = emoji_map.get(level, "ℹ️")

    summary_text = _format_summary(summary)

    # Build email
    msg = MIMEMultipart()
    msg["From"] = os.environ.get("SMTP_FROM", "health-check@localhost")
    msg["To"] = email_address
    msg["Subject"] = f"{emoji} {title} - {level}"

    body = f"""
Health Check Report: {title}

Level: {level}

{summary_text}

---
This is an automated message from the Health Check Watchdog.
"""

    msg.attach(MIMEText(body, "plain"))

    try:
        # Get SMTP config from environment
        smtp_host = os.environ.get("SMTP_HOST", "localhost")
        smtp_port = int(os.environ.get("SMTP_PORT", "25"))
        smtp_user = os.environ.get("SMTP_USER")
        smtp_password = os.environ.get("SMTP_PASSWORD")

        # Send email
        server = smtplib.SMTP(smtp_host, smtp_port)

        # Enable TLS if credentials provided
        if smtp_user and smtp_password:
            server.starttls()
            server.login(smtp_user, smtp_password)

        text = msg.as_string()
        server.sendmail(msg["From"], email_address, text)
        server.quit()

        return True
    except Exception as e:
        logger.error("Failed to send email: %s", e, exc_info=True)
        return False


def _send_slack_webhook(
    webhook_url: str, title: str, summary: Dict[str, Any], level: str
) -> bool:
    """
    Send notification to Slack via Incoming Webhook.

    Args:
        webhook_url: Slack webhook URL
        title: Notification title
        summary: Check results dict
        level: Severity level

    Returns:
        True if sent successfully, False otherwise
    """
    try:
        import requests  # type: ignore
    except ImportError:
        logger.warning("requests library not available for Slack webhook")
        return False

    # Map level to color and emoji
    color_map = {"INFO": "#36a64f", "WARN": "#ff9900", "FAIL": "#ff0000"}
    emoji_map = {"INFO": "✅", "WARN": "⚠️", "FAIL": "❌"}

    color = color_map.get(level, "#36a64f")
    emoji = emoji_map.get(level, "ℹ️")

    # Format summary as text
    summary_text = _format_summary(summary)

    # Build Slack message payload
    payload = {
        "username": "Health Check Monitor",
        "text": f"{emoji} *{title}*",
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "Level", "value": level, "short": True},
                    {"title": "Summary", "value": summary_text, "short": False},
                ],
                "footer": "Health Check Watchdog",
                "ts": int(__import__("time").time()),
            }
        ],
    }

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error("Failed to send Slack webhook: %s", e, exc_info=True)
        return False


def _print_stdout(title: str, summary: Dict[str, Any], level: str) -> None:
    """
    Print formatted notification to stdout.

    Args:
        title: Notification title
        summary: Check results dict
        level: Severity level
    """
    emoji_map = {"INFO": "✅", "WARN": "⚠️", "FAIL": "❌"}
    emoji = emoji_map.get(level, "ℹ️")

    print()
    print("=" * 80)
    print(f"{emoji} {title} - {level}")
    print("=" * 80)

    summary_text = _format_summary(summary)
    print(summary_text)
    print("=" * 80)
    print()


def _format_summary(summary: Dict[str, Any]) -> str:
    """
    Format summary dict as human-readable text.

    Args:
        summary: Check results dict

    Returns:
        Formatted text string
    """
    lines = []

    if "url" in summary:
        lines.append(f"URL: {summary['url']}")

    if "status_code" in summary:
        status = summary["status_code"]
        status_ok = 200 <= status < 300
        icon = "✓" if status_ok else "✗"
        lines.append(f"{icon} Status Code: {status}")

    if "size_bytes" in summary:
        lines.append(f"Size: {summary['size_bytes']:,} bytes")

    if "checksum" in summary:
        checksum = summary["checksum"]
        lines.append(f"Checksum: {checksum[:16]}...")

    if "size_change_pct" in summary:
        pct = summary["size_change_pct"]
        icon = "↓" if pct < 0 else "↑"
        lines.append(f"{icon} Size Change: {pct:+.1f}%")

    # Check results
    if "checks" in summary:
        checks = summary["checks"]
        lines.append("\nCheck Results:")
        for check_name, check_result in checks.items():
            if isinstance(check_result, dict):
                check_status = check_result.get("valid", True)
                icon = "✓" if check_status else "✗"
                status_text = "PASS" if check_status else "FAIL"

                # Add error message if present
                error_msg = check_result.get("error")
                error_text = f" - {error_msg}" if error_msg else ""

                lines.append(f"  {icon} {check_name}: {status_text}{error_text}")
            else:
                icon = "✓" if check_result else "✗"
                status_text = "PASS" if check_result else "FAIL"
                lines.append(f"  {icon} {check_name}: {status_text}")

    # Historical comparison
    if "history" in summary:
        history = summary["history"]
        if history.get("changed"):
            lines.append("\n⚠️  Content has changed (checksum mismatch)")
        if history.get("size_dropped_50pct"):
            lines.append("⚠️  Size dropped >50%")
        if history.get("anomaly"):
            lines.append("⚠️  ANOMALY DETECTED: Content changed and size dropped >50%")

    return "\n".join(lines) if lines else "No details available"

