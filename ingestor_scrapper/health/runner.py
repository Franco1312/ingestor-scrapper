"""
Health runner - Orchestrates health checks for a site.

This module coordinates fetching, checking, and notifying for health monitoring.
"""

import logging
from typing import Any, Dict, Optional

from ingestor_scrapper.health import checks, config, notify, store

logger = logging.getLogger(__name__)


def run_health_check(
    site_id: str, config_path: Optional[str] = None, dry_run: bool = False
) -> int:
    """
    Run health check for a site and return exit code.

    Args:
        site_id: Site identifier
        config_path: Path to config file (optional)
        dry_run: If True, don't send notifications

    Returns:
        Exit code: 0 (INFO), 2 (WARN), 3 (FAIL)
    """
    try:
        # Load configuration
        all_configs = config.load_config(config_path)
        if site_id not in all_configs:
            logger.error("Site not found in config: %s", site_id)
            return 3

        site_config = all_configs[site_id]

        # Fetch content
        logger.info("Fetching %s...", site_id)
        try:
            # Allow SSL verification to be disabled via environment for testing
            verify_ssl = site_config.get("verify_ssl", True)
            if not verify_ssl:
                logger.warning("SSL verification disabled for %s", site_id)

            content_bytes, headers, status_code, final_url = checks.fetch(
                site_config["url"], verify_ssl=verify_ssl
            )
        except Exception as e:
            logger.error("Failed to fetch %s: %s", site_id, e)
            # Return FAIL exit code
            if not dry_run:
                notify_config = site_config.get("notify", {})
                notify.notify(
                    slack_webhook_env=notify_config.get("slack_webhook_env"),
                    email_env=notify_config.get("email_env"),
                    title=f"Health Check: {site_id} - Fetch Failed",
                    summary={
                        "url": site_config["url"],
                        "error": str(e),
                    },
                    level="FAIL",
                )
            return 3

        # Run checks based on type
        summary = _run_checks_for_type(
            site_config, content_bytes, headers, status_code, final_url
        )

        # Calculate checksum
        checksum = checks.checksum_sha256(content_bytes)
        summary["checksum"] = checksum

        # Compare with history
        historical = store.compare_with_history(
            len(content_bytes), checksum, site_id
        )
        summary["history"] = historical

        # Determine level
        level = _determine_level(summary, historical)

        # Update metrics
        rowcount = summary.get("row_count")
        store.update_metrics(
            site_id,
            checksum,
            len(content_bytes),
            rowcount=rowcount,
            checksum_window=site_config.get("checksum_window", 10),
        )

        # Notify
        exit_code = 0
        if not dry_run:
            notify_config = site_config.get("notify", {})
            exit_code = notify.notify(
                slack_webhook_env=notify_config.get("slack_webhook_env"),
                email_env=notify_config.get("email_env"),
                title=f"Health Check: {site_id}",
                summary=summary,
                level=level,
            )
        else:
            # In dry-run mode, just print to stdout
            notify._print_stdout(  # type: ignore
                f"Health Check: {site_id}", summary, level
            )
            # Map level to exit code
            exit_codes = {"INFO": 0, "WARN": 2, "FAIL": 3}
            exit_code = exit_codes.get(level, 0)

        return exit_code

    except Exception as e:
        logger.error("Health check failed for %s: %s", site_id, e, exc_info=True)
        return 3


def _run_checks_for_type(
    site_config: Dict[str, Any],
    content_bytes: bytes,
    headers: Dict[str, str],
    status_code: int,
    final_url: str,
) -> Dict[str, Any]:
    """
    Run checks based on site type.

    Args:
        site_config: Site configuration
        content_bytes: Fetched content
        headers: Response headers
        status_code: HTTP status code
        final_url: Final URL after redirects

    Returns:
        Summary dict with check results
    """
    summary: Dict[str, Any] = {
        "url": final_url,
        "status_code": status_code,
        "size_bytes": len(content_bytes),
        "checks": {},
    }

    check_type = site_config["type"]

    # Common checks for all types
    status_ok = checks.check_status(status_code)
    summary["checks"]["status"] = status_ok

    min_bytes_ok = checks.check_min_bytes(
        content_bytes, site_config.get("min_bytes", 0)
    )
    summary["checks"]["min_bytes"] = min_bytes_ok

    if site_config.get("content_type"):
        content_type_ok = checks.check_content_type(
            headers, site_config["content_type"]
        )
        summary["checks"]["content_type"] = content_type_ok

    # Type-specific checks
    if check_type == "html":
        if site_config.get("selectors"):
            selectors_result = checks.check_html_contains(
                content_bytes, site_config["selectors"]
            )
            # Convert dict to overall pass/fail
            all_found = all(selectors_result.values())
            summary["checks"]["html_selectors"] = {
                "valid": all_found,
                "results": selectors_result,
            }

    elif check_type in ("csv", "excel"):
        if site_config.get("expected_columns") or site_config.get("min_rows"):
            if check_type == "csv":
                schema_result = checks.check_csv_schema(
                    content_bytes,
                    site_config.get("expected_columns", []),
                    site_config.get("min_rows"),
                )
            else:  # excel
                schema_result = checks.check_excel_schema(
                    content_bytes,
                    site_config.get("expected_columns", []),
                    site_config.get("min_rows"),
                )

            summary["checks"]["schema"] = schema_result
            # Also store row_count for metrics
            if "row_count" in schema_result:
                summary["row_count"] = schema_result["row_count"]

    # For pdf/binary, no additional checks beyond common ones

    return summary


def _determine_level(summary: Dict[str, Any], historical: Dict[str, Any]) -> str:
    """
    Determine severity level based on check results and history.

    Args:
        summary: Check results summary
        historical: Historical comparison results

    Returns:
        Level: "INFO", "WARN", or "FAIL"
    """
    checks_dict = summary.get("checks", {})

    # FAIL conditions
    if not checks_dict.get("status", True):
        return "FAIL"

    if not checks_dict.get("min_bytes", True):
        return "FAIL"

    if "schema" in checks_dict:
        schema = checks_dict["schema"]
        if not schema.get("valid", True) and not schema.get("skipped", False):
            return "FAIL"

    if "html_selectors" in checks_dict:
        selectors = checks_dict["html_selectors"]
        if not selectors.get("valid", True):
            return "FAIL"

    # WARN conditions
    if historical.get("anomaly"):
        return "WARN"

    if historical.get("size_dropped_50pct"):
        return "WARN"

    # Default to INFO if all checks passed
    return "INFO"

