#!/usr/bin/env python3
"""
Health Watch CLI - Command-line interface for health monitoring.

Usage:
    python -m interface.watch <site_id> [--config configs/watch.yaml] [--dry-run]

Exit codes:
    0: INFO - All checks passed
    2: WARN - Warnings detected
    3: FAIL - Critical checks failed
"""

import argparse
import logging
import sys
from pathlib import Path

# Setup path to import from ingestor_scrapper
# This assumes the script is run from project root or via module execution
try:
    from ingestor_scrapper.health import run_health_check
except ImportError:
    # Fall back for direct execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from ingestor_scrapper.health import run_health_check


def setup_logging(verbose: bool = False) -> None:
    """
    Setup logging configuration.

    Args:
        verbose: If True, enable DEBUG level logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """
    Main CLI entry point.

    Returns:
        Exit code: 0 (INFO), 2 (WARN), 3 (FAIL)
    """
    parser = argparse.ArgumentParser(
        description="Health check watchdog for scraping targets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  - All checks passed (INFO)
  2  - Warnings detected (WARN)
  3  - Critical checks failed (FAIL)

Examples:
  python -m interface.watch bcra_principal
  python -m interface.watch bcra_principal --config custom-config.yaml
  python -m interface.watch bcra_monetario --dry-run
        """,
    )

    parser.add_argument(
        "site_id",
        help="Site identifier (e.g., 'bcra_principal', 'bcra_monetario')",
    )

    parser.add_argument(
        "--config",
        default=None,
        help="Path to config file (default: configs/watch.yaml or configs/watch.example.yaml)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't send notifications, just print results",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(verbose=args.verbose)

    logger = logging.getLogger(__name__)
    logger.info("Starting health check for site: %s", args.site_id)

    if args.dry_run:
        logger.info("Dry-run mode: No notifications will be sent")

    # Run health check
    try:
        exit_code = run_health_check(
            args.site_id, config_path=args.config, dry_run=args.dry_run
        )
        logger.info("Health check completed with exit code: %d", exit_code)
        return exit_code
    except KeyboardInterrupt:
        logger.error("Health check interrupted by user")
        return 130
    except Exception as e:
        logger.error("Health check failed: %s", e, exc_info=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())

