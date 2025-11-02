"""
Health module - Watchdog for detecting scraping breakages.

This module provides generic health checks for monitoring scraping targets,
detecting HTML/file changes, and sending notifications when issues are detected.
"""

from ingestor_scrapper.health.config import load_config
from ingestor_scrapper.health.runner import run_health_check

__all__ = ["load_config", "run_health_check"]

