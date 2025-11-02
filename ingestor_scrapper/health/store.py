"""
Health store - Lightweight persistence for health check metrics.

This module stores and retrieves historical metrics for comparison.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# Metrics file structure
# {
#   "site_id": {
#     "checksum": str,
#     "last_size": int,
#     "last_rowcount": Optional[int],
#     "history_checksums": List[str],  # Limited to checksum_window
#   }
# }


def load_metrics(metrics_dir: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Load metrics from JSON file.

    Args:
        metrics_dir: Directory containing metrics.json. If None, uses .watch/

    Returns:
        Dict with site_id -> metrics mappings
    """
    if metrics_dir is None:
        project_root = Path(__file__).parent.parent.parent
        metrics_dir = project_root / ".watch"
    else:
        metrics_dir = Path(metrics_dir)

    metrics_file = metrics_dir / "metrics.json"

    if not metrics_file.exists():
        logger.debug("Metrics file not found: %s. Starting fresh.", metrics_file)
        return {}

    try:
        with open(metrics_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.debug("Loaded metrics for %d sites from %s", len(data), metrics_file)
        return data
    except Exception as e:
        logger.error("Failed to load metrics: %s", e, exc_info=True)
        return {}


def save_metrics(
    metrics: Dict[str, Dict[str, Any]], metrics_dir: Optional[Path] = None
) -> None:
    """
    Save metrics to JSON file.

    Args:
        metrics: Metrics dict to save
        metrics_dir: Directory to save metrics.json. If None, uses .watch/
    """
    if metrics_dir is None:
        project_root = Path(__file__).parent.parent.parent
        metrics_dir = project_root / ".watch"
    else:
        metrics_dir = Path(metrics_dir)

    # Create directory if it doesn't exist
    metrics_dir.mkdir(parents=True, exist_ok=True)

    metrics_file = metrics_dir / "metrics.json"

    try:
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        logger.debug("Saved metrics for %d sites to %s", len(metrics), metrics_file)
    except Exception as e:
        logger.error("Failed to save metrics: %s", e, exc_info=True)


def update_metrics(
    site_id: str,
    checksum: str,
    size: int,
    rowcount: Optional[int] = None,
    checksum_window: int = 10,
    metrics_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Update metrics for a site and save to disk.

    Args:
        site_id: Site identifier
        checksum: Current checksum
        size: Current file size in bytes
        rowcount: Current row count (if applicable)
        checksum_window: Number of checksums to keep in history
        metrics_dir: Directory for metrics file

    Returns:
        Updated metrics dict for the site
    """
    all_metrics = load_metrics(metrics_dir)

    site_metrics = all_metrics.get(site_id, {})

    # Update current values
    site_metrics["checksum"] = checksum
    site_metrics["last_size"] = size
    if rowcount is not None:
        site_metrics["last_rowcount"] = rowcount

    # Update history
    if "history_checksums" not in site_metrics:
        site_metrics["history_checksums"] = []

    history = site_metrics["history_checksums"]

    # Add new checksum if different from last
    if not history or history[-1] != checksum:
        history.append(checksum)

    # Limit history size
    if len(history) > checksum_window:
        history = history[-checksum_window:]
        site_metrics["history_checksums"] = history

    # Save updated metrics
    all_metrics[site_id] = site_metrics
    save_metrics(all_metrics, metrics_dir)

    return site_metrics


def get_site_metrics(site_id: str, metrics_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Get metrics for a specific site.

    Args:
        site_id: Site identifier
        metrics_dir: Directory for metrics file

    Returns:
        Metrics dict for the site, or empty dict if not found
    """
    all_metrics = load_metrics(metrics_dir)
    return all_metrics.get(site_id, {})


def compare_with_history(
    current_size: int,
    current_checksum: str,
    site_id: str,
    metrics_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Compare current metrics with historical data to detect anomalies.

    Args:
        current_size: Current file size in bytes
        current_checksum: Current checksum
        site_id: Site identifier
        metrics_dir: Directory for metrics file

    Returns:
        Dict with:
            - changed: bool (checksum changed)
            - size_change_pct: float (percentage size change)
            - size_dropped_50pct: bool
            - anomaly: bool (any anomaly detected)
    """
    historical = get_site_metrics(site_id, metrics_dir)

    result = {
        "changed": False,
        "size_change_pct": 0.0,
        "size_dropped_50pct": False,
        "anomaly": False,
    }

    if not historical:
        logger.debug("No historical data for site: %s", site_id)
        return result

    # Check if checksum changed
    last_checksum = historical.get("checksum")
    result["changed"] = last_checksum != current_checksum

    # Check size change
    last_size = historical.get("last_size", 0)
    if last_size > 0:
        change_pct = ((current_size - last_size) / last_size) * 100
        result["size_change_pct"] = change_pct
        result["size_dropped_50pct"] = change_pct < -50

    # Detect anomaly if size dropped significantly but content changed
    result["anomaly"] = result["changed"] and result["size_dropped_50pct"]

    return result

