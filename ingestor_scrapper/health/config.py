"""
Health configuration - Loads and validates watch configuration.

This module loads configuration from YAML/JSON files defining health checks
for different sites.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# Configuration schema structure
# {
#   "site_id": {
#     "url": str,
#     "type": str,  # "html", "csv", "excel", "pdf", "binary"
#     "selectors": Optional[List[str]],
#     "min_bytes": Optional[int],
#     "expected_columns": Optional[List[str]],
#     "min_rows": Optional[int],
#     "content_type": Optional[str],
#     "checksum_window": Optional[int],
#     "notify": Optional[Dict[str, str]]
#   }
# }


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load health check configuration from YAML/JSON file.

    Args:
        config_path: Path to config file. If None, looks for configs/watch.yaml
                     or falls back to configs/watch.example.yaml

    Returns:
        Dict with site_id -> config mappings

    Raises:
        FileNotFoundError: If no config file found
        ValueError: If config is invalid
    """
    if config_path is None:
        # Try watch.yaml/watch.json first, then fall back to examples
        project_root = Path(__file__).parent.parent.parent
        yaml_path = project_root / "configs" / "watch.yaml"
        json_path = project_root / "configs" / "watch.json"
        example_yaml = project_root / "configs" / "watch.example.yaml"
        example_json = project_root / "configs" / "watch.example.json"

        if yaml_path.exists():
            config_path = str(yaml_path)
        elif json_path.exists():
            config_path = str(json_path)
        elif example_yaml.exists():
            config_path = str(example_yaml)
            logger.warning(
                "Using example config file: %s. "
                "Create configs/watch.yaml or configs/watch.json for production.",
                example_yaml,
            )
        elif example_json.exists():
            config_path = str(example_json)
            logger.warning(
                "Using example config file: %s. "
                "Create configs/watch.yaml or configs/watch.json for production.",
                example_json,
            )
        else:
            raise FileNotFoundError(
                f"Config file not found. Expected one of: {yaml_path}, {json_path}, "
                f"{example_yaml}, or {example_json}"
            )

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # For now, support JSON only (no YAML dependency)
    # In the future, can add PyYAML for YAML support
    if config_file.suffix in (".yaml", ".yml"):
        logger.warning(
            "YAML support not yet implemented. Rename to .json or add PyYAML."
        )
        raise ValueError(
            "YAML support requires PyYAML library. "
            "Use JSON format for now or install: pip install pyyaml"
        )

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")

    # Validate and normalize config
    validated_config: Dict[str, Any] = {}
    for site_id, site_config in data.items():
        try:
            validated_config[site_id] = _validate_site_config(site_id, site_config)
        except ValueError as e:
            logger.error("Invalid config for site %s: %s", site_id, e)
            continue

    logger.info(
        "Loaded health config: %d sites configured from %s",
        len(validated_config),
        config_path,
    )

    return validated_config


def _validate_site_config(site_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a single site's configuration.

    Args:
        site_id: Site identifier
        config: Raw configuration dict

    Returns:
        Validated configuration dict

    Raises:
        ValueError: If config is invalid
    """
    if not isinstance(config, dict):
        raise ValueError(f"Config must be a dict for site: {site_id}")

    # Required fields
    if "url" not in config:
        raise ValueError(f"Missing required field 'url' for site: {site_id}")
    if not isinstance(config["url"], str):
        raise ValueError(f"Field 'url' must be a string for site: {site_id}")

    if "type" not in config:
        raise ValueError(f"Missing required field 'type' for site: {site_id}")

    valid_types = ("html", "csv", "excel", "pdf", "binary")
    if config["type"] not in valid_types:
        raise ValueError(
            f"Field 'type' must be one of {valid_types} for site: {site_id}"
        )

    # Optional fields with defaults
    validated = {
        "url": config["url"],
        "type": config["type"],
        "selectors": config.get("selectors", []),
        "min_bytes": config.get("min_bytes", 0),
        "expected_columns": config.get("expected_columns", []),
        "min_rows": config.get("min_rows", 0),
        "content_type": config.get("content_type"),
        "verify_ssl": config.get("verify_ssl", True),
        "checksum_window": config.get("checksum_window", 10),
        "notify": config.get("notify", {}),
    }

    # Type validation
    if not isinstance(validated["selectors"], list):
        raise ValueError(f"Field 'selectors' must be a list for site: {site_id}")
    if not isinstance(validated["min_bytes"], int):
        raise ValueError(f"Field 'min_bytes' must be an int for site: {site_id}")
    if not isinstance(validated["expected_columns"], list):
        raise ValueError(
            f"Field 'expected_columns' must be a list for site: {site_id}"
        )
    if not isinstance(validated["min_rows"], int):
        raise ValueError(f"Field 'min_rows' must be an int for site: {site_id}")
    if validated["content_type"] and not isinstance(validated["content_type"], str):
        raise ValueError(
            f"Field 'content_type' must be a string for site: {site_id}"
        )
    if not isinstance(validated["verify_ssl"], bool):
        raise ValueError(f"Field 'verify_ssl' must be a bool for site: {site_id}")
    if not isinstance(validated["checksum_window"], int):
        raise ValueError(
            f"Field 'checksum_window' must be an int for site: {site_id}"
        )
    if not isinstance(validated["notify"], dict):
        raise ValueError(f"Field 'notify' must be a dict for site: {site_id}")

    return validated

