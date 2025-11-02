"""
Stdout adapter for OutputPort - Simple logging/output implementation.

This adapter implements the OutputPort interface by logging items
to stdout/console. This is a minimal implementation for demonstration.

TODO: Add more sophisticated output options:
    - File output
    - JSON formatting
    - Database storage
    - API endpoints
"""

import logging
from typing import List

from ingestor_scrapper.core.entities import Item
from ingestor_scrapper.core.ports import OutputPort

logger = logging.getLogger(__name__)


class AdapterStdoutOutput(OutputPort):
    """
    Adapter that implements OutputPort by logging items to stdout.

    This is a simple stub implementation. More sophisticated output
    strategies can be added later (file writing, database storage, etc.).
    """

    def __init__(self):
        """
        Initialize the output adapter.

        TODO: Accept configuration (log level, format, destination, etc.)
        """
        pass

    def emit(self, items: List[Item]) -> None:
        """
        Log items to stdout.

        Args:
            items: List of items to output

        TODO: Add formatting options (JSON, CSV, custom format)
        TODO: Add file output capability
        TODO: Add batch processing for large item lists
        """
        if not items:
            logger.info("No items to output")
            return

        logger.info("Found %d item(s):", len(items))
        for i, item in enumerate(items, start=1):
            logger.info("  [%d] Title: %s", i, item.title)
            if item.content:
                logger.info("      Content: %s", item.content)
            if item.url:
                logger.info("      URL: %s", item.url)
