"""
JSON adapter for OutputPort - Outputs items as JSON.

This adapter implements the OutputPort interface by converting items
to JSON format and outputting them.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

from ingestor_scrapper.core.entities import Item
from ingestor_scrapper.core.ports import OutputPort

logger = logging.getLogger(__name__)


class AdapterJsonOutput(OutputPort):
    """
    Adapter that implements OutputPort by outputting items as JSON.

    This adapter converts items to JSON format and logs/prints them.
    """

    def __init__(
        self,
        indent: int = 2,
        ensure_ascii: bool = False,
        output_file: Optional[str] = None,
    ):
        """
        Initialize the JSON output adapter.

        Args:
            indent: Number of spaces for JSON indentation (default: 2)
            ensure_ascii: If False, allows non-ASCII characters
                         (default: False)
            output_file: Path to output file. If None, defaults to
                        "output.json" in current directory
        """
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.output_file = output_file or "output.json"
        self._output_path = Path(self.output_file)

    def emit(self, items: List[Item]) -> None:
        """
        Convert items to JSON and output them.

        Args:
            items: List of items to output as JSON

        TODO: Add file output capability
        TODO: Add option to output to stdout vs file
        """
        if not items:
            logger.info("No items to output")
            return

        # Convert items to dictionaries
        items_dict = [
            {
                "title": item.title,
                "content": item.content,
                "url": item.url,
            }
            for item in items
        ]

        # Create JSON structure
        output_data = {
            "total_items": len(items),
            "items": items_dict,
        }

        # Convert to JSON string
        json_output = json.dumps(
            output_data, indent=self.indent, ensure_ascii=self.ensure_ascii
        )

        # Save JSON to file
        self._save_to_file(json_output)

        # Also print to stdout for immediate feedback
        print(json_output)

    def _save_to_file(self, json_output: str) -> None:
        """
        Save JSON output to file.

        Args:
            json_output: JSON string to save

        This method handles file creation and error handling
        following Clean Code principles.
        """
        try:
            # Create parent directory if it doesn't exist
            self._output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write JSON to file
            with open(self._output_path, "w", encoding="utf-8") as file:
                file.write(json_output)

            logger.info("JSON saved to: %s", self._output_path.absolute())
        except OSError as e:
            logger.error(
                "Failed to save JSON to file %s: %s",
                self._output_path,
                e,
            )
