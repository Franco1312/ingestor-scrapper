"""
Generic Normalizer - Fallback normalizer for generic Records.

This normalizer provides a fallback mechanism that converts any
Record structure into Item entities using generic serialization.
"""

import json
import logging
from typing import List

from ingestor_scrapper.core.entities import Item, Record
from ingestor_scrapper.core.ports import Normalizer

logger = logging.getLogger(__name__)


class AdapterGenericNormalizer(Normalizer):
    """
    Adapter that implements Normalizer with generic fallback logic.

    This normalizer converts any Record structure into Item entities
    by serializing the Record.data dictionary as JSON in Item.content.

    Use this as a fallback when no site-specific normalizer exists.

    TODO:
    - Improve serialization logic
    - Extract title from data if possible
    - Handle nested data structures
    """

    def normalize(self, records: List[Record]) -> List[Item]:
        """
        Normalize records into Items using generic logic.

        Args:
            records: List of records to normalize

        Returns:
            List[Item]: List of normalized items

        This method:
        - Serializes Record.data as JSON in Item.content
        - Uses empty string for Item.title
        - Uses Record.source_url for Item.url
        """
        items = []

        for record in records:
            try:
                # Serialize data as JSON
                content = json.dumps(record.data, ensure_ascii=False)

                # Try to extract title from data if available
                title = (
                    record.data.get("title")
                    or record.data.get("name")
                    or record.data.get("detalle")
                    or ""
                )

                item = Item(
                    title=title,
                    content=content,
                    url=record.source_url,
                )
                items.append(item)

            except Exception as e:
                logger.warning("Failed to normalize record: %s", e)
                continue

        logger.debug(
            "Normalized %d records into items using generic normalizer",
            len(items),
        )

        return items
