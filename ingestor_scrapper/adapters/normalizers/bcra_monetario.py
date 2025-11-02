"""
BCRA Monetario Normalizer - Normalizes BCRA Monetario records.

This normalizer converts BCRA Monetario records into Item entities,
keeping the content as a dictionary instead of JSON string.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from ingestor_scrapper.core.entities import Item, Record
from ingestor_scrapper.core.ports import Normalizer

logger = logging.getLogger(__name__)


class AdapterBcraMonetarioNormalizer(Normalizer):
    """
    Adapter that implements Normalizer for BCRA Monetario records.

    This normalizer converts BCRA Monetario records into Item entities,
    keeping the content as a dictionary (not stringified JSON).
    """

    def _transform_data_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data types in the record data.

        This method converts:
        - valor: string -> number (float)
        - fecha: string -> ISO date string

        Args:
            data: Raw data dictionary

        Returns:
            Dict with proper data types
        """
        transformed = data.copy()

        # Convert valor to number
        if "valor" in transformed:
            try:
                transformed["valor"] = float(transformed["valor"])
            except (ValueError, TypeError):
                logger.warning(
                    "Could not convert valor to float: %s",
                    transformed.get("valor"),
                )

        # Convert fecha to ISO format if it's a string
        if "fecha" in transformed:
            try:
                fecha_str = str(transformed["fecha"])
                # Try to parse and convert to ISO format
                if "T" not in fecha_str:
                    # Parse string like "2025-10-28 00:00:00"
                    parsed_date = datetime.fromisoformat(
                        fecha_str.replace(" ", "T")
                    )
                    transformed["fecha"] = parsed_date.isoformat()
                else:
                    # Already has time component
                    parsed_date = datetime.fromisoformat(fecha_str)
                    transformed["fecha"] = parsed_date.isoformat()
            except (ValueError, AttributeError) as e:
                logger.warning(
                    "Could not convert fecha to ISO format: %s. Error: %s",
                    transformed.get("fecha"),
                    e,
                )

        return transformed

    def normalize(self, records: List[Record]) -> List[Item]:
        """
        Normalize BCRA Monetario records into Items.

        Args:
            records: List of BCRA Monetario records to normalize

        Returns:
            List[Item]: List of normalized items with content as dict

        This method:
        - Transforms data types (valor -> number, fecha -> ISO date)
        - Uses Record.data as content (keep it as dict)
        - Extracts title from indicador field
        - Uses Record.source_url for Item.url
        """
        items = []

        for record in records:
            try:
                # Transform data types: valor -> number, fecha -> Date string
                content = self._transform_data_types(record.data)

                # Extract title from indicador field
                title = record.data.get("indicador", "")

                item = Item(
                    title=title,
                    content=content,
                    url=record.source_url,
                )
                items.append(item)

            except Exception as e:
                logger.warning(
                    "Failed to normalize BCRA Monetario record: %s", e
                )
                continue

        logger.debug(
            "Normalized %d BCRA Monetario records into items",
            len(items),
        )

        return items
