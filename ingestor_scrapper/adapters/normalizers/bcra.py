"""
BCRA Normalizer - Normalizes BCRA-specific Records into Items.

This normalizer converts BCRA-specific Record structures into
domain-specific Item entities.

The content field is structured as an object with:
- fecha: Date string (ISO 8601 format)
- valor: Numeric value (float)
"""

import logging
from datetime import datetime
from typing import Dict, List, Union

from ingestor_scrapper.core.entities import Item, Record
from ingestor_scrapper.core.ports import Normalizer

logger = logging.getLogger(__name__)


class AdapterBcraNormalizer(Normalizer):
    """
    Adapter that implements Normalizer for BCRA data.

    This normalizer converts BCRA-specific Record structures
    (with fields like detalle, fecha, valor) into Item entities.

    TODO:
    - Implement field-specific mapping rules
    - Handle different BCRA table structures
    - Validate data before normalization
    - Add transformation logic if needed
    """

    def normalize(self, records: List[Record]) -> List[Item]:
        """
        Normalize BCRA records into Items.

        Args:
            records: List of BCRA records to normalize

        Returns:
            List[Item]: List of normalized items

        The content field is structured as an object with:
        - fecha: Date string (ISO 8601 format YYYY-MM-DD)
        - valor: Numeric value (float)
        """
        items = []

        for record in records:
            try:
                # Extract fields from record data
                title = record.data.get(
                    "detalle", record.data.get("title", "")
                )
                fecha_str = record.data.get("fecha", "")
                valor_str = record.data.get("valor", "")

                # Parse and format fecha to ISO 8601 (YYYY-MM-DD)
                fecha_iso = self._parse_fecha(fecha_str)

                # Parse valor to number (float)
                valor_num = self._parse_valor(valor_str)

                # Create content as structured object
                content: Dict[str, Union[str, float]] = {
                    "fecha": fecha_iso,
                    "valor": valor_num,
                }

                url = record.source_url

                item = Item(title=title, content=content, url=url)
                items.append(item)

            except Exception as e:
                logger.warning("Failed to normalize BCRA record: %s", e)
                continue

        logger.debug("Normalized %d BCRA records into items", len(items))

        return items

    def _parse_fecha(self, fecha_str: str) -> str:
        """
        Parse BCRA fecha string to ISO 8601 format (YYYY-MM-DD).

        Args:
            fecha_str: Date string in format "DD/MM/YYYY"

        Returns:
            ISO 8601 date string (YYYY-MM-DD) or original string if parsing fails
        """
        if not fecha_str or not fecha_str.strip():
            return ""

        try:
            # BCRA date format: "29/10/2025" (DD/MM/YYYY)
            # Parse to datetime and format as ISO 8601
            fecha_obj = datetime.strptime(fecha_str.strip(), "%d/%m/%Y")
            return fecha_obj.strftime("%Y-%m-%d")
        except (ValueError, AttributeError) as e:
            logger.debug(
                "Failed to parse fecha '%s': %s, using original",
                fecha_str,
                e,
            )
            # Return original if parsing fails
            return fecha_str

    def _parse_valor(self, valor_str: str) -> float:
        """
        Parse BCRA valor string to numeric value (float).

        Args:
            valor_str: Value string in BCRA format
                      Examples: "40.764", "1.496,02" (punto thousands, comma decimal)

        Returns:
            Float value or 0.0 if parsing fails
        """
        if not valor_str or not valor_str.strip():
            return 0.0

        try:
            # Remove spaces
            cleaned = valor_str.strip().replace(" ", "")

            # BCRA format uses:
            # - Punto (.) as thousands separator: "1.496"
            # - Coma (,) as decimal separator: "1.496,02"
            # If contains comma, it's the decimal separator
            # Remove thousands separators (dots) and replace comma with dot
            if "," in cleaned:
                # Has decimal part: "1.496,02" -> remove dots, replace comma
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                # No decimal part: "40.764" -> remove dots (thousands separator)
                cleaned = cleaned.replace(".", "")

            # Try to parse as float
            return float(cleaned)
        except (ValueError, AttributeError) as e:
            logger.debug(
                "Failed to parse valor '%s': %s, using 0.0",
                valor_str,
                e,
            )
            # Return 0.0 if parsing fails
            return 0.0
