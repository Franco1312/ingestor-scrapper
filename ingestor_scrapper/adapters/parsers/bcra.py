"""
Parser adapter for BCRA (Banco Central de la República Argentina) pages.

This parser is specific to the structure of www.bcra.gob.ar
and extracts financial/statistical data.
"""

import logging
from datetime import datetime
from typing import List, Optional

from bs4 import BeautifulSoup, Tag

from ingestor_scrapper.core.entities import Document, Record
from ingestor_scrapper.core.ports import HtmlParser

logger = logging.getLogger(__name__)

# Constants for better maintainability
EXPECTED_CELLS_PER_ROW = 3
CELL_INDEX_DETALLE = 0
CELL_INDEX_FECHA = 1
CELL_INDEX_VALOR = 2


class AdapterBcraParser(HtmlParser):
    """
    Parser adapter specifically for BCRA (www.bcra.gob.ar) structure.

    This parser extracts financial and statistical data from BCRA pages.
    Following Clean Code principles:
    - Single Responsibility: Each method has one clear purpose
    - Extract Methods: Complex logic broken into smaller methods
    - Constants: Magic numbers replaced with named constants
    """

    def __init__(self, parser: str = "html.parser"):
        """
        Initialize the parser.

        Args:
            parser: Parser type to use ('html.parser', 'lxml', 'html5lib')
        """
        self.parser = parser

    def parse(self, document: Document) -> List[Record]:
        """
        Parse HTML document from BCRA pages and extract structured records.

        Args:
            document: Document containing HTML content

        Returns:
            List[Record]: List of extracted records

        Raises:
            Exception: If parsing fails
        """
        try:
            # Validate input
            if not document.text or not document.text.strip():
                logger.warning("Empty HTML content for URL: %s", document.url)
                return []

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(document.text, self.parser)

            # Find all table rows and extract data
            rows = soup.find_all("tr")
            records = []

            for row in rows:
                record = self._extract_record_from_row(row, document.url)
                if record:
                    records.append(record)

            logger.info(
                "Extracted %d records from BCRA page: %s",
                len(records),
                document.url,
            )

            return records

        except Exception as e:
            logger.error("Error parsing BCRA page %s: %s", document.url, e)
            return []

    def _extract_record_from_row(self, row: Tag, url: str) -> Optional[Record]:
        """
        Extract a Record from a table row (tr element).

        Args:
            row: BeautifulSoup Tag representing a table row
            url: Source URL of the page

        Returns:
            Record if extraction successful, None otherwise

        This method follows Single Responsibility Principle:
        - One method, one purpose: extract data from one row
        """
        # Find all cells (td) in this row
        cells = row.find_all("td")

        # Validate row structure
        if not self._is_valid_row(cells):
            return None

        # Extract data from cells
        detalle = self._extract_detalle(cells[CELL_INDEX_DETALLE])
        fecha = self._extract_fecha(cells[CELL_INDEX_FECHA])
        valor = self._extract_valor(cells[CELL_INDEX_VALOR])

        # Validate extracted data
        if not self._is_valid_data(detalle, fecha, valor):
            return None

        # Log extracted data (helpful for debugging)
        logger.debug(
            "Extracted - Detalle: %s, Fecha: %s, Valor: %s",
            detalle,
            fecha,
            valor,
        )

        # Create and return record with data dict
        data = {
            "detalle": detalle,
            "fecha": fecha,
            "valor": valor,
        }

        return Record(
            data=data,
            source_url=url,
            fetched_at=datetime.now(),
        )

    def _is_valid_row(self, cells: List[Tag]) -> bool:
        """
        Validate that a row has the expected structure.

        Args:
            cells: List of table cell (td) elements

        Returns:
            True if row is valid, False otherwise
        """
        return len(cells) >= EXPECTED_CELLS_PER_ROW

    def _extract_detalle(self, cell: Tag) -> str:
        """
        Extract detalle (description) from first cell.

        Args:
            cell: First table cell containing a link

        Returns:
            Detalle text or empty string if not found
        """
        link = cell.find("a")
        if not link:
            return ""
        return link.get_text(strip=True)

    def _extract_fecha(self, cell: Tag) -> str:
        """
        Extract fecha (date) from second cell.

        Args:
            cell: Second table cell containing date

        Returns:
            Fecha text or empty string if not found
        """
        return cell.get_text(strip=True)

    def _extract_valor(self, cell: Tag) -> str:
        """
        Extract valor (value) from third cell.

        Args:
            cell: Third table cell containing value

        Returns:
            Valor text or empty string if not found
        """
        return cell.get_text(strip=True)

    def _is_valid_data(self, detalle: str, fecha: str, valor: str) -> bool:
        """
        Validate that extracted data is not empty.

        Args:
            detalle: Detalle text
            fecha: Fecha text
            valor: Valor text

        Returns:
            True if all data is valid, False otherwise
        """
        return bool(detalle and fecha and valor)
