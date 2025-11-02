"""
BCRA Excel Parser adapter - Parses BCRA Excel files and extracts specific indicators.

This parser is specific to the BCRA Informe Monetario Diario Excel structure
and extracts: Reservas Internacionales, Tipo de Cambio, and Base Monetaria.
Based on API_Series sheet mapping:
- Reservas Internacionales: sheet 'RESERVAS', column 2 (C)
- Tipo de Cambio: sheet 'RESERVAS', column 15 (P)
- Base Monetaria: sheet 'BASE MONETARIA', column 29 (AD)
"""

import logging
from datetime import datetime
from io import BytesIO
from typing import List

import pandas as pd  # type: ignore

from ingestor_scrapper.core.entities import ContentType, Document, Record
from ingestor_scrapper.core.ports import TabularParser

logger = logging.getLogger(__name__)

# Constants for BCRA Excel indicators
# Sheet and column mappings for BCRA Informe Monetario Diario
SHEET_RESERVAS = "RESERVAS"
SHEET_BASE_MONETARIA = "BASE MONETARIA"
COLUMN_DATE = 0
COLUMN_RESERVAS_INTERNACIONALES = 2
COLUMN_TIPO_CAMBIO = 15
COLUMN_BASE_MONETARIA = 29


class AdapterBcraExcelParser(TabularParser):
    """
    Adapter that implements TabularParser for BCRA Excel files.

    This parser extracts specific indicators from BCRA Informe Monetario Diario:
    - Reservas Internacionales del BCRA (en millones de dólares) - column C (2)
    - Tipo de Cambio Mayorista ($ por USD) - column P (15)
    - Base Monetaria Total (en millones de pesos) - column AD (29)
    """

    def __init__(self):
        """
        Initialize the BCRA Excel parser.
        """
        try:
            import openpyxl  # type: ignore

            self.openpyxl = openpyxl
        except ImportError as e:
            logger.error(
                "Required libraries not installed: %s. "
                "Install with: pip install pandas openpyxl",
                e,
            )
            raise

    def _extract_most_recent_value(
        self,
        excel_file: BytesIO,
        sheet_name: str,
        value_column: int,
        source_url: str,
        indicador: str,
        unidad: str,
        variable_interna: str,
    ) -> Record | None:
        """
        Extract the most recent value from an Excel sheet column.

        This is a flexible helper method that:
        1. Reads the specified Excel sheet
        2. Finds all valid rows with dates and values
        3. Sorts by date and returns the most recent value

        Args:
            excel_file: BytesIO object with Excel content
            sheet_name: Name of the sheet to read
            value_column: Column index containing the value to extract
            source_url: URL of the Excel file
            indicador: Human-readable indicator name
            unidad: Unit of measurement
            variable_interna: Internal variable name

        Returns:
            Record with the most recent value or None if not found
        """
        try:
            excel_file.seek(0)
            df = pd.read_excel(
                excel_file,
                sheet_name=sheet_name,
                header=None,
                engine="openpyxl",
            )

            # Extract all valid rows with dates and values
            valid_rows = []
            for idx in range(len(df)):
                date_val = df.iloc[idx, COLUMN_DATE]
                value = df.iloc[idx, value_column]

                # Check if we have valid data
                # Support both Python and numpy numeric types
                is_numeric = isinstance(
                    value, (int, float)
                ) or pd.api.types.is_numeric_dtype(type(value))
                if (
                    pd.notna(date_val)
                    and pd.notna(value)
                    and is_numeric
                    and value > 0
                ):
                    try:
                        # Try to parse date to ensure it's a real date
                        pd.to_datetime(date_val)
                        valid_rows.append((idx, date_val, value))
                    except (ValueError, TypeError):
                        # Skip if date can't be parsed
                        continue

            if not valid_rows:
                logger.warning(
                    "Could not find valid data for indicator: %s",
                    indicador,
                )
                return None

            # Sort by date and get the most recent
            sorted_rows = sorted(
                valid_rows,
                key=lambda x: pd.to_datetime(x[1]),
                reverse=True,
            )

            # Get the most recent row
            most_recent = sorted_rows[0]
            date_val, value = most_recent[1], most_recent[2]

            data = {
                "indicador": indicador,
                "valor": str(value),
                "fecha": str(date_val),
                "unidad": unidad,
                "variable_interna": variable_interna,
            }

            record = Record(
                data=data,
                source_url=source_url,
                fetched_at=datetime.now(),
            )

            logger.info(
                "Extracted %s: %s (fecha: %s)",
                indicador,
                value,
                date_val,
            )
            return record

        except Exception as e:
            logger.error(
                "Error extracting %s: %s",
                indicador,
                e,
                exc_info=True,
            )
            return None

    def parse(self, document: Document) -> List[Record]:
        """
        Parse BCRA Excel document and extract specific indicators.

        Args:
            document: Document containing Excel content (bytes)

        Returns:
            List[Record]: List of extracted records with indicator data
        """
        # Validate content type
        if document.content_type not in (ContentType.XLS, ContentType.XLSX):
            logger.warning(
                "AdapterBcraExcelParser received document with content type: %s "
                "(expected XLS or XLSX)",
                document.content_type.value,
            )
            return []

        if not document.bytes:
            logger.warning(
                "Excel document has no bytes content for URL: %s",
                document.url,
            )
            return []

        try:
            # Read Excel file from bytes
            excel_file = BytesIO(document.bytes)

            # Extract indicators from different sheets
            records = []

            # 1. Extract Reservas Internacionales from RESERVAS sheet
            reservas_record = self._extract_reservas_internacionales(
                excel_file, document.url
            )
            if reservas_record:
                records.append(reservas_record)

            # 2. Extract Tipo de Cambio from RESERVAS sheet
            tc_record = self._extract_tipo_cambio(excel_file, document.url)
            if tc_record:
                records.append(tc_record)

            # 3. Extract Base Monetaria from BASE MONETARIA sheet
            bm_record = self._extract_base_monetaria(excel_file, document.url)
            if bm_record:
                records.append(bm_record)

            logger.info(
                "Extracted %d indicator(s) from BCRA Excel file: %s",
                len(records),
                document.url,
            )

            return records

        except Exception as e:
            logger.error(
                "Error parsing BCRA Excel file from %s: %s",
                document.url,
                e,
                exc_info=True,
            )
            return []

    def _extract_reservas_internacionales(
        self, excel_file: BytesIO, source_url: str
    ) -> Record | None:
        """
        Extract Reservas Internacionales from RESERVAS sheet, column C (2).

        Args:
            excel_file: BytesIO object with Excel content
            source_url: URL of the Excel file

        Returns:
            Record with reservas_internacionales_usd or None if not found
        """
        return self._extract_most_recent_value(
            excel_file=excel_file,
            sheet_name=SHEET_RESERVAS,
            value_column=COLUMN_RESERVAS_INTERNACIONALES,
            source_url=source_url,
            indicador="Reservas Internacionales del BCRA",
            unidad="Millones de USD",
            variable_interna="reservas_internacionales_usd",
        )

    def _extract_tipo_cambio(
        self, excel_file: BytesIO, source_url: str
    ) -> Record | None:
        """
        Extract Tipo de Cambio from RESERVAS sheet, column P (15).

        Args:
            excel_file: BytesIO object with Excel content
            source_url: URL of the Excel file

        Returns:
            Record with tipo_cambio_oficial or None if not found
        """
        return self._extract_most_recent_value(
            excel_file=excel_file,
            sheet_name=SHEET_RESERVAS,
            value_column=COLUMN_TIPO_CAMBIO,
            source_url=source_url,
            indicador="Tipo de Cambio Mayorista",
            unidad="Pesos por USD",
            variable_interna="tipo_cambio_oficial",
        )

    def _extract_base_monetaria(
        self, excel_file: BytesIO, source_url: str
    ) -> Record | None:
        """
        Extract Base Monetaria from BASE MONETARIA sheet, column AD (29).

        Args:
            excel_file: BytesIO object with Excel content
            source_url: URL of the Excel file

        Returns:
            Record with base_monetaria_total_ars or None if not found
        """
        return self._extract_most_recent_value(
            excel_file=excel_file,
            sheet_name=SHEET_BASE_MONETARIA,
            value_column=COLUMN_BASE_MONETARIA,
            source_url=source_url,
            indicador="Base Monetaria Total",
            unidad="Millones de ARS",
            variable_interna="base_monetaria_total_ars",
        )
