"""
Excel Parser adapter - Parses Excel files (XLS/XLSX) into Records.

This adapter implements TabularParser for Excel files using pandas.
"""

import logging
from datetime import datetime
from io import BytesIO
from typing import List

import pandas as pd

from ingestor_scrapper.core.entities import ContentType, Document, Record
from ingestor_scrapper.core.ports import TabularParser

logger = logging.getLogger(__name__)


class AdapterExcelParser(TabularParser):
    """
    Adapter that implements TabularParser for Excel files (XLS/XLSX).

    This parser supports both .xls and .xlsx formats using pandas.
    """

    def __init__(self):
        """
        Initialize the Excel parser.

        Raises:
            ImportError: If pandas or openpyxl are not installed
        """
        try:
            import openpyxl  # type: ignore
            import pandas  # type: ignore

            self.openpyxl = openpyxl
            self.pandas = pandas
        except ImportError as e:
            logger.error(
                "Required libraries not installed: %s. "
                "Install with: pip install pandas openpyxl",
                e,
            )
            raise

    def parse(self, document: Document) -> List[Record]:
        """
        Parse Excel document into Records.

        Args:
            document: Document containing Excel content (bytes)

        Returns:
            List[Record]: List of extracted records

        Raises:
            ValueError: If content type is not XLS or XLSX
            OSError: If Excel file cannot be read
        """
        # Validate content type
        if document.content_type not in (ContentType.XLS, ContentType.XLSX):
            logger.warning(
                "AdapterExcelParser received document with content type: %s "
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
            df = pd.read_excel(excel_file, engine="openpyxl")

            # Log the content to see what we're working with
            logger.info(
                "Successfully parsed Excel file from %s. Shape: %dx%d",
                document.url,
                len(df),
                len(df.columns),
            )
            logger.info("Columns: %s", list(df.columns))
            logger.info("First few rows:\n%s", df.head(10).to_string())

            # Convert DataFrame to Records
            records = []
            for idx, row in df.iterrows():
                # Convert row to dictionary, handling NaN values
                row_dict = row.to_dict()
                # Convert all values to strings, replacing NaN with empty string
                clean_dict = {
                    str(k): str(v) if pd.notna(v) else ""
                    for k, v in row_dict.items()
                }

                # Create Record
                record = Record(
                    data=clean_dict,
                    source_url=document.url,
                    fetched_at=datetime.now(),
                )
                records.append(record)

            logger.info(
                "Extracted %d records from Excel file: %s",
                len(records),
                document.url,
            )

            return records

        except Exception as e:
            logger.error(
                "Error parsing Excel file from %s: %s",
                document.url,
                e,
                exc_info=True,
            )
            return []
