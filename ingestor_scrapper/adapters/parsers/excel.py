"""
Excel Parser adapter - Parses Excel files (XLS/XLSX) into Records.

This adapter implements TabularParser for Excel files using openpyxl/xlrd.

TODO: Install openpyxl for XLSX support:
    pip install openpyxl

TODO: Install xlrd for XLS support (legacy):
    pip install xlrd
    Note: xlrd 2.0+ only supports XLSX, use older version for XLS
"""

import logging
from typing import List

from ingestor_scrapper.core.entities import ContentType, Document, Record
from ingestor_scrapper.core.ports import TabularParser

logger = logging.getLogger(__name__)


class AdapterExcelParser(TabularParser):
    """
    Adapter that implements TabularParser for Excel files (XLS/XLSX).

    This parser supports both .xls and .xlsx formats.

    TODO:
    - Install openpyxl: pip install openpyxl (for XLSX)
    - Install xlrd: pip install xlrd (for XLS, legacy format)
    - Implement parsing logic using openpyxl/xlrd
    - Handle multiple sheets
    - Support header row detection
    - Handle different data types (dates, numbers, text)
    """

    def __init__(self):
        """
        Initialize the Excel parser.

        TODO: Check for installed libraries and raise error if missing
        """
        # TODO: Check for openpyxl/xlrd when ready
        # try:
        #     import openpyxl
        #     self.openpyxl = openpyxl
        # except ImportError:
        #     logger.warning(
        #         "openpyxl not installed. "
        #         "Install with: pip install openpyxl"
        #     )

    def parse(self, document: Document) -> List[Record]:
        """
        Parse Excel document into Records.

        Args:
            document: Document containing Excel content (bytes)

        Returns:
            List[Record]: List of extracted records

        Raises:
            NotImplementedError: This is a stub - not yet implemented

        TODO:
        - Validate content type is XLS or XLSX
        - Use openpyxl/xlrd to parse document.bytes
        - Read sheets and rows
        - Convert rows to Records with appropriate field names
        - Handle errors gracefully
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

        logger.warning(
            "AdapterExcelParser is a stub - not yet implemented. "
            "TODO: Install openpyxl/xlrd and implement parsing logic. "
            "URL: %s",
            document.url,
        )

        # Stub implementation
        return []
