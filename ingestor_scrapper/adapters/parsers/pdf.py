"""
PDF Parser adapter - Parses PDF files into Records.

This adapter implements PdfParser for PDF files using pdfplumber or tabula-py.

TODO: Install pdfplumber for text and table extraction:
    pip install pdfplumber

TODO: Alternative - Install tabula-py for table extraction:
    pip install tabula-py
    Note: Requires Java runtime environment

Recommended: Start with pdfplumber (pure Python, easier setup).
"""

import logging
from typing import List

from ingestor_scrapper.core.entities import Document, Record
from ingestor_scrapper.core.ports import PdfParser

logger = logging.getLogger(__name__)


class AdapterPdfParser(PdfParser):
    """
    Adapter that implements PdfParser for PDF files.

    This parser can extract text and tables from PDF documents.

    TODO:
    - Install pdfplumber: pip install pdfplumber
    - Alternative: Install tabula-py: pip install tabula-py (requires Java)
    - Implement text extraction
    - Implement table extraction
    - Handle multi-page PDFs
    - Handle different PDF structures (scanned, text-based, etc.)
    """

    def __init__(self):
        """
        Initialize the PDF parser.

        TODO: Check for installed libraries and raise error if missing
        """
        # TODO: Check for pdfplumber when ready
        # try:
        #     import pdfplumber
        #     self.pdfplumber = pdfplumber
        # except ImportError:
        #     logger.warning(
        #         "pdfplumber not installed. "
        #         "Install with: pip install pdfplumber"
        #     )

    def parse(self, document: Document) -> List[Record]:
        """
        Parse PDF document into Records.

        Args:
            document: Document containing PDF content (bytes)

        Returns:
            List[Record]: List of extracted records

        Raises:
            NotImplementedError: This is a stub - not yet implemented

        TODO:
        - Validate content type is PDF
        - Use pdfplumber/tabula-py to parse document.bytes
        - Extract text content or tables
        - Convert to Records with appropriate field names
        - Handle errors gracefully
        """
        if not document.bytes:
            logger.warning(
                "PDF document has no bytes content for URL: %s",
                document.url,
            )
            return []

        logger.warning(
            "AdapterPdfParser is a stub - not yet implemented. "
            "TODO: Install pdfplumber/tabula-py and implement parsing logic. "
            "URL: %s",
            document.url,
        )

        # Stub implementation
        return []
