"""
Parser Registry - Central registry for parsers by content type.

This module provides a centralized registry mapping ContentType to
Parser implementations. This allows the ParserRouter to select the
appropriate parser for each content type.
"""

from ingestor_scrapper.adapters.parsers.bs4 import AdapterBs4Parser
from ingestor_scrapper.adapters.parsers.csv import AdapterCsvParser
from ingestor_scrapper.adapters.parsers.excel import AdapterExcelParser
from ingestor_scrapper.adapters.parsers.pdf import AdapterPdfParser
from ingestor_scrapper.core.entities import ContentType

# Initialize parsers
# TODO: Add error handling if parsers fail to initialize

_BS4_PARSER = AdapterBs4Parser()
_CSV_PARSER = AdapterCsvParser()
_EXCEL_PARSER = AdapterExcelParser()
_PDF_PARSER = AdapterPdfParser()

# Registry mapping ContentType to Parser
PARSER_REGISTRY = {
    ContentType.HTML: _BS4_PARSER,
    ContentType.CSV: _CSV_PARSER,
    ContentType.XLS: _EXCEL_PARSER,
    ContentType.XLSX: _EXCEL_PARSER,
    ContentType.PDF: _PDF_PARSER,
    # UNKNOWN is not included - will return None from router
}
