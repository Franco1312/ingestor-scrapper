"""
Parsers module - Parser implementations.

This module contains adapters that implement parser ports (HtmlParser,
TabularParser, PdfParser), providing parsing capabilities for different
content types and formats.
"""

from ingestor_scrapper.adapters.parsers.bcra import AdapterBcraParser
from ingestor_scrapper.adapters.parsers.bs4 import AdapterBs4Parser
from ingestor_scrapper.adapters.parsers.csv import AdapterCsvParser
from ingestor_scrapper.adapters.parsers.excel import AdapterExcelParser
from ingestor_scrapper.adapters.parsers.pdf import AdapterPdfParser
from ingestor_scrapper.adapters.parsers.registry import PARSER_REGISTRY

__all__ = [
    "AdapterBcraParser",
    "AdapterBs4Parser",
    "AdapterCsvParser",
    "AdapterExcelParser",
    "AdapterPdfParser",
    "PARSER_REGISTRY",
]
