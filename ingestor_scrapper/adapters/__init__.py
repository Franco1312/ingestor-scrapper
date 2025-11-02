"""
Adapters module - Framework-dependent implementations.

This module contains concrete implementations of the ports (interfaces)
defined in core/ports.py. These adapters bridge between the application
logic and external frameworks/libraries (Scrapy, BeautifulSoup, etc.).

The adapters are organized into submodules:
- fetchers: Document fetcher implementations (HTTP, Scrapy)
- parsers: Parser implementations (HTML, CSV, Excel, PDF, etc.)
- normalizers: Normalizer implementations (transform Records to Items)
- outputs: Output port implementations (JSON, stdout, etc.)
"""

# Re-export from submodules for convenience
from ingestor_scrapper.adapters.fetchers import (
    AdapterHttpFetcher,
    AdapterScrapyDocumentFetcher,
    AdapterScrapyFetcher,
)
from ingestor_scrapper.adapters.normalizers import (
    AdapterBcraNormalizer,
    AdapterGenericNormalizer,
)
from ingestor_scrapper.adapters.outputs import (
    AdapterJsonOutput,
    AdapterStdoutOutput,
)
from ingestor_scrapper.adapters.parsers import (
    AdapterBcraParser,
    AdapterBs4Parser,
    AdapterCsvParser,
    AdapterExcelParser,
    AdapterPdfParser,
    PARSER_REGISTRY,
)

__all__ = [
    # Fetchers
    "AdapterHttpFetcher",
    "AdapterScrapyDocumentFetcher",
    "AdapterScrapyFetcher",
    # Parsers
    "AdapterBcraParser",
    "AdapterBs4Parser",
    "AdapterCsvParser",
    "AdapterExcelParser",
    "AdapterPdfParser",
    "PARSER_REGISTRY",
    # Normalizers
    "AdapterBcraNormalizer",
    "AdapterGenericNormalizer",
    # Outputs
    "AdapterJsonOutput",
    "AdapterStdoutOutput",
]
