"""
Ports - Interfaces/Protocols for dependencies (Hexagonal Architecture).

This module defines the interfaces that the application layer will use.
Adapters in the adapters/ module will implement these interfaces.

These are abstract contracts that allow the core logic to remain
independent of specific implementations (Scrapy, BeautifulSoup, etc.).
"""

from abc import ABC, abstractmethod
from typing import List

from ingestor_scrapper.core.entities import Document, Item, Page, Record


class HtmlFetcher(ABC):
    """
    Port for fetching HTML content from URLs.

    Implementations:
    - AdapterScrapyFetcher (in adapters/fetcher_scrapy.py) uses Scrapy's Request/Response
    - Could also have AdapterRequestsFetcher, AdapterPlaywrightFetcher, etc.

    Example usage:
        fetcher = AdapterScrapyFetcher()
        page = await fetcher.fetch("https://example.com")
    """

    @abstractmethod
    def fetch(self, url: str) -> Page:
        """
        Fetch HTML content from a URL.

        Args:
            url: The URL to fetch

        Returns:
            Page: A Page entity containing the URL and HTML content

        Raises:
            Exception: If the fetch fails (implementation-specific)
        """
        pass


class Parser(ABC):
    """
    Port for parsing HTML content into structured Items.

    Implementations:
    - AdapterScrapeThisSiteParser (in adapters/parser_scrapethissite.py) uses BeautifulSoup4
    - Could also have AdapterLxmlParser, AdapterSelectolaxParser, etc.

    Example usage:
        parser = AdapterScrapeThisSiteParser()
        items = parser.parse(page.html, page.url)
    """

    @abstractmethod
    def parse(self, html: str, url: str) -> List[Item]:
        """
        Parse HTML content and extract structured data.

        Args:
            html: Raw HTML content
            url: Source URL of the HTML

        Returns:
            List[Item]: List of extracted items

        Raises:
            Exception: If parsing fails (implementation-specific)
        """
        pass


class OutputPort(ABC):
    """
    Port for outputting/emitting scraped items.

    Implementations:
    - AdapterStdoutOutput (in adapters/output_stdout.py) prints/logs items
    - Could also have AdapterFileOutput, AdapterDatabaseOutput, AdapterApiOutput, etc.

    Example usage:
        output = AdapterStdoutOutput()
        output.emit(items)
    """

    @abstractmethod
    def emit(self, items: List[Item]) -> None:
        """
        Emit/output scraped items.

        Args:
            items: List of items to output

        Returns:
            None
        """
        pass


class DocumentFetcher(ABC):
    """
    Port for fetching documents from URLs.

    This is a more generic version of HtmlFetcher that supports
    multiple content types (HTML, CSV, Excel, PDF, etc.).

    Implementations:
    - AdapterScrapyFetcher (uses Scrapy's Request/Response)
    - AdapterHttpFetcher (uses requests library, stub for now)
    - Could also have AdapterPlaywrightFetcher, etc.

    Example usage:
        fetcher = AdapterScrapyFetcher(response)
        document = fetcher.fetch("https://example.com")
    """

    @abstractmethod
    def fetch(self, url: str) -> Document:
        """
        Fetch a document from a URL.

        Args:
            url: The URL to fetch

        Returns:
            Document: A Document entity containing the URL, content,
                     content type, and status code

        Raises:
            Exception: If the fetch fails (implementation-specific)
        """
        pass


class HtmlParser(ABC):
    """
    Port for parsing HTML content into structured Records.

    This is the new interface for HTML parsing. Existing HTML parsers
    should implement this port.

    Implementations:
    - AdapterBs4Parser (uses BeautifulSoup4, TODO: install beautifulsoup4)
    - AdapterBcraParser (site-specific HTML parser)
    - Could also have AdapterLxmlParser, AdapterSelectolaxParser, etc.

    Example usage:
        parser = AdapterBs4Parser()
        records = parser.parse(document)

    Note: For site-specific parsing, use site-specific parsers like
    AdapterBcraParser that implement this port.
    """

    @abstractmethod
    def parse(self, document: Document) -> List[Record]:
        """
        Parse HTML document and extract structured records.

        Args:
            document: Document containing HTML content

        Returns:
            List[Record]: List of extracted records

        Raises:
            Exception: If parsing fails (implementation-specific)
        """
        pass


class TabularParser(ABC):
    """
    Port for parsing tabular data (CSV, Excel) into structured Records.

    Implementations:
    - AdapterCsvParser (uses csv module, native Python)
    - AdapterExcelParser (uses openpyxl/xlrd, TODO: install openpyxl)
    - Could also have AdapterPandasParser (uses pandas, optional)

    Example usage:
        parser = AdapterCsvParser()
        records = parser.parse(document)
    """

    @abstractmethod
    def parse(self, document: Document) -> List[Record]:
        """
        Parse tabular document and extract structured records.

        Args:
            document: Document containing CSV/Excel content

        Returns:
            List[Record]: List of extracted records

        Raises:
            Exception: If parsing fails (implementation-specific)
        """
        pass


class PdfParser(ABC):
    """
    Port for parsing PDF documents into structured Records.

    Implementations:
    - AdapterPdfParser (uses pdfplumber/tabula-py, TODO: install pdfplumber)

    Example usage:
        parser = AdapterPdfParser()
        records = parser.parse(document)
    """

    @abstractmethod
    def parse(self, document: Document) -> List[Record]:
        """
        Parse PDF document and extract structured records.

        Args:
            document: Document containing PDF content

        Returns:
            List[Record]: List of extracted records

        Raises:
            Exception: If parsing fails (implementation-specific)
        """
        pass


class Normalizer(ABC):
    """
    Port for normalizing Records into Items.

    Normalizers convert generic Record structures into domain-specific
    Item entities. Each site/case can have its own Normalizer to handle
    site-specific data structures.

    Implementations:
    - AdapterBcraNormalizer (BCRA-specific normalization)
    - AdapterGenericNormalizer (fallback generic normalization)
    - Could have more site-specific normalizers

    Example usage:
        normalizer = AdapterBcraNormalizer()
        items = normalizer.normalize(records)
    """

    @abstractmethod
    def normalize(self, records: List[Record]) -> List[Item]:
        """
        Normalize records into Items.

        Args:
            records: List of records to normalize

        Returns:
            List[Item]: List of normalized items

        Raises:
            Exception: If normalization fails (implementation-specific)
        """
        pass
