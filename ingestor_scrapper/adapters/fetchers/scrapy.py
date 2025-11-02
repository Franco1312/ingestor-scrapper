"""
Scrapy adapter for HtmlFetcher and DocumentFetcher ports.

This adapter implements both HtmlFetcher (for backward compatibility)
and DocumentFetcher (for new universal flows) using Scrapy's
Request/Response mechanism.
"""

from scrapy.http import Response

from ingestor_scrapper.core.entities import ContentType, Document, Page
from ingestor_scrapper.core.ports import DocumentFetcher, HtmlFetcher


class AdapterScrapyFetcher(HtmlFetcher):
    """
    Adapter that implements HtmlFetcher using Scrapy's Response object.

    This adapter is typically instantiated within a Scrapy spider's
    parse method, where the Response object is available.

    Usage in a spider:
        response = scrapy.Request(...)  # from spider
        fetcher = AdapterScrapyFetcher(response)
        page = fetcher.fetch(url)  # Uses the response's HTML
    """

    def __init__(self, response: Response):
        """
        Initialize the fetcher with a Scrapy Response.

        Args:
            response: Scrapy Response object containing the HTML
        """
        self.response = response

    def fetch(self, url: str) -> Page:
        """
        Extract HTML from the Scrapy Response and create a Page entity.

        Note: The url parameter is kept for interface consistency,
        but in practice we use self.response.url.

        Args:
            url: URL string (usually same as response.url)

        Returns:
            Page: Page entity with URL and HTML content
        """
        # Use the response's URL and HTML content
        return Page(
            url=self.response.url,
            html=self.response.text,
            status_code=self.response.status,
        )


class AdapterScrapyDocumentFetcher(DocumentFetcher):
    """
    Adapter that implements DocumentFetcher using Scrapy's Response object.

    This adapter detects content type from response headers and creates
    a Document entity. Supports multiple formats (HTML, CSV, Excel, PDF, etc.).

    Usage in a spider:
        response = scrapy.Request(...)  # from spider
        fetcher = AdapterScrapyDocumentFetcher(response)
        document = fetcher.fetch(url)  # Returns Document with detected content type
    """

    def __init__(self, response: Response):
        """
        Initialize the document fetcher with a Scrapy Response.

        Args:
            response: Scrapy Response object
        """
        self.response = response

    def _detect_content_type(self) -> ContentType:
        """
        Detect content type from response headers.

        Returns:
            ContentType: Detected content type enum

        TODO: Improve content type detection:
        - Check Content-Type header
        - Check file extension from URL
        - Check content (magic bytes) for binary formats
        """
        content_type_header = (
            self.response.headers.get("Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .lower()
        )

        if "html" in content_type_header:
            return ContentType.HTML
        elif "csv" in content_type_header:
            return ContentType.CSV
        elif (
            "excel" in content_type_header
            or "spreadsheet" in content_type_header
        ):
            return ContentType.XLSX
        elif "xls" in content_type_header or content_type_header.endswith(
            ".xls"
        ):
            return ContentType.XLS
        elif "xlsx" in content_type_header or content_type_header.endswith(
            ".xlsx"
        ):
            return ContentType.XLSX
        elif "pdf" in content_type_header:
            return ContentType.PDF
        elif self.response.url.endswith(".csv"):
            return ContentType.CSV
        elif self.response.url.endswith(".xls"):
            return ContentType.XLS
        elif self.response.url.endswith(".xlsx"):
            return ContentType.XLSX
        elif self.response.url.endswith(".pdf"):
            return ContentType.PDF
        else:
            # Default to HTML for text responses
            return ContentType.HTML

    def fetch(self, url: str) -> Document:
        """
        Fetch a document from the Scrapy Response.

        Detects content type and creates appropriate Document entity.

        Args:
            url: URL string (usually same as response.url)

        Returns:
            Document: Document entity with URL, content, and content type
        """
        content_type = self._detect_content_type()

        # Determine if we need bytes or text
        if content_type in (
            ContentType.PDF,
            ContentType.XLS,
            ContentType.XLSX,
        ):
            # Binary formats need bytes
            return Document(
                url=self.response.url,
                content_type=content_type,
                bytes=self.response.body,
                status_code=self.response.status,
            )
        else:
            # Text formats (HTML, CSV) use text
            return Document(
                url=self.response.url,
                content_type=content_type,
                text=self.response.text,
                status_code=self.response.status,
            )
