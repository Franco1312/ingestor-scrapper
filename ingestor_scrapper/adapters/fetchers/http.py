"""
HTTP Fetcher adapter - Fetches documents using requests library.

This is a stub implementation for future use when we need to fetch
documents without Scrapy (e.g., for standalone scripts).

TODO: Install requests library when needed:
    pip install requests

Usage (future):
    import requests
    fetcher = AdapterHttpFetcher()
    document = fetcher.fetch("https://example.com")
"""

import logging
from typing import TYPE_CHECKING

from ingestor_scrapper.core.entities import Document
from ingestor_scrapper.core.ports import DocumentFetcher

if TYPE_CHECKING:
    import requests  # noqa: F401

logger = logging.getLogger(__name__)


class AdapterHttpFetcher(DocumentFetcher):
    """
    Adapter that implements DocumentFetcher using requests library.

    This adapter is a stub for future implementation.
    It will be used for non-Scrapy flows (standalone scripts, CLI, etc.).

    TODO:
    - Install requests: pip install requests
    - Implement fetch method using requests.get()
    - Detect content type from response headers or content
    - Handle errors (timeout, network errors, etc.)
    - Add retry logic if needed
    """

    def __init__(self, timeout: int = 30):
        """
        Initialize the HTTP fetcher.

        Args:
            timeout: Request timeout in seconds (default: 30)

        TODO: Add user-agent, headers configuration
        """
        self.timeout = timeout
        # TODO: Install requests when needed
        # try:
        #     import requests
        #     self.requests = requests
        # except ImportError:
        #     raise ImportError(
        #         "requests library not installed. "
        #         "Install with: pip install requests"
        #     )

    def fetch(self, url: str) -> Document:
        """
        Fetch a document from a URL using requests.

        Args:
            url: URL to fetch

        Returns:
            Document: Document entity with content and content type

        Raises:
            NotImplementedError: This is a stub - not yet implemented

        TODO:
        - Implement using requests.get()
        - Detect Content-Type from response.headers
        - Map Content-Type to ContentType enum
        - Handle binary content (PDF, Excel) vs text (HTML, CSV)
        - Return Document with appropriate fields (bytes vs text)
        """
        logger.warning(
            "AdapterHttpFetcher is a stub - not yet implemented. "
            "Use AdapterScrapyFetcher for Scrapy spiders."
        )
        raise NotImplementedError(
            "AdapterHttpFetcher is a stub. "
            "TODO: Implement using requests library."
        )
