"""
BeautifulSoup4 Parser adapter - Parses HTML using BeautifulSoup4.

This adapter implements HtmlParser and provides generic HTML parsing
capabilities using BeautifulSoup4.

TODO: Install beautifulsoup4 and lxml when ready:
    pip install beautifulsoup4 lxml

The parser can be configured to use different parsers:
- 'html.parser' (built-in, no dependencies)
- 'lxml' (faster, requires: pip install lxml)
- 'html5lib' (strict, requires: pip install html5lib)
"""

import logging
from typing import TYPE_CHECKING, List

from ingestor_scrapper.core.entities import Document, Record
from ingestor_scrapper.core.ports import HtmlParser

if TYPE_CHECKING:
    from bs4 import BeautifulSoup  # noqa: F401

logger = logging.getLogger(__name__)


class AdapterBs4Parser(HtmlParser):
    """
    Adapter that implements HtmlParser using BeautifulSoup4.

    This parser provides generic HTML parsing capabilities.
    For site-specific parsing, use site-specific parsers like
    AdapterBcraParser.

    TODO:
    - Install beautifulsoup4: pip install beautifulsoup4
    - Optional: Install lxml for faster parsing: pip install lxml
    - Implement basic HTML parsing (extract title, links, text, etc.)
    - Handle malformed HTML gracefully
    """

    def __init__(self, parser: str = "html.parser"):
        """
        Initialize the BS4 parser.

        Args:
            parser: Parser type ('html.parser', 'lxml', 'html5lib')
                    Default: 'html.parser' (built-in, no dependencies)

        TODO: Add BeautifulSoup initialization when library is installed
        """
        self.parser = parser
        # TODO: Initialize BeautifulSoup when library is installed
        # try:
        #     from bs4 import BeautifulSoup
        #     self.BeautifulSoup = BeautifulSoup
        # except ImportError:
        #     logger.warning(
        #         "BeautifulSoup4 not installed. "
        #         "Install with: pip install beautifulsoup4"
        #     )

    def parse(self, document: Document) -> List[Record]:
        """
        Parse HTML document using BeautifulSoup4.

        Args:
            document: Document containing HTML content

        Returns:
            List[Record]: List of extracted records

        Raises:
            NotImplementedError: This is a stub - not yet implemented

        TODO:
        - Use BeautifulSoup to parse document.text
        - Extract basic information (title, links, text content)
        - Convert to Record format with data dict
        - Handle parsing errors gracefully
        - Return empty list if parsing fails
        """
        logger.warning(
            "AdapterBs4Parser is a stub - not yet fully implemented. "
            "TODO: Install beautifulsoup4 and implement parsing logic."
        )

        # Stub implementation - returns empty list
        # TODO: Implement actual parsing when beautifulsoup4 is installed
        return []
