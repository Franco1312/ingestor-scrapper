"""
BCRA Use Case - Use case for scraping BCRA pages.

This use case handles the specific logic for scraping pages from
www.bcra.gob.ar, following Clean Architecture principles.

Updated to use the new scalable architecture:
- DocumentFetcher instead of HtmlFetcher
- HtmlParser instead of Parser
- Normalizer to convert Records to Items
"""

import logging
from typing import List

from ingestor_scrapper.application.use_cases import UseCase
from ingestor_scrapper.core.entities import Item, Record
from ingestor_scrapper.core.ports import (
    DocumentFetcher,
    HtmlParser,
    Normalizer,
    OutputPort,
)

logger = logging.getLogger(__name__)


class BcraUseCase(UseCase):  # pylint: disable=too-few-public-methods
    """
    Use case for crawling BCRA URLs and parsing their content.

    Parses BCRA pages into structured items using the new architecture:
    1. Fetches document from URL using DocumentFetcher
    2. Parses document into Records using HtmlParser
    3. Normalizes Records into Items using Normalizer
    4. Outputs the results using OutputPort

    The use case is decoupled from specific implementations through dependency
    injection of the ports (interfaces).
    """

    def __init__(
        self,
        fetcher: DocumentFetcher,
        parser: HtmlParser,
        normalizer: Normalizer,
        output: OutputPort,
    ):
        """
        Initialize the use case with its dependencies.

        Args:
            fetcher: Implementation of DocumentFetcher port
            parser: Implementation of HtmlParser port
            normalizer: Implementation of Normalizer port
            output: Implementation of OutputPort
        """
        self.fetcher = fetcher
        self.parser = parser
        self.normalizer = normalizer
        self.output = output

    def execute(self, url: str) -> List[Item]:
        """
        Execute the crawl and parse workflow for BCRA pages.

        Args:
            url: URL to crawl and parse

        Returns:
            List[Item]: List of extracted items

        Best practices:
        - Validate inputs early
        - Check for empty results
        - Clear logging at each step
        """
        # Step 1: Fetch document
        # Best practice: Validate input URL
        if not url or not url.strip():
            logger.warning("Empty URL provided to use case")
            return []

        try:
            document = self.fetcher.fetch(url)
        except Exception as e:
            logger.error("Failed to fetch URL %s: %s", url, e)
            return []

        # Step 2: Validate fetched content
        if not document.text or not document.text.strip():
            logger.warning("Empty HTML content fetched from %s", url)
            return []

        # Step 3: Parse document into records
        try:
            records: List[Record] = self.parser.parse(document)
        except Exception as e:
            logger.error("Failed to parse document from %s: %s", url, e)
            return []

        # Step 4: Validate parsing results
        if not records:
            logger.warning("No records extracted from %s", url)
            return []

        # Step 5: Normalize records into items
        try:
            items: List[Item] = self.normalizer.normalize(records)
        except Exception as e:
            logger.error("Failed to normalize records from %s: %s", url, e)
            return []

        # Step 6: Validate normalization results
        if not items:
            logger.warning("No items normalized from %s", url)
            return []

        # Step 7: Output the results
        try:
            self.output.emit(items)
        except Exception as e:
            logger.error("Failed to output items: %s", e)

        return items
