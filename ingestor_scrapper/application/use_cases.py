"""
Use cases - Application business logic.

This module contains the use cases that orchestrate the ports
(HtmlFetcher, Parser, OutputPort) to fulfill business requirements.

Use cases are framework-agnostic and depend only on the interfaces (ports),
not on concrete implementations (adapters).
"""

from abc import ABC
from typing import List

from ingestor_scrapper.core.entities import Item, Page
from ingestor_scrapper.core.ports import HtmlFetcher, OutputPort, Parser


class UseCase(ABC):
    """
    Base class for all use cases.

    Provides a common interface for use case implementations.
    """

    pass


class CrawlAndParseUseCase(UseCase):  # pylint: disable=too-few-public-methods
    """
    Use case for crawling a URL and parsing its content into structured items.

    This use case:
    1. Fetches HTML content from a URL using HtmlFetcher
    2. Parses the HTML into Items using Parser
    3. Outputs the results using OutputPort

    The use case is decoupled from specific implementations through dependency
    injection of the ports (interfaces).
    """

    def __init__(
        self, fetcher: HtmlFetcher, parser: Parser, output: OutputPort
    ):
        """
        Initialize the use case with its dependencies.

        Args:
            fetcher: Implementation of HtmlFetcher port
            parser: Implementation of Parser port
            output: Implementation of OutputPort
        """
        self.fetcher = fetcher
        self.parser = parser
        self.output = output

    def execute(self, url: str) -> List[Item]:
        """
        Execute the crawl and parse workflow.

        Args:
            url: URL to crawl and parse

        Returns:
            List[Item]: List of extracted items
        """
        # TODO: Add error handling for fetch failures
        # Step 1: Fetch HTML content
        page: Page = self.fetcher.fetch(url)

        # TODO: Add validation for page content
        # TODO: Add retry logic if needed

        # Step 2: Parse HTML into structured items
        items: List[Item] = self.parser.parse(page.html, page.url)

        # TODO: Add filtering/transformation logic if needed

        # Step 3: Output the results
        self.output.emit(items)

        return items
