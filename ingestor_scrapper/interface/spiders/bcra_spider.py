"""
BCRA spider - Scrapes pages from www.bcra.gob.ar.

This spider demonstrates the Clean Architecture layers:
1. Uses BcraUseCase from application layer
2. Injects adapters (Scrapy fetcher, BCRA parser, JSON output)
3. Executes the use case to crawl and parse BCRA pages

Best practices applied:
- Constants for URLs and configuration
- Input validation
- Specific exception handling
- Clear error messages

Run with: scrapy crawl bcra
"""

import logging

import scrapy
from scrapy.http import Response

from ingestor_scrapper.adapters.fetchers import AdapterScrapyDocumentFetcher
from ingestor_scrapper.adapters.normalizers import AdapterBcraNormalizer
from ingestor_scrapper.adapters.outputs import AdapterJsonOutput
from ingestor_scrapper.adapters.parsers import AdapterBcraParser
from ingestor_scrapper.application.bcra_use_case import BcraUseCase

logger = logging.getLogger(__name__)

# Constants for better maintainability
BCRA_BASE_URL = "https://www.bcra.gob.ar"
BCRA_PRINCIPALES_VARIABLES_URL = (
    f"{BCRA_BASE_URL}/PublicacionesEstadisticas/Principales_variables.asp"
)
BCRA_DOMAINS = ["bcra.gob.ar", "www.bcra.gob.ar"]
JSON_OUTPUT_FILE = "bcra_data.json"


class BcraSpider(scrapy.Spider):
    """
    Spider that crawls www.bcra.gob.ar and extracts financial/statistical data.

    This spider demonstrates the Clean Architecture pattern:
    - Spider (interface layer) orchestrates the use case
    - Use case (application layer) coordinates the ports
    - Adapters (adapters layer) implement the ports with specific frameworks

    Best practices:
    - Constants for configuration
    - Input validation
    - Proper error handling
    - Clear separation of concerns
    """

    name = "bcra"
    allowed_domains = BCRA_DOMAINS
    start_urls = [BCRA_PRINCIPALES_VARIABLES_URL]

    def __init__(self, *args, **kwargs):
        """
        Initialize the spider.

        Note: Spiders are instantiated by Scrapy, so we can't easily
        inject dependencies here. Instead, we instantiate adapters in parse().

        TODO: Consider using spider middleware or custom spider loader
        for better dependency injection.
        """
        super().__init__(*args, **kwargs)

    def parse(self, response: Response) -> None:
        """
        Parse the response using Clean Architecture layers.

        This method:
        1. Validates the response
        2. Creates adapters (fetcher, parser, output)
        3. Creates and executes the use case
        4. Logs the results

        Args:
            response: Scrapy Response object

        Best practices:
        - Validate response before processing
        - Use specific exceptions when possible
        - Clear error messages for debugging
        """
        # Validate response (best practice: check early)
        if not self._is_valid_response(response):
            logger.warning(
                "Invalid response for URL %s: status %d",
                response.url,
                response.status,
            )
            return

        # Step 1: Create adapters (wire dependencies)
        # Following Dependency Injection pattern
        fetcher = AdapterScrapyDocumentFetcher(response)
        parser = AdapterBcraParser()
        normalizer = AdapterBcraNormalizer()
        output = AdapterJsonOutput(output_file=JSON_OUTPUT_FILE)

        # Step 2: Create use case and inject dependencies
        use_case = BcraUseCase(
            fetcher=fetcher,
            parser=parser,
            normalizer=normalizer,
            output=output,
        )

        # Step 3: Execute the use case
        try:
            items = use_case.execute(response.url)

            # Log success with useful information
            self._log_results(response.url, items)

        except Exception as e:
            # Better error handling: log specific error details
            logger.error(
                "Error processing %s: %s",
                response.url,
                e,
                exc_info=True,
            )

    def _is_valid_response(self, response: Response) -> bool:
        """
        Validate that the response is suitable for processing.

        Args:
            response: Scrapy Response object

        Returns:
            True if response is valid, False otherwise

        Best practice: Validate early, fail fast
        """
        # Check HTTP status code
        if response.status != 200:
            return False

        # Check that response has content
        if not response.text or len(response.text.strip()) == 0:
            return False

        return True

    def _log_results(self, url: str, items: list) -> None:
        """
        Log parsing results in a clear, structured way.

        Args:
            url: URL that was processed
            items: List of extracted items

        Best practice: Extract logging logic to separate method
        """
        item_count = len(items) if items else 0
        logger.info("Successfully parsed %s", url)
        logger.info("Extracted %d item(s)", item_count)

        if item_count == 0:
            logger.warning("No items extracted from %s", url)
