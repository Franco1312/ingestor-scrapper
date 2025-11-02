"""
Universal Spider - Example spider using UniversalIngestUseCase.

This spider demonstrates how to use the new universal ingest flow
with ParserRouter to handle multiple content types (HTML, CSV, Excel, PDF).

This spider can handle any URL and automatically routes to the
appropriate parser based on the document's content type.
"""

import logging

import scrapy
from scrapy.http import Response

from ingestor_scrapper.adapters.fetchers import (
    AdapterScrapyDocumentFetcher,
)
from ingestor_scrapper.adapters.normalizers import (
    AdapterGenericNormalizer,
)
from ingestor_scrapper.adapters.outputs import AdapterStdoutOutput
from ingestor_scrapper.adapters.parsers import PARSER_REGISTRY
from ingestor_scrapper.application.parser_router import ParserRouter
from ingestor_scrapper.application.universal_ingest_use_case import (
    UniversalIngestUseCase,
)

logger = logging.getLogger(__name__)


class UniversalSpider(scrapy.Spider):
    """
    Universal spider that can handle multiple content types.

    This spider:
    1. Takes start_urls from command line arguments
    2. Uses UniversalIngestUseCase with ParserRouter
    3. Automatically routes to appropriate parser based on content type
    4. Uses generic normalizer (can be swapped for site-specific ones)

    Usage:
        scrapy crawl universal -a url="https://example.com"
        scrapy crawl universal -a url="https://example.com/data.csv"
        scrapy crawl universal -a url="https://example.com/data.xlsx"
    """

    name = "universal"

    def __init__(self, url: str = None, *args, **kwargs):
        """
        Initialize the universal spider.

        Args:
            url: Single URL to scrape (can be passed via -a url=...)

        If url is provided, it overrides start_urls.
        """
        super().__init__(*args, **kwargs)
        if url:
            self.start_urls = [url]

    def parse(self, response: Response) -> None:
        """
        Parse the response using UniversalIngestUseCase.

        This method:
        1. Creates DocumentFetcher (detects content type)
        2. Creates ParserRouter (routes to appropriate parser)
        3. Creates Normalizer (converts Records to Items)
        4. Creates OutputPort (emits items)
        5. Executes UniversalIngestUseCase

        Args:
            response: Scrapy Response object

        Best practices:
        - Validates response before processing
        - Uses ParserRouter for automatic parser selection
        - Logs content type detection for debugging
        """
        # Validate response
        if response.status != 200:
            logger.warning(
                "Invalid response status %d for URL %s",
                response.status,
                response.url,
            )
            return

        # Step 1: Create DocumentFetcher (detects content type)
        fetcher = AdapterScrapyDocumentFetcher(response)

        # Step 2: Create ParserRouter with registry
        router = ParserRouter(PARSER_REGISTRY)

        # Step 3: Create Normalizer (generic for now)
        # TODO: Allow injection of site-specific normalizers
        normalizer = AdapterGenericNormalizer()

        # Step 4: Create OutputPort
        output = AdapterStdoutOutput()

        # Step 5: Create and execute UniversalIngestUseCase
        use_case = UniversalIngestUseCase(
            fetcher=fetcher,
            router=router,
            normalizer=normalizer,
            output=output,
        )

        try:
            items = use_case.execute(response.url)

            logger.info(
                "Successfully processed %s: extracted %d item(s)",
                response.url,
                len(items) if items else 0,
            )

        except Exception as e:
            logger.error(
                "Error processing %s: %s",
                response.url,
                e,
                exc_info=True,
            )
