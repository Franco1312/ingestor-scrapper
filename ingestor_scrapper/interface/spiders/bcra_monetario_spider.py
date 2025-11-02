"""
BCRA Monetario Spider - Finds and downloads Excel files from BCRA Monetario Diario page.

This spider demonstrates the Clean Architecture layers:
1. Uses BcraMonetarioUseCase from application layer
2. Injects adapters (Scrapy fetcher, Excel parser, normalizer, output)
3. Executes the use case to crawl and parse BCRA Excel files

Best practices applied:
- Constants for URLs and configuration
- Input validation
- Specific exception handling
- Clear error messages

Run with: scrapy crawl bcra_monetario [--output=json|database]
"""

import logging
import os

import scrapy  # type: ignore
from scrapy.http import Response  # type: ignore

from ingestor_scrapper.adapters.fetchers import AdapterScrapyDocumentFetcher
from ingestor_scrapper.adapters.normalizers import (
    AdapterBcraMonetarioNormalizer,
)
from ingestor_scrapper.adapters.outputs import (
    AdapterDatabaseOutput,
    AdapterJsonOutput,
)
from ingestor_scrapper.adapters.parsers.bcra_excel import (
    AdapterBcraExcelParser,
)
from ingestor_scrapper.application.bcra_monetario_use_case import (
    BcraMonetarioUseCase,
)

logger = logging.getLogger(__name__)

# Constants for better maintainability
BCRA_BASE_URL = "https://www.bcra.gob.ar"
BCRA_MONETARIO_URL = (
    f"{BCRA_BASE_URL}/PublicacionesEstadisticas/Informe_monetario_diario.asp"
)
BCRA_DOMAINS = ["bcra.gob.ar", "www.bcra.gob.ar"]
JSON_OUTPUT_FILE = "bcra_monetario_data.json"

# Database configuration from environment
DB_HOST = os.environ.get(
    "DB_HOST", "base-instances.cvmcecq8y08d.us-east-2.rds.amazonaws.com"
)
DB_NAME = os.environ.get("DB_NAME", "ingestordb")
DB_USER = os.environ.get("DB_USER", "masteruser")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "bluesunday12")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))


class BcraMonetarioSpider(scrapy.Spider):
    """
    Spider that finds and downloads Excel files from BCRA Monetario Diario page.

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

    name = "bcra_monetario"
    allowed_domains = BCRA_DOMAINS
    start_urls = [BCRA_MONETARIO_URL]

    def __init__(self, output="json", *args, **kwargs):
        """
        Initialize spider with output configuration.

        Args:
            output: Output type ('json' or 'database')
        """
        super().__init__(*args, **kwargs)
        self.output_type = output.lower()

    def parse(self, response: Response) -> None:
        """
        Parse the BCRA Monetario page to find Excel download links.

        Args:
            response: Scrapy Response object
        """
        if not self._is_valid_response(response):
            logger.warning(
                "Invalid response for URL %s: status %d",
                response.url,
                response.status,
            )
            return

        # Look for Excel download links
        excel_links = response.xpath(
            '//a[contains(@href, ".xls") or contains(@href, ".xlsx")]/@href'
        ).getall()

        # Also try case-insensitive search
        if not excel_links:
            excel_links = response.xpath(
                '//a[contains(translate(@href, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), ".xls")]/@href'
            ).getall()

        logger.info(
            "Found %d Excel link(s) on %s",
            len(excel_links),
            response.url,
        )

        if not excel_links:
            # Log the page content for debugging
            logger.warning(
                "No Excel links found. Page content (first 2000 chars):"
            )
            logger.warning(response.text[:2000])
            return

        # Process each Excel link found
        for link in excel_links:
            # Make absolute URL
            absolute_url = response.urljoin(link)
            logger.info("Found Excel link: %s", absolute_url)

            # Request the Excel file
            yield scrapy.Request(
                url=absolute_url,
                callback=self.parse_excel,
                meta={"original_url": response.url},
            )

    def parse_excel(self, response: Response) -> None:
        """
        Parse an Excel file downloaded from BCRA.

        This method:
        1. Validates the response
        2. Creates adapters (fetcher, parser, normalizer, output)
        3. Creates and executes the use case
        4. Logs the results

        Args:
            response: Scrapy Response object containing Excel file
        """
        logger.info("Processing Excel file: %s", response.url)

        # Validate response (best practice: check early)
        if not self._is_valid_response(response):
            logger.warning(
                "Invalid response for Excel file %s: status %d",
                response.url,
                response.status,
            )
            return

        # Step 1: Create adapters (wire dependencies)
        # Following Dependency Injection pattern
        fetcher = AdapterScrapyDocumentFetcher(response)
        parser = AdapterBcraExcelParser()
        normalizer = AdapterBcraMonetarioNormalizer()

        # Select output adapter based on configuration
        if self.output_type == "database":
            output = AdapterDatabaseOutput(
                db_host=DB_HOST,
                db_name=DB_NAME,
                db_user=DB_USER,
                db_password=DB_PASSWORD,
                db_port=DB_PORT,
            )
            logger.info("Using database output")
        else:
            output = AdapterJsonOutput(output_file=JSON_OUTPUT_FILE)
            logger.info("Using JSON output")

        # Step 2: Create use case and inject dependencies
        use_case = BcraMonetarioUseCase(
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

        except (ValueError, AttributeError, KeyError) as e:
            # Specific exceptions from parsing/processing
            logger.error(
                "Error processing %s: %s",
                response.url,
                e,
                exc_info=True,
            )
        except (RuntimeError, OSError, IOError) as e:
            # Catch-all for unexpected runtime/system errors
            logger.error(
                "Unexpected error processing %s: %s",
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
        if not response.body or len(response.body) == 0:
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
