"""
Universal Ingest Use Case - Generic use case for ingesting multiple formats.

This use case provides a generic flow that can handle HTML, CSV, Excel,
and PDF documents through the ParserRouter.

It's designed as an alternative to site-specific use cases and can be
used for generic spiders that need to handle multiple formats.
"""

import logging
from typing import List

from ingestor_scrapper.application.parser_router import ParserRouter
from ingestor_scrapper.core.entities import ContentType, Item, Record
from ingestor_scrapper.core.ports import (
    DocumentFetcher,
    Normalizer,
    OutputPort,
)

logger = logging.getLogger(__name__)


class UniversalIngestUseCase:
    """
    Universal use case for ingesting documents in multiple formats.

    This use case:
    1. Fetches a document using DocumentFetcher
    2. Routes to appropriate parser using ParserRouter
    3. Parses document into Records
    4. Normalizes Records into Items using Normalizer
    5. Outputs Items using OutputPort

    Usage:
        use_case = UniversalIngestUseCase(
            fetcher=fetcher,
            router=router,
            normalizer=normalizer,
            output=output
        )
        items = use_case.execute(url)
    """

    def __init__(
        self,
        fetcher: DocumentFetcher,
        router: ParserRouter,
        normalizer: Normalizer,
        output: OutputPort,
    ):
        """
        Initialize the universal ingest use case.

        Args:
            fetcher: DocumentFetcher implementation
            router: ParserRouter for selecting parsers
            normalizer: Normalizer for converting Records to Items
            output: OutputPort for emitting items
        """
        self.fetcher = fetcher
        self.router = router
        self.normalizer = normalizer
        self.output = output

    def execute(self, url: str) -> List[Item]:
        """
        Execute the universal ingest workflow.

        Args:
            url: URL to fetch and ingest

        Returns:
            List[Item]: List of normalized items

        This method:
        1. Fetches the document
        2. Routes to appropriate parser based on content type
        3. Parses document into records
        4. Normalizes records into items
        5. Outputs items
        """
        # Step 1: Fetch document
        try:
            document = self.fetcher.fetch(url)
        except Exception as e:
            logger.error("Failed to fetch document from %s: %s", url, e)
            return []

        # Step 2: Validate document
        if document.content_type == ContentType.UNKNOWN:
            logger.warning("Unknown content type for URL %s, skipping", url)
            return []

        # Step 3: Route to appropriate parser
        parser = self.router.select(document.content_type)

        if parser is None:
            logger.warning(
                "No parser available for content type %s (URL: %s)",
                document.content_type.value,
                url,
            )
            return []

        # Step 4: Parse document into records
        try:
            records: List[Record] = parser.parse(document)
        except Exception as e:
            logger.error("Failed to parse document from %s: %s", url, e)
            return []

        # Step 5: Normalize records into items
        try:
            items: List[Item] = self.normalizer.normalize(records)
        except Exception as e:
            logger.error("Failed to normalize records from %s: %s", url, e)
            return []

        # Step 6: Output items
        try:
            self.output.emit(items)
        except Exception as e:
            logger.error("Failed to output items: %s", e)

        return items
