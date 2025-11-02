"""
Parser Router - Routes documents to appropriate parsers based on content type.

This component selects the appropriate parser based on the document's
content type and routes it accordingly.
"""

import logging
from typing import Optional, Union

from ingestor_scrapper.core.entities import ContentType
from ingestor_scrapper.core.ports import (
    HtmlParser,
    PdfParser,
    TabularParser,
)

logger = logging.getLogger(__name__)

Parser = Union[HtmlParser, TabularParser, PdfParser]


class ParserRouter:
    """
    Router that selects the appropriate parser based on content type.

    This component maintains a registry of parsers for each content type
    and routes documents to the correct parser.

    Usage:
        router = ParserRouter(parser_registry)
        parser = router.select(ContentType.HTML)
    """

    def __init__(self, parser_registry: dict[ContentType, Parser]):
        """
        Initialize the parser router with a registry.

        Args:
            parser_registry: Dictionary mapping ContentType to Parser
        """
        self.parser_registry = parser_registry

    def select(self, content_type: ContentType) -> Optional[Parser]:
        """
        Select the appropriate parser for a content type.

        Args:
            content_type: ContentType enum to select parser for

        Returns:
            Parser if found, None otherwise

        If no parser is found, logs a warning and returns None.
        """
        parser = self.parser_registry.get(content_type)

        if parser is None:
            logger.warning(
                "No parser found for content type: %s", content_type.value
            )

        return parser
