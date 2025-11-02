"""
Domain entities - Framework-agnostic models.

This module contains the core domain models used throughout the application.
These are simple data structures without business logic.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Union


@dataclass
class Item:
    """
    Represents a scraped item from a webpage.

    This is a minimal entity that can be extended with additional fields
    as the project grows.

    Attributes:
        title: Title extracted from the page
        content: Main content - can be a string or a dict/object
                 (for structured data like BCRA with fecha and valor)
        url: Source URL of the item
    """

    title: str
    content: Optional[Union[str, Dict]] = None
    url: Optional[str] = None


@dataclass
class Page:
    """
    Represents a webpage with its raw content.

    This entity encapsulates the HTML content and metadata of a page.

    Attributes:
        url: URL of the page
        html: Raw HTML content
        status_code: HTTP status code (if available)
    """

    url: str
    html: str
    status_code: Optional[int] = None


class ContentType(Enum):
    """
    Enum for document content types.

    Used to identify the format of a document and route to the
    appropriate parser.
    """

    HTML = "html"
    CSV = "csv"
    XLS = "xls"
    XLSX = "xlsx"
    PDF = "pdf"
    UNKNOWN = "unknown"


@dataclass
class Document:
    """
    Represents a document with its raw content.

    This is a more generic version of Page that supports multiple
    content types (HTML, CSV, Excel, PDF, etc.).

    Attributes:
        url: URL of the document
        bytes: Raw bytes content (optional, for binary formats)
        text: Text content (optional, for text formats)
        content_type: ContentType enum identifying the format
        status_code: HTTP status code (if available)
    """

    url: str
    content_type: ContentType
    bytes: Optional[bytes] = None
    text: Optional[str] = None
    status_code: Optional[int] = None


@dataclass
class Record:
    """
    Represents a normalized record extracted from a document.

    This is a generic output from parsers, containing structured data
    that will be normalized to Items by Normalizers.

    Attributes:
        data: Dictionary containing the extracted data fields
        source_url: URL from which this record was extracted
        fetched_at: Timestamp when the record was fetched
    """

    data: Dict[str, str]
    source_url: str
    fetched_at: datetime
