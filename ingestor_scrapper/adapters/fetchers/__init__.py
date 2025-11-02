"""
Fetchers module - Document fetcher implementations.

This module contains adapters that implement the DocumentFetcher port,
providing different mechanisms for fetching documents (HTTP, Scrapy, etc.).
"""

from ingestor_scrapper.adapters.fetchers.http import AdapterHttpFetcher
from ingestor_scrapper.adapters.fetchers.scrapy import (
    AdapterScrapyDocumentFetcher,
    AdapterScrapyFetcher,
)

__all__ = [
    "AdapterHttpFetcher",
    "AdapterScrapyDocumentFetcher",
    "AdapterScrapyFetcher",
]

