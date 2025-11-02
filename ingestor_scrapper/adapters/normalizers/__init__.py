"""
Normalizers module - Normalizer implementations.

This module contains adapters that implement the Normalizer port,
transforming Record objects into Item objects with proper data types
and structures.
"""

from ingestor_scrapper.adapters.normalizers.bcra import (
    AdapterBcraNormalizer,
)
from ingestor_scrapper.adapters.normalizers.generic import (
    AdapterGenericNormalizer,
)

__all__ = [
    "AdapterBcraNormalizer",
    "AdapterGenericNormalizer",
]

