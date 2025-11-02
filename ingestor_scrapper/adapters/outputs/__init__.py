"""
Outputs module - Output port implementations.

This module contains adapters that implement the OutputPort, providing
different mechanisms for emitting extracted items (JSON, stdout, database, etc.).
"""

from ingestor_scrapper.adapters.outputs.database import AdapterDatabaseOutput
from ingestor_scrapper.adapters.outputs.json import AdapterJsonOutput
from ingestor_scrapper.adapters.outputs.stdout import AdapterStdoutOutput

__all__ = [
    "AdapterDatabaseOutput",
    "AdapterJsonOutput",
    "AdapterStdoutOutput",
]
