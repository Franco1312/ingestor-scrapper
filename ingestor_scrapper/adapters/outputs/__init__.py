"""
Outputs module - Output port implementations.

This module contains adapters that implement the OutputPort, providing
different mechanisms for emitting extracted items (JSON, stdout, etc.).
"""

from ingestor_scrapper.adapters.outputs.json import AdapterJsonOutput
from ingestor_scrapper.adapters.outputs.stdout import AdapterStdoutOutput

__all__ = [
    "AdapterJsonOutput",
    "AdapterStdoutOutput",
]
