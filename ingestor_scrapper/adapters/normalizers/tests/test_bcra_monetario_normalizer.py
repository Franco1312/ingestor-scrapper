"""
Tests for BCRA Monetario Normalizer.

Tests the data transformation logic that converts:
- valor: string -> number (float)
- fecha: string -> ISO date string
"""

from datetime import datetime

import pytest

from ingestor_scrapper.adapters.normalizers.bcra_monetario import (
    AdapterBcraMonetarioNormalizer,
)
from ingestor_scrapper.core.entities import Record


class TestAdapterBcraMonetarioNormalizer:
    """Test suite for AdapterBcraMonetarioNormalizer."""

    @pytest.fixture
    def normalizer(self):
        """Create a normalizer instance for testing."""
        return AdapterBcraMonetarioNormalizer()

    @pytest.fixture
    def sample_record(self):
        """Create a sample BCRA Monetario record."""
        return Record(
            data={
                "indicador": "Reservas Internacionales del BCRA",
                "valor": "40771",
                "fecha": "2025-10-28 00:00:00",
                "unidad": "Millones de USD",
                "variable_interna": "reservas_internacionales_usd",
            },
            source_url="https://example.com/test.xlsm",
            fetched_at=datetime.now(),
        )

    def test_transform_valor_string_to_float(self, normalizer):
        """Test that valor is converted from string to float."""
        data = {"valor": "40771.5", "fecha": "2025-10-28 00:00:00"}
        transformed = normalizer._transform_data_types(data)

        assert isinstance(transformed["valor"], float)
        assert transformed["valor"] == 40771.5

    def test_transform_valor_int_stays_numeric(self, normalizer):
        """Test that valor that's already numeric stays numeric."""
        data = {"valor": 40771, "fecha": "2025-10-28 00:00:00"}
        transformed = normalizer._transform_data_types(data)

        assert isinstance(transformed["valor"], float)
        assert transformed["valor"] == 40771.0

    def test_transform_fecha_string_to_iso(self, normalizer):
        """Test that fecha is converted to ISO format."""
        data = {"valor": "100", "fecha": "2025-10-28 00:00:00"}
        transformed = normalizer._transform_data_types(data)

        assert transformed["fecha"] == "2025-10-28T00:00:00"

    def test_transform_fecha_with_seconds(self, normalizer):
        """Test fecha conversion with seconds."""
        data = {"valor": "100", "fecha": "2025-10-28 12:30:45"}
        transformed = normalizer._transform_data_types(data)

        assert transformed["fecha"] == "2025-10-28T12:30:45"

    def test_transform_invalid_valor_keeps_original(self, normalizer):
        """Test that invalid valor values are kept as-is."""
        data = {"valor": "not-a-number", "fecha": "2025-10-28 00:00:00"}
        transformed = normalizer._transform_data_types(data)

        assert transformed["valor"] == "not-a-number"

    def test_normalize_complete_record(self, normalizer, sample_record):
        """Test normalizing a complete record."""
        items = normalizer.normalize([sample_record])

        assert len(items) == 1
        item = items[0]
        assert item.title == "Reservas Internacionales del BCRA"
        assert item.content["valor"] == 40771.0  # Converted to float
        assert item.content["fecha"] == "2025-10-28T00:00:00"  # ISO format
        assert item.url == "https://example.com/test.xlsm"

    def test_normalize_multiple_records(self, normalizer):
        """Test normalizing multiple records."""
        record1 = Record(
            data={
                "indicador": "Indicador 1",
                "valor": "100",
                "fecha": "2025-10-28 00:00:00",
            },
            source_url="url1",
            fetched_at=datetime.now(),
        )
        record2 = Record(
            data={
                "indicador": "Indicador 2",
                "valor": "200",
                "fecha": "2025-10-29 00:00:00",
            },
            source_url="url2",
            fetched_at=datetime.now(),
        )

        items = normalizer.normalize([record1, record2])

        assert len(items) == 2
        assert items[0].content["valor"] == 100.0
        assert items[1].content["valor"] == 200.0

    def test_normalize_empty_list(self, normalizer):
        """Test normalizing an empty list."""
        items = normalizer.normalize([])
        assert len(items) == 0

    def test_normalize_preserves_other_fields(self, normalizer):
        """Test that other fields are preserved during normalization."""
        record = Record(
            data={
                "indicador": "Test",
                "valor": "100",
                "fecha": "2025-10-28 00:00:00",
                "unidad": "Millones",
                "variable_interna": "test_var",
                "extra_field": "extra_value",
            },
            source_url="https://example.com",
            fetched_at=datetime.now(),
        )

        items = normalizer.normalize([record])
        content = items[0].content

        assert content["unidad"] == "Millones"
        assert content["variable_interna"] == "test_var"
        assert content["extra_field"] == "extra_value"
