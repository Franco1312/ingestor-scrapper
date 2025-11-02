"""
Database adapter for OutputPort - Outputs items to PostgreSQL database.

This adapter implements the OutputPort interface by inserting items
into the series_points table following the established schema.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2  # type: ignore
from psycopg2.extras import Json, execute_values

from ingestor_scrapper.core.entities import Item
from ingestor_scrapper.core.ports import OutputPort

logger = logging.getLogger(__name__)


class AdapterDatabaseOutput(OutputPort):
    """
    Adapter that implements OutputPort by outputting items to PostgreSQL database.

    This adapter maps items to the series_points table schema:
    - series_id: text
    - ts: date
    - value: numeric
    - metadata: jsonb
    """

    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        db_port: int = 5432,
        series_id_mapping: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the database output adapter.

        Args:
            db_host: Database host
            db_name: Database name
            db_user: Database user
            db_password: Database password
            db_port: Database port (default: 5432)
            series_id_mapping: Dict mapping variable_interna to series_id
                              (default: auto-detect from variable_interna)
        """
        self.db_config = {
            "host": db_host,
            "database": db_name,
            "user": db_user,
            "password": db_password,
            "port": db_port,
        }
        self.series_id_mapping = series_id_mapping or {}
        self._connection = None

    def emit(self, items: List[Item]) -> None:
        """
        Insert items into database.

        Args:
            items: List of items to insert

        This method:
        1. Connects to database
        2. Maps items to series_points format
        3. Inserts or updates data (UPSERT)
        4. Handles errors gracefully
        """
        if not items:
            logger.info("No items to output")
            return

        try:
            # Connect to database
            self._connect()

            # Prepare data for insertion
            data_points = self._prepare_data_points(items)

            if not data_points:
                logger.warning("No data points prepared for insertion")
                return

            # Insert data
            self._insert_data_points(data_points)

            logger.info("Successfully inserted %d data points", len(data_points))

        except Exception as e:
            logger.error("Failed to insert items: %s", e, exc_info=True)
        finally:
            self._disconnect()

    def _connect(self) -> None:
        """Establish database connection."""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self.db_config)
            logger.debug("Connected to database")

    def _disconnect(self) -> None:
        """Close database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None
            logger.debug("Disconnected from database")

    def _prepare_data_points(self, items: List[Item]) -> List[tuple]:
        """
        Prepare items for database insertion.

        Args:
            items: List of items

        Returns:
            List of tuples (series_id, ts, value, metadata)
        """
        data_points = []

        for item in items:
            try:
                # Extract data from item
                content = item.content
                if not isinstance(content, dict):
                    logger.warning(
                        "Item content is not a dict, skipping: %s", item.title
                    )
                    continue

                # Get variable_interna to determine series_id
                variable_interna = content.get("variable_interna")
                if not variable_interna:
                    logger.warning(
                        "No variable_interna found in item: %s", item.title
                    )
                    continue

                # Map to series_id
                series_id = self._get_series_id(variable_interna)
                if not series_id:
                    logger.warning(
                        "No series_id mapping for: %s", variable_interna
                    )
                    continue

                # Extract value and fecha
                value = content.get("valor")
                if value is None:
                    logger.warning("No valor found in item: %s", item.title)
                    continue

                fecha_str = content.get("fecha")
                if not fecha_str:
                    logger.warning("No fecha found in item: %s", item.title)
                    continue

                # Parse date
                ts = self._parse_date(fecha_str)
                if not ts:
                    continue

                # Prepare metadata
                metadata = {
                    "indicador": content.get("indicador", item.title),
                    "unidad": content.get("unidad"),
                    "source_url": item.url,
                    "scraped_at": datetime.utcnow().isoformat(),
                }

                # Wrap metadata in Json() for proper JSONB handling
                data_points.append((series_id, ts, float(value), Json(metadata)))

            except Exception as e:
                logger.error(
                    "Error preparing data point for item %s: %s",
                    item.title,
                    e,
                )
                continue

        return data_points

    def _get_series_id(self, variable_interna: str) -> Optional[str]:
        """
        Map variable_interna to series_id.

        Args:
            variable_interna: Internal variable identifier

        Returns:
            series_id for database
        """
        # Check explicit mapping first
        if variable_interna in self.series_id_mapping:
            return self.series_id_mapping[variable_interna]

        # Default mappings for BCRA Monetario
        default_mappings = {
            "reservas_internacionales_usd": "1",
            "tipo_cambio_oficial": "bcra.tipo_cambio_oficial",
            "base_monetaria_total_ars": "15",
        }

        return default_mappings.get(variable_interna)

    def _parse_date(self, fecha_str: str) -> Optional[str]:
        """
        Parse fecha string to date format.

        Args:
            fecha_str: Date string in various formats

        Returns:
            ISO date string (YYYY-MM-DD) or None
        """
        try:
            # Try ISO format with time
            if "T" in fecha_str:
                dt = datetime.fromisoformat(fecha_str.replace("Z", "+00:00"))
                return dt.date().isoformat()
            # Try date only
            elif "-" in fecha_str:
                dt = datetime.strptime(fecha_str.split()[0], "%Y-%m-%d")
                return dt.date().isoformat()
            else:
                logger.warning("Unknown date format: %s", fecha_str)
                return None
        except Exception as e:
            logger.error("Failed to parse date %s: %s", fecha_str, e)
            return None

    def _insert_data_points(
        self, data_points: List[tuple]
    ) -> None:
        """
        Insert data points into series_points table.

        Uses UPSERT (ON CONFLICT DO UPDATE) to handle duplicates.

        Args:
            data_points: List of (series_id, ts, value, metadata) tuples
        """
        if not self._connection:
            raise RuntimeError("No database connection")

        # Prepare query
        insert_query = """
        INSERT INTO series_points (series_id, ts, value, metadata, created_at, updated_at)
        VALUES %s
        ON CONFLICT (series_id, ts)
        DO UPDATE SET
            value = EXCLUDED.value,
            metadata = EXCLUDED.metadata,
            updated_at = EXCLUDED.updated_at
        """

        # Add timestamps to each row
        now = datetime.utcnow()
        rows_with_timestamps = [
            (series_id, ts, value, metadata, now, now)
            for series_id, ts, value, metadata in data_points
        ]

        cursor = self._connection.cursor()
        try:
            execute_values(
                cursor,
                insert_query,
                rows_with_timestamps,
                page_size=100,
            )
            self._connection.commit()
            logger.debug("Committed %d rows", len(data_points))
        except Exception as e:
            self._connection.rollback()
            logger.error("Failed to insert data: %s", e)
            raise
        finally:
            cursor.close()

