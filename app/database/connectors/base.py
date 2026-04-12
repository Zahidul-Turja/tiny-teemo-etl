from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd

from app.models.schemas import ColumnMapping, DatabaseConnection


class BaseDatabaseConnector(ABC):
    """Abstract base class for all database connectors."""

    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self._conn = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    # ── abstract interface ───────────────────────────────────────────────────

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def summarize(self, preview_rows: int = 5) -> Dict[str, Any]:
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def create_table(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> None:
        pass

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        pass

    @abstractmethod
    def insert_data(
        self, table_name: str, df: pd.DataFrame, batch_size: int = 10_000
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_index(
        self,
        table_name: str,
        columns: List[str],
        index_name: Optional[str] = None,
    ) -> None:
        pass

    # ── concrete helpers ─────────────────────────────────────────────────────

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        column_mappings: List[ColumnMapping],
        if_exists: str = "fail",
        batch_size: int = 10_000,
    ) -> Dict[str, Any]:
        """
        High-level helper: connects, creates/replaces table, inserts data.
        Always disconnects in the finally block.
        """
        try:
            self.connect()
            exists = self.table_exists(table_name)

            if exists:
                if if_exists == "fail":
                    raise ValueError(f"Table '{table_name}' already exists.")
                elif if_exists == "replace":
                    self.drop_table(table_name)
                    self.create_table(table_name, column_mappings)
                # "append" → fall through to insert without recreating

            if not exists:
                self.create_table(table_name, column_mappings)

            result = self.insert_data(table_name, df, batch_size)

            return {
                "success": True,
                "table_name": table_name,
                "rows_inserted": result.get("rows_inserted", 0),
                "rows_failed": result.get("rows_failed", 0),
                "message": "Data uploaded successfully.",
            }
        finally:
            self.disconnect()

    def _format_default_value(self, value: Any) -> str:
        """Format a Python value as a SQL literal."""
        if value is None:
            return "NULL"
        if isinstance(value, str):
            safe = value.replace("'", "''")
            return f"'{safe}'"
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)
