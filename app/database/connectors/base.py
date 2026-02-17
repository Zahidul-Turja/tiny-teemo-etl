from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


from app.models.schemas import DatabaseConnection, ColumnMapping
import pandas as pd


class BaseDatabaseConnector(ABC):
    """Abstract base class for database connectors"""

    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self._conn = None

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def create_table(
        self, table_name: str, column_mapping: List[ColumnMapping]
    ) -> None:
        pass

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        pass

    @abstractmethod
    def insert_data(
        self, table_name: str, df: pd.DataFrame, batch_size: int = 1000
    ) -> Dict[str, Any]:
        pass

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        column_mapping: List[ColumnMapping],
        if_exists: str = "fail",
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        """
        Upload pandas DataFrame to database

        Args:
            df (pd.DataFrame): DataFrame to upload
            table_name (str): Name of the table
            column_mapping (List[ColumnMapping]): Column mapping configurations
            if_exists (str, optional): Action if table already exists ("fail", "replace", "append"). Defaults to "fail".
            batch_size (int, optional): Batch size for insertion. Defaults to 1000.

        Returns:
            Dict[str, Any]: Upload results
        """
        try:
            self.connect()

            exists = self.table_exists(table_name)

            if exists and if_exists == "fail":
                raise ValueError(f"Table '{table_name}' already exists")

            elif exists and if_exists == "replace":
                self.drop_table(table_name=table_name)

            elif not exists:
                self.create_table(table_name=table_name, column_mapping=column_mapping)

            result = self.insert_data(
                table_name=table_name,
                df=df,
                batch_size=batch_size,
            )

            return {
                "success": True,
                "table_name": table_name,
                "rows_inserted": result.get("rows_inserted", 0),
                "rows_failed": result.get("rows_failed", 0),
                "message": "Data uploaded successfully",
            }

        finally:
            self.disconnect()

    @abstractmethod
    def create_index(
        self,
        table_name: str,
        columns: List[str],
        index_name: Optional[str] = None,
    ) -> None:
        pass

    def _format_default_value(self, value: Any) -> str:
        """Format default value for SQL"""
        if isinstance(value, str):
            return f"'{value}'"
        elif value is None:
            return "NULL"
        else:
            return str(value)
