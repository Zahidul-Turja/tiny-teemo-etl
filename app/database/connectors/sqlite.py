import sqlite3
from typing import Any, Dict, List

from app.database.connectors.base import BaseDatabaseConnector
from app.models.schemas import ColumnMapping
from app.core.constants import DataType


class SQLiteConnector(BaseDatabaseConnector):

    def connect(self):
        try:
            db_path = self.connection.database
            self._conn = sqlite3.connect(db_path)
            self._conn.execute(
                "PRAGMA foreign_keys = ON"
            )  # Enable the enforcement of foreign key constraints for the current database connection

        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to SQLite: {str(e)}")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            self.connect()

            cursor = self._conn.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]

            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"SQLite {version}",
                "database": self.connection.database,
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
            }

    def table_exists(self, table_name) -> bool:
        query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """
        cursor = self._conn.cursor()
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()

        return result is not None

    def create_table(self, table_name, column_mapping) -> None:
        query = self._build_create_table_query(
            table_name=table_name, column_mapping=column_mapping
        )

        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()

    def _map_datatype_to_sql(self, dtype: DataType) -> str:
        type_map = {
            DataType.INTEGER: "INTEGER",
            DataType.BIGINT: "INTEGER",
            DataType.FLOAT: "REAL",
            DataType.DECIMAL: "REAL",
            DataType.STRING: "TEXT",
            DataType.TEXT: "TEXT",
            DataType.BOOLEAN: "INTEGER",  # SQLite doesn't have boolean
            DataType.DATE: "TEXT",
            DataType.DATETIME: "TEXT",
            DataType.TIMESTAMP: "TEXT",
            DataType.JSON: "TEXT",
        }
        return type_map.get(dtype, "TEXT")

    def _build_create_table_query(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> str:
        columns = []
        primary_keys = []

        for mapping in column_mappings:
            sql_type = self._map_datatype_to_sql(mapping.target_dtype)
            col_def = f"{mapping.column_name} {sql_type}"

            if not mapping.is_nullable:
                col_def += " NOT NULL"

            if mapping.is_unique and not mapping.is_primary_key:
                col_def += " UNIQUE"

            if mapping.default_value is not None:
                col_def += (
                    f" DEFAULT {self._format_default_value(mapping.default_value)}"
                )

            # SQLite AUTOINCREMENT only works with INTEGER PRIMARY KEY
            if mapping.is_primary_key and mapping.target_dtype in [
                DataType.INTEGER,
                DataType.BIGINT,
            ]:
                col_def += "PRIMARY KEY AUTOINCREMENT"

            columns.append(col_def)

            if mapping.is_primary_key and mapping.target_dtype not in [
                DataType.INTEGER,
                DataType.BIGINT,
            ]:
                primary_keys.append(mapping.column_name)

        if primary_keys:
            columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

        query = f"CREATE TABLE {table_name} (\n " + ",\n ".join(columns) + "\n)"
        return query
