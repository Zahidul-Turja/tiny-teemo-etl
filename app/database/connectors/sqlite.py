import sqlite3
from typing import Any, Dict, List

import pandas as pd

from app.core.constants import DataType
from app.database.connectors.base import BaseDatabaseConnector
from app.models.schemas import ColumnMapping


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

            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            )
            list_of_tables = cursor.fetchall()

            previews = []
            for (table_name,) in list_of_tables:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
                table_data = cursor.fetchall()

                cursor.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}');")
                columns = cursor.fetchall()

                data = {
                    "table": table_name,
                    "columns": columns,
                    "data": cursor.fetchall(),
                }
                previews.append(data)

            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"SQLite {version}",
                "database": self.connection.database,
                "list_of_tables": list_of_tables,
                "previews": previews,
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
            }

    def summarize(self):
        self.connect()

        cursor = self._conn.cursor()

    def table_exists(self, table_name) -> bool:
        query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """
        cursor = self._conn.cursor()
        cursor.execute(query, (table_name,))
        result = cursor.fetchone()

        return result is not None

    def create_table(self, table_name, column_mappings) -> None:
        query = self._build_create_table_query(
            table_name=table_name, column_mappings=column_mappings
        )

        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()

    def drop_table(self, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_name}"

        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()

    def insert_data(
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_size=1000,
    ) -> Dict[str, Any]:
        columns = df.columns.to_list()

        # ? the double quotation "?" might cause issue
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        data = [tuple(None if pd.isna(x) else x for x in row) for row in df.values]

        rows_inserted = 0
        rows_failed = 0

        try:
            cursor = self._conn.cursor()

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                cursor.executemany(query, batch)
                rows_inserted += len(batch)

            self._conn.commit()

        except Exception as e:
            # ? Need proper logs for invalid rows
            self._conn.rollback()
            rows_failed = len(data)
            raise Exception(f"Failed to insert data: {str(e)}")

        return {
            "rows_inserted": rows_inserted,
            "rows_failed": rows_failed,
        }

    def create_index(
        self,
        table_name: str,
        columns: List[str],
        index_name=None,
    ) -> None:
        if not index_name:
            index_name = f"idx_{table_name}_{'_'.join(columns)}"

        column_str = ", ".join(columns)
        query = f"CREATE INDEX {index_name} ON {table_name} ({column_str})"

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
