from typing import Any, Dict, List, Optional

import pandas as pd

from app.core.constants import DataType
from app.database.connectors.base import BaseDatabaseConnector
from app.models.schemas import ColumnMapping


class MySQLConnector(BaseDatabaseConnector):

    def connect(self) -> None:
        try:
            import mysql.connector

            c = self.connection
            self._conn = mysql.connector.connect(
                host=c.host,
                port=c.port or 3306,
                database=c.database,
                user=c.username,
                password=c.password,
                autocommit=False,
            )
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to MySQL: {exc}") from exc

    def disconnect(self) -> None:
        if self._conn and self._conn.is_connected():
            self._conn.close()
            self._conn = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            self.connect()
            cursor = self._conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"MySQL {version}",
            }
        except Exception as exc:
            return {"success": False, "message": f"Connection failed: {exc}"}
        finally:
            self.disconnect()

    def summarize(self, preview_rows: int = 5) -> Dict[str, Any]:
        try:
            self.connect()
            cursor = self._conn.cursor(dictionary=True)

            cursor.execute("SHOW TABLES")
            list_of_tables = [list(row.values())[0] for row in cursor.fetchall()]

            previews = []
            for table_name in list_of_tables:
                cursor.execute(
                    f"SELECT * FROM `{table_name}` LIMIT %s", (preview_rows,)
                )
                table_data = cursor.fetchall()

                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = [
                    {
                        "name": row["Field"],
                        "type": row["Type"],
                        "not_null": row["Null"] == "NO",
                        "default": row["Default"],
                        "primary_key": row["Key"] == "PRI",
                    }
                    for row in cursor.fetchall()
                ]

                cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
                num_rows = cursor.fetchone()["cnt"]

                previews.append(
                    {
                        "table": table_name,
                        "row_count": num_rows,
                        "columns": columns,
                        "data": table_data,
                    }
                )

            return {
                "database": self.connection.database,
                "list_of_tables": list_of_tables,
                "previews": previews,
            }
        except Exception as exc:
            raise RuntimeError(f"Error summarizing MySQL: {exc}") from exc
        finally:
            self.disconnect()

    def table_exists(self, table_name: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            (table_name,),
        )
        return cursor.fetchone()[0] > 0

    def create_table(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> None:
        query = self._build_create_table_query(table_name, column_mappings)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()

    def drop_table(self, table_name: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        self._conn.commit()

    def insert_data(
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_size: int = 10_000,
    ) -> Dict[str, Any]:
        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(f"`{c}`" for c in columns)
        query = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"

        rows_inserted = 0
        rows_failed = 0
        cursor = self._conn.cursor()

        try:
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                rows = [
                    tuple(None if pd.isna(v) else v for v in row)
                    for row in batch_df.itertuples(index=False)
                ]
                cursor.executemany(query, rows)
                rows_inserted += len(rows)

            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            rows_failed = len(df) - rows_inserted
            raise RuntimeError(f"Failed to insert data: {exc}") from exc

        return {"rows_inserted": rows_inserted, "rows_failed": rows_failed}

    def create_index(
        self,
        table_name: str,
        columns: List[str],
        index_name: Optional[str] = None,
    ) -> None:
        if not index_name:
            index_name = f"idx_{table_name}_{'_'.join(columns)}"
        col_str = ", ".join(f"`{c}`" for c in columns)
        cursor = self._conn.cursor()
        cursor.execute(f"CREATE INDEX `{index_name}` ON `{table_name}` ({col_str})")
        self._conn.commit()

    def _map_datatype_to_sql(self, dtype: DataType) -> str:
        return {
            DataType.INTEGER: "INT",
            DataType.BIGINT: "BIGINT",
            DataType.FLOAT: "DOUBLE",
            DataType.DECIMAL: "DECIMAL(18,2)",
            DataType.STRING: "VARCHAR(255)",
            DataType.TEXT: "TEXT",
            DataType.BOOLEAN: "TINYINT(1)",
            DataType.DATE: "DATE",
            DataType.DATETIME: "DATETIME",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.JSON: "JSON",
        }.get(dtype, "TEXT")

    def _build_create_table_query(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> str:
        col_defs = []
        pk_cols = []

        for m in column_mappings:
            sql_type = self._map_datatype_to_sql(m.target_dtype)

            if m.is_primary_key and m.target_dtype in (
                DataType.INTEGER,
                DataType.BIGINT,
            ):
                col_defs.append(
                    f"`{m.column_name}` {sql_type} NOT NULL AUTO_INCREMENT PRIMARY KEY"
                )
                continue

            col_def = f"`{m.column_name}` {sql_type}"

            if not m.is_nullable:
                col_def += " NOT NULL"

            if m.is_unique and not m.is_primary_key:
                col_def += " UNIQUE"

            if m.default_value is not None:
                col_def += f" DEFAULT {self._format_default_value(m.default_value)}"

            col_defs.append(col_def)

            if m.is_primary_key:
                pk_cols.append(f"`{m.column_name}`")

        if pk_cols:
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        return (
            f"CREATE TABLE `{table_name}` (\n  "
            + ",\n  ".join(col_defs)
            + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )
