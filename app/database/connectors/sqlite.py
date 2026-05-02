import datetime
import sqlite3
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.core.constants import DataType
from app.database.connectors.base import BaseDatabaseConnector
from app.models.schemas import ColumnMapping


def _to_sqlite_native(val: Any) -> Any:
    """Convert a value to a type accepted by sqlite3 (None/int/float/str/bytes)."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, (np.bool_, bool)):
        return int(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    return val


def _effective_name(m: ColumnMapping) -> str:
    """Return the final column name after rename/prefix/suffix are applied."""
    name = m.rename_to or m.column_name
    if m.prefix:
        name = f"{m.prefix}{name}"
    if m.suffix:
        name = f"{name}{m.suffix}"
    return name


class SQLiteConnector(BaseDatabaseConnector):

    def connect(self) -> None:
        try:
            db_path = self.connection.database
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            raise ConnectionError(f"Failed to connect to SQLite: {exc}") from exc

    def disconnect(self) -> None:
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
            }
        except Exception as exc:
            return {"success": False, "message": f"Connection failed: {exc}"}
        finally:
            self.disconnect()

    def summarize(self, preview_rows: int = 5) -> Dict[str, Any]:
        try:
            self.connect()
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            )
            list_of_tables = [row[0] for row in cursor.fetchall()]

            previews = []
            for table_name in list_of_tables:
                cursor.execute(
                    f"SELECT * FROM '{table_name}' LIMIT ?;", (preview_rows,)
                )
                table_data = [dict(row) for row in cursor.fetchall()]
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = [
                    {
                        "name": row[1],
                        "type": row[2],
                        "not_null": bool(row[3]),
                        "default": row[4],
                        "primary_key": bool(row[5]),
                    }
                    for row in cursor.fetchall()
                ]
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
                num_of_rows = cursor.fetchone()[0]
                previews.append(
                    {
                        "table": table_name,
                        "row_count": num_of_rows,
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
            raise RuntimeError(f"Error summarizing SQLite: {exc}") from exc
        finally:
            self.disconnect()

    def table_exists(self, table_name: str) -> bool:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    def create_table(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> None:
        query = self._build_create_table_query(table_name, column_mappings)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()

    def drop_table(self, table_name: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        self._conn.commit()

    def insert_data(
        self, table_name: str, df: pd.DataFrame, batch_size: int = 10_000
    ) -> Dict[str, Any]:
        columns = df.columns.tolist()
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(f'"{c}"' for c in columns)
        query = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
        data = [
            tuple(_to_sqlite_native(v) for v in row)
            for row in df.itertuples(index=False)
        ]

        rows_inserted = 0
        cursor = self._conn.cursor()
        try:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                cursor.executemany(query, batch)
                rows_inserted += len(batch)
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            raise RuntimeError(f"Failed to insert data: {exc}") from exc
        return {"rows_inserted": rows_inserted, "rows_failed": 0}

    def create_index(
        self, table_name: str, columns: List[str], index_name: Optional[str] = None
    ) -> None:
        if not index_name:
            index_name = f"idx_{table_name}_{'_'.join(columns)}"
        col_str = ", ".join(f'"{c}"' for c in columns)
        cursor = self._conn.cursor()
        cursor.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({col_str})'
        )
        self._conn.commit()

    # ── internal ──────────────────────────────────────────────────────────────

    # ── read support ─────────────────────────────────────────────────────────

    def _quote_columns(self, columns):
        return ", ".join(f'"{c}"' for c in columns)

    def _select_sql(self, table_name, col_str):
        return f'SELECT {col_str} FROM "{table_name}"'

    def _execute_to_df(self, sql: str):
        import pandas as pd

        self.connect()
        try:
            return pd.read_sql_query(sql, self._conn)
        finally:
            self.disconnect()

    def _map_datatype_to_sql(self, dtype: DataType) -> str:
        return {
            DataType.INTEGER: "INTEGER",
            DataType.BIGINT: "INTEGER",
            DataType.FLOAT: "REAL",
            DataType.DECIMAL: "REAL",
            DataType.STRING: "TEXT",
            DataType.TEXT: "TEXT",
            DataType.BOOLEAN: "INTEGER",
            DataType.DATE: "TEXT",
            DataType.DATETIME: "TEXT",
            DataType.TIMESTAMP: "TEXT",
            DataType.JSON: "TEXT",
        }.get(dtype, "TEXT")

    def _build_create_table_query(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> str:
        col_defs = []
        composite_pks = []

        for m in column_mappings:
            sql_type = self._map_datatype_to_sql(m.target_dtype)
            eff = _effective_name(m)
            is_int_pk = m.is_primary_key and m.target_dtype in (
                DataType.INTEGER,
                DataType.BIGINT,
            )

            if is_int_pk:
                col_defs.append(f'"{eff}" INTEGER PRIMARY KEY AUTOINCREMENT')
                continue

            col_def = f'"{eff}" {sql_type}'
            if not m.is_nullable:
                col_def += " NOT NULL"
            if m.is_unique and not m.is_primary_key:
                col_def += " UNIQUE"
            if m.default_value is not None:
                col_def += f" DEFAULT {self._format_default_value(m.default_value)}"
            col_defs.append(col_def)

            if m.is_primary_key:
                composite_pks.append(f'"{eff}"')

        if composite_pks:
            col_defs.append(f"PRIMARY KEY ({', '.join(composite_pks)})")

        return f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(col_defs) + "\n)"
