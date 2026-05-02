from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from app.core.constants import DataType
from app.database.connectors.base import BaseDatabaseConnector
from app.models.schemas import ColumnMapping


class PostgresConnector(BaseDatabaseConnector):

    def connect(self) -> None:
        try:
            c = self.connection
            self._conn = psycopg2.connect(
                host=c.host,
                port=c.port or 5432,
                database=c.database,
                user=c.username,
                password=c.password,
            )
            self._conn.autocommit = False
        except psycopg2.Error as exc:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {exc}") from exc

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> Dict[str, Any]:
        try:
            self.connect()
            version = self._conn.server_version
            return {
                "success": True,
                "message": "Connection successful",
                "server_version": f"PostgreSQL {version}",
            }
        except Exception as exc:
            return {"success": False, "message": f"Connection failed: {exc}"}
        finally:
            self.disconnect()

    def summarize(self, preview_rows: int = 5) -> Dict[str, Any]:
        try:
            self.connect()
            cursor = self._conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
                """)
            list_of_tables = [row["table_name"] for row in cursor.fetchall()]

            previews = []
            for table_name in list_of_tables:
                cursor.execute(
                    f'SELECT * FROM "{table_name}" LIMIT %s', (preview_rows,)
                )
                table_data = [dict(row) for row in cursor.fetchall()]

                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position;
                    """,
                    (table_name,),
                )
                col_rows = cursor.fetchall()

                # Primary key lookup
                cursor.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = 'public'
                      AND tc.table_name = %s;
                    """,
                    (table_name,),
                )
                pk_cols = {row["column_name"] for row in cursor.fetchall()}

                columns = [
                    {
                        "name": row["column_name"],
                        "type": row["data_type"],
                        "not_null": row["is_nullable"] == "NO",
                        "default": row["column_default"],
                        "primary_key": row["column_name"] in pk_cols,
                    }
                    for row in col_rows
                ]

                cursor.execute(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
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
            raise RuntimeError(f"Error summarizing PostgreSQL: {exc}") from exc
        finally:
            self.disconnect()

    def table_exists(self, table_name: str) -> bool:
        # Caller owns the connection lifecycle
        cursor = self._conn.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            (table_name,),
        )
        return cursor.fetchone()[0]

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
        self,
        table_name: str,
        df: pd.DataFrame,
        batch_size: int = 10_000,
    ) -> Dict[str, Any]:
        columns = df.columns.tolist()
        col_names = ", ".join(f'"{c}"' for c in columns)
        query = f'INSERT INTO "{table_name}" ({col_names}) VALUES %s'

        rows_inserted = 0
        rows_failed = 0
        cursor = self._conn.cursor()

        try:
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                rows = [tuple(row) for row in batch_df.itertuples(index=False)]
                execute_values(cursor, query, rows)
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
        col_str = ", ".join(f'"{c}"' for c in columns)
        cursor = self._conn.cursor()
        cursor.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({col_str})'
        )
        self._conn.commit()

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

    # ── internal ─────────────────────────────────────────────────────────────

    def _map_datatype_to_sql(self, dtype: DataType) -> str:
        return {
            DataType.INTEGER: "INTEGER",
            DataType.BIGINT: "BIGINT",
            DataType.FLOAT: "DOUBLE PRECISION",
            DataType.DECIMAL: "NUMERIC(18,2)",
            DataType.STRING: "VARCHAR(255)",
            DataType.TEXT: "TEXT",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP",
            DataType.TIMESTAMP: "TIMESTAMPTZ",
            DataType.JSON: "JSONB",
        }.get(dtype, "TEXT")

    def _build_create_table_query(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> str:
        col_defs = []
        for m in column_mappings:
            sql_type = self._map_datatype_to_sql(m.target_dtype)

            if m.is_primary_key and m.target_dtype in (
                DataType.INTEGER,
                DataType.BIGINT,
            ):
                col_defs.append(f'"{m.column_name}" SERIAL PRIMARY KEY')
                continue

            col_def = f'"{m.column_name}" {sql_type}'

            if not m.is_nullable:
                col_def += " NOT NULL"

            if m.is_unique and not m.is_primary_key:
                col_def += " UNIQUE"

            if m.default_value is not None:
                col_def += f" DEFAULT {self._format_default_value(m.default_value)}"

            col_defs.append(col_def)

        return f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(col_defs) + "\n)"
