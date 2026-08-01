from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.core.constants import DataType, DatabaseType
from app.database.connectors.mysql import MySQLConnector
from app.database.connectors.postgres import PostgresConnector
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import ColumnMapping, DatabaseConnection, DatabaseSource


def _get_connector(connection: DatabaseConnection):
    mapping = {
        DatabaseType.POSTGRESQL: PostgresConnector,
        DatabaseType.MYSQL: MySQLConnector,
        DatabaseType.SQLITE: SQLiteConnector,
    }
    cls = mapping.get(connection.db_type)
    if not cls:
        raise ValueError(f"Unsupported database type: {connection.db_type}")
    return cls(connection)


# ── pandas dtype → our DataType ───────────────────────────────────────────────


def _pandas_dtype_to_datatype(dtype) -> DataType:
    name = str(dtype).lower()
    if "int" in name:
        return DataType.BIGINT
    if "float" in name or "double" in name or "decimal" in name or "numeric" in name:
        return DataType.FLOAT
    if "bool" in name:
        return DataType.BOOLEAN
    if "datetime" in name or "timestamp" in name:
        return DataType.TIMESTAMP
    if "date" in name:
        return DataType.DATE
    return DataType.TEXT


def _auto_column_mappings(df: pd.DataFrame) -> List[ColumnMapping]:
    """
    Generate pass-through ColumnMappings inferred from the DataFrame's dtypes.
    Used when the user hasn't supplied explicit mappings (migrate-as-is).
    """
    mappings = []
    for col in df.columns:
        target = _pandas_dtype_to_datatype(df[col].dtype)
        mappings.append(
            ColumnMapping(
                column_name=col,
                source_dtype=str(df[col].dtype),
                target_dtype=target,
                is_nullable=True,
            )
        )
    return mappings


# ── public API ────────────────────────────────────────────────────────────────


def read_from_db(source: DatabaseSource) -> Tuple[pd.DataFrame, List[ColumnMapping]]:
    """
    Extract data from the source database.

    Returns
    -------
    df : pd.DataFrame
        All rows from the requested table / query.
    auto_mappings : List[ColumnMapping]
        Auto-generated mappings inferred from the DataFrame dtypes.
        The ETL runner merges these with any user-supplied mappings
        (user mappings win on a per-column basis).
    """
    connector = _get_connector(source.connection)
    df = connector.read_dataframe(
        table_name=source.table_name,
        query=source.query,
        columns=source.columns,
        chunk_size=source.chunk_size,
    )
    auto_mappings = _auto_column_mappings(df)
    return df, auto_mappings


def get_source_schema(source: DatabaseSource) -> List[Dict[str, Any]]:
    """
    Return column metadata for a source table without fetching any rows.
    Used by the /migrate/preview endpoint.
    """
    connector = _get_connector(source.connection)

    # Use a LIMIT 0 query to get schema without data
    if source.query:
        preview_sql = f"SELECT * FROM ({source.query}) AS _q LIMIT 0"
    else:
        col_str = "*"
        if source.columns:
            # quoting handled by connector type
            if source.connection.db_type == DatabaseType.MYSQL:
                col_str = ", ".join(f"`{c}`" for c in source.columns)
            else:
                col_str = ", ".join(f'"{c}"' for c in source.columns)

        if source.connection.db_type == DatabaseType.MYSQL:
            preview_sql = f"SELECT {col_str} FROM `{source.table_name}` LIMIT 0"
        else:
            preview_sql = f'SELECT {col_str} FROM "{source.table_name}" LIMIT 0'

    connector2 = _get_connector(source.connection)
    empty_df = connector2.read_dataframe(query=preview_sql)

    return [
        {
            "column_name": col,
            "source_dtype": str(empty_df[col].dtype),
            "suggested_target_dtype": _pandas_dtype_to_datatype(
                empty_df[col].dtype
            ).value,
        }
        for col in empty_df.columns
    ]
