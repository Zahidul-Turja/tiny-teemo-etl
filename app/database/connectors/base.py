import datetime
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.models.schemas import ColumnMapping


class BaseDatabaseConnector(ABC):

    def __init__(self, connection):
        self.connection = connection
        self._conn = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    # ── abstract operations ───────────────────────────────────────────────────

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]: ...

    @abstractmethod
    def summarize(self, preview_rows: int = 5) -> Dict[str, Any]: ...

    @abstractmethod
    def table_exists(self, table_name: str) -> bool: ...

    @abstractmethod
    def create_table(
        self, table_name: str, column_mappings: List[ColumnMapping]
    ) -> None: ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None: ...

    @abstractmethod
    def insert_data(
        self, table_name: str, df: pd.DataFrame, batch_size: int
    ) -> Dict[str, Any]: ...

    @abstractmethod
    def _map_datatype_to_sql(self, dtype) -> str: ...

    # ── main entry point ──────────────────────────────────────────────────────

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        column_mappings: List[ColumnMapping],
        if_exists: str = "fail",
        batch_size: int = 10_000,
    ) -> Dict[str, Any]:
        self.connect()
        try:
            exists = self.table_exists(table_name)

            if exists and if_exists == "fail":
                raise ValueError(
                    f"Table '{table_name}' already exists and if_exists='fail'."
                )
            if exists and if_exists == "replace":
                self.drop_table(table_name)
                exists = False

            if not exists:
                self.create_table(table_name, column_mappings)

            # Sanitize BEFORE insert — converts all numpy/pandas types to
            # plain Python so the DB driver never sees exotic types.
            clean_df = self.sanitize_df(df)

            return self.insert_data(table_name, clean_df, batch_size)
        finally:
            self.disconnect()

    # ── sanitizer ─────────────────────────────────────────────────────────────

    @staticmethod
    def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert every column to plain Python types that all DB drivers accept.

        Key design decisions:
          - series.to_numpy(dtype=object, na_value=None) is used to extract values
            BEFORE cleaning. This correctly converts pandas StringDtype NA, pd.NA,
            and numpy NaN to Python None before we inspect any value — preventing
            the subtle bug where pd.isna() on a StringDtype series returns nan
            instead of a boolean.
          - All cleaned lists are assigned as pd.Series(..., dtype=object). Using
            dtype=object prevents pandas from re-inferring float64/StringDtype and
            silently converting None back to NaN — which would cause
            "nan can not be used with MySQL" from PyMySQL/psycopg2.

        Handles:
          - pandas Int8/16/32/64 nullable integers  → int / None
          - numpy int*/uint*                         → int
          - numpy float*                             → float / None (for NaN)
          - pandas Timestamp / numpy datetime64      → datetime.datetime / None
          - datetime.date objects                    → kept as-is (drivers handle them)
          - numpy bool_                              → bool
          - pandas StringDtype                       → str / None
          - Everything else                          → str / None (safe fallback)
          - Currency strings like "$1,234.56"        → cleaned before cast
        """
        result = df.copy()

        for col in result.columns:
            dtype = result[col].dtype

            # Extract as a plain numpy object array — this normalises pd.NA,
            # StringDtype NA, and NaN all to Python None before any cleaning.
            raw: np.ndarray = result[col].to_numpy(dtype=object, na_value=None)

            # ── pandas nullable integer (Int8, Int16, Int32, Int64) ──────────
            if hasattr(dtype, "numpy_dtype") and pd.api.types.is_integer_dtype(dtype):
                cleaned = [None if v is None else int(v) for v in raw]

            # ── numpy signed/unsigned integers ───────────────────────────────
            elif pd.api.types.is_integer_dtype(dtype):
                cleaned = [None if v is None else int(v) for v in raw]

            # ── floats (numpy float32/64) ────────────────────────────────────
            # IMPORTANT: assign with dtype=object so pandas does NOT re-infer
            # float64 and silently convert None back to NaN before itertuples()
            # is called. PyMySQL/psycopg2 reject nan — it must be None.
            elif pd.api.types.is_float_dtype(dtype):
                cleaned = [
                    (
                        None
                        if (v is None or (isinstance(v, float) and np.isnan(v)))
                        else float(v)
                    )
                    for v in raw
                ]

            # ── datetime64 / Timestamp ───────────────────────────────────────
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                cleaned = [
                    (
                        None
                        if v is None
                        else v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
                    )
                    for v in raw
                ]

            # ── boolean ──────────────────────────────────────────────────────
            elif pd.api.types.is_bool_dtype(dtype):
                cleaned = [None if v is None else bool(v) for v in raw]

            # ── object / string / everything else ────────────────────────────
            else:

                def _clean(v):
                    if v is None:
                        return None
                    # datetime.date objects — pass through, drivers handle them
                    if isinstance(v, (datetime.date, datetime.datetime)):
                        return v
                    # numpy scalars
                    if isinstance(v, np.integer):
                        return int(v)
                    if isinstance(v, np.floating):
                        return None if np.isnan(v) else float(v)
                    if isinstance(v, np.bool_):
                        return bool(v)
                    # plain string — keep as str
                    return str(v) if not isinstance(v, str) else v

                cleaned = [_clean(v) for v in raw]

            # Assign as object-dtype Series to prevent pandas re-inference
            result[col] = pd.Series(cleaned, index=result.index, dtype=object)

        return result

    # ── helpers ───────────────────────────────────────────────────────────────

    def _format_default_value(self, value: Any) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        return str(value)

    def create_index(
        self,
        table_name: str,
        columns: List[str],
        index_name: Optional[str] = None,
    ) -> None:
        """Default no-op — override in connectors that support indexing."""
        pass

    def read_dataframe(
        self,
        table_name: Optional[str] = None,
        query: Optional[str] = None,
        columns: Optional[List[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> "pd.DataFrame":
        """
        Read data from the database into a DataFrame.
        Default implementation uses a simple SELECT; connectors can override.
        """
        import pandas as pd

        self.connect()
        try:
            if query:
                sql = query
            elif table_name:
                col_str = self._quote_columns(columns) if columns else "*"
                sql = self._select_sql(table_name, col_str)
            else:
                raise ValueError("Either table_name or query must be provided.")

            return self._execute_to_df(sql)
        finally:
            self.disconnect()

    def _quote_columns(self, columns: List[str]) -> str:
        """Quote column names — override per connector for dialect differences."""
        return ", ".join(f'"{c}"' for c in columns)

    def _select_sql(self, table_name: str, col_str: str) -> str:
        """Build a SELECT statement — override per connector for dialect differences."""
        return f'SELECT {col_str} FROM "{table_name}"'

    def _execute_to_df(self, sql: str) -> "pd.DataFrame":
        """Execute SQL and return a DataFrame — must be implemented by each connector."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _execute_to_df()"
        )
