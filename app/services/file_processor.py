import os
from typing import Any, Dict

import numpy as np
import pandas as pd

from app.core.constants import ALLOWED_EXTENSIONS


def _is_string_col(col: pd.Series) -> bool:
    """
    True for both legacy numpy 'object' dtype and pandas 2.x StringDtype.
    pd.api.types.is_string_dtype() covers both, but also matches 'object' columns
    that hold mixed types. Checking dtype.name avoids that ambiguity.
    """
    return pd.api.types.is_string_dtype(col) or col.dtype == object


# Common date format strings tried in order during type suggestion.
# Trying explicit formats avoids pandas UserWarning about format inference.
_COMMON_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d.%m.%Y",
    "%Y.%m.%d",
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
]


def _infer_date(sample: "pd.Series") -> None:
    """
    Raise if the sample cannot be parsed as dates.
    Tries explicit formats first to avoid the pandas format-inference warning.
    """
    import warnings

    for fmt in _COMMON_DATE_FORMATS:
        try:
            pd.to_datetime(sample, format=fmt, errors="raise")
            return  # success
        except Exception:
            pass
    # Last resort: let pandas guess — suppress the UserWarning it emits
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        result = pd.to_datetime(sample, errors="raise")
    if result.isnull().all():
        raise ValueError("all null after parsing")


class FileProcessor:

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_ext = os.path.splitext(file_path)[1].lower()
        self._df = None

    @property
    def df(self) -> pd.DataFrame:
        """Lazy-load the DataFrame."""
        if self._df is None:
            self._df = self._read_file()
        return self._df

    def _read_file(self) -> pd.DataFrame:
        if self.file_ext == ".csv":
            for encoding in ["utf-8", "latin-1", "iso-8859-1", "cp1252"]:
                try:
                    return pd.read_csv(self.file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not decode CSV file with any supported encoding.")

        elif self.file_ext in (".xls", ".xlsx"):
            return pd.read_excel(
                self.file_path,
                engine="openpyxl" if self.file_ext == ".xlsx" else None,
            )

        elif self.file_ext == ".parquet":
            return pd.read_parquet(self.file_path)

        else:
            raise ValueError(
                f"Unsupported file extension: {self.file_ext}. "
                f"Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
            )

    def get_file_metadata(self) -> Dict[str, Any]:
        df = self.df

        # Derive a clean table name from the filename
        filename = os.path.basename(self.file_path)
        base = os.path.splitext(filename)[0]
        # Strip timestamp/uuid prefix added by generate_unique_filename
        table_name = base.split("_name")[-1] if "_name" in base else base
        table_name = table_name.replace(" ", "_").replace("-", "_").lower() or base

        columns_info: Dict[str, Any] = {}
        for col in df.columns:
            col_data = df[col]
            sample_values = col_data.dropna().head(5).tolist()
            sample_values = [
                v.item() if hasattr(v, "item") else v for v in sample_values
            ]

            columns_info[col] = {
                "dtype": str(col_data.dtype),
                "missing_count": int(col_data.isnull().sum()),
                "unique_count": int(col_data.nunique()),
                "sample_values": sample_values,
                "suggested_type": self._suggest_data_type(col_data),
                "is_numeric": bool(pd.api.types.is_numeric_dtype(col_data)),
                "is_datetime": bool(pd.api.types.is_datetime64_dtype(col_data)),
            }

        preview = df.head(5).replace({np.nan: None}).to_dict(orient="records")
        preview = _make_json_safe(preview)

        return {
            "table_name": table_name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": columns_info,
            "preview": preview,
            "has_missing_values": bool(df.isnull().any().any()),
            "total_missing_values": int(df.isnull().sum().sum()),
        }

    def _suggest_data_type(self, series: pd.Series) -> str:
        if series.isnull().all():
            return "string"

        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        if pd.api.types.is_integer_dtype(series):
            return "integer" if series.dropna().max() <= 2_147_483_647 else "bigint"

        if pd.api.types.is_float_dtype(series):
            return "float"

        if pd.api.types.is_datetime64_dtype(series):
            return "datetime"

        if _is_string_col(series):
            sample = series.dropna().head(100)
            if len(sample) > 0:
                unique_vals = {str(v).lower().strip() for v in sample.unique()}
                if unique_vals.issubset({"true", "false", "1", "0", "yes", "no"}):
                    return "boolean"

                try:
                    _infer_date(sample)
                    return "date"
                except Exception:
                    pass

                max_len = series.dropna().astype(str).str.len().max()
                return "text" if max_len > 255 else "string"

        return "string"

    def get_column_stats(self, column_name: str) -> Dict[str, Any]:
        if column_name not in self.df.columns:
            raise ValueError(f"Column '{column_name}' not found in file.")

        col = self.df[column_name]
        stats: Dict[str, Any] = {
            "column_name": column_name,
            "dtype": str(col.dtype),
            "count": len(col),
            "missing_count": int(col.isnull().sum()),
            "unique_count": int(col.nunique()),
        }

        if pd.api.types.is_numeric_dtype(col):
            clean = col.dropna()
            stats.update(
                {
                    "min": float(clean.min()) if len(clean) else None,
                    "max": float(clean.max()) if len(clean) else None,
                    "mean": float(clean.mean()) if len(clean) else None,
                    "median": float(clean.median()) if len(clean) else None,
                    "std": float(clean.std()) if len(clean) else None,
                }
            )
        elif _is_string_col(col):
            lengths = col.dropna().astype(str).str.len()
            stats.update(
                {
                    "max_length": int(lengths.max()) if len(lengths) else 0,
                    "min_length": int(lengths.min()) if len(lengths) else 0,
                    "avg_length": float(lengths.mean()) if len(lengths) else 0.0,
                }
            )

        value_counts = col.value_counts().head(10)
        stats["top_values"] = [
            {"value": str(k), "count": int(v)} for k, v in value_counts.items()
        ]
        return stats


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_json_safe(obj: Any) -> Any:
    """Recursively convert numpy/pandas scalars to native Python types."""
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    if hasattr(obj, "item"):
        return obj.item()
    return obj
