import os
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from app.core.constants import ALLOWED_EXTENSIONS


class FileProcessor:

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_ext = os.path.splitext(file_path)[1].lower()
        self._df = None

    @property
    def df(self) -> pd.DataFrame:
        """Lazy load the DataFrame"""

        if self._df is None:
            self._df = self._read_file()
        return self._df

    def _read_file(self) -> pd.DataFrame:
        """
        Read file into pandas DataFrame

        Returns:
            DataFrame: contain file data
        """

        if self.file_ext == ".csv":
            for encoding in ["utf-8", "latin-1", "iso-8859-1"]:
                try:
                    return pd.read_csv(self.file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not decode CSV file with supported encodings")

        elif self.file_ext in [".xls", "xlsx"]:
            return pd.read_excel(self.file_path)

        else:
            raise ValueError(
                f"Unsupported file extension: {self.file_ext}. Allowed extensions are: {", ".join(ALLOWED_EXTENSIONS)}"
            )

    def get_file_metadata(self) -> Dict[str, Any]:
        """
        Extract metadata from the file

        Returns:
            Dict[str, Any]: Dictionary containing file metadata
        """

        df = self.df

        filename = os.path.basename(self.file_path)
        table_name = os.path.splitext(filename)[0].split("_")[-1]

        columns_info = {}
        for col in df.columns:
            col_data = df[col]

            dtype = str(col_data.dtype)

            missing_count = int(col_data.isnull().sum())
            unique_count = int(col_data.nunique())

            sample_values = col_data.dropna().head(5).tolist()

            suggested_types = self._suggest_data_type(col_data)

            columns_info[col] = {
                "dtype": dtype,
                "missing_count": missing_count,
                "unique_count": unique_count,
                "sample_values": sample_values,
                "suggested_type": suggested_types,
                "is_numeric": pd.api.types.is_numeric_dtype(col_data),
                "is_datetime": pd.api.types.is_datetime64_dtype(col_data),
            }

        preview = df.head(5).replace({np.nan: None}).to_dict(orient="records")

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
        """
        Suggest appropriate SQL datatype based on column data

        Args:
            series (pd.Series): Pandas series to analyze

        Returns:
            str: Suggested SQL datatype
        """

        # Check if all null
        if series.isnull().all():
            return "string"

        # Numerics
        if pd.api.types.is_numeric_dtype(series):
            if pd.api.types.is_integer_dtype(series):
                if series.max() <= 2147483647:
                    return "integer"
                else:
                    return "bigint"
            else:
                return "float"

        # Boolean
        if pd.api.types.is_bool_dtype(series):
            return "boolean"

        # Try to detect dates in object columns
        if series.dtype == "object":
            sample = series.dropna().head(100)

            if len(sample) > 0:
                try:
                    pd.to_datetime(sample, errors="raise")
                    return "date"
                except:
                    pass

                # Check for boolean
                unique_vals = set(str(v).lower() for v in sample.unique())
                if unique_vals.issubset({"true", "false", "1", "0", "yes", "no"}):
                    return "boolean"

        # String or Text
        if series.dtype == "object":
            max_length = series.astype(str).str.len().max()
            if max_length > 255:
                return "text"

            return "string"

    def get_column_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Get detailed statistics for a specific column

        Args:
            column_name (str): Name of the column

        Returns:
            Dict[str, Any]: Dictionary with column statistics
        """

        if column_name not in self.df.columns:
            raise ValueError(f"Column: '{column_name}' not found in file")

        col = self.df[column_name]

        stats = {
            "column_name": column_name,
            "dtype": str(col.dtype),
            "count": len(col),
            "missing_count": int(col.isnull().sum()),
            "unique_count": int(col.nunique()),
        }

        if pd.api.types.is_numeric_dtype(col):
            stats.update(
                {
                    "min": float(col.min()) if not pd.isna(col.min()) else None,
                    "max": float(col.max()) if not pd.isna(col.max()) else None,
                    "mean": float(col.mean()) if not pd.isna(col.mean()) else None,
                    "median": float(col.median()) if not pd.isna(col.mean()) else None,
                    "std": float(col.std()) if not pd.isna(col.mean()) else None,
                }
            )

        elif col.dtype == "object":
            stats.update(
                {
                    "max_length": int(col.astype(str).str.len().max()),
                    "min_length": int(col.astype(str).str.len().min()),
                    "avg_length": float(col.astype(str).str.len().mean()),
                }
            )

        # Top values
        value_counts = col.value_counts().head(10)
        stats["top_values"] = [
            {"value": str(k), "count": int(v)} for k, v in value_counts.items()
        ]

        return stats
