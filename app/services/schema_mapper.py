from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from app.core.constants import DataType, DateFormat, DateTimeFormat
from app.models.schemas import ColumnMapping


class SchemaMapper:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.transformed_df = None
        self.transformation_errors = []

    def apply_column_mapping(
        self, column_mappings: List[ColumnMapping]
    ) -> pd.DataFrame:
        self.transformed_df = self.df.copy()
        self.transformation_errors = []

        for mapping in column_mappings:
            try:
                self._transform_column(mapping)
            except Exception as e:
                self.transformation_errors.append(
                    {
                        "column": mapping.column_name,
                        "error": str(e),
                    }
                )

        return self.transformed_df

    def _transform_column(self, mapping: ColumnMapping) -> None:
        """
        Transform a single column based on mapping

        Args:
            mapping (ColumnMapping): Column mapping configuration
        """

        col_name = mapping.column_name

        if not col_name in self.transformed_df.columns:
            raise ValueError(f"Column '{col_name}' not found in dataframe")

        col = self.transformed_df[col_name]

        # Manage different target data types
        if mapping.target_dtype == DataType.INTEGER:
            self.transformed_df[col_name] = self._to_integer(col, mapping)

        elif mapping.target_dtype == DataType.BIGINT:
            self.transformed_df[col_name] = self._to_integer(col, mapping)

        elif mapping.target_dtype == DataType.FLOAT:
            self.transformed_df[col_name] = self._to_float(col, mapping)

        elif mapping.target_dtype == DataType.DECIMAL:
            self.transformed_df[col_name] = self._to_decimal(col, mapping)

        elif mapping.target_dtype in [DataType.STRING, DataType.TEXT]:
            self.transformed_df[col_name] = self._to_string(col, mapping)

        elif mapping.target_dtype == DataType.BOOLEAN:
            self.transformed_df[col_name] = self._to_boolean(col, mapping)

        elif mapping.target_dtype == DataType.DATE:
            self.transformed_df[col_name] = self._to_date(col, mapping)

        elif mapping.target_dtype in [DataType.DATETIME, DataType.TIMESTAMP]:
            self.transformed_df[col_name] = self._to_datetime(col, mapping)

        # Manage nullability
        if not mapping.is_nullable:
            if self.transformed_df[col_name].isnull().any():
                if mapping.default_value is not None:
                    self.transformed_df[col_name].fillna(
                        mapping.default_value,
                        inplace=True,
                    )
                else:
                    raise ValueError(
                        f"Column '{col_name}' contains NULL values but marked as NOT NULL"
                    )

    def _to_integer(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        """Convert column to integer"""
        try:
            if col.dtype == "object":
                col = (
                    col.astype(str)
                    .str.replace(",", "")
                    .str.replace("$", "")
                    .str.strip()
                )
            return pd.to_numeric(col, errors="coerce").astype("Int64")
        except Exception as e:
            raise ValueError(f"Cannot convert to integer: {str(e)}")

    def _to_float(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        try:
            if col.dtype == "object":
                col = col.astype(str).str.replace(",", "").str.replace("$", "")
            return pd.to_numeric(col, errors="coerce")
        except Exception as e:
            raise ValueError(f"Cannot convert to integer: {str(e)}")

    def _to_decimal(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        return self._to_float(col, mapping).round(2)

    def _to_string(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        result = col.astype(str)

        # Truncate if max_length specified
        if mapping.max_length:
            # ? Need to show the user the deleted texts
            result = result.str[: mapping.max_length]

        result = result.replace("nan", np.nan)

        return result

    def _to_boolean(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        true_values = {"true", "t", "yes", "y", "1", 1, True}
        false_values = {"false", "f", "no", "n", "0", 0, False}

        def convert_value(val):
            if pd.isna(val):
                return None

            normalized = str(val).lower().strip()

            if normalized in true_values:
                return True
            elif normalized in false_values:
                return False
            else:
                return None

        # pd.Series.apply(func, convert_dtype=True, args=(), **kwds)
        # used to invoke a Python function on every single value (element-wise) of a Series
        return col.apply(convert_value)

    def _to_date(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        date_format = mapping.date_format

        if date_format:
            format_str = self._convert_date_format(date_format.value)
            try:
                return pd.to_datetime(col, format=format_str, errors="coerce").dt.date
            except:
                return pd.to_datetime(col, errors="coerce").dt.date
        else:
            return pd.to_datetime(col, errors="coerce").dt.date

    def _to_datetime(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        datetime_format = mapping.datetime_format

        if datetime_format:
            format_str = self._convert_datetime_format(datetime_format)
            try:
                return pd.to_datetime(col, format=format_str, errors="coerce")
            except:
                return pd.to_datetime(col, errors="coerce")

        return pd.to_datetime(col, errors="coerce")

    def _convert_date_format(self, format_enum: str) -> str:
        """convert date format enum to pandas format string"""
        format_map = {
            "YYYY-MM-DD": "%Y-%m-%d",
            "YY-MM-DD": "%y-%m-%d",
            "DD-MM-YYYY": "%d-%m-%Y",
            "DD-MM-YY": "%d-%m-%y",
            "YYYY/MM/DD": "%Y/%m/%d",
            "YY/MM/DD": "%y/%m/%d",
            "DD/MM/YYYY": "%d/%m/%Y",
            "DD/MM/YY": "%d/%m/%y",
            "MMM DD, YYYY": "%b %d, %Y",
            "MMMM DD, YYYY": "%B %d, %Y",
        }
        return format_map.get(format_enum, "%Y-%m-%d")

    def _convert_datetime_format(self, format_enum: str) -> str:
        """Convert datetime format enum to pandas format string"""
        format_map = {
            "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%M:%S",
            "DD-MM-YYYY HH:MM:SS": "%d-%m-%Y %H:%M:%S",
            "MM-DD-YYYY HH:MM:SS": "%m-%d-%Y %H:%M:%S",
            "YYYY-MM-DDTHH:MM:SS": "%Y-%m-%dT%H:%M:%S",
        }
        return format_map.get(format_enum, "%Y-%m-%d %H:%M:%S")

    def validate_transformations(self) -> Dict[str, Any]:
        if self.transformed_df is None:
            raise ValueError("No transformations have been applied")

        validation_results = {
            "total_rows": len(self.transformed_df),
            "errors": self.transformation_errors,
            "has_errors": len(self.transformation_errors),
            "column_validation": {},
        }

        for col in self.transformed_df.columns:
            validation_results["column_validation"][col] = {
                "null_count": int(self.transformed_df[col]),
                "dtype": str(self.transformed_df[col].dtype),
            }

        return validation_results
