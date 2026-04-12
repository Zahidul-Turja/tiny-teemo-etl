import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.core.constants import DataType, ValidationRuleType
from app.models.schemas import (
    AggregationRule,
    ColumnMapping,
    FilterRule,
    ValidationRule,
)


class SchemaMapper:
    """Applies column mappings, renames, prefix/suffix, type casts."""

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.transformed_df: Optional[pd.DataFrame] = None
        self.transformation_errors: List[Dict[str, str]] = []

    def apply_column_mapping(
        self, column_mappings: List[ColumnMapping]
    ) -> pd.DataFrame:
        self.transformed_df = self.df.copy()
        self.transformation_errors = []

        for mapping in column_mappings:
            try:
                self._transform_column(mapping)
            except Exception as exc:
                self.transformation_errors.append(
                    {
                        "column": mapping.column_name,
                        "error": str(exc),
                    }
                )

        # Apply renames after all type casts so we don't confuse references
        rename_map: Dict[str, str] = {}
        for mapping in column_mappings:
            final_name = mapping.column_name
            if mapping.rename_to:
                final_name = mapping.rename_to
            if mapping.prefix:
                final_name = f"{mapping.prefix}{final_name}"
            if mapping.suffix:
                final_name = f"{final_name}{mapping.suffix}"
            if final_name != mapping.column_name:
                rename_map[mapping.column_name] = final_name

        if rename_map:
            self.transformed_df.rename(columns=rename_map, inplace=True)

        return self.transformed_df

    def _transform_column(self, mapping: ColumnMapping) -> None:
        col_name = mapping.column_name
        if col_name not in self.transformed_df.columns:
            raise ValueError(f"Column '{col_name}' not found in dataframe.")

        col = self.transformed_df[col_name]

        dispatch = {
            DataType.INTEGER: self._to_integer,
            DataType.BIGINT: self._to_integer,
            DataType.FLOAT: self._to_float,
            DataType.DECIMAL: self._to_decimal,
            DataType.STRING: self._to_string,
            DataType.TEXT: self._to_string,
            DataType.BOOLEAN: self._to_boolean,
            DataType.DATE: self._to_date,
            DataType.DATETIME: self._to_datetime,
            DataType.TIMESTAMP: self._to_datetime,
            DataType.JSON: self._to_string,  # stored as text
        }

        handler = dispatch.get(mapping.target_dtype)
        if handler:
            self.transformed_df[col_name] = handler(col, mapping)

        # Nullability enforcement
        if not mapping.is_nullable:
            if self.transformed_df[col_name].isnull().any():
                if mapping.default_value is not None:
                    self.transformed_df[col_name] = self.transformed_df[
                        col_name
                    ].fillna(mapping.default_value)
                else:
                    null_count = int(self.transformed_df[col_name].isnull().sum())
                    raise ValueError(
                        f"Column '{col_name}' has {null_count} NULL value(s) "
                        "but is marked NOT NULL and no default was supplied."
                    )

    # ── type converters ─────────────────────────────────────────────────────

    def _to_integer(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        try:
            # FIX: cover both numpy 'object' and pandas 2.x StringDtype
            if pd.api.types.is_string_dtype(col) or col.dtype == object:
                col = (
                    col.astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("$", "", regex=False)
                    .str.strip()
                )
            return pd.to_numeric(col, errors="coerce").astype("Int64")
        except Exception as exc:
            raise ValueError(f"Cannot convert to integer: {exc}") from exc

    def _to_float(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        try:
            if pd.api.types.is_string_dtype(col) or col.dtype == object:
                col = (
                    col.astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("$", "", regex=False)
                    .str.strip()
                )
            return pd.to_numeric(col, errors="coerce")
        except Exception as exc:
            raise ValueError(f"Cannot convert to float: {exc}") from exc

    def _to_decimal(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        return self._to_float(col, mapping).round(2)

    def _to_string(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        result = col.where(col.isnull(), col.astype(str))
        if mapping.max_length:
            result = result.str[: mapping.max_length]
        # BUG FIX: original used replace("nan", np.nan) which doesn't work on Series
        # .where() above preserves NaN correctly
        return result

    def _to_boolean(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        TRUE_VALS = {"true", "t", "yes", "y", "1"}
        FALSE_VALS = {"false", "f", "no", "n", "0"}

        def _convert(val):
            if pd.isna(val):
                return None
            norm = str(val).lower().strip()
            if norm in TRUE_VALS:
                return True
            if norm in FALSE_VALS:
                return False
            return None

        return col.apply(_convert)

    def _to_date(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        fmt = (
            self._convert_date_format(mapping.date_format.value)
            if mapping.date_format
            else None
        )
        try:
            parsed = (
                pd.to_datetime(col, format=fmt, errors="coerce")
                if fmt
                else pd.to_datetime(col, errors="coerce")
            )
        except Exception:
            parsed = pd.to_datetime(col, errors="coerce")
        return parsed.dt.date

    def _to_datetime(self, col: pd.Series, mapping: ColumnMapping) -> pd.Series:
        fmt_val = mapping.datetime_format.value if mapping.datetime_format else None
        fmt = self._convert_datetime_format(fmt_val) if fmt_val else None
        try:
            return (
                pd.to_datetime(col, format=fmt, errors="coerce")
                if fmt
                else pd.to_datetime(col, errors="coerce")
            )
        except Exception:
            return pd.to_datetime(col, errors="coerce")

    # ── format converters ───────────────────────────────────────────────────

    def _convert_date_format(self, fmt: str) -> str:
        return {
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
        }.get(fmt, "%Y-%m-%d")

    def _convert_datetime_format(self, fmt: str) -> str:
        return {
            "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%M:%S",
            "DD-MM-YYYY HH:MM:SS": "%d-%m-%Y %H:%M:%S",
            "MM-DD-YYYY HH:MM:SS": "%m-%d-%Y %H:%M:%S",
            "YYYY-MM-DDTHH:MM:SS": "%Y-%m-%dT%H:%M:%S",
            "ISO8601": "%Y-%m-%dT%H:%M:%SZ",
        }.get(fmt, "%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
#  Row Filtering
# ─────────────────────────────────────────────────────────────────────────────


class RowFilter:
    """Apply filter rules to a DataFrame, returning (kept_df, filtered_df)."""

    def apply(
        self, df: pd.DataFrame, rules: List[FilterRule]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if not rules:
            return df, pd.DataFrame(columns=df.columns)

        mask = pd.Series([True] * len(df), index=df.index)

        for rule in rules:
            col = rule.column
            if col not in df.columns:
                continue
            series = df[col]

            from app.core.constants import FilterOperator as FO

            op = rule.operator

            if op == FO.EQUALS:
                mask &= series == rule.value
            elif op == FO.NOT_EQUALS:
                mask &= series != rule.value
            elif op == FO.GREATER_THAN:
                mask &= series > rule.value
            elif op == FO.LESS_THAN:
                mask &= series < rule.value
            elif op == FO.GREATER_THAN_OR_EQUAL:
                mask &= series >= rule.value
            elif op == FO.LESS_THAN_OR_EQUAL:
                mask &= series <= rule.value
            elif op == FO.CONTAINS:
                mask &= series.astype(str).str.contains(str(rule.value), na=False)
            elif op == FO.NOT_CONTAINS:
                mask &= ~series.astype(str).str.contains(str(rule.value), na=False)
            elif op == FO.IS_NULL:
                mask &= series.isnull()
            elif op == FO.IS_NOT_NULL:
                mask &= series.notnull()
            elif op == FO.IN:
                mask &= series.isin(rule.values or [])
            elif op == FO.NOT_IN:
                mask &= ~series.isin(rule.values or [])

        kept = df[mask].reset_index(drop=True)
        filtered_out = df[~mask].reset_index(drop=True)
        return kept, filtered_out


# ─────────────────────────────────────────────────────────────────────────────
#  Aggregation
# ─────────────────────────────────────────────────────────────────────────────


class Aggregator:
    """Apply group-by aggregations."""

    def apply(self, df: pd.DataFrame, rule: AggregationRule) -> pd.DataFrame:
        agg_spec: Dict[str, Any] = {}
        rename_map: Dict[str, str] = {}

        for agg in rule.aggregations:
            col = agg["column"]
            func = agg["function"]
            alias = agg.get("alias", f"{func}_{col}")

            if func == "count_distinct":
                agg_spec[col] = pd.NamedAgg(column=col, aggfunc="nunique")
            else:
                agg_spec[col] = pd.NamedAgg(column=col, aggfunc=func)

            rename_map[col] = alias

        result = df.groupby(rule.group_by).agg(**agg_spec).reset_index()
        result.rename(columns=rename_map, inplace=True)
        return result


# ─────────────────────────────────────────────────────────────────────────────
#  Data Validation
# ─────────────────────────────────────────────────────────────────────────────


class DataValidator:
    """
    Validates rows against rules.
    Returns (valid_df, invalid_df, errors_list).
    errors_list: list of dicts {row_index, column, rule, message}.
    """

    def validate(
        self, df: pd.DataFrame, rules: List[ValidationRule]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
        if not rules:
            return df, pd.DataFrame(columns=df.columns), []

        invalid_mask = pd.Series([False] * len(df), index=df.index)
        errors: List[Dict[str, Any]] = []

        for rule in rules:
            col = rule.column
            if col not in df.columns:
                continue
            series = df[col]
            params = rule.params or {}

            VR = ValidationRuleType
            bad: pd.Series

            if rule.rule_type == VR.NOT_NULL:
                bad = series.isnull()

            elif rule.rule_type == VR.UNIQUE:
                bad = series.duplicated(keep="first")

            elif rule.rule_type == VR.MIN_VALUE:
                bad = pd.to_numeric(series, errors="coerce") < params.get("min", 0)

            elif rule.rule_type == VR.MAX_VALUE:
                bad = pd.to_numeric(series, errors="coerce") > params.get("max", 0)

            elif rule.rule_type == VR.MIN_LENGTH:
                bad = series.astype(str).str.len() < params.get("min_length", 0)

            elif rule.rule_type == VR.MAX_LENGTH:
                bad = series.astype(str).str.len() > params.get("max_length", 255)

            elif rule.rule_type == VR.REGEX:
                pattern = params.get("pattern", "")
                bad = ~series.astype(str).str.match(pattern, na=False)

            elif rule.rule_type == VR.ALLOWED_VALUES:
                allowed = params.get("values", [])
                bad = ~series.isin(allowed)

            elif rule.rule_type == VR.DATE_FORMAT:
                fmt = params.get("format", "%Y-%m-%d")

                def _bad_date(v):
                    if pd.isna(v):
                        return False
                    try:
                        pd.to_datetime(v, format=fmt)
                        return False
                    except Exception:
                        return True

                bad = series.apply(_bad_date)

            elif rule.rule_type == VR.NUMERIC:
                bad = pd.to_numeric(series, errors="coerce").isnull() & series.notnull()

            elif rule.rule_type == VR.EMAIL:
                email_re = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
                bad = ~series.astype(str).str.match(email_re, na=False)

            else:
                continue

            for idx in df.index[bad]:
                errors.append(
                    {
                        "row_index": int(idx),
                        "column": col,
                        "rule": rule.rule_type.value,
                        "value": str(df.at[idx, col]),
                        "message": rule.error_message
                        or f"Failed rule '{rule.rule_type.value}' on column '{col}'",
                    }
                )
            invalid_mask |= bad

        valid_df = df[~invalid_mask].reset_index(drop=True)
        invalid_df = df[invalid_mask].reset_index(drop=True)
        return valid_df, invalid_df, errors
