import pandas as pd
import pytest

from app.core.constants import (
    AggregationFunction,
    DataType,
    FilterOperator,
    ValidationRuleType,
)
from app.models.schemas import (
    AggregationRule,
    ColumnMapping,
    FilterRule,
    ValidationRule,
)
from app.services.schema_mapper import (
    Aggregator,
    DataValidator,
    RowFilter,
    SchemaMapper,
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _mapping(col, dtype, **kwargs):
    return ColumnMapping(
        column_name=col, source_dtype="object", target_dtype=dtype, **kwargs
    )


# ── SchemaMapper ─────────────────────────────────────────────────────────────


class TestSchemaMapper:
    def test_integer_cast(self):
        df = pd.DataFrame({"n": ["1", "2", "3"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("n", DataType.INTEGER)]
        )
        assert result["n"].dropna().tolist() == [1, 2, 3]

    def test_integer_strips_commas_and_dollar(self):
        df = pd.DataFrame({"price": ["$1,000", "$2,500"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("price", DataType.INTEGER)]
        )
        assert result["price"].dropna().tolist() == [1000, 2500]

    def test_float_cast(self):
        df = pd.DataFrame({"v": ["3.14", "2.71"]})
        result = SchemaMapper(df).apply_column_mapping([_mapping("v", DataType.FLOAT)])
        assert abs(result["v"][0] - 3.14) < 0.001

    def test_decimal_rounds_to_2(self):
        df = pd.DataFrame({"v": ["3.14159"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("v", DataType.DECIMAL)]
        )
        assert result["v"][0] == 3.14

    def test_string_truncation(self):
        df = pd.DataFrame({"s": ["HelloWorld"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("s", DataType.STRING, max_length=5)]
        )
        assert result["s"][0] == "Hello"

    def test_boolean_true_values(self):
        df = pd.DataFrame({"b": ["true", "yes", "1", "True", "YES"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("b", DataType.BOOLEAN)]
        )
        assert all(result["b"].dropna())

    def test_boolean_false_values(self):
        df = pd.DataFrame({"b": ["false", "no", "0", "False", "NO"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("b", DataType.BOOLEAN)]
        )
        assert not any(result["b"].dropna())

    def test_boolean_unknown_becomes_none(self):
        df = pd.DataFrame({"b": ["maybe"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("b", DataType.BOOLEAN)]
        )
        assert result["b"][0] is None

    def test_date_cast(self):
        df = pd.DataFrame({"d": ["2024-01-15", "2023-06-30"]})
        result = SchemaMapper(df).apply_column_mapping([_mapping("d", DataType.DATE)])
        import datetime

        assert result["d"][0] == datetime.date(2024, 1, 15)

    def test_date_bad_value_becomes_nat(self):
        df = pd.DataFrame({"d": ["not-a-date"]})
        result = SchemaMapper(df).apply_column_mapping([_mapping("d", DataType.DATE)])
        assert pd.isna(result["d"][0])

    def test_rename(self):
        df = pd.DataFrame({"old_name": [1, 2]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("old_name", DataType.INTEGER, rename_to="new_name")]
        )
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_prefix_suffix(self):
        df = pd.DataFrame({"col": ["a", "b"]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("col", DataType.STRING, prefix="pre_", suffix="_suf")]
        )
        assert "pre_col_suf" in result.columns

    def test_not_null_with_default(self):
        df = pd.DataFrame({"v": [1, None, 3]})
        result = SchemaMapper(df).apply_column_mapping(
            [_mapping("v", DataType.INTEGER, is_nullable=False, default_value=0)]
        )
        assert result["v"][1] == 0

    def test_not_null_without_default_raises(self):
        df = pd.DataFrame({"v": [1, None, 3]})
        mapper = SchemaMapper(df)
        mapper.apply_column_mapping(
            [_mapping("v", DataType.INTEGER, is_nullable=False)]
        )
        assert len(mapper.transformation_errors) == 1

    def test_missing_column_recorded_as_error(self):
        df = pd.DataFrame({"a": [1]})
        mapper = SchemaMapper(df)
        mapper.apply_column_mapping([_mapping("nonexistent", DataType.INTEGER)])
        assert len(mapper.transformation_errors) == 1


# ── RowFilter ─────────────────────────────────────────────────────────────────


class TestRowFilter:
    def _df(self):
        return pd.DataFrame(
            {
                "score": [90, 70, 50, 80],
                "name": ["Alice", None, "Charlie", "Dave"],
                "tag": ["A", "B", "A", "C"],
            }
        )

    def test_eq(self):
        df, _ = RowFilter().apply(
            self._df(),
            [FilterRule(column="tag", operator=FilterOperator.EQUALS, value="A")],
        )
        assert len(df) == 2

    def test_gt(self):
        df, _ = RowFilter().apply(
            self._df(),
            [
                FilterRule(
                    column="score", operator=FilterOperator.GREATER_THAN, value=75
                )
            ],
        )
        assert len(df) == 2

    def test_is_null(self):
        df, _ = RowFilter().apply(
            self._df(), [FilterRule(column="name", operator=FilterOperator.IS_NULL)]
        )
        assert len(df) == 1

    def test_is_not_null(self):
        df, _ = RowFilter().apply(
            self._df(), [FilterRule(column="name", operator=FilterOperator.IS_NOT_NULL)]
        )
        assert len(df) == 3

    def test_in(self):
        df, _ = RowFilter().apply(
            self._df(),
            [FilterRule(column="tag", operator=FilterOperator.IN, values=["A", "B"])],
        )
        assert len(df) == 3

    def test_not_in(self):
        df, _ = RowFilter().apply(
            self._df(),
            [FilterRule(column="tag", operator=FilterOperator.NOT_IN, values=["C"])],
        )
        assert len(df) == 3

    def test_contains(self):
        df, _ = RowFilter().apply(
            self._df(),
            [FilterRule(column="name", operator=FilterOperator.CONTAINS, value="li")],
        )
        assert len(df) == 2  # Alice, Charlie

    def test_filtered_out_df_is_complement(self):
        source = self._df()
        kept, dropped = RowFilter().apply(
            source,
            [
                FilterRule(
                    column="score", operator=FilterOperator.GREATER_THAN, value=75
                )
            ],
        )
        assert len(kept) + len(dropped) == len(source)

    def test_no_rules_returns_full_df(self):
        df = self._df()
        kept, dropped = RowFilter().apply(df, [])
        assert len(kept) == len(df)
        assert len(dropped) == 0

    def test_unknown_column_ignored(self):
        df = self._df()
        kept, _ = RowFilter().apply(
            df, [FilterRule(column="nope", operator=FilterOperator.EQUALS, value="x")]
        )
        assert len(kept) == len(df)


# ── Aggregator ────────────────────────────────────────────────────────────────


class TestAggregator:
    def _df(self):
        return pd.DataFrame(
            {
                "dept": ["eng", "eng", "sales", "sales"],
                "salary": [100, 120, 80, 90],
                "id": [1, 2, 3, 4],
            }
        )

    def test_sum(self):
        rule = AggregationRule(
            group_by=["dept"],
            aggregations=[{"column": "salary", "function": "sum", "alias": "total"}],
        )
        result = Aggregator().apply(self._df(), rule)
        eng_row = result[result["dept"] == "eng"].iloc[0]
        assert eng_row["total"] == 220

    def test_count(self):
        rule = AggregationRule(
            group_by=["dept"],
            aggregations=[{"column": "id", "function": "count", "alias": "headcount"}],
        )
        result = Aggregator().apply(self._df(), rule)
        assert result["headcount"].sum() == 4

    def test_avg(self):
        rule = AggregationRule(
            group_by=["dept"],
            aggregations=[
                {"column": "salary", "function": "mean", "alias": "avg_salary"}
            ],
        )
        result = Aggregator().apply(self._df(), rule)
        eng_row = result[result["dept"] == "eng"].iloc[0]
        assert eng_row["avg_salary"] == 110.0

    def test_result_columns(self):
        rule = AggregationRule(
            group_by=["dept"],
            aggregations=[{"column": "salary", "function": "sum", "alias": "total"}],
        )
        result = Aggregator().apply(self._df(), rule)
        assert "dept" in result.columns
        assert "total" in result.columns


# ── DataValidator ─────────────────────────────────────────────────────────────


class TestDataValidator:
    def _df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 2, 3],
                "name": ["Alice", None, "Charlie", "Dave"],
                "score": [100, 85, 200, 50],
                "email": ["a@b.com", "bad-email", "c@d.com", "e@f.com"],
            }
        )

    def test_not_null(self):
        rule = ValidationRule(column="name", rule_type=ValidationRuleType.NOT_NULL)
        valid, invalid, errors = DataValidator().validate(self._df(), [rule])
        assert len(invalid) == 1
        assert len(errors) == 1
        assert errors[0]["column"] == "name"

    def test_unique(self):
        rule = ValidationRule(column="id", rule_type=ValidationRuleType.UNIQUE)
        valid, invalid, errors = DataValidator().validate(self._df(), [rule])
        assert len(invalid) == 1  # row with duplicate id=2

    def test_max_value(self):
        rule = ValidationRule(
            column="score", rule_type=ValidationRuleType.MAX_VALUE, params={"max": 150}
        )
        valid, invalid, errors = DataValidator().validate(self._df(), [rule])
        assert len(invalid) == 1

    def test_min_value(self):
        rule = ValidationRule(
            column="score", rule_type=ValidationRuleType.MIN_VALUE, params={"min": 60}
        )
        valid, invalid, errors = DataValidator().validate(self._df(), [rule])
        assert len(invalid) == 1

    def test_email(self):
        rule = ValidationRule(column="email", rule_type=ValidationRuleType.EMAIL)
        valid, invalid, errors = DataValidator().validate(self._df(), [rule])
        assert len(invalid) == 1

    def test_regex(self):
        df = pd.DataFrame({"code": ["ABC-123", "bad", "XYZ-999"]})
        rule = ValidationRule(
            column="code",
            rule_type=ValidationRuleType.REGEX,
            params={"pattern": r"^[A-Z]{3}-\d{3}$"},
        )
        valid, invalid, errors = DataValidator().validate(df, [rule])
        assert len(invalid) == 1

    def test_allowed_values(self):
        df = pd.DataFrame({"status": ["active", "inactive", "deleted", "active"]})
        rule = ValidationRule(
            column="status",
            rule_type=ValidationRuleType.ALLOWED_VALUES,
            params={"values": ["active", "inactive"]},
        )
        valid, invalid, errors = DataValidator().validate(df, [rule])
        assert len(invalid) == 1

    def test_custom_error_message(self):
        rule = ValidationRule(
            column="name",
            rule_type=ValidationRuleType.NOT_NULL,
            error_message="Name is required!",
        )
        _, _, errors = DataValidator().validate(self._df(), [rule])
        assert errors[0]["message"] == "Name is required!"

    def test_valid_plus_invalid_equals_total(self):
        rule = ValidationRule(column="name", rule_type=ValidationRuleType.NOT_NULL)
        df = self._df()
        valid, invalid, _ = DataValidator().validate(df, [rule])
        assert len(valid) + len(invalid) == len(df)

    def test_no_rules_returns_all_valid(self):
        df = self._df()
        valid, invalid, errors = DataValidator().validate(df, [])
        assert len(valid) == len(df)
        assert len(invalid) == 0
