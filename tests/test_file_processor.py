import os

import pandas as pd
import pytest

from app.services.file_processor import FileProcessor


class TestFileReading:
    def test_read_csv(self, sample_csv):
        fp = FileProcessor(sample_csv)
        assert len(fp.df) == 5
        assert "id" in fp.df.columns

    def test_read_excel(self, sample_excel):
        fp = FileProcessor(sample_excel)
        assert len(fp.df) == 5

    def test_read_parquet(self, sample_parquet):
        fp = FileProcessor(sample_parquet)
        assert len(fp.df) == 5

    def test_unsupported_extension(self, tmp_path):
        path = tmp_path / "data.json"
        path.write_text('{"a": 1}')
        with pytest.raises(ValueError, match="Unsupported"):
            FileProcessor(str(path)).df

    def test_lazy_load(self, sample_csv):
        fp = FileProcessor(sample_csv)
        assert fp._df is None
        _ = fp.df
        assert fp._df is not None

    def test_csv_encoding_fallback(self, tmp_path):
        # Write a latin-1 encoded CSV
        path = tmp_path / "latin.csv"
        path.write_bytes("name,city\nJoão,São Paulo\n".encode("latin-1"))
        fp = FileProcessor(str(path))
        assert len(fp.df) == 1


class TestGetMetadata:
    def test_returns_expected_keys(self, sample_csv):
        meta = FileProcessor(sample_csv).get_file_metadata()
        assert "table_name" in meta
        assert "row_count" in meta
        assert "column_count" in meta
        assert "columns" in meta
        assert "preview" in meta
        assert "has_missing_values" in meta

    def test_row_count(self, sample_csv):
        meta = FileProcessor(sample_csv).get_file_metadata()
        assert meta["row_count"] == 5

    def test_detects_missing_values(self, sample_csv):
        meta = FileProcessor(sample_csv).get_file_metadata()
        assert meta["has_missing_values"] is True
        assert meta["total_missing_values"] >= 1

    def test_preview_is_json_safe(self, sample_csv):
        import json

        meta = FileProcessor(sample_csv).get_file_metadata()
        # Should not raise
        json.dumps(meta["preview"])

    def test_column_info_populated(self, sample_csv):
        meta = FileProcessor(sample_csv).get_file_metadata()
        col = meta["columns"]["score"]
        assert col["missing_count"] == 0
        assert col["unique_count"] == 5
        assert col["suggested_type"] == "float"


class TestSuggestDataType:
    def test_integer(self, sample_csv):
        fp = FileProcessor(sample_csv)
        meta = fp.get_file_metadata()
        assert meta["columns"]["id"]["suggested_type"] == "integer"

    def test_float(self, sample_csv):
        fp = FileProcessor(sample_csv)
        meta = fp.get_file_metadata()
        assert meta["columns"]["score"]["suggested_type"] == "float"

    def test_boolean_like(self, tmp_path):
        df = pd.DataFrame({"flag": ["true", "false", "true"]})
        p = tmp_path / "b.csv"
        df.to_csv(p, index=False)
        meta = FileProcessor(str(p)).get_file_metadata()
        assert meta["columns"]["flag"]["suggested_type"] == "boolean"

    def test_date_like(self, tmp_path):
        df = pd.DataFrame({"created": ["2023-01-01", "2024-06-15", "2022-12-31"]})
        p = tmp_path / "d.csv"
        df.to_csv(p, index=False)
        meta = FileProcessor(str(p)).get_file_metadata()
        assert meta["columns"]["created"]["suggested_type"] == "date"

    def test_long_text(self, tmp_path):
        df = pd.DataFrame({"notes": ["x" * 300, "y" * 300]})
        p = tmp_path / "t.csv"
        df.to_csv(p, index=False)
        meta = FileProcessor(str(p)).get_file_metadata()
        assert meta["columns"]["notes"]["suggested_type"] == "text"


class TestColumnStats:
    def test_numeric_stats(self, sample_csv):
        stats = FileProcessor(sample_csv).get_column_stats("score")
        assert stats["min"] == 60.0
        assert stats["max"] == 95.5
        assert "mean" in stats
        assert "median" in stats

    def test_median_not_mean(self, tmp_path):
        # Median of [1,2,100] = 2, mean ≈ 34.3 — verify they differ
        df = pd.DataFrame({"v": [1, 2, 100]})
        p = tmp_path / "m.csv"
        df.to_csv(p, index=False)
        stats = FileProcessor(str(p)).get_column_stats("v")
        assert stats["median"] == 2.0
        assert stats["mean"] != 2.0

    def test_string_stats(self, sample_csv):
        stats = FileProcessor(sample_csv).get_column_stats("name")
        assert "max_length" in stats
        assert "min_length" in stats

    def test_top_values(self, sample_csv):
        stats = FileProcessor(sample_csv).get_column_stats("active")
        assert len(stats["top_values"]) > 0

    def test_missing_column_raises(self, sample_csv):
        with pytest.raises(ValueError, match="not found"):
            FileProcessor(sample_csv).get_column_stats("nonexistent")
