import sqlite3

import pandas as pd
import pytest

from app.core.constants import DataType, DatabaseType
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import ColumnMapping, DatabaseConnection


def _conn(db_path=":memory:"):
    return SQLiteConnector(
        DatabaseConnection(db_type=DatabaseType.SQLITE, database=db_path)
    )


def _mappings():
    return [
        ColumnMapping(
            column_name="id",
            source_dtype="int64",
            target_dtype=DataType.INTEGER,
            is_primary_key=True,
        ),
        ColumnMapping(
            column_name="name", source_dtype="object", target_dtype=DataType.STRING
        ),
        ColumnMapping(
            column_name="score", source_dtype="float64", target_dtype=DataType.DECIMAL
        ),
    ]


class TestSQLiteConnector:
    def test_connect_disconnect(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        assert conn._conn is not None
        conn.disconnect()
        assert conn._conn is None

    def test_context_manager(self, tmp_path):
        with _conn(str(tmp_path / "test.db")) as conn:
            assert conn._conn is not None

    def test_test_connection(self, tmp_path):
        result = _conn(str(tmp_path / "test.db")).test_connection()
        assert result["success"] is True
        assert "SQLite" in result["server_version"]

    def test_create_table(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        # table should exist now
        cursor = conn._conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert cursor.fetchone() is not None
        conn.disconnect()

    def test_table_exists_true(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        assert conn.table_exists("users") is True
        conn.disconnect()

    def test_table_exists_false(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        assert conn.table_exists("nonexistent") is False
        conn.disconnect()

    def test_drop_table(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        conn.drop_table("users")
        assert conn.table_exists("users") is False
        conn.disconnect()

    def test_insert_data(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        df = pd.DataFrame(
            {"id": [1, 2], "name": ["Alice", "Bob"], "score": [95.5, 82.0]}
        )
        result = conn.insert_data("users", df)
        assert result["rows_inserted"] == 2
        assert result["rows_failed"] == 0
        conn.disconnect()

    def test_upload_dataframe_full_cycle(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        df = pd.DataFrame(
            {"id": [1, 2], "name": ["Alice", "Bob"], "score": [90.0, 80.0]}
        )
        result = conn.upload_dataframe(df, "users", _mappings(), if_exists="replace")
        assert result["success"] is True
        assert result["rows_inserted"] == 2

    def test_upload_fail_on_existing_table(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]})
        conn.upload_dataframe(df, "users", _mappings())
        with pytest.raises(ValueError, match="already exists"):
            conn.upload_dataframe(df, "users", _mappings(), if_exists="fail")

    def test_upload_append(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]})
        conn.upload_dataframe(df, "users", _mappings())
        df2 = pd.DataFrame({"id": [2], "name": ["B"], "score": [2.0]})
        conn.upload_dataframe(df2, "users", _mappings(), if_exists="append")
        # Verify both rows present
        rows = (
            sqlite3.connect(str(tmp_path / "test.db"))
            .execute("SELECT COUNT(*) FROM users")
            .fetchone()[0]
        )
        assert rows == 2

    def test_upload_replace(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "score": [1.0, 2.0]})
        conn.upload_dataframe(df, "users", _mappings())
        df2 = pd.DataFrame({"id": [99], "name": ["Only"], "score": [0.0]})
        conn.upload_dataframe(df2, "users", _mappings(), if_exists="replace")
        rows = (
            sqlite3.connect(str(tmp_path / "test.db"))
            .execute("SELECT COUNT(*) FROM users")
            .fetchone()[0]
        )
        assert rows == 1

    def test_create_index(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]})
        conn.insert_data("users", df)
        conn.create_index("users", ["name"])
        cursor = conn._conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='users'"
        )
        indexes = [row[0] for row in cursor.fetchall()]
        assert any("name" in idx for idx in indexes)
        conn.disconnect()

    def test_summarize(self, tmp_path):
        conn = _conn(str(tmp_path / "test.db"))
        conn.connect()
        conn.create_table("users", _mappings())
        conn.disconnect()
        summary = conn.summarize()
        assert "users" in summary["list_of_tables"]
        assert len(summary["previews"]) == 1

    def test_native_type_conversion(self, tmp_path):
        """Verifies numpy/date types don't cause sqlite3 datatype mismatch."""
        import datetime
        import numpy as np
        from app.services.schema_mapper import SchemaMapper

        raw_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "score": [95.5, 82.0],
                "active": ["true", "false"],
                "joined": ["2023-01-10", "2024-06-20"],
            }
        )
        mappings = [
            ColumnMapping(
                column_name="id", source_dtype="int64", target_dtype=DataType.INTEGER
            ),
            ColumnMapping(
                column_name="name", source_dtype="object", target_dtype=DataType.STRING
            ),
            ColumnMapping(
                column_name="score",
                source_dtype="float64",
                target_dtype=DataType.DECIMAL,
            ),
            ColumnMapping(
                column_name="active",
                source_dtype="object",
                target_dtype=DataType.BOOLEAN,
            ),
            ColumnMapping(
                column_name="joined", source_dtype="object", target_dtype=DataType.DATE
            ),
        ]
        transformed = SchemaMapper(raw_df).apply_column_mapping(mappings)
        conn = _conn(str(tmp_path / "types.db"))
        result = conn.upload_dataframe(
            transformed, "typed", mappings, if_exists="replace"
        )
        assert result["rows_inserted"] == 2
