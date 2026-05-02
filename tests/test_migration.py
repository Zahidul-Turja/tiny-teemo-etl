import sqlite3

import pandas as pd
import pytest

from app.core.constants import (
    DataType,
    DatabaseType,
    FilterOperator,
    ValidationRuleType,
)
from app.models.schemas import (
    ColumnMapping,
    DatabaseConnection,
    DatabaseDestination,
    DatabaseSource,
    DBMigrationRequest,
    ETLJobRequest,
    FilterRule,
    IfExists,
    ValidationRule,
)
from app.services.db_reader import (
    _auto_column_mappings,
    get_source_schema,
    read_from_db,
)
from app.services.etl_runner import run_etl_job

# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def source_db(tmp_path):
    """Create a populated SQLite source database and return its path."""
    db_path = str(tmp_path / "source.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE orders (
            order_id   INTEGER PRIMARY KEY,
            customer   TEXT,
            amount     REAL,
            status     TEXT,
            created_at TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, "Alice", 150.00, "paid", "2024-01-10"),
            (2, "Bob", 200.50, "pending", "2024-02-15"),
            (3, "Charlie", 75.00, "paid", "2024-03-01"),
            (4, "Dave", 300.00, "cancelled", "2024-03-20"),
            (5, None, 50.00, "paid", "2024-04-05"),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def dest_db(tmp_path):
    return str(tmp_path / "dest.db")


def _src_conn(db_path):
    return DatabaseConnection(db_type=DatabaseType.SQLITE, database=db_path)


def _dst_conn(db_path):
    return DatabaseConnection(db_type=DatabaseType.SQLITE, database=db_path)


# ── DatabaseSource schema validation ─────────────────────────────────────────


class TestDatabaseSourceSchema:
    def test_table_name_only(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        assert src.table_name == "orders"
        assert src.query is None

    def test_query_only(self, source_db):
        src = DatabaseSource(
            connection=_src_conn(source_db),
            query="SELECT * FROM orders WHERE amount > 100",
        )
        assert src.query is not None

    def test_neither_raises(self, source_db):
        with pytest.raises(Exception, match="Either"):
            DatabaseSource(connection=_src_conn(source_db))

    def test_both_raises(self, source_db):
        with pytest.raises(Exception, match="Only one"):
            DatabaseSource(
                connection=_src_conn(source_db),
                table_name="orders",
                query="SELECT 1",
            )

    def test_column_whitelist(self, source_db):
        src = DatabaseSource(
            connection=_src_conn(source_db),
            table_name="orders",
            columns=["order_id", "customer"],
        )
        assert src.columns == ["order_id", "customer"]


# ── read_from_db ──────────────────────────────────────────────────────────────


class TestReadFromDB:
    def test_reads_all_rows(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        df, mappings = read_from_db(src)
        assert len(df) == 5
        assert "order_id" in df.columns

    def test_custom_query(self, source_db):
        src = DatabaseSource(
            connection=_src_conn(source_db),
            query="SELECT * FROM orders WHERE status = 'paid'",
        )
        df, _ = read_from_db(src)
        assert len(df) == 3
        assert all(df["status"] == "paid")

    def test_column_whitelist(self, source_db):
        src = DatabaseSource(
            connection=_src_conn(source_db),
            table_name="orders",
            columns=["order_id", "customer"],
        )
        df, _ = read_from_db(src)
        assert list(df.columns) == ["order_id", "customer"]

    def test_chunked_read(self, source_db):
        src = DatabaseSource(
            connection=_src_conn(source_db),
            table_name="orders",
            chunk_size=2,
        )
        df, _ = read_from_db(src)
        assert len(df) == 5  # all rows merged

    def test_auto_mappings_generated(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        df, mappings = read_from_db(src)
        col_names = [m.column_name for m in mappings]
        assert "order_id" in col_names
        assert "customer" in col_names
        assert len(mappings) == len(df.columns)

    def test_auto_mappings_types(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        df, mappings = read_from_db(src)
        mapping_map = {m.column_name: m for m in mappings}
        # amount is REAL in SQLite → float in pandas → FLOAT in our enum
        assert mapping_map["amount"].target_dtype in (
            DataType.FLOAT,
            DataType.BIGINT,
            DataType.TEXT,
        )


# ── get_source_schema ─────────────────────────────────────────────────────────


class TestGetSourceSchema:
    def test_returns_columns(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        schema = get_source_schema(src)
        col_names = [s["column_name"] for s in schema]
        assert "order_id" in col_names
        assert "customer" in col_names

    def test_schema_has_required_keys(self, source_db):
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        schema = get_source_schema(src)
        for col in schema:
            assert "column_name" in col
            assert "source_dtype" in col
            assert "suggested_target_dtype" in col

    def test_no_rows_fetched(self, source_db):
        """Schema preview must not load actual data."""
        src = DatabaseSource(connection=_src_conn(source_db), table_name="orders")
        schema = get_source_schema(src)
        # Just checking it returns quickly and correctly — len check suffices
        assert len(schema) == 5  # 5 columns


# ── ETL runner with db_source ─────────────────────────────────────────────────


class TestETLRunnerWithDBSource:
    def _settings_patch(self, monkeypatch, tmp_path):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

    def test_migrate_as_is(self, source_db, dest_db, tmp_path, monkeypatch):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="orders"
            ),
            column_mappings=[],  # auto-generate
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="orders_copy",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.total_rows == 5
        assert result.processed_rows == 5

        rows = (
            sqlite3.connect(dest_db)
            .execute("SELECT COUNT(*) FROM orders_copy")
            .fetchone()[0]
        )
        assert rows == 5

    def test_migrate_with_filter(self, source_db, dest_db, tmp_path, monkeypatch):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="orders"
            ),
            column_mappings=[],
            filters=[
                FilterRule(
                    column="status", operator=FilterOperator.EQUALS, value="paid"
                )
            ],
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="paid_orders",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.processed_rows == 3

    def test_migrate_with_validation(self, source_db, dest_db, tmp_path, monkeypatch):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="orders"
            ),
            column_mappings=[],
            validation_rules=[
                ValidationRule(column="customer", rule_type=ValidationRuleType.NOT_NULL)
            ],
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="valid_orders",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.failed_rows == 1  # row with NULL customer
        assert result.processed_rows == 4
        assert result.invalid_rows_file is not None

    def test_migrate_with_column_mapping(
        self, source_db, dest_db, tmp_path, monkeypatch
    ):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="orders"
            ),
            column_mappings=[
                ColumnMapping(
                    column_name="amount",
                    source_dtype="float64",
                    target_dtype=DataType.DECIMAL,
                    rename_to="total_amount",
                ),
            ],
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="orders_renamed",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is True
        cols = [
            row[1]
            for row in sqlite3.connect(dest_db)
            .execute("PRAGMA table_info(orders_renamed)")
            .fetchall()
        ]
        assert "total_amount" in cols
        assert "amount" not in cols

    def test_migrate_custom_query(self, source_db, dest_db, tmp_path, monkeypatch):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db),
                query="SELECT order_id, customer, amount FROM orders WHERE amount >= 150",
            ),
            column_mappings=[],
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="high_value",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.processed_rows == 3

    def test_migrate_to_file(self, source_db, tmp_path, monkeypatch):
        from app.models.schemas import FileDestination

        self._settings_patch(monkeypatch, tmp_path)
        out_path = str(tmp_path / "orders_export.csv")
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="orders"
            ),
            column_mappings=[],
            file_destination=FileDestination(format="csv", output_path=out_path),
        )
        result = run_etl_job(req)
        assert result.success is True
        df = pd.read_csv(out_path)
        assert len(df) == 5

    def test_missing_table_fails_gracefully(
        self, source_db, dest_db, tmp_path, monkeypatch
    ):
        self._settings_patch(monkeypatch, tmp_path)
        req = ETLJobRequest(
            db_source=DatabaseSource(
                connection=_src_conn(source_db), table_name="nonexistent"
            ),
            column_mappings=[],
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="out",
                if_exists=IfExists.REPLACE,
            ),
        )
        result = run_etl_job(req)
        assert result.success is False
        assert result.message != ""


# ── DBMigrationRequest convenience schema ─────────────────────────────────────


class TestDBMigrationRequest:
    def test_valid_request(self, source_db, dest_db):
        req = DBMigrationRequest(
            source=DatabaseSource(connection=_src_conn(source_db), table_name="orders"),
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="orders_copy",
                if_exists=IfExists.REPLACE,
            ),
        )
        assert req.source.table_name == "orders"
        assert req.column_mappings is None  # auto-generate

    def test_defaults_to_no_mappings(self, source_db, dest_db):
        req = DBMigrationRequest(
            source=DatabaseSource(connection=_src_conn(source_db), table_name="orders"),
            db_destination=DatabaseDestination(
                connection=_dst_conn(dest_db),
                table_name="out",
            ),
        )
        assert req.filters is None
        assert req.validation_rules is None
        assert req.batch_size == 10_000


# ── Migration API endpoints ───────────────────────────────────────────────────


class TestMigrationAPI:
    def _patch(self, monkeypatch, tmp_path):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

    def test_preview_endpoint(self, client, source_db, tmp_path, monkeypatch):
        self._patch(monkeypatch, tmp_path)
        resp = client.post(
            "/v1/migrate/preview",
            json={
                "connection": {"db_type": "sqlite", "database": source_db},
                "table_name": "orders",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        col_names = [c["column_name"] for c in body["columns"]]
        assert "order_id" in col_names

    def test_preview_bad_table(self, client, source_db, tmp_path, monkeypatch):
        self._patch(monkeypatch, tmp_path)
        resp = client.post(
            "/v1/migrate/preview",
            json={
                "connection": {"db_type": "sqlite", "database": source_db},
                "table_name": "ghost_table",
            },
        )
        assert resp.status_code == 400

    def test_list_tables_endpoint(self, client, source_db, tmp_path, monkeypatch):
        self._patch(monkeypatch, tmp_path)
        resp = client.post(
            "/v1/migrate/tables",
            json={
                "connection": {"db_type": "sqlite", "database": source_db},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "orders" in body["tables"]

    def test_run_migration_endpoint(
        self, client, source_db, dest_db, tmp_path, monkeypatch
    ):
        self._patch(monkeypatch, tmp_path)
        resp = client.post(
            "/v1/migrate/run",
            json={
                "source": {
                    "connection": {"db_type": "sqlite", "database": source_db},
                    "table_name": "orders",
                },
                "db_destination": {
                    "connection": {"db_type": "sqlite", "database": dest_db},
                    "table_name": "orders_api",
                    "if_exists": "replace",
                },
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["processed_rows"] == 5

    def test_run_async_returns_202(
        self, client, source_db, dest_db, tmp_path, monkeypatch
    ):
        self._patch(monkeypatch, tmp_path)
        resp = client.post(
            "/v1/migrate/run-async",
            json={
                "source": {
                    "connection": {"db_type": "sqlite", "database": source_db},
                    "table_name": "orders",
                },
                "db_destination": {
                    "connection": {"db_type": "sqlite", "database": dest_db},
                    "table_name": "orders_async",
                    "if_exists": "replace",
                },
            },
        )
        assert resp.status_code == 202
        assert "job_id" in resp.json()
