"""Integration tests for the ETL pipeline and API endpoints."""

import os

import pandas as pd
import pytest

from app.core.constants import (
    DataType,
    FilterOperator,
    ValidationRuleType,
    DatabaseType,
)
from app.models.schemas import (
    ColumnMapping,
    DatabaseConnection,
    DatabaseDestination,
    ETLJobRequest,
    FileDestination,
    FilterRule,
    IfExists,
    ValidationRule,
)
from app.services.etl_runner import JOB_STORE, run_etl_job


def _base_mappings():
    return [
        ColumnMapping(
            column_name="id", source_dtype="int64", target_dtype=DataType.INTEGER
        ),
        ColumnMapping(
            column_name="name", source_dtype="object", target_dtype=DataType.STRING
        ),
        ColumnMapping(
            column_name="score", source_dtype="float64", target_dtype=DataType.DECIMAL
        ),
    ]


def _sqlite_dest(db_path, table="output"):
    return DatabaseDestination(
        connection=DatabaseConnection(db_type=DatabaseType.SQLITE, database=db_path),
        table_name=table,
        if_exists=IfExists.REPLACE,
    )


# ── ETL Runner ────────────────────────────────────────────────────────────────


class TestETLRunner:
    def test_basic_pipeline(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["A", "B", "C"], "score": [90.0, 80.0, 70.0]}
        )
        df.to_csv(tmp_path / "data.csv", index=False)

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.total_rows == 3
        assert result.processed_rows == 3

    def test_filter_reduces_rows(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["A", "B", "C"], "score": [90.0, 50.0, 70.0]}
        )
        df.to_csv(tmp_path / "data.csv", index=False)

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            filters=[
                FilterRule(
                    column="score", operator=FilterOperator.GREATER_THAN, value=60
                )
            ],
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.processed_rows == 2

    def test_validation_splits_rows(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["A", None, "C"], "score": [90.0, 80.0, 70.0]}
        )
        df.to_csv(tmp_path / "data.csv", index=False)

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            validation_rules=[
                ValidationRule(column="name", rule_type=ValidationRuleType.NOT_NULL)
            ],
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert result.processed_rows == 2
        assert result.failed_rows == 1
        assert result.invalid_rows_file is not None
        assert os.path.exists(result.invalid_rows_file)

    def test_file_destination_csv(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "score": [90.0, 80.0]})
        df.to_csv(tmp_path / "data.csv", index=False)

        out_path = str(tmp_path / "output.csv")
        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            file_destination=FileDestination(format="csv", output_path=out_path),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert os.path.exists(out_path)
        assert len(pd.read_csv(out_path)) == 2

    def test_file_destination_parquet(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [90.0]})
        df.to_csv(tmp_path / "data.csv", index=False)
        out_path = str(tmp_path / "out.parquet")

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            file_destination=FileDestination(format="parquet", output_path=out_path),
        )
        result = run_etl_job(req)
        assert result.success is True
        assert len(pd.read_parquet(out_path)) == 1

    def test_missing_file_fails_gracefully(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        req = ETLJobRequest(
            file_id="does_not_exist.csv",
            column_mappings=_base_mappings(),
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.success is False
        assert "not found" in result.message.lower()

    def test_job_registered_in_store(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]})
        df.to_csv(tmp_path / "data.csv", index=False)

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.job_id in JOB_STORE
        assert JOB_STORE[result.job_id].success is True

    def test_log_file_written(self, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        df = pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]})
        df.to_csv(tmp_path / "data.csv", index=False)

        req = ETLJobRequest(
            file_id="data.csv",
            column_mappings=_base_mappings(),
            db_destination=_sqlite_dest(str(tmp_path / "out.db")),
        )
        result = run_etl_job(req)
        assert result.log_file is not None
        assert os.path.exists(result.log_file)

        from app.services.etl_logger import read_log_file

        events = read_log_file(result.job_id)
        assert len(events) > 0


# ── API endpoints ─────────────────────────────────────────────────────────────


class TestFilesAPI:
    def test_upload_csv(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))

        csv_content = b"id,name,score\n1,Alice,90\n2,Bob,80\n"
        resp = client.post(
            "/v1/files/upload",
            files={"file": ("test.csv", csv_content, "text/csv")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["data"]["row_count"] == 2
        assert data["data"]["file_id"].endswith(".csv")

    def test_upload_invalid_extension(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))

        resp = client.post(
            "/v1/files/upload",
            files={"file": ("test.json", b'{"a":1}', "application/json")},
        )
        assert resp.status_code == 400

    def test_list_files(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))

        # Write a CSV directly into upload dir
        (tmp_path / "a.csv").write_text("id\n1\n2\n")
        resp = client.get("/v1/files/list")
        assert resp.status_code == 200
        assert resp.json()["data"]["total"] >= 1

    def test_get_file_info(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        (tmp_path / "info.csv").write_text("id,val\n1,a\n2,b\n")

        resp = client.get("/v1/files/info/info.csv")
        assert resp.status_code == 200
        assert resp.json()["data"]["row_count"] == 2

    def test_get_file_info_not_found(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        resp = client.get("/v1/files/info/ghost.csv")
        assert resp.status_code == 404

    def test_delete_file(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        (tmp_path / "del.csv").write_text("id\n1\n")
        resp = client.delete("/v1/files/del.csv")
        assert resp.status_code == 200
        assert not (tmp_path / "del.csv").exists()


class TestUtilitiesAPI:
    def test_data_types(self, client):
        resp = client.get("/v1/utilities/data-types")
        assert resp.status_code == 200
        types = resp.json()["data"]["data_types"]
        ids = [t["type_id"] for t in types]
        assert "integer" in ids
        assert "string" in ids
        assert "date" in ids

    def test_date_formats(self, client):
        resp = client.get("/v1/utilities/date-formats")
        assert resp.status_code == 200
        formats = resp.json()["data"]["formats"]
        assert len(formats) > 0
        assert "format" in formats[0]
        assert "example" in formats[0]

    def test_datetime_formats(self, client):
        resp = client.get("/v1/utilities/datetime-formats")
        assert resp.status_code == 200


class TestDatabaseAPI:
    def test_supported_types(self, client):
        resp = client.get("/v1/databases/supported-types")
        assert resp.status_code == 200
        db_types = [d["type"] for d in resp.json()["data"]]
        assert "postgresql" in db_types
        assert "mysql" in db_types
        assert "sqlite" in db_types

    def test_sqlite_test_connection(self, client, tmp_path):
        resp = client.post(
            "/v1/databases/test-connection",
            json={
                "connection": {
                    "db_type": "sqlite",
                    "database": str(tmp_path / "conn_test.db"),
                }
            },
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True


class TestETLAPI:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_run_etl_job(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "score": [90.0, 80.0]}).to_csv(
            tmp_path / "etl_test.csv", index=False
        )

        resp = client.post(
            "/v1/etl/run",
            json={
                "file_id": "etl_test.csv",
                "column_mappings": [
                    {
                        "column_name": "id",
                        "source_dtype": "int64",
                        "target_dtype": "integer",
                    },
                    {
                        "column_name": "name",
                        "source_dtype": "object",
                        "target_dtype": "string",
                    },
                    {
                        "column_name": "score",
                        "source_dtype": "float64",
                        "target_dtype": "decimal",
                    },
                ],
                "db_destination": {
                    "connection": {
                        "db_type": "sqlite",
                        "database": str(tmp_path / "api_out.db"),
                    },
                    "table_name": "results",
                    "if_exists": "replace",
                },
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["processed_rows"] == 2

    def test_run_etl_missing_source_rejected(self, client):
        resp = client.post(
            "/v1/etl/run",
            json={
                "column_mappings": [
                    {
                        "column_name": "id",
                        "source_dtype": "int64",
                        "target_dtype": "integer",
                    }
                ],
                "db_destination": {
                    "connection": {"db_type": "sqlite", "database": ":memory:"},
                    "table_name": "t",
                },
            },
        )
        # Pydantic validation should reject this (no file_id or api_source)
        assert resp.status_code == 422

    def test_job_status_endpoint(self, client, tmp_path, monkeypatch):
        from app.core import config

        monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path))
        monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path))

        pd.DataFrame({"id": [1], "name": ["A"], "score": [1.0]}).to_csv(
            tmp_path / "s.csv", index=False
        )
        run_resp = client.post(
            "/v1/etl/run",
            json={
                "file_id": "s.csv",
                "column_mappings": [
                    {
                        "column_name": "id",
                        "source_dtype": "int64",
                        "target_dtype": "integer",
                    },
                    {
                        "column_name": "name",
                        "source_dtype": "object",
                        "target_dtype": "string",
                    },
                    {
                        "column_name": "score",
                        "source_dtype": "float64",
                        "target_dtype": "decimal",
                    },
                ],
                "db_destination": {
                    "connection": {
                        "db_type": "sqlite",
                        "database": str(tmp_path / "s_out.db"),
                    },
                    "table_name": "t",
                    "if_exists": "replace",
                },
            },
        )
        job_id = run_resp.json()["job_id"]
        status_resp = client.get(f"/v1/etl/status/{job_id}")
        assert status_resp.status_code == 200
        assert status_resp.json()["job_id"] == job_id

    def test_status_unknown_job_404(self, client):
        resp = client.get("/v1/etl/status/nonexistent-job-id")
        assert resp.status_code == 404
