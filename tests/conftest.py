"""Shared pytest fixtures."""

import os
import sys
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from main import app


# ── FastAPI test client ──────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


# ── Sample DataFrames ────────────────────────────────────────────────────────
@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", None, "Eve"],
            "score": [95.5, 82.0, 77.3, 60.0, 91.1],
            "active": ["true", "false", "yes", "no", "true"],
            "joined": [
                "2023-01-10",
                "2023-03-15",
                "bad-date",
                "2022-12-01",
                "2024-06-20",
            ],
        }
    )


@pytest.fixture
def sample_csv(tmp_path, sample_df):
    """Write sample_df to a temp CSV and return the path."""
    path = tmp_path / "sample.csv"
    sample_df.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def sample_excel(tmp_path, sample_df):
    path = tmp_path / "sample.xlsx"
    sample_df.to_excel(path, index=False)
    return str(path)


@pytest.fixture
def sample_parquet(tmp_path, sample_df):
    path = tmp_path / "sample.parquet"
    sample_df.to_parquet(path, index=False)
    return str(path)


# ── Uploaded file in UPLOAD_DIR (for API tests) ──────────────────────────────
@pytest.fixture
def uploaded_csv(tmp_path, sample_df, monkeypatch):
    """
    Write a CSV into a temp directory, patch settings.UPLOAD_DIR to point there,
    and return the filename so it can be passed as file_id.
    """
    from app.core import config

    monkeypatch.setattr(config.settings, "UPLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config.settings, "LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(config.settings, "INVALID_ROWS_DIR", str(tmp_path / "invalid"))
    os.makedirs(tmp_path / "logs", exist_ok=True)
    os.makedirs(tmp_path / "invalid", exist_ok=True)

    filename = "test_sample.csv"
    sample_df.to_csv(tmp_path / filename, index=False)
    return filename
