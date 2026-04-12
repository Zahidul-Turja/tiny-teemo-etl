import csv
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from app.core.config import settings
from app.core.constants import LogLevel


class ETLLogger:
    """
    Structured logger for ETL jobs.
    - Writes JSON-lines to logs/{job_id}.jsonl
    - Saves invalid rows to invalid_rows/{job_id}_invalid.csv (or .xlsx)
    - Exposes in-memory event list for the API response
    """

    def __init__(self, job_id: Optional[str] = None):
        self.job_id = job_id or str(uuid.uuid4())
        self.events: List[Dict[str, Any]] = []

        os.makedirs(settings.LOG_DIR, exist_ok=True)
        os.makedirs(settings.INVALID_ROWS_DIR, exist_ok=True)

        self.log_file = os.path.join(settings.LOG_DIR, f"{self.job_id}.jsonl")
        self._file_handle = open(self.log_file, "w", encoding="utf-8")

        self.log(LogLevel.INFO, "ETL job started", {"job_id": self.job_id})

    # ── public API ──────────────────────────────────────────────────────────

    def log(
        self,
        level: LogLevel,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level.value,
            "job_id": self.job_id,
            "message": message,
            **(extra or {}),
        }
        self.events.append(event)
        self._file_handle.write(json.dumps(event) + "\n")
        self._file_handle.flush()

        # Mirror to Python stdlib logging so uvicorn picks it up
        py_level = getattr(logging, level.value, logging.INFO)
        logging.getLogger("etl").log(py_level, f"[{self.job_id}] {message}")

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.log(LogLevel.INFO, message, extra)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.log(LogLevel.WARNING, message, extra)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.log(LogLevel.ERROR, message, extra)

    def save_invalid_rows(
        self,
        invalid_df: pd.DataFrame,
        validation_errors: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[str]:
        if invalid_df.empty:
            return None

        out_path = os.path.join(settings.INVALID_ROWS_DIR, f"{self.job_id}_invalid.csv")

        # Attach error reasons if provided
        if validation_errors:
            reasons: Dict[int, List[str]] = {}
            for err in validation_errors:
                idx = err.get("row_index", -1)
                reasons.setdefault(idx, []).append(f"{err['column']}: {err['message']}")
            invalid_df = invalid_df.copy()
            invalid_df["_validation_errors"] = [
                "; ".join(reasons.get(i, [])) for i in range(len(invalid_df))
            ]

        invalid_df.to_csv(out_path, index=False)
        self.info(
            f"Saved {len(invalid_df)} invalid row(s) to {out_path}",
            {"invalid_rows_file": out_path, "count": len(invalid_df)},
        )
        return out_path

    def close(self) -> None:
        self.log(LogLevel.INFO, "ETL job finished")
        self._file_handle.close()

    def summary(self) -> Dict[str, Any]:
        levels = [e["level"] for e in self.events]
        return {
            "job_id": self.job_id,
            "log_file": self.log_file,
            "total_events": len(self.events),
            "errors": levels.count("ERROR"),
            "warnings": levels.count("WARNING"),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── utility: read a past log file ───────────────────────────────────────────


def read_log_file(job_id: str) -> List[Dict[str, Any]]:
    path = os.path.join(settings.LOG_DIR, f"{job_id}.jsonl")
    if not os.path.exists(path):
        return []
    events = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events
