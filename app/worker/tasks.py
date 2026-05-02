from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Optional

from celery import Task
from celery.exceptions import MaxRetriesExceededError
from celery.utils.log import get_task_logger

from app.core.config import settings
from app.models.schemas import ETLJobRequest, ETLJobResult
from app.worker.celery_app import celery_app
from app.worker.job_store import (
    get_idempotent_job_id,
    publish_progress,
    save_job,
    set_idempotent_job_id,
)

logger = get_task_logger(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────


def _progress(job_id: str, stage: str, pct: int, message: str, **extra) -> None:
    """Publish a progress event and log it."""
    event = {
        "job_id": job_id,
        "stage": stage,
        "progress": pct,
        "message": message,
        **extra,
    }
    publish_progress(job_id, event)
    logger.info("[%s] %s — %d%% — %s", job_id, stage, pct, message)


def _fail_job(job_id: str, message: str) -> ETLJobResult:
    result = ETLJobResult(
        job_id=job_id,
        success=False,
        message=message,
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )
    save_job(result)
    publish_progress(
        job_id,
        {"job_id": job_id, "stage": "failed", "progress": 100, "message": message},
    )
    return result


# ── dead-letter task ──────────────────────────────────────────────────────────


@celery_app.task(name="app.worker.tasks.etl_dead_letter", queue="etl.dlq")
def etl_dead_letter(job_id: str, request_dict: Dict[str, Any], reason: str) -> None:
    """
    Receives jobs that exhausted all retries.
    Currently just marks them as permanently failed in the job store.
    In production you'd alert/page here.
    """
    logger.error("[DLQ] job_id=%s permanently failed. Reason: %s", job_id, reason)
    _fail_job(job_id, f"[DLQ] Permanently failed after all retries: {reason}")


# ── main ETL task ─────────────────────────────────────────────────────────────


class ETLTask(Task):
    """Custom base Task that routes to DLQ on MaxRetriesExceededError."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if isinstance(exc, MaxRetriesExceededError):
            job_id = kwargs.get("job_id") or (args[1] if len(args) > 1 else "unknown")
            request_dict = kwargs.get("request_dict") or (args[0] if args else {})
            etl_dead_letter.apply_async(
                kwargs={
                    "job_id": job_id,
                    "request_dict": request_dict,
                    "reason": str(einfo),
                },
                queue="etl.dlq",
            )


@celery_app.task(
    name="app.worker.tasks.run_etl_task",
    base=ETLTask,
    bind=True,
    queue="etl.default",
    # Retry config — exponential backoff: 2s, 4s, 8s …
    autoretry_for=(Exception,),
    max_retries=settings.MAX_RETRIES,
    retry_backoff=True,  # 2^attempt * retry_backoff_max (capped)
    retry_backoff_max=60,  # cap at 60 s
    retry_jitter=True,  # add randomness to avoid thundering herd
    # Don't retry on these — they are data/config errors, not transient failures
    dont_autoretry_for=(
        ValueError,
        FileNotFoundError,
        KeyError,
    ),
    # Acknowledge AFTER execution so the task is re-queued on worker crash
    acks_late=True,
)
def run_etl_task(
    self: Task,
    request_dict: Dict[str, Any],
    job_id: str,
) -> Dict[str, Any]:
    """
    Execute the full ETL pipeline inside a Celery task.

    Parameters
    ----------
    request_dict : dict
        Serialised ETLJobRequest (JSON-safe).
    job_id : str
        Pre-assigned job ID (set by the API endpoint for idempotency).
    """
    import os
    import pandas as pd

    from app.core.constants import DatabaseType
    from app.database.connectors.mysql import MySQLConnector
    from app.database.connectors.postgres import PostgresConnector
    from app.database.connectors.sqlite import SQLiteConnector
    from app.core.config import settings as cfg
    from app.services.api_writer import APIWriter
    from app.services.db_reader import read_from_db
    from app.services.etl_logger import ETLLogger
    from app.services.file_processor import FileProcessor
    from app.services.file_writer import FileWriter
    from app.services.schema_mapper import (
        Aggregator,
        DataValidator,
        RowFilter,
        SchemaMapper,
    )

    request = ETLJobRequest.model_validate(request_dict)

    def connector_for(connection):
        mapping = {
            DatabaseType.POSTGRESQL: PostgresConnector,
            DatabaseType.MYSQL: MySQLConnector,
            DatabaseType.SQLITE: SQLiteConnector,
        }
        cls = mapping.get(connection.db_type)
        if not cls:
            raise ValueError(f"Unsupported database type: {connection.db_type}")
        return cls(connection)

    # Mark job as running
    running_result = ETLJobResult(
        job_id=job_id,
        success=False,
        message="running",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )
    save_job(running_result)
    _progress(job_id, "started", 0, "ETL job started")

    with ETLLogger(job_id=job_id) as etl_log:
        try:
            # ── 1. EXTRACT ────────────────────────────────────────────────────
            _progress(job_id, "extract", 5, "Extracting data from source")

            if request.api_source:
                etl_log.info("Extracting from API", {"url": request.api_source.url})
                df = _read_from_api_local(request.api_source, etl_log)

            elif request.db_source:
                src = request.db_source
                label = src.table_name or "custom query"
                etl_log.info(
                    f"Extracting from DB: {src.connection.db_type.value} / {label}"
                )
                df, auto_mappings = read_from_db(src)
                if not request.column_mappings:
                    request = request.model_copy(
                        update={"column_mappings": auto_mappings}
                    )
                else:
                    user_cols = {m.column_name for m in request.column_mappings}
                    merged = list(request.column_mappings) + [
                        m for m in auto_mappings if m.column_name not in user_cols
                    ]
                    request = request.model_copy(update={"column_mappings": merged})

            else:
                if not request.file_id:
                    raise ValueError("No source specified.")
                file_path = os.path.join(cfg.UPLOAD_DIR, request.file_id)
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Source file not found: {request.file_id}")
                etl_log.info("Extracting file", {"file_id": request.file_id})
                df = FileProcessor(file_path).df

            total_rows = len(df)
            _progress(
                job_id,
                "extract",
                20,
                f"Extracted {total_rows} rows",
                total_rows=total_rows,
            )

            # ── 2. FILTER ─────────────────────────────────────────────────────
            if request.filters:
                _progress(
                    job_id,
                    "filter",
                    30,
                    f"Applying {len(request.filters)} filter rule(s)",
                )
                df, filtered_out = RowFilter().apply(df, request.filters)
                _progress(
                    job_id,
                    "filter",
                    35,
                    f"{len(df)} kept, {len(filtered_out)} discarded",
                )

            # ── 3. TRANSFORM ──────────────────────────────────────────────────
            _progress(job_id, "transform", 40, "Applying schema mappings")
            mapper = SchemaMapper(df)
            df = mapper.apply_column_mapping(request.column_mappings)

            if mapper.transformation_errors:
                for err in mapper.transformation_errors:
                    etl_log.warning(
                        f"Transform warning on '{err['column']}': {err['error']}"
                    )
                _progress(
                    job_id,
                    "transform",
                    55,
                    f"Schema mapping complete ({len(mapper.transformation_errors)} column warning(s))",
                    warnings=len(mapper.transformation_errors),
                )
            else:
                _progress(job_id, "transform", 55, "Schema mapping complete")

            # ── 4. VALIDATE ───────────────────────────────────────────────────
            invalid_rows_file: Optional[str] = None
            failed_rows = 0

            if request.validation_rules:
                _progress(
                    job_id,
                    "validate",
                    60,
                    f"Running {len(request.validation_rules)} validation rule(s)",
                )
                df, invalid_df, validation_errors = DataValidator().validate(
                    df, request.validation_rules
                )
                failed_rows = len(invalid_df)
                if failed_rows > 0:
                    invalid_rows_file = etl_log.save_invalid_rows(
                        invalid_df, validation_errors
                    )
                _progress(
                    job_id,
                    "validate",
                    70,
                    f"{len(df)} valid, {failed_rows} invalid",
                    failed_rows=failed_rows,
                )

            # ── 5. AGGREGATE ──────────────────────────────────────────────────
            if request.aggregations:
                _progress(job_id, "aggregate", 75, "Applying aggregations")
                df = Aggregator().apply(df, request.aggregations)
                _progress(job_id, "aggregate", 78, f"After aggregation: {len(df)} rows")

            # ── 6. LOAD ───────────────────────────────────────────────────────
            _progress(job_id, "load", 80, "Loading data to destination(s)")
            load_details: Dict[str, Any] = {}

            if request.db_destination:
                dest = request.db_destination
                _progress(
                    job_id, "load", 82, f"Writing to DB table '{dest.table_name}'"
                )
                conn = connector_for(dest.connection)
                db_result = _db_upload_with_retry(
                    connector=conn,
                    df=df,
                    table_name=dest.table_name,
                    column_mappings=request.column_mappings,
                    if_exists=dest.if_exists.value,
                    batch_size=request.batch_size,
                    max_retries=request.max_retries,
                    logger=etl_log,
                )
                if dest.create_index and dest.index_columns:
                    try:
                        with connector_for(dest.connection) as c:
                            c.create_index(dest.table_name, dest.index_columns)
                    except Exception as idx_exc:
                        etl_log.warning(f"Index creation failed (non-fatal): {idx_exc}")
                load_details["database"] = db_result

            if request.file_destination:
                dest = request.file_destination
                _progress(job_id, "load", 90, f"Writing to file: {dest.output_path}")
                file_result = FileWriter().write(df, dest.output_path, dest.format)
                load_details["file"] = file_result

            if request.api_destination:
                dest = request.api_destination
                _progress(job_id, "load", 93, f"Sending to API: {dest.url}")
                api_result = APIWriter(
                    dest,
                    max_retries=request.max_retries,
                    retry_delay=cfg.RETRY_DELAY_SECONDS,
                ).write(df)
                load_details["api"] = api_result

            # ── DONE ──────────────────────────────────────────────────────────
            final = ETLJobResult(
                job_id=job_id,
                success=True,
                message="ETL job completed successfully.",
                total_rows=total_rows,
                processed_rows=len(df),
                failed_rows=failed_rows,
                invalid_rows_file=invalid_rows_file,
                log_file=etl_log.log_file,
                details=load_details,
            )
            save_job(final)
            _progress(
                job_id,
                "done",
                100,
                "ETL job completed successfully",
                processed_rows=len(df),
                failed_rows=failed_rows,
            )
            return final.model_dump()

        except Exception as exc:
            etl_log.error(f"ETL job failed: {exc}", {"error_type": type(exc).__name__})
            attempt = self.request.retries + 1
            max_r = self.max_retries or 0
            if attempt <= max_r and not isinstance(
                exc, (ValueError, FileNotFoundError, KeyError)
            ):
                # Transient failure — will be retried; tell the UI
                _progress(
                    job_id,
                    "retrying",
                    0,
                    f"Transient error — retry {attempt}/{max_r}: {exc}",
                )
            else:
                # Permanent failure or data error — mark done so UI stops spinning
                _fail_job(job_id, str(exc))
            raise


# ── internal helpers (kept local to avoid import cycles) ─────────────────────


def _read_from_api_local(source, etl_log) -> "pd.DataFrame":
    """Paginated REST API reader — identical logic to original etl_runner."""
    import httpx
    import pandas as pd
    from app.services.api_writer import APIWriter

    stub = object.__new__(APIWriter)
    stub.dest = source
    auth_headers = stub._auth_headers()
    headers = dict(source.headers or {})
    headers.update(auth_headers)

    all_records = []
    page = source.start_page
    next_url = source.url

    with httpx.Client(timeout=30) as client:
        while next_url:
            params = {}
            if source.page_param and page is not None:
                params[source.page_param] = page
                if source.page_size_param:
                    params[source.page_size_param] = source.page_size

            resp = client.get(next_url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            records = data
            if source.records_key:
                for key in source.records_key.split("."):
                    records = records[key]

            if not isinstance(records, list) or not records:
                break

            all_records.extend(records)
            etl_log.info(
                f"Fetched page {page}: +{len(records)} (total: {len(all_records)})"
            )

            if source.next_url_key:
                next_url = data.get(source.next_url_key)
                page = None
            elif source.page_param:
                page += 1
                next_url = source.url
                if source.max_pages and (page - source.start_page) >= source.max_pages:
                    break
            else:
                break

    return pd.DataFrame(all_records)


def _db_upload_with_retry(
    connector,
    df,
    table_name,
    column_mappings,
    if_exists,
    batch_size,
    max_retries,
    logger,
) -> Dict[str, Any]:
    """Synchronous retry loop for DB upload (within a single task execution)."""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return connector.upload_dataframe(
                df=df,
                table_name=table_name,
                column_mappings=column_mappings,
                if_exists=if_exists,
                batch_size=batch_size,
            )
        except Exception as exc:
            last_exc = exc
            logger.warning(f"DB upload attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                time.sleep(settings.RETRY_DELAY_SECONDS * (2 ** (attempt - 1)))
    raise RuntimeError(
        f"DB upload failed after {max_retries} attempt(s): {last_exc}"
    ) from last_exc
