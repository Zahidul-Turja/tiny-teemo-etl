import os
import time
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd

from app.core.config import settings
from app.core.constants import DatabaseType
from app.database.connectors.mysql import MySQLConnector
from app.database.connectors.postgres import PostgresConnector
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import ETLJobRequest, ETLJobResult
from app.services.api_writer import APIWriter
from app.services.etl_logger import ETLLogger
from app.services.file_processor import FileProcessor
from app.services.file_writer import FileWriter
from app.services.schema_mapper import (
    Aggregator,
    DataValidator,
    RowFilter,
    SchemaMapper,
)

# ── in-memory job status store ────────────────────────────────────────────────
# Maps job_id → ETLJobResult. For production, swap with Redis or a DB table.
JOB_STORE: Dict[str, ETLJobResult] = {}


def get_job_status(job_id: str) -> Optional[ETLJobResult]:
    return JOB_STORE.get(job_id)


def list_jobs() -> List[ETLJobResult]:
    return list(JOB_STORE.values())


def _get_connector(connection):
    mapping = {
        DatabaseType.POSTGRESQL: PostgresConnector,
        DatabaseType.MYSQL: MySQLConnector,
        DatabaseType.SQLITE: SQLiteConnector,
    }
    cls = mapping.get(connection.db_type)
    if not cls:
        raise ValueError(f"Unsupported database type: {connection.db_type}")
    return cls(connection)


def run_etl_job(request: ETLJobRequest) -> ETLJobResult:
    """
    Full ETL pipeline:
      1. Extract   – file (CSV/Excel/Parquet) or paginated REST API
      2. Filter    – row-level filter rules
      3. Transform – type cast, rename, prefix/suffix
      4. Validate  – split valid/invalid rows; save invalid to CSV
      5. Aggregate – optional group-by
      6. Load      – DB / file / API with retry + batching
    """
    job_id = str(uuid.uuid4())
    JOB_STORE[job_id] = ETLJobResult(
        job_id=job_id,
        success=False,
        message="running",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )

    with ETLLogger(job_id=job_id) as logger:
        try:
            # 1. Extract
            if request.api_source:
                logger.info("Extracting from API", {"url": request.api_source.url})
                df = _read_from_api(request.api_source, logger)
            else:
                file_path = os.path.join(settings.UPLOAD_DIR, request.file_id)
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"Source file not found: {request.file_id}")
                logger.info("Extracting file", {"file_id": request.file_id})
                df = FileProcessor(file_path).df

            total_rows = len(df)
            logger.info(f"Extracted {total_rows} rows, {len(df.columns)} columns")

            # 2. Filter
            if request.filters:
                logger.info(f"Applying {len(request.filters)} filter rule(s)")
                df, filtered_out = RowFilter().apply(df, request.filters)
                logger.info(
                    f"After filtering: {len(df)} kept, {len(filtered_out)} discarded",
                    {"discarded_rows": len(filtered_out)},
                )

            # 3. Transform
            logger.info("Applying schema mappings")
            mapper = SchemaMapper(df)
            df = mapper.apply_column_mapping(request.column_mappings)

            if mapper.transformation_errors:
                for err in mapper.transformation_errors:
                    logger.warning(
                        f"Transform error on '{err['column']}': {err['error']}"
                    )
                raise ValueError(
                    f"Schema transformation failed on "
                    f"{len(mapper.transformation_errors)} column(s). See warnings."
                )

            # 4. Validate
            invalid_rows_file: Optional[str] = None
            failed_rows = 0

            if request.validation_rules:
                logger.info(
                    f"Running {len(request.validation_rules)} validation rule(s)"
                )
                df, invalid_df, validation_errors = DataValidator().validate(
                    df, request.validation_rules
                )
                failed_rows = len(invalid_df)
                logger.info(
                    f"Validation: {len(df)} valid, {failed_rows} invalid",
                    {"invalid_count": failed_rows},
                )
                if failed_rows > 0:
                    invalid_rows_file = logger.save_invalid_rows(
                        invalid_df, validation_errors
                    )

            # 5. Aggregate
            if request.aggregations:
                logger.info("Applying aggregations")
                df = Aggregator().apply(df, request.aggregations)
                logger.info(f"After aggregation: {len(df)} rows")

            # 6. Load
            load_details: Dict[str, Any] = {}

            if request.db_destination:
                dest = request.db_destination
                logger.info(
                    f"Loading to DB table '{dest.table_name}' (if_exists={dest.if_exists.value})"
                )
                connector = _get_connector(dest.connection)
                db_result = _upload_with_retry(
                    connector=connector,
                    df=df,
                    table_name=dest.table_name,
                    column_mappings=request.column_mappings,
                    if_exists=dest.if_exists.value,
                    batch_size=request.batch_size,
                    max_retries=request.max_retries,
                    logger=logger,
                )
                if dest.create_index and dest.index_columns:
                    try:
                        with _get_connector(dest.connection) as conn:
                            conn.create_index(dest.table_name, dest.index_columns)
                        logger.info(f"Index created on {dest.index_columns}")
                    except Exception as exc:
                        logger.warning(f"Index creation failed (non-fatal): {exc}")
                load_details["database"] = db_result

            if request.file_destination:
                dest = request.file_destination
                logger.info(f"Writing to file: {dest.output_path} ({dest.format})")
                file_result = FileWriter().write(df, dest.output_path, dest.format)
                logger.info(
                    f"Wrote {file_result['rows_written']} rows to {dest.output_path}"
                )
                load_details["file"] = file_result

            if request.api_destination:
                dest = request.api_destination
                logger.info(f"Sending to API: {dest.url}")
                api_result = APIWriter(
                    dest,
                    max_retries=request.max_retries,
                    retry_delay=settings.RETRY_DELAY_SECONDS,
                ).write(df)
                logger.info(
                    f"API load: {api_result['sent']} sent, {api_result['failed']} failed",
                    {"api_errors": api_result["errors"]},
                )
                load_details["api"] = api_result

            final = ETLJobResult(
                job_id=job_id,
                success=True,
                message="ETL job completed successfully.",
                total_rows=total_rows,
                processed_rows=len(df),
                failed_rows=failed_rows,
                invalid_rows_file=invalid_rows_file,
                log_file=logger.log_file,
                details=load_details,
            )

        except Exception as exc:
            logger.error(f"ETL job failed: {exc}", {"error_type": type(exc).__name__})
            final = ETLJobResult(
                job_id=job_id,
                success=False,
                message=str(exc),
                total_rows=0,
                processed_rows=0,
                failed_rows=0,
                log_file=logger.log_file,
            )

    JOB_STORE[job_id] = final
    return final


def _read_from_api(source, logger: ETLLogger) -> pd.DataFrame:
    """Fetch records from a paginated REST API."""
    import httpx

    # Build auth headers via a lightweight stub
    stub_writer = object.__new__(APIWriter)
    stub_writer.dest = source  # source has same auth/headers fields as APIDestination
    auth_headers = stub_writer._auth_headers()

    headers = dict(source.headers or {})
    headers.update(auth_headers)

    all_records: List[Dict[str, Any]] = []
    page = source.start_page
    next_url: Optional[str] = source.url

    with httpx.Client(timeout=30) as client:
        while next_url:
            params: Dict[str, Any] = {}
            if source.page_param and page is not None:
                params[source.page_param] = page
                if source.page_size_param:
                    params[source.page_size_param] = source.page_size

            resp = client.get(next_url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            # Drill into nested key e.g. "results" or "data.items"
            records = data
            if source.records_key:
                for key in source.records_key.split("."):
                    records = records[key]

            if not isinstance(records, list) or len(records) == 0:
                break

            all_records.extend(records)
            logger.info(
                f"Fetched page {page}: +{len(records)} records "
                f"(running total: {len(all_records)})"
            )

            # Advance pagination
            if source.next_url_key:
                next_url = data.get(source.next_url_key)
                page = None
            elif source.page_param:
                page += 1
                next_url = source.url
                if source.max_pages and (page - source.start_page) >= source.max_pages:
                    logger.info(f"Reached max_pages={source.max_pages}, stopping.")
                    break
            else:
                break

    logger.info(f"API extraction done: {len(all_records)} total records")
    return pd.DataFrame(all_records)


def _upload_with_retry(
    connector,
    df,
    table_name,
    column_mappings,
    if_exists,
    batch_size,
    max_retries,
    logger: ETLLogger,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            result = connector.upload_dataframe(
                df=df,
                table_name=table_name,
                column_mappings=column_mappings,
                if_exists=if_exists,
                batch_size=batch_size,
            )
            logger.info(
                f"DB upload succeeded on attempt {attempt}",
                {
                    "rows_inserted": result.get("rows_inserted"),
                    "rows_failed": result.get("rows_failed"),
                },
            )
            return result
        except Exception as exc:
            last_exc = exc
            logger.warning(f"DB upload attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                time.sleep(settings.RETRY_DELAY_SECONDS * attempt)

    raise RuntimeError(
        f"DB upload failed after {max_retries} attempt(s): {last_exc}"
    ) from last_exc
