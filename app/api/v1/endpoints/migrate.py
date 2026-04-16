from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from fastapi.responses import JSONResponse

from app.models.schemas import (
    DBMigrationRequest,
    DatabaseSource,
    ETLJobRequest,
    TestConnectionRequest,
)
from app.services.db_reader import get_source_schema
from app.services.etl_runner import JOB_STORE, ETLJobResult, run_etl_job

router = APIRouter()


# ── helpers ───────────────────────────────────────────────────────────────────


def _migration_to_etl(req: DBMigrationRequest) -> ETLJobRequest:
    """Convert a DBMigrationRequest into the standard ETLJobRequest."""
    return ETLJobRequest(
        db_source=req.source,
        column_mappings=req.column_mappings or [],  # empty → auto-generated in runner
        filters=req.filters,
        aggregations=req.aggregations,
        validation_rules=req.validation_rules,
        db_destination=req.db_destination,
        batch_size=req.batch_size,
        max_retries=req.max_retries,
    )


# ── routes ────────────────────────────────────────────────────────────────────


@router.post(
    "/preview",
    summary="Inspect the source table/query schema before migrating",
    description=(
        "Returns column names and inferred target data types without fetching any rows. "
        "Use this to build or validate your `column_mappings` before calling `/migrate/run`."
    ),
)
async def preview_source_schema(source: DatabaseSource) -> JSONResponse:
    try:
        schema = get_source_schema(source)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "column_count": len(schema),
                "columns": schema,
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not read source schema: {exc}",
        )


@router.post(
    "/tables",
    summary="List all tables in a source database",
)
async def list_source_tables(request: TestConnectionRequest) -> JSONResponse:
    from app.core.constants import DatabaseType
    from app.database.connectors.mysql import MySQLConnector
    from app.database.connectors.postgres import PostgresConnector
    from app.database.connectors.sqlite import SQLiteConnector

    mapping = {
        DatabaseType.POSTGRESQL: PostgresConnector,
        DatabaseType.MYSQL: MySQLConnector,
        DatabaseType.SQLITE: SQLiteConnector,
    }
    cls = mapping.get(request.connection.db_type)
    if not cls:
        raise HTTPException(status_code=400, detail="Unsupported database type.")

    try:
        connector = cls(request.connection)
        summary = connector.summarize(preview_rows=0)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "database": summary["database"],
                "tables": summary["list_of_tables"],
                "table_count": len(summary["list_of_tables"]),
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not list tables: {exc}",
        )


@router.post(
    "/run",
    summary="Run a database-to-database migration (synchronous)",
    description=(
        "Extracts from the source DB, applies optional transforms/filters/validation, "
        "and loads into the destination DB. Waits for completion before responding. "
        "For large tables, use `/migrate/run-async` instead."
    ),
)
async def run_migration(request: DBMigrationRequest) -> JSONResponse:
    try:
        etl_request = _migration_to_etl(request)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        )

    result = run_etl_job(etl_request)
    http_status = (
        status.HTTP_200_OK if result.success else status.HTTP_422_UNPROCESSABLE_ENTITY
    )
    return JSONResponse(status_code=http_status, content=result.model_dump())


@router.post(
    "/run-async",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start a DB migration in the background",
    description=(
        "Returns a `job_id` immediately. "
        "Poll `GET /v1/etl/status/{job_id}` to check progress and get the final result."
    ),
)
async def run_migration_async(
    request: DBMigrationRequest,
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    import uuid

    try:
        etl_request = _migration_to_etl(request)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        )

    job_id_preview = str(uuid.uuid4())
    JOB_STORE[job_id_preview] = ETLJobResult(
        job_id=job_id_preview,
        success=False,
        message="running",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )

    def _run():
        import app.services.etl_runner as runner

        result = run_etl_job(etl_request)
        runner.JOB_STORE[job_id_preview] = result

    background_tasks.add_task(_run)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "success": True,
            "message": "Migration queued. Poll /v1/etl/status/{job_id} for progress.",
            "job_id": job_id_preview,
        },
    )
