import json
import uuid

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from app.models.schemas import (
    DBMigrationRequest,
    DatabaseSource,
    ETLJobRequest,
    TestConnectionRequest,
)
from app.services.db_reader import get_source_schema
from app.worker.job_store import (
    compute_request_hash,
    get_idempotent_job_id,
    get_job,
    save_job,
    set_idempotent_job_id,
)
from app.worker.tasks import run_etl_task
from app.models.schemas import ETLJobResult

router = APIRouter()


# ── helpers ───────────────────────────────────────────────────────────────────


def _migration_to_etl(req: DBMigrationRequest) -> ETLJobRequest:
    return ETLJobRequest(
        db_source=req.source,
        column_mappings=req.column_mappings or [],
        filters=req.filters,
        aggregations=req.aggregations,
        validation_rules=req.validation_rules,
        db_destination=req.db_destination,
        batch_size=req.batch_size,
        max_retries=req.max_retries,
    )


def _new_pending(job_id: str) -> ETLJobResult:
    r = ETLJobResult(
        job_id=job_id,
        success=False,
        message="queued",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )
    save_job(r)
    return r


# ── routes ────────────────────────────────────────────────────────────────────


@router.post("/preview", summary="Inspect source schema before migrating")
async def preview_source_schema(source: DatabaseSource) -> JSONResponse:
    try:
        schema = get_source_schema(source)
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "column_count": len(schema),
                "columns": schema,
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail=f"Could not read source schema: {exc}"
        )


@router.post("/tables", summary="List all tables in a source database")
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
        summary = cls(request.connection).summarize(preview_rows=0)
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "database": summary["database"],
                "tables": summary["list_of_tables"],
                "table_count": len(summary["list_of_tables"]),
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not list tables: {exc}")


@router.post("/run", summary="Run DB migration synchronously")
async def run_migration(request: DBMigrationRequest) -> JSONResponse:
    import asyncio

    try:
        etl_request = _migration_to_etl(request)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    request_dict = json.loads(etl_request.model_dump_json())
    req_hash = compute_request_hash(request_dict)
    existing = get_idempotent_job_id(req_hash)
    if existing:
        cached = get_job(existing)
        if cached and cached.message not in ("queued", "running"):
            return JSONResponse(
                status_code=200 if cached.success else 422,
                content={**cached.model_dump(), "idempotent": True},
            )

    job_id = existing or str(uuid.uuid4())
    set_idempotent_job_id(req_hash, job_id)
    _new_pending(job_id)

    loop = asyncio.get_event_loop()
    result_dict = await loop.run_in_executor(
        None, lambda: run_etl_task(request_dict, job_id)
    )
    result = ETLJobResult.model_validate(result_dict)
    return JSONResponse(
        status_code=200 if result.success else 422,
        content=result.model_dump(),
    )


@router.post("/run-async", status_code=202, summary="Start DB migration in background")
async def run_migration_async(request: DBMigrationRequest) -> JSONResponse:
    try:
        etl_request = _migration_to_etl(request)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    request_dict = json.loads(etl_request.model_dump_json())
    req_hash = compute_request_hash(request_dict)
    existing = get_idempotent_job_id(req_hash)
    if existing:
        cached = get_job(existing)
        if cached:
            return JSONResponse(
                status_code=202,
                content={
                    "success": True,
                    "message": "Duplicate — returning existing job.",
                    "job_id": existing,
                    "status": cached.message,
                    "idempotent": True,
                },
            )

    job_id = str(uuid.uuid4())
    set_idempotent_job_id(req_hash, job_id)
    _new_pending(job_id)

    run_etl_task.apply_async(
        kwargs={"request_dict": request_dict, "job_id": job_id},
        queue="etl.default",
        task_id=job_id,
    )

    return JSONResponse(
        status_code=202,
        content={
            "success": True,
            "message": "Migration queued. Connect to /ws/etl/{job_id} for live progress.",
            "job_id": job_id,
            "ws_url": f"/ws/etl/{job_id}",
        },
    )
