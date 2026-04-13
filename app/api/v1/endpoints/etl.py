import os

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse

from app.core.config import settings
from app.models.schemas import ETLJobRequest, ETLJobResult
from app.services.etl_logger import read_log_file
from app.services.etl_runner import JOB_STORE, get_job_status, list_jobs, run_etl_job

router = APIRouter()


@router.post(
    "/run",
    response_model=ETLJobResult,
    status_code=status.HTTP_200_OK,
    summary="Run a full ETL job synchronously",
    description=(
        "Runs the complete pipeline and waits for the result. "
        "Good for small-to-medium files. For large files, use **/run-async**."
    ),
)
async def run_job(request: ETLJobRequest) -> JSONResponse:
    result = run_etl_job(request)
    http_status = (
        status.HTTP_200_OK if result.success else status.HTTP_422_UNPROCESSABLE_ENTITY
    )
    return JSONResponse(status_code=http_status, content=result.model_dump())


@router.post(
    "/run-async",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start an ETL job in the background",
    description=(
        "Accepts the job immediately (HTTP 202) and runs it in a background task. "
        "Poll **/etl/status/{job_id}** to check progress."
    ),
)
async def run_job_async(
    request: ETLJobRequest,
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    import uuid

    # Pre-register a job_id so the client can start polling before the worker starts
    job_id_preview = str(uuid.uuid4())

    def _run():
        # Override the uuid so it matches what we returned to the client
        import app.services.etl_runner as runner
        from app.services.etl_runner import ETLJobResult

        runner.JOB_STORE[job_id_preview] = ETLJobResult(
            job_id=job_id_preview,
            success=False,
            message="running",
            total_rows=0,
            processed_rows=0,
            failed_rows=0,
        )
        result = run_etl_job(request)
        # run_etl_job generates its own uuid internally, so copy to our reserved slot
        runner.JOB_STORE[job_id_preview] = result

    background_tasks.add_task(_run)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "success": True,
            "message": "ETL job queued. Poll /etl/status/{job_id} for progress.",
            "job_id": job_id_preview,
        },
    )


@router.get(
    "/status/{job_id}",
    response_model=ETLJobResult,
    summary="Get the status / result of an ETL job",
)
async def get_job(job_id: str) -> JSONResponse:
    result = get_job_status(job_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No job found with id '{job_id}'.",
        )
    http_status = (
        status.HTTP_200_OK if result.message != "running" else status.HTTP_202_ACCEPTED
    )
    return JSONResponse(status_code=http_status, content=result.model_dump())


@router.get(
    "/jobs",
    summary="List all ETL jobs (current server session)",
)
async def get_all_jobs() -> JSONResponse:
    jobs = list_jobs()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "total": len(jobs),
            "jobs": [j.model_dump() for j in jobs],
        },
    )


@router.get(
    "/logs/{job_id}",
    summary="Retrieve structured log events for a past ETL job",
)
async def get_job_logs(job_id: str) -> JSONResponse:
    events = read_log_file(job_id)
    if not events:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No log found for job '{job_id}'.",
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "job_id": job_id,
            "event_count": len(events),
            "events": events,
        },
    )


@router.get(
    "/invalid-rows/{job_id}",
    summary="Download the CSV of invalid rows for a past ETL job",
)
async def download_invalid_rows(job_id: str) -> FileResponse:
    path = os.path.join(settings.INVALID_ROWS_DIR, f"{job_id}_invalid.csv")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No invalid-rows file found for job '{job_id}'.",
        )
    return FileResponse(
        path=path,
        media_type="text/csv",
        filename=f"{job_id}_invalid_rows.csv",
    )
