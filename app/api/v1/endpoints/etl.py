import asyncio
import json
import os
import uuid

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, JSONResponse

from app.core.config import settings
from app.models.schemas import ETLJobRequest, ETLJobResult
from app.services.etl_logger import read_log_file
from app.worker.job_store import (
    compute_request_hash,
    get_idempotent_job_id,
    get_job,
    list_jobs,
    save_job,
    set_idempotent_job_id,
    subscribe_to_job,
)
from app.worker.tasks import run_etl_task

router = APIRouter()


# ── helpers ───────────────────────────────────────────────────────────────────


def _new_pending_job(job_id: str) -> ETLJobResult:
    result = ETLJobResult(
        job_id=job_id,
        success=False,
        message="queued",
        total_rows=0,
        processed_rows=0,
        failed_rows=0,
    )
    save_job(result)
    return result


def _dispatch(request: ETLJobRequest, job_id: str) -> None:
    """Send job to Celery worker queue."""
    request_dict = json.loads(request.model_dump_json())
    run_etl_task.apply_async(
        kwargs={"request_dict": request_dict, "job_id": job_id},
        queue="etl.default",
        task_id=job_id,
    )


# ── endpoints ─────────────────────────────────────────────────────────────────


@router.post(
    "/run",
    response_model=ETLJobResult,
    status_code=status.HTTP_200_OK,
    summary="Run ETL job synchronously (small datasets)",
    description=(
        "Blocks until the job completes. For large datasets use /run-async "
        "and stream progress via /ws/etl/{job_id}."
    ),
)
async def run_job(request: ETLJobRequest) -> JSONResponse:
    request_dict = json.loads(request.model_dump_json())
    req_hash = compute_request_hash(request_dict)

    existing_job_id = get_idempotent_job_id(req_hash)
    if existing_job_id:
        cached = get_job(existing_job_id)
        if cached and cached.message not in ("queued", "running"):
            return JSONResponse(
                status_code=(
                    status.HTTP_200_OK
                    if cached.success
                    else status.HTTP_422_UNPROCESSABLE_ENTITY
                ),
                content={**cached.model_dump(), "idempotent": True},
            )

    job_id = existing_job_id or str(uuid.uuid4())
    set_idempotent_job_id(req_hash, job_id)
    _new_pending_job(job_id)

    loop = asyncio.get_event_loop()
    result_dict = await loop.run_in_executor(
        None,
        lambda: run_etl_task(request_dict, job_id),
    )
    result = ETLJobResult.model_validate(result_dict)
    http_status = (
        status.HTTP_200_OK if result.success else status.HTTP_422_UNPROCESSABLE_ENTITY
    )
    return JSONResponse(status_code=http_status, content=result.model_dump())


@router.post(
    "/run-async",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start ETL job in background — returns immediately",
    description=(
        "Accepts the job immediately (HTTP 202). "
        "Connect to /ws/etl/{job_id} for live progress, "
        "or poll /etl/status/{job_id}."
    ),
)
async def run_job_async(request: ETLJobRequest) -> JSONResponse:
    request_dict = json.loads(request.model_dump_json())
    req_hash = compute_request_hash(request_dict)

    existing_job_id = get_idempotent_job_id(req_hash)
    if existing_job_id:
        cached = get_job(existing_job_id)
        if cached:
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "success": True,
                    "message": "Duplicate request — returning existing job.",
                    "job_id": existing_job_id,
                    "status": cached.message,
                    "idempotent": True,
                },
            )

    job_id = str(uuid.uuid4())
    set_idempotent_job_id(req_hash, job_id)
    _new_pending_job(job_id)
    _dispatch(request, job_id)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "success": True,
            "message": "ETL job queued. Connect to /ws/etl/{job_id} for live progress.",
            "job_id": job_id,
            "ws_url": f"/ws/etl/{job_id}",
        },
    )


@router.get(
    "/status/{job_id}",
    response_model=ETLJobResult,
    summary="Poll job status / result",
)
async def get_job_status(job_id: str) -> JSONResponse:
    result = get_job(job_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No job found with id '{job_id}'.",
        )
    http_status = (
        status.HTTP_202_ACCEPTED
        if result.message in ("queued", "running")
        else status.HTTP_200_OK
    )
    return JSONResponse(status_code=http_status, content=result.model_dump())


@router.get("/jobs", summary="List all ETL jobs")
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


@router.get("/logs/{job_id}", summary="Retrieve structured log events for a job")
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


@router.get("/invalid-rows/{job_id}", summary="Download invalid-rows CSV for a job")
async def download_invalid_rows(job_id: str) -> FileResponse:
    path = os.path.join(settings.INVALID_ROWS_DIR, f"{job_id}_invalid.csv")
    if not os.path.exists(path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No invalid-rows file found for job '{job_id}'.",
        )
    return FileResponse(
        path=path, media_type="text/csv", filename=f"{job_id}_invalid_rows.csv"
    )


# ── WebSocket ─────────────────────────────────────────────────────────────────


@router.websocket("/ws/etl/{job_id}")
async def etl_progress_ws(websocket: WebSocket, job_id: str):
    """
    Stream live ETL progress to the client.

    The Celery task publishes JSON events to Redis channel `etl:<job_id>`.
    This handler subscribes and forwards each event to the WebSocket client.

    Event shape:
        {
            "job_id":   "...",
            "stage":    "extract|filter|transform|validate|aggregate|load|done|failed",
            "progress": 0-100,
            "message":  "...",
            // optional: total_rows, processed_rows, failed_rows
        }
    Connection closes automatically on stage "done" or "failed".
    """
    await websocket.accept()

    current = get_job(job_id)
    if not current:
        await websocket.send_json({"error": f"Job '{job_id}' not found."})
        await websocket.close(code=1008)
        return

    # Send snapshot of current state immediately
    await websocket.send_json(
        {
            "job_id": job_id,
            "stage": "current_status",
            "progress": 100 if current.message not in ("queued", "running") else 0,
            "message": current.message,
        }
    )

    if current.message not in ("queued", "running"):
        await websocket.send_json(
            {
                "job_id": job_id,
                "stage": "done" if current.success else "failed",
                "result": current.model_dump(),
            }
        )
        await websocket.close()
        return

    loop = asyncio.get_event_loop()
    pubsub = await loop.run_in_executor(None, subscribe_to_job, job_id)

    try:
        while True:
            message = await loop.run_in_executor(
                None, lambda: pubsub.get_message(timeout=0.1)
            )
            if message and message.get("type") == "message":
                try:
                    event = json.loads(message["data"])
                    await websocket.send_json(event)
                    if event.get("stage") in ("done", "failed"):
                        final = get_job(job_id)
                        if final:
                            await websocket.send_json(
                                {
                                    "job_id": job_id,
                                    "stage": event["stage"],
                                    "result": final.model_dump(),
                                }
                            )
                        break
                except (json.JSONDecodeError, RuntimeError):
                    break
            else:
                await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
    finally:
        await loop.run_in_executor(None, pubsub.close)
        try:
            await websocket.close()
        except Exception:
            pass
