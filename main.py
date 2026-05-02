import logging
import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.v1 import router
from app.core.config import settings

# ── logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


# ── startup: ensure required directories exist ────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    for directory in (
        settings.UPLOAD_DIR,
        settings.LOG_DIR,
        settings.INVALID_ROWS_DIR,
    ):
        os.makedirs(directory, exist_ok=True)
        logging.getLogger("startup").info(f"Directory ready: {directory}")
    yield  # server runs here


# ── app ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    lifespan=lifespan,
    title="TinyTeemo — ETL",
    description=(
        "A lightweight ETL system for extracting data from files (CSV, Excel, Parquet), "
        "transforming it (type casting, filtering, validation, aggregation), "
        "and loading it into databases (Postgres, MySQL, SQLite), files, or REST APIs."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── static files ─────────────────────────────────────────────────────────────
import os as _os

if _os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ── routes ────────────────────────────────────────────────────────────────────
app.include_router(router.router, prefix="/v1")


@app.get("/health", status_code=status.HTTP_200_OK, tags=["Health"])
def health_check() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "message": "Teemo is still tiny.",
            "version": "0.1.0",
        },
    )


# ── exception handlers ────────────────────────────────────────────────────────
@app.exception_handler(StarletteHTTPException)
async def general_http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    return await http_exception_handler(request, exc)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return await request_validation_exception_handler(request, exc)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logging.getLogger("app").error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "message": "An unexpected server error occurred.",
            "detail": str(exc),
        },
    )
