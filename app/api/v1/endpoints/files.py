import os
import uuid
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.constants import ALLOWED_EXTENSIONS, CHUNK_SIZE, MAX_FILE_SIZE
from app.models.schemas import ColumnInfo, FileUploadResponse
from app.services.file_processor import FileProcessor
from app.utils.file_helpers import generate_unique_filename, save_upload_file

router = APIRouter()


UPLOAD_DIR = settings.UPLOAD_DIR

os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)) -> JSONResponse:

    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}",
        )

    file.file.seek(0, 2)  # move to the end of the file
    file_size = file.file.tell()
    file.file.seek(0)  # reset to beginning

    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"File size exceeds maximum allowed size of {MAX_FILE_SIZE / (1024 * 1024)}MB",
        )

    unique_filename = generate_unique_filename(file.filename)
    file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)

    try:
        await save_upload_file(file=file, file_path=file_path)

        processor = FileProcessor(file_path=file_path)
        metadata = processor.get_file_metadata()

        columns = []
        for col_name, col_info in metadata["columns"].items():
            columns.append(
                ColumnInfo(
                    name=col_name,
                    dtype=col_info["dtype"],
                    missing_value_count=col_info["missing_count"],
                    unique_value_count=col_info["unique_count"],
                    sample_values=col_info.get("sample_values", []),
                )
            )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "File uploaded successfully",
                "data": {
                    "file_id": unique_filename,
                    "table_name": metadata["table_name"],
                    "row_count": metadata["row_count"],
                    "columns": [col.model_dump() for col in columns],
                    "preview": metadata["preview"],
                },
            },
        )
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {str(e)}",
        )
    finally:
        await file.close()


@router.get("/list")
async def list_files() -> JSONResponse:

    # ? Need pagination late

    try:
        files = []

        for filename in os.listdir(settings.UPLOAD_DIR):
            name, file_ext = os.path.splitext(filename)
            if file_ext.lower() in ALLOWED_EXTENSIONS:
                file_path = os.path.join(settings.UPLOAD_DIR, filename)
                file_stat = os.stat(file_path)

                creation_timestamp = file_stat.st_birthtime
                creation_date = datetime.fromtimestamp(creation_timestamp)

                files.append(
                    {
                        "file_id": filename,
                        "filename": name,
                        "size": file_stat.st_size,
                        "uploaded_at": creation_date.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "files": files,
                    "total": len(files),
                },
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing files: {str(e)}",
        )


@router.get("/info/{file_id}")
async def get_file_info(file_id: str) -> JSONResponse:

    file_path = os.path.join(settings.UPLOAD_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    try:
        processor = FileProcessor(file_path=file_path)
        metadata = processor.get_file_metadata()

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": metadata,
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading file: {str(e)}",
        )


@router.delete("/{file_id}")
async def delete_file(file_id: str) -> JSONResponse:
    file_path = os.path.join(settings.UPLOAD_DIR, file_id)

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    try:
        os.remove(file_path)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "File deleted successfully",
            },
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting file: {str(e)}",
        )
