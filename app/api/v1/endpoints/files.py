import os
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.constants import ALLOWED_EXTENSIONS, MAX_FILE_SIZE
from app.models.schemas import ColumnInfo, FileUploadResponse
from app.services.file_processor import FileProcessor
from app.utils.file_helpers import generate_unique_filename, save_upload_file

router = APIRouter()

os.makedirs(settings.UPLOAD_DIR, exist_ok=True)


@router.post("/upload", response_model=FileUploadResponse, summary="Upload a data file")
async def upload_file(file: UploadFile = File(...)) -> JSONResponse:
    file_ext = os.path.splitext(file.filename or "")[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file type '{file_ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    # Check size before full read
    file.file.seek(0, 2)
    file_size = file.file.tell()
    file.file.seek(0)

    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the {MAX_FILE_SIZE // (1024 * 1024)} MB limit.",
        )

    unique_filename = generate_unique_filename(file.filename or "upload")
    file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)

    try:
        await save_upload_file(file=file, file_path=file_path)

        processor = FileProcessor(file_path=file_path)
        metadata = processor.get_file_metadata()

        columns = [
            ColumnInfo(
                name=col_name,
                dtype=col_info["dtype"],
                missing_value_count=col_info["missing_count"],
                unique_value_count=col_info["unique_count"],
                sample_values=col_info.get("sample_values", []),
                suggested_type=col_info.get("suggested_type"),
            )
            for col_name, col_info in metadata["columns"].items()
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "File uploaded successfully.",
                "data": {
                    "file_id": unique_filename,
                    "table_name": metadata["table_name"],
                    "row_count": metadata["row_count"],
                    "column_count": metadata["column_count"],
                    "has_missing_values": metadata["has_missing_values"],
                    "columns": [col.model_dump() for col in columns],
                    "preview": metadata["preview"],
                },
            },
        )
    except Exception as exc:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {exc}",
        )
    finally:
        await file.close()


@router.get("/list", summary="List all uploaded files")
async def list_files() -> JSONResponse:
    try:
        files = []
        for filename in sorted(os.listdir(settings.UPLOAD_DIR)):
            _, ext = os.path.splitext(filename)
            if ext.lower() not in ALLOWED_EXTENSIONS:
                continue
            file_path = os.path.join(settings.UPLOAD_DIR, filename)
            stat = os.stat(file_path)
            files.append(
                {
                    "file_id": filename,
                    "filename": os.path.splitext(filename)[0],
                    "extension": ext,
                    "size_bytes": stat.st_size,
                    "uploaded_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                }
            )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": True, "data": {"files": files, "total": len(files)}},
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing files: {exc}",
        )


@router.get("/info/{file_id}", summary="Get metadata for a specific file")
async def get_file_info(file_id: str) -> JSONResponse:
    file_path = os.path.join(settings.UPLOAD_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found."
        )

    try:
        metadata = FileProcessor(file_path).get_file_metadata()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": True, "data": metadata},
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading file: {exc}",
        )


@router.get(
    "/column-stats/{file_id}/{column_name}",
    summary="Get statistics for a single column",
)
async def get_column_stats(file_id: str, column_name: str) -> JSONResponse:
    file_path = os.path.join(settings.UPLOAD_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found."
        )

    try:
        stats = FileProcessor(file_path).get_column_stats(column_name)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": True, "data": stats},
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error computing stats: {exc}",
        )


@router.delete("/{file_id}", summary="Delete an uploaded file")
async def delete_file(file_id: str) -> JSONResponse:
    file_path = os.path.join(settings.UPLOAD_DIR, file_id)
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found."
        )

    try:
        os.remove(file_path)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": True, "message": "File deleted successfully."},
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting file: {exc}",
        )
