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


# @router.post("/upload_file", name="upload file")
# async def upload_file(file: UploadFile = File(...)) -> dict:
#     """
#     Need to implement:
#         1. Progress bar for larger files
#     """

#     if not file.filename.endswith(ALLOWED_EXTENSIONS):
#         return JSONResponse(
#             {
#                 "success": False,
#                 "message": "Invalid file type",
#             },
#             status_code=status.HTTP_406_NOT_ACCEPTABLE,
#         )

#     file_name = generate_unique_filename(file.filename)
#     name, ext = os.path.splitext(file.filename)

#     try:
#         file_path = os.path.join(UPLOAD_DIR, file_name)

#         if ext == ".csv":
#             df = pd.read_csv(file.file)
#         elif ext == ".xls":
#             df = pd.read_excel(file.file)

#         # Summary of the file
#         columns = []
#         for col, dtype in df.dtypes.items():
#             data = {
#                 "title": col,
#                 "dtype": str(dtype),
#                 "missing_value_count": int(df[col].isnull().sum()),
#                 "unique_value_count": int(df[col].nunique()),
#             }
#             columns.append(data)

#         # Write to disk
#         with open(file_path, "wb") as buffer:
#             while True:
#                 chunk = await file.read(CHUNK_SIZE)
#                 if not chunk:
#                     break

#                 buffer.write(chunk)

#         return JSONResponse(
#             {
#                 "success": True,
#                 "message": "File uploaded successfully",
#                 "data": {
#                     "file_id": file_name,
#                     "table_name": name,
#                     "number_of_rows": df.shape[0],
#                     "columns": list(columns),
#                     "preview": df.head(5).to_dict(orient="records"),
#                 },
#             },
#             status_code=status.HTTP_200_OK,
#         )
#     except Exception as e:
#         if os.path.exists(file_path):
#             os.remove(file_path)

#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error processing file: {str(e)}",
#         )
#     finally:
#         await file.close()


# 20260208_222901_66fa45bc_housing.csv
# 20260212_191352_9e74ab56_Chocolate Sales (2).csv


@router.get("/read_file/{file_id}")
async def read_uploaded_file(
    file_id: str,
) -> dict:
    try:
        name, ext = os.path.splitext(file_id)
        if ext[0:] not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                detail="Invalid file id",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
    except Exception as e:
        raise HTTPException(
            detail="Invalid file id",
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    try:
        if ext == ".csv":
            df = pd.read_csv(os.path.join(UPLOAD_DIR, file_id))
        elif ext == ".xls":
            df = pd.read_excel(os.path.join(UPLOAD_DIR, file_id))

        missing_value_count = {}
        for key, val in df.isna().sum().items():
            if val > 0:
                missing_value_count[key] = val

        return {
            "table_name": name.split("_")[-1],
            "columns": list(df.columns),
            "missing_value_count": missing_value_count,
        }
    except Exception as e:
        raise HTTPException(
            detail=f"Error: {str(e)}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
