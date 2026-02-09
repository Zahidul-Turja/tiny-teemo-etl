import os
import uuid
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse

from config import settings

router = APIRouter()


UPLOAD_DIR = settings.upload_dir
CHUNK_SIZE = 1024 * 1024  # 1MB
ALLOWED_EXTENSIONS = (".csv", ".xls")

os.makedirs(UPLOAD_DIR, exist_ok=True)


def generate_unique_file_name(original_file_name: str) -> str:
    """
    Given a file name
    Returns an unique file name
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    name, ext = os.path.splitext(original_file_name)
    return f"{timestamp}_{unique_id}_{name}{ext}"


@router.post("/upload_file", name="upload file")
async def upload_file(file: UploadFile = File(...)) -> dict:
    """
    Need to implement:
        1. Progress bar for larger files
    """

    if not file.filename.endswith(ALLOWED_EXTENSIONS):
        return JSONResponse(
            {
                "success": False,
                "message": "Invalid file type",
            },
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
        )

    file_name = generate_unique_file_name(file.filename)
    _, ext = os.path.splitext(file.filename)

    try:
        file_path = os.path.join(UPLOAD_DIR, file_name)

        if ext == ".csv":
            df = pd.read_csv(file.file)
        elif ext == ".xls":
            df = pd.read_excel(file.file)

        # Summary of the file
        columns = []
        for col, dtype in df.dtypes.items():
            data = {
                "title": col,
                "dtype": str(dtype),
                "missing_value_count": int(df[col].isnull().sum()),
                "unique_value_count": int(df[col].nunique()),
            }
            columns.append(data)

        # Write to disk
        with open(file_path, "wb") as buffer:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break

                buffer.write(chunk)

        return JSONResponse(
            {
                "success": True,
                "message": "File uploaded successfully",
                "data": {
                    "file_id": file_name,
                    "table_name": file_name.split("_")[-1],
                    "number_of_rows": df.shape[0],
                    "columns": list(columns),
                    "preview": df.head(5).to_dict(orient="records"),
                },
            },
            status_code=status.HTTP_200_OK,
        )
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error: {str(e)}"
        )
    finally:
        await file.close()


# 20260208_222901_66fa45bc_housing.csv


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
