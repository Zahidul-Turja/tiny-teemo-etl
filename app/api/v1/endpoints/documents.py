import os
import uuid
from datetime import datetime

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse

from config import settings

router = APIRouter()


UPLOAD_DIR = settings.upload_dir
CHUNK_SIZE = 1024 * 1024  # 1MB
ALLOWED_EXTENSIONS = (".csv", ".xls")

os.makedirs(UPLOAD_DIR, exist_ok=True)


def generate_unique_file(original_file_name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    name, ext = os.path.splitext(original_file_name)
    return f"{timestamp}_{unique_id}_{name}{ext}"


@router.post("/upload_file", name="upload file")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload CSV with progress tracking
    Returns an upload_id that can be used to track progress
    """

    if not file.filename.endswith(ALLOWED_EXTENSIONS):
        return JSONResponse(
            {
                "success": False,
                "message": "Not allowed",
            },
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
        )

    file_name = generate_unique_file(file.filename)

    try:
        file_path = os.path.join(UPLOAD_DIR, file_name)

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
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error: {str(e)}"
        )
    finally:
        await file.close()
