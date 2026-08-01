import os
import uuid
from datetime import datetime

from fastapi import UploadFile

from app.core.constants import CHUNK_SIZE


def generate_unique_filename(original_filename: str) -> str:
    """
    Generate a unique filename: {timestamp}_{uuid}{ext}
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:18]
    _, ext = os.path.splitext(original_filename)

    return f"{timestamp}_{unique_id}{ext}"


async def save_upload_file(file: UploadFile, file_path: str) -> None:
    """Save an uploaded file to disk in chunks to avoid memory spikes."""
    with open(file_path, "wb") as buffer:
        while True:
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:
                break
            buffer.write(chunk)


def get_file_size(file_path: str) -> int:
    return os.path.getsize(file_path)
