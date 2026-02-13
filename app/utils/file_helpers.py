import os
import uuid
from datetime import datetime

from fastapi import UploadFile

from app.core.constants import CHUNK_SIZE


def generate_unique_filename(original_filename: str) -> str:
    """
    Generate a unique filename with timestamp and UUID

    Args:
        original_filename (str): the original name of the file

    Returns:
        str: unique filename
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    name, ext = os.path.splitext(original_filename)

    # Sanitize filename
    name = "".join(c for c in name if c.isalnum() or c in [" ", "-", "_"])
    name = name.strip().replace(" ", "_")

    return f"{timestamp}_{unique_id}_name{ext}"


async def save_upload_file(file: UploadFile, file_path: str) -> None:
    """
    Save uploaded file ot disk as Chunks

    Args:
        file (UploadFile): FastAPI upload file object
        file_path (str): Destination path to save the file

    Returns:
        None: nothing is returned
    """

    with open(file_path, "wb") as buffer:
        while True:
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:
                break
            buffer.write(chunk)


def get_file_size(file_path: str) -> int:
    """
    Get file size in bytes

    Args:
        file_path (str): path to the file

    Returns:
        int: File size in bytes
    """
    return os.path.getsize(filename=file_path)
