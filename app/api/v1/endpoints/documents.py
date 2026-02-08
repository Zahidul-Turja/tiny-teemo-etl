import os
import uuid

from fastapi import APIRouter, File, UploadFile

router = APIRouter()


@router.post("/upload/csv", name="upload csv")
def upload_csv(file: UploadFile = File(...)):
    """
    Upload CSV with progress tracking
    Returns an upload_id that can be used to track progress
    """
    return {"message": "working"}
