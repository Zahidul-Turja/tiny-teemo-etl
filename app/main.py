from api.v1 import router
from fastapi import FastAPI, Request, status
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from starlette.exceptions import HTTPException as StarletteHTTPException

app = FastAPI(
    title="TinyTeemo - ETL",
    description="Basic ETL (Extract, Transform, Load) system for data migration",
    version="0.0.1",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router.router, prefix="/v1")


@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"message": "Server healthy"}


@app.exception_handler(StarletteHTTPException)
async def general_http_exception_handler(
    request: Request, exception: StarletteHTTPException
):
    return await http_exception_handler(request, exception)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exception: RequestValidationError
):
    return await request_validation_exception_handler(
        request,
        exception,
    )


# import asyncio
# import os
# import uuid
# from datetime import datetime
# from typing import Dict, Optional

# # ---------------------------------------------------------
# from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
# from fastapi.middleware.cors import CORSMiddleware

# app = FastAPI()

# # Enable CORS for frontend communication
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # In-memory progress tracking
# # In production, use Redis or a database
# upload_progress: Dict[str, dict] = {}

# UPLOAD_DIR = "uploaded_files"
# os.makedirs(UPLOAD_DIR, exist_ok=True)


# @app.post("/upload/csv-with-progress")
# async def upload_csv_with_progress(
#     file: UploadFile = File(...),
#     save_permanently: bool = Form(False),
#     description: Optional[str] = Form(None),
# ):
#     """
#     Upload CSV with progress tracking
#     Returns an upload_id that can be used to track progress
#     """
#     # Generate unique upload ID
#     upload_id = str(uuid.uuid4())

#     # Initialize progress tracking
#     upload_progress[upload_id] = {
#         "status": "uploading",
#         "progress": 0,
#         "bytes_uploaded": 0,
#         "total_bytes": 0,
#         "filename": file.filename,
#         "message": "Starting upload...",
#     }

#     try:
#         # Determine file path
#         unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{upload_id[:8]}_{file.filename}"
#         file_path = os.path.join(UPLOAD_DIR, unique_filename)

#         # Get content length if available (from headers)
#         content_length = 0
#         if hasattr(file, "size") and file.size:
#             content_length = file.size

#         upload_progress[upload_id]["total_bytes"] = content_length

#         # Stream upload with progress tracking
#         bytes_uploaded = 0
#         chunk_size = 1024 * 1024  # 1MB chunks

#         with open(file_path, "wb") as buffer:
#             while True:
#                 chunk = await file.read(chunk_size)
#                 if not chunk:
#                     break

#                 buffer.write(chunk)
#                 bytes_uploaded += len(chunk)

#                 # Update progress
#                 if content_length > 0:
#                     progress = (bytes_uploaded / content_length) * 100
#                 else:
#                     # If content-length not available, just track bytes
#                     progress = 0

#                 upload_progress[upload_id].update(
#                     {
#                         "progress": min(
#                             progress, 99
#                         ),  # Keep at 99% until processing done
#                         "bytes_uploaded": bytes_uploaded,
#                         "message": f"Uploading... {bytes_uploaded / (1024*1024):.2f} MB",
#                     }
#                 )

#                 # Small delay to allow progress checks
#                 await asyncio.sleep(0.01)

#         # Update to processing status
#         upload_progress[upload_id].update(
#             {"status": "processing", "progress": 99, "message": "Processing file..."}
#         )

#         # Process the file (count rows, get metadata)
#         import pandas as pd

#         df_preview = pd.read_csv(file_path, nrows=5)
#         total_rows = sum(1 for _ in open(file_path)) - 1

#         file_size = os.path.getsize(file_path)

#         # Complete
#         upload_progress[upload_id].update(
#             {
#                 "status": "completed",
#                 "progress": 100,
#                 "message": "Upload complete!",
#                 "result": {
#                     "upload_id": upload_id,
#                     "original_filename": file.filename,
#                     "stored_filename": unique_filename,
#                     "file_path": file_path,
#                     "size_mb": round(file_size / (1024 * 1024), 2),
#                     "total_rows": total_rows,
#                     "columns": list(df_preview.columns),
#                     "column_count": len(df_preview.columns),
#                     "uploaded_at": datetime.now().isoformat(),
#                 },
#             }
#         )

#         return {
#             "upload_id": upload_id,
#             "status": "success",
#             "message": "File uploaded successfully",
#         }

#     except Exception as e:
#         upload_progress[upload_id].update(
#             {"status": "failed", "progress": 0, "message": f"Upload failed: {str(e)}"}
#         )

#         # Clean up file if it exists
#         if os.path.exists(file_path):
#             os.remove(file_path)

#         raise HTTPException(500, f"Error: {str(e)}")

#     finally:
#         await file.close()


# @app.get("/upload/progress/{upload_id}")
# async def get_upload_progress(upload_id: str):
#     """
#     Get current progress of an upload
#     Frontend polls this endpoint every second
#     """
#     if upload_id not in upload_progress:
#         raise HTTPException(404, "Upload not found")

#     return upload_progress[upload_id]


# @app.delete("/upload/progress/{upload_id}")
# async def clear_upload_progress(upload_id: str):
#     """
#     Clear progress data after upload completes
#     Good practice to prevent memory leaks
#     """
#     if upload_id in upload_progress:
#         del upload_progress[upload_id]
#         return {"status": "success", "message": "Progress data cleared"}

#     raise HTTPException(404, "Upload not found")
