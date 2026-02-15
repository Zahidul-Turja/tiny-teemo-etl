import os

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.constants import DatabaseType
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import (
    TestConnectionRequest,
    TestConnectionResponse,
    UploadToDBRequest,
    UploadToDBResponse,
)
from app.services.file_processor import FileProcessor

router = APIRouter()


def get_database_connector(connection):
    connectors = {
        DatabaseType.SQLITE: SQLiteConnector,
    }
    connector_class = connectors.get(connection.db_type)
    if not connector_class:
        raise ValueError(f"Unsupported database type: {connection.db_type}")

    return connector_class(connection)


@router.post("/test-connection", response_model=TestConnectionResponse)
async def test_database_connection(request: TestConnectionRequest) -> JSONResponse:
    try:
        connector = get_database_connector(request.connection)
        result = connector.test_connection()

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "content": result,
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": False,
                "message": f"Connection test failed: {str(e)}",
            },
        )


@router.get("/databases")
def get_supported_databases() -> JSONResponse:
    databases = [
        {
            "type": DatabaseType.POSTGRESQL.value,
            "name": "PostgreSQL",
            "default_port": 5432,
            "supports_schema": True,
        },
        {
            "type": DatabaseType.MSSQL.value,
            "name": "MySQL",
            "default_port": 3302,
            "supports_schema": False,
        },
        {
            "type": DatabaseType.SQLITE.value,
            "name": "SQLite",
            "default_port": None,
            "supports_schema": False,
            "note": "File-based database, no host/port required",
        },
    ]

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "success": True,
            "data": databases,
        },
    )


@router.post("/upload", response_model=UploadToDBResponse)
async def upload_to_database(request: UploadToDBRequest) -> JSONResponse:
    file_path = os.path.join(
        settings.UPLOAD_DIR,
        request.field_id,
    )

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )
    try:
        processor = FileProcessor(file_path=file_path)
        df = processor.df

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading to database: {str(e)}",
        )


@router.post("/list-tables")
async def list_tables(request: TestConnectionRequest) -> JSONResponse:

    try:
        connector = get_database_connector(request.connection)

        # ? Need to implement the listing tables
        tables = []

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "tables": tables,
                },
            },
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing tables: {str(e)}",
        )
