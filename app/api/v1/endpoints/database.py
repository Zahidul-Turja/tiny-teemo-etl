import os

from fastapi import APIRouter, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


from app.core.constants import DatabaseType
from app.database.connectors.mysql import MySQLConnector
from app.database.connectors.postgres import PostgresConnector
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import (
    TestConnectionRequest,
    TestConnectionResponse,
    UploadToDBRequest,
    UploadToDBResponse,
)
from app.services.file_processor import FileProcessor
from app.services.schema_mapper import SchemaMapper
from app.core.config import settings

router = APIRouter()


def _get_connector(connection):
    mapping = {
        DatabaseType.POSTGRESQL: PostgresConnector,
        DatabaseType.MYSQL: MySQLConnector,
        DatabaseType.SQLITE: SQLiteConnector,
    }
    cls = mapping.get(connection.db_type)
    if not cls:
        raise ValueError(f"Unsupported database type: {connection.db_type}")
    return cls(connection)


@router.post(
    "/test-connection",
    response_model=TestConnectionResponse,
    summary="Test a database connection",
)
async def test_database_connection(request: TestConnectionRequest) -> JSONResponse:
    try:
        connector = _get_connector(request.connection)
        result = connector.test_connection()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": result.get("success", False), "content": result},
        )
    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"success": False, "message": f"Connection test failed: {exc}"},
        )


@router.get("/supported-types", summary="List supported database types")
def get_supported_databases() -> JSONResponse:
    databases = [
        {
            "type": DatabaseType.POSTGRESQL.value,
            "name": "PostgreSQL",
            "default_port": 5432,
            "supports_schema": True,
        },
        {
            "type": DatabaseType.MYSQL.value,
            "name": "MySQL",
            "default_port": 3306,
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
        content={"success": True, "data": databases},
    )


@router.post(
    "/upload",
    response_model=UploadToDBResponse,
    summary="Upload file data into a database table",
)
async def upload_to_database(request: UploadToDBRequest) -> JSONResponse:
    file_path = os.path.join(settings.UPLOAD_DIR, request.file_id)

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File not found."
        )

    try:
        processor = FileProcessor(file_path=file_path)
        df = processor.df

        mapper = SchemaMapper(df)
        transformed_df = mapper.apply_column_mapping(request.column_mappings)

        if mapper.transformation_errors:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={
                    "success": False,
                    "message": "Schema transformation failed.",
                    "data": {"errors": mapper.transformation_errors},
                },
            )

        connector = _get_connector(request.connection)
        result = connector.upload_dataframe(
            df=transformed_df,
            table_name=request.table_name,
            column_mappings=request.column_mappings,
            if_exists=request.if_exists.value,
            batch_size=request.batch_size,
        )

        if request.create_index and request.index_columns:
            try:
                with _get_connector(request.connection) as conn:
                    conn.create_index(
                        table_name=request.table_name,
                        columns=request.index_columns,
                    )
            except Exception as exc:
                # Non-fatal: log but don't fail the whole upload
                result["index_warning"] = str(exc)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": result["message"],
                "data": {
                    "table_name": result["table_name"],
                    "rows_inserted": result.get("rows_inserted", 0),
                    "rows_failed": result.get("rows_failed", 0),
                },
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error uploading to database: {exc}",
        )


@router.post("/summary", summary="Get a full database summary with table previews")
async def database_summary(request: TestConnectionRequest) -> JSONResponse:
    try:
        connector = _get_connector(request.connection)
        data = connector.summarize()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=jsonable_encoder(
                {
                    "success": True,
                    "message": "Data fetched successfully.",
                    "data": data,
                }
            ),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching database summary balaalal: {exc}",
        )


@router.post("/table-exists/{table_name}", summary="Check whether a table exists")
async def check_table_exists(
    request: TestConnectionRequest, table_name: str
) -> JSONResponse:
    try:
        connector = _get_connector(request.connection)
        with connector:
            exists = connector.table_exists(table_name)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Table exists." if exists else "Table does not exist.",
                "data": {"exists": exists},
            },
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking table: {exc}",
        )
