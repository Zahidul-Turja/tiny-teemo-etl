from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.core.constants import DatabaseType
from app.database.connectors.sqlite import SQLiteConnector
from app.models.schemas import TestConnectionRequest, TestConnectionResponse

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
