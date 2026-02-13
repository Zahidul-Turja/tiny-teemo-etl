from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from app.core.constants import DatabaseType


# -------- File Uploads ---------
class ColumnInfo(BaseModel):
    name: str
    dtype: str
    missing_count: int = Field(alias="missing_value_count")
    unique_count: int = Field(alias="unique_value_count")
    sample_values: Optional[List[Any]] = None


class FileUploadResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class FileMetadata(BaseModel):
    file_id: str
    table_name: str
    row_count: int
    columns: List[ColumnInfo]
    preview: List[Dict[str, Any]]


# ========= Date Types =========
class DataTypeInfo(BaseModel):
    type_id: str
    display_name: str
    description: Optional[str] = None
    requires_forma: bool = False
    available_formats: Optional[List[str]] = None


class AvailableDataTypesResponse(BaseModel):
    data_types: List[DataTypeInfo]


# ================= Database Connections =======================
class DatabaseConnection(BaseModel):
    db_type: DatabaseType
    host: str = "localhost"
    port: Optional[int] = None
    database: str
    username: str
    password: str

    @field_validator("port")
    def set_default_port(cls, val: str, info: Dict[str, Any]):
        if val is None:
            db_type = info.data.get("db_type")
            default_ports = {
                DatabaseType.POSTGRESQL: 5432,
                DatabaseType.MSSQL: 3306,
                DatabaseType.MSSQL: 1433,
            }
            return default_ports.get(db_type)
        return val


class TestConnectionRequest(BaseModel):
    connection: DatabaseConnection


class TestConnectionResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
