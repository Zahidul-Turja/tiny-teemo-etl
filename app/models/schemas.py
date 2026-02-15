from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from app.core.constants import DatabaseType, DataType, DateFormat, DateTimeFormat


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


# --------------- Schema Transforming -----------------
class ColumnMapping(BaseModel):
    column_name: str
    source_dtype: str
    target_dtype: DataType
    date_format: Optional[DateFormat] = None
    datetime_format: Optional[DateTimeFormat] = None
    max_length: Optional[str] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[Any] = None

    @field_validator("max_length")
    def validate_max_value(cls, val, info: Dict[str, Any]):
        target_dtype = info.data.get("target_dtype")
        if val is not None and target_dtype not in [DataType.STRING, DataType.TEXT]:
            raise ValueError(f"max_length can only be set for String/Text data types.")
        return val


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
    def set_default_port(cls, val, info: Dict[str, Any]):
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


# ------------------------------ Upload to Database -------------------
class UploadToDBRequest(BaseModel):
    field_id: str
    connection: DatabaseConnection
    table_name: str
    column_mappings: List[ColumnMapping]
    if_exists: str = Field(default="fail", pattern="^(fail|replace|append)$")
    batch_size: int = Field(default=1000, gt=0, le=10000)
    create_index: bool = False
    index_columns: Optional[List[str]] = None


class UploadProgress(BaseModel):
    total_rows: int
    uploaded_rows: int
    failed_rows: int
    progress_percentage: float
    status: str


class UploadToDBResponse(BaseModel):
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
    progress: Optional[UploadProgress] = None
