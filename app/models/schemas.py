from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


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
