from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from app.core.constants import (
    AggregationFunction,
    DatabaseType,
    DataType,
    DateFormat,
    DateTimeFormat,
    FilterOperator,
    IfExists,
    ValidationRuleType,
)


# ─────────────────────────────────────────────
#  File Upload
# ─────────────────────────────────────────────
class ColumnInfo(BaseModel):
    name: str
    dtype: str
    missing_value_count: int = 0
    unique_value_count: int = 0
    sample_values: Optional[List[Any]] = None
    suggested_type: Optional[str] = None


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


# ─────────────────────────────────────────────
#  Column Mapping / Transformation
# ─────────────────────────────────────────────
class ColumnMapping(BaseModel):
    column_name: str
    source_dtype: str
    target_dtype: DataType
    rename_to: Optional[str] = None
    prefix: Optional[str] = None
    suffix: Optional[str] = None
    date_format: Optional[DateFormat] = None
    datetime_format: Optional[DateTimeFormat] = None
    max_length: Optional[int] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[Any] = None

    @field_validator("max_length")
    @classmethod
    def validate_max_length(cls, val, info):
        target_dtype = info.data.get("target_dtype")
        if val is not None and target_dtype not in [DataType.STRING, DataType.TEXT]:
            raise ValueError("max_length can only be set for String/Text data types.")
        return val


# ─────────────────────────────────────────────
#  Filtering
# ─────────────────────────────────────────────
class FilterRule(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[Any] = None
    values: Optional[List[Any]] = None  # used by IN / NOT_IN

    @model_validator(mode="after")
    def check_value_presence(self):
        op = self.operator
        needs_list = op in (FilterOperator.IN, FilterOperator.NOT_IN)
        needs_nothing = op in (FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL)

        if needs_list and not self.values:
            raise ValueError(f"Operator '{op}' requires the 'values' list.")
        if not needs_nothing and not needs_list and self.value is None:
            raise ValueError(f"Operator '{op}' requires a 'value'.")
        return self


# ─────────────────────────────────────────────
#  Aggregation
# ─────────────────────────────────────────────
class AggregationRule(BaseModel):
    group_by: List[str]
    aggregations: List[Dict[str, str]]
    # e.g. [{"column": "amount", "function": "sum", "alias": "total_amount"}]


# ─────────────────────────────────────────────
#  Validation Rules
# ─────────────────────────────────────────────
class ValidationRule(BaseModel):
    column: str
    rule_type: ValidationRuleType
    params: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


# ─────────────────────────────────────────────
#  Data Types endpoint
# ─────────────────────────────────────────────
class DataTypeInfo(BaseModel):
    type_id: str
    display_name: str
    description: Optional[str] = None
    requires_format: bool = False
    available_formats: Optional[List[str]] = None


class AvailableDataTypesResponse(BaseModel):
    data_types: List[DataTypeInfo]


# ─────────────────────────────────────────────
#  Database Connection
# ─────────────────────────────────────────────
class DatabaseConnection(BaseModel):
    db_type: DatabaseType
    host: str = "localhost"
    port: Optional[int] = None
    database: str
    username: Optional[str] = None
    password: Optional[str] = None

    @model_validator(mode="after")
    def set_default_port(self):
        """
        Fill in port defaults AFTER all fields are parsed so db_type is
        guaranteed to be present. A field_validator(mode='before') is
        unreliable here because Pydantic v2 may not have validated db_type
        yet when the port validator runs.
        """
        if self.port is None:
            default_ports = {
                DatabaseType.POSTGRESQL: 5432,
                DatabaseType.MYSQL: 3306,
            }
            self.port = default_ports.get(self.db_type)
        return self


class TestConnectionRequest(BaseModel):
    connection: DatabaseConnection


class TestConnectionResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


# ─────────────────────────────────────────────
#  Destination (write target)
# ─────────────────────────────────────────────
class DatabaseDestination(BaseModel):
    connection: DatabaseConnection
    table_name: str
    if_exists: IfExists = IfExists.FAIL
    create_index: bool = False
    index_columns: Optional[List[str]] = None


class FileDestination(BaseModel):
    format: str = Field(pattern="^(csv|excel|parquet)$")
    output_path: str


class APIDestinationAuth(BaseModel):
    type: str = Field(pattern="^(bearer|basic|api_key)$")
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    header_name: Optional[str] = "X-API-Key"
    api_key: Optional[str] = None


class APIDestination(BaseModel):
    url: str
    method: str = Field(default="POST", pattern="^(POST|PUT|PATCH)$")
    auth: Optional[APIDestinationAuth] = None
    batch_size: int = Field(default=100, gt=0, le=10_000)
    # JSON path to wrap records: e.g. "data" → {"data": [...]}
    records_key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None


# ─────────────────────────────────────────────
#  API Source (read from a paginated REST API)
# ─────────────────────────────────────────────
class APISource(BaseModel):
    url: str
    auth: Optional[APIDestinationAuth] = None
    headers: Optional[Dict[str, str]] = None
    # Dot-separated key to drill into JSON response: "results" or "data.items"
    records_key: Optional[str] = None
    # Page-number pagination
    page_param: Optional[str] = "page"
    page_size_param: Optional[str] = "page_size"
    page_size: int = Field(default=100, gt=0)
    start_page: int = 1
    max_pages: Optional[int] = None
    # Cursor/next-URL pagination (takes priority over page_param)
    next_url_key: Optional[str] = None


# ─────────────────────────────────────────────
#  Database Source (read from a DB table/query)
# ─────────────────────────────────────────────
class DatabaseSource(BaseModel):
    connection: DatabaseConnection

    # Exactly one of table_name or query must be set
    table_name: Optional[str] = None
    query: Optional[str] = None

    # Optional column whitelist — if omitted, all columns are extracted
    columns: Optional[List[str]] = None

    # chunk_size=0 means load everything in one shot
    chunk_size: int = Field(
        default=0, ge=0, description="Rows per chunk. 0 = no chunking."
    )

    @model_validator(mode="after")
    def validate_source(self):
        if not self.table_name and not self.query:
            raise ValueError("Either 'table_name' or 'query' must be provided.")
        if self.table_name and self.query:
            raise ValueError("Only one of 'table_name' or 'query' may be provided.")
        return self


# ─────────────────────────────────────────────
#  Full ETL Job request
# ─────────────────────────────────────────────
class ETLJobRequest(BaseModel):
    # Source — exactly one required
    file_id: Optional[str] = None
    api_source: Optional[APISource] = None
    db_source: Optional[DatabaseSource] = None

    # Transformations
    column_mappings: List[ColumnMapping]
    filters: Optional[List[FilterRule]] = None
    aggregations: Optional[AggregationRule] = None
    validation_rules: Optional[List[ValidationRule]] = None

    # Load options
    batch_size: int = Field(default=10_000, gt=0, le=100_000)
    max_retries: int = Field(default=3, ge=0, le=10)

    # Destinations (at least one required)
    db_destination: Optional[DatabaseDestination] = None
    file_destination: Optional[FileDestination] = None
    api_destination: Optional[APIDestination] = None

    @model_validator(mode="after")
    def validate_source_and_destinations(self):
        sources = [
            s for s in [self.file_id, self.api_source, self.db_source] if s is not None
        ]
        if len(sources) == 0:
            raise ValueError(
                "One of 'file_id', 'api_source', or 'db_source' must be provided."
            )
        if len(sources) > 1:
            raise ValueError("Only one source may be provided at a time.")
        if not any([self.db_destination, self.file_destination, self.api_destination]):
            raise ValueError("At least one destination must be specified.")
        return self


# ─────────────────────────────────────────────
#  Upload-to-DB (legacy simple endpoint kept)
# ─────────────────────────────────────────────
class UploadToDBRequest(BaseModel):
    file_id: str
    connection: DatabaseConnection
    table_name: str
    column_mappings: List[ColumnMapping]
    if_exists: IfExists = IfExists.FAIL
    batch_size: int = Field(default=10_000, gt=0, le=100_000)
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


# ─────────────────────────────────────────────
#  ETL Job Result
# ─────────────────────────────────────────────
class ETLJobResult(BaseModel):
    job_id: str
    success: bool
    message: str
    total_rows: int
    processed_rows: int
    failed_rows: int
    invalid_rows_file: Optional[str] = None
    log_file: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


# ─────────────────────────────────────────────
#  DB Migration request (convenience wrapper)
# ─────────────────────────────────────────────
class DBMigrationRequest(BaseModel):
    """
    Migrate data from a source DB to a destination DB.
    All transform/filter/validate features of ETLJobRequest are available.
    column_mappings is optional — omit to migrate columns as-is.
    """

    source: DatabaseSource

    column_mappings: Optional[List[ColumnMapping]] = None
    filters: Optional[List[FilterRule]] = None
    aggregations: Optional[AggregationRule] = None
    validation_rules: Optional[List[ValidationRule]] = None

    db_destination: DatabaseDestination

    batch_size: int = Field(default=10_000, gt=0, le=100_000)
    max_retries: int = Field(default=3, ge=0, le=10)
