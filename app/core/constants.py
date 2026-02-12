from enum import Enum


class DataType(str, Enum):
    """
    Supported data types for schema mapping (will move to database later)
    """

    INTEGER = "integer"
    BIGINT = "bigint"
    FLOAT = "float"
    DECIMAL = "decimal"
    STRING = "string"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    JSON = "json"


class DateFormat(str, Enum):
    YYYY_MM_DD = "YYYY-MM-DD"
    YY_MM_DD = "YY-MM-DD"
    DD_MM_YYYY = "DD-MM-YYYY"
    DD_MM_YY = "DD-MM-YY"

    YYYY_MM_DD_SLASH = "YYYY/MM/DD"
    YY_MM_DD_SLASH = "YY/MM/DD"
    DD_MM_YYYY_SLASH = "DD/MM/YYYY"
    DD_MM_YY_SLASH = "DD/MM/YY"

    MMM_DD_YYYY = "MMM DD, YYYY"
    MMMM_DD_YYYY = "MMMM DD, YYYY"


class DateTimeFormat(str, Enum):
    YYYY_MM_DD_HH_MM_SS = "YYYY-MM-DD HH:MM:SS"
    DD_MM_YYYY_HH_MM_SS = "DD-MM-YYYY HH:MM:SS"
    MM_DD_YYYY_HH_MM_SS = "MM-DD-YYYY HH:MM:SS"
    YYYY_MM_DD_T_HH_MM_SS = "YYYY-MM-DDTHH:MM:SS"
    ISO8601 = "ISO8601"


class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MSSQL = "mssql"


PANDAS_TO_SQL_TYPE_MAP = {
    "int64": DataType.BIGINT,
    "int32": DataType.INTEGER,
    "float64": DataType.FLOAT,
    "float32": DataType.FLOAT,
    "object": DataType.STRING,
    "bool": DataType.BOOLEAN,
    "datetime64[ns]": DataType.TIMESTAMP,
}


ALLOWED_EXTENSIONS = {".csv", ".xls", "xlsx"}

MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
CHUNK_SIZE = 1024 * 1024  # 1MB chunked read
