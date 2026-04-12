from enum import Enum


class DataType(str, Enum):
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


class IfExists(str, Enum):
    FAIL = "fail"
    REPLACE = "replace"
    APPEND = "append"


class FilterOperator(str, Enum):
    EQUALS = "eq"
    NOT_EQUALS = "neq"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_THAN_OR_EQUAL = "gte"
    LESS_THAN_OR_EQUAL = "lte"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IN = "in"
    NOT_IN = "not_in"


class AggregationFunction(str, Enum):
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"


class ValidationRuleType(str, Enum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    REGEX = "regex"
    ALLOWED_VALUES = "allowed_values"
    DATE_FORMAT = "date_format"
    NUMERIC = "numeric"
    EMAIL = "email"


class LogLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


PANDAS_TO_SQL_TYPE_MAP = {
    "int64": DataType.BIGINT,
    "int32": DataType.INTEGER,
    "float64": DataType.FLOAT,
    "float32": DataType.FLOAT,
    "object": DataType.STRING,
    "bool": DataType.BOOLEAN,
    "datetime64[ns]": DataType.TIMESTAMP,
}

# BUG FIX: original was missing leading dot on "xlsx"
ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".parquet"}

MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
CHUNK_SIZE = 1024 * 1024  # 1MB
DEFAULT_BATCH_SIZE = 10_000
MAX_BATCH_SIZE = 100_000
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
