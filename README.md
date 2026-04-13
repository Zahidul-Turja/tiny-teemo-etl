# TinyTeemo — ETL

A lightweight, self-contained ETL system built with **FastAPI** and **pandas**.  
Upload a file (or pull from a REST API), transform it, validate it, and push it to a database, another file, or an external API — with retry logic, batch inserts, structured logs, and an invalid-rows report for every run.

---

## Features

| Category            | What's supported                                                                                                                         |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Sources**         | CSV, Excel (`.xls`/`.xlsx`), Parquet, paginated REST API                                                                                 |
| **Transform**       | Type casting, column rename, prefix/suffix, `$`/`,` stripping for numbers                                                                |
| **Filter**          | `eq`, `neq`, `gt`, `lt`, `gte`, `lte`, `contains`, `not_contains`, `is_null`, `is_not_null`, `in`, `not_in`                              |
| **Validate**        | `not_null`, `unique`, `min_value`, `max_value`, `min_length`, `max_length`, `regex`, `allowed_values`, `date_format`, `numeric`, `email` |
| **Aggregate**       | `sum`, `count`, `avg`, `min`, `max`, `count_distinct` with group-by                                                                      |
| **Destinations**    | PostgreSQL, MySQL, SQLite, CSV, Excel, Parquet, REST API (POST/PUT/PATCH)                                                                |
| **Auth (API dest)** | Bearer token, HTTP Basic, API-key header                                                                                                 |
| **Reliability**     | Configurable retry with backoff, batch inserts (up to 100 k rows/batch), partial success                                                 |
| **Observability**   | Per-job JSON-lines log file, invalid-rows CSV with error reasons, job status polling                                                     |

---

## Quick start

```bash
# 1. Clone / copy the project
cd tinyteemo

# 2. Copy the env template and fill in your values
cp .env.example .env

# 3. Install dependencies (uv recommended)
uv sync

# 4. Run in development mode (auto-reload)
uv run dev
# or: uvicorn main:app --reload

# 5. Open the interactive docs
open http://localhost:8000/docs
```

### Running tests

```bash
uv run test
# or: pytest tests/ -v
```

---

## Project layout

```
tinyteemo/
├── main.py                          # FastAPI app, lifespan, exception handlers
├── pyproject.toml                   # deps + uv run scripts
├── .env.example                     # configuration template
│
├── app/
│   ├── api/v1/
│   │   ├── router.py                # mounts all endpoint groups
│   │   └── endpoints/
│   │       ├── files.py             # upload, list, info, column-stats, delete
│   │       ├── database.py          # test-connection, upload, summary, table-exists
│   │       ├── utilities.py         # data-types, date-formats, datetime-formats
│   │       └── etl.py               # run, run-async, status, jobs, logs, invalid-rows
│   │
│   ├── core/
│   │   ├── config.py                # Settings (pydantic-settings, reads .env)
│   │   └── constants.py             # all enums and constants
│   │
│   ├── database/connectors/
│   │   ├── base.py                  # abstract base + upload_dataframe()
│   │   ├── sqlite.py
│   │   ├── postgres.py
│   │   └── mysql.py
│   │
│   ├── models/
│   │   └── schemas.py               # all Pydantic models
│   │
│   └── services/
│       ├── file_processor.py        # read CSV/Excel/Parquet, metadata, stats
│       ├── schema_mapper.py         # SchemaMapper, RowFilter, Aggregator, DataValidator
│       ├── etl_runner.py            # full pipeline orchestration + job store
│       ├── etl_logger.py            # structured JSON-lines logging per job
│       ├── file_writer.py           # write DataFrame → CSV / Excel / Parquet
│       └── api_writer.py            # write DataFrame → REST API with auth + retry
│
└── tests/
    ├── conftest.py                  # shared fixtures
    ├── test_file_processor.py       # 20 tests
    ├── test_transformations.py      # 40 tests
    ├── test_connectors.py           # 15 tests
    └── test_etl.py                  # 24 tests (pipeline + API endpoints)
```

---

## API overview

All routes are prefixed with `/v1`. Interactive docs at `/docs`.

### Files

| Method   | Path                                        | Description                              |
| -------- | ------------------------------------------- | ---------------------------------------- |
| `POST`   | `/v1/files/upload`                          | Upload a CSV / Excel / Parquet file      |
| `GET`    | `/v1/files/list`                            | List all uploaded files                  |
| `GET`    | `/v1/files/info/{file_id}`                  | Metadata + column suggestions for a file |
| `GET`    | `/v1/files/column-stats/{file_id}/{column}` | Detailed stats for one column            |
| `DELETE` | `/v1/files/{file_id}`                       | Delete an uploaded file                  |

### ETL Jobs

| Method | Path                            | Description                                                  |
| ------ | ------------------------------- | ------------------------------------------------------------ |
| `POST` | `/v1/etl/run`                   | Run a full ETL job (synchronous)                             |
| `POST` | `/v1/etl/run-async`             | Queue a job in the background (returns `job_id` immediately) |
| `GET`  | `/v1/etl/status/{job_id}`       | Poll job status / get final result                           |
| `GET`  | `/v1/etl/jobs`                  | List all jobs (current server session)                       |
| `GET`  | `/v1/etl/logs/{job_id}`         | Stream structured log events for a job                       |
| `GET`  | `/v1/etl/invalid-rows/{job_id}` | Download invalid-rows CSV                                    |

### Databases

| Method | Path                                      | Description                               |
| ------ | ----------------------------------------- | ----------------------------------------- |
| `POST` | `/v1/databases/test-connection`           | Test a database connection                |
| `GET`  | `/v1/databases/supported-types`           | List supported DB engines                 |
| `POST` | `/v1/databases/upload`                    | Simple: upload a file directly to a table |
| `POST` | `/v1/databases/summary`                   | Full DB summary (all tables + previews)   |
| `POST` | `/v1/databases/table-exists/{table_name}` | Check if a table exists                   |

### Utilities

| Method | Path                             | Description                                   |
| ------ | -------------------------------- | --------------------------------------------- |
| `GET`  | `/v1/utilities/data-types`       | Supported data types with format requirements |
| `GET`  | `/v1/utilities/date-formats`     | Supported date formats with examples          |
| `GET`  | `/v1/utilities/datetime-formats` | Supported datetime formats with examples      |

---

## ETL job request — full example

```jsonc
POST /v1/etl/run
{
  // ── Source ────────────────────────────────────────────────────────────────
  "file_id": "20240101_abc12345_sales.csv",   // from /v1/files/upload

  // OR pull from a paginated API instead of a file:
  // "api_source": {
  //   "url": "https://api.example.com/orders",
  //   "auth": { "type": "bearer", "token": "..." },
  //   "records_key": "data.items",       // drill into {"data": {"items": [...]}}
  //   "page_param": "page",
  //   "page_size_param": "per_page",
  //   "page_size": 200,
  //   "max_pages": 50
  // },

  // ── Transform ─────────────────────────────────────────────────────────────
  "column_mappings": [
    { "column_name": "order_id",  "source_dtype": "int64",   "target_dtype": "integer", "is_primary_key": true },
    { "column_name": "customer",  "source_dtype": "object",  "target_dtype": "string",  "max_length": 100 },
    { "column_name": "amount",    "source_dtype": "object",  "target_dtype": "decimal"  },   // strips $,
    { "column_name": "status",    "source_dtype": "object",  "target_dtype": "string",  "rename_to": "order_status" },
    { "column_name": "created_at","source_dtype": "object",  "target_dtype": "date",    "date_format": "DD/MM/YYYY" }
  ],

  // ── Filter ────────────────────────────────────────────────────────────────
  "filters": [
    { "column": "amount", "operator": "gt",  "value": 0 },
    { "column": "status", "operator": "in",  "values": ["paid", "shipped"] }
  ],

  // ── Validate ──────────────────────────────────────────────────────────────
  "validation_rules": [
    { "column": "customer",  "rule_type": "not_null",   "error_message": "customer name is required" },
    { "column": "order_id",  "rule_type": "unique" },
    { "column": "amount",    "rule_type": "min_value",  "params": { "min": 0.01 } }
  ],

  // ── Aggregate (optional) ──────────────────────────────────────────────────
  // "aggregations": {
  //   "group_by": ["order_status"],
  //   "aggregations": [
  //     { "column": "amount", "function": "sum",   "alias": "total_revenue" },
  //     { "column": "order_id","function": "count", "alias": "order_count"  }
  //   ]
  // },

  // ── Load ──────────────────────────────────────────────────────────────────
  "batch_size": 10000,
  "max_retries": 3,

  "db_destination": {
    "connection": {
      "db_type": "postgresql",
      "host": "localhost",
      "port": 5432,
      "database": "mydb",
      "username": "user",
      "password": "secret"
    },
    "table_name": "orders",
    "if_exists": "replace",
    "create_index": true,
    "index_columns": ["order_status"]
  },

  // Also write a clean CSV backup
  "file_destination": {
    "format": "csv",
    "output_path": "exports/orders_clean.csv"
  }
}
```

### Response

```jsonc
{
  "job_id": "3f7a1c2d-...",
  "success": true,
  "message": "ETL job completed successfully.",
  "total_rows": 50000,
  "processed_rows": 48312,
  "failed_rows": 1688,
  "invalid_rows_file": "invalid_rows/3f7a1c2d-..._invalid.csv",
  "log_file": "logs/3f7a1c2d-....jsonl",
  "details": {
    "database": { "rows_inserted": 48312, "rows_failed": 0 },
    "file": {
      "rows_written": 48312,
      "output_path": "exports/orders_clean.csv",
    },
  },
}
```

---

## Configuration (`.env`)

| Variable              | Default          | Description                                               |
| --------------------- | ---------------- | --------------------------------------------------------- |
| `SECRET_KEY`          | _(required)_     | App secret — change in production                         |
| `UPLOAD_DIR`          | `uploaded_files` | Where uploaded files are stored                           |
| `LOG_DIR`             | `logs`           | Where per-job `.jsonl` log files go                       |
| `INVALID_ROWS_DIR`    | `invalid_rows`   | Where invalid-row CSVs are saved                          |
| `DEFAULT_BATCH_SIZE`  | `10000`          | Default rows per insert batch                             |
| `MAX_RETRIES`         | `3`              | Default DB upload retry attempts                          |
| `RETRY_DELAY_SECONDS` | `2.0`            | Base delay between retries (multiplied by attempt number) |

---

## Supported data types

`integer` · `bigint` · `float` · `decimal` · `string` · `text` · `boolean` · `date` · `datetime` · `timestamp` · `json`

Use `GET /v1/utilities/data-types` at runtime for the full list with format requirements.

---

## Extending

**Add a new source format** — add a branch in `FileProcessor._read_file()` and register the extension in `ALLOWED_EXTENSIONS`.

**Add a new database** — subclass `BaseDatabaseConnector`, implement the six abstract methods, and add the enum + connector mapping in `_get_connector()`.

**Add a new validation rule** — add a value to `ValidationRuleType` and a branch in `DataValidator.validate()`.

**Persist job state across restarts** — replace the in-memory `JOB_STORE` dict in `etl_runner.py` with a Redis client or a SQLAlchemy model.
