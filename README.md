# TinyTeemo — ETL

A lightweight, self-contained ETL system built with **FastAPI** and **pandas**.

Connect it to your existing databases and files, configure your transformations, and run. No infrastructure to own — TinyTeemo is just the pipeline tool.

---

## What it does

| Step          | What happens                                                                                                                             |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Extract**   | Read from CSV / Excel / Parquet, a paginated REST API, or any Postgres / MySQL / SQLite database (table or custom SQL query)             |
| **Filter**    | Drop rows that don't match your rules before any processing                                                                              |
| **Transform** | Cast types, rename columns, add prefix/suffix, strip `$` and `,` from numbers                                                            |
| **Validate**  | Check rules (not-null, unique, min/max, regex, email, allowed values…); invalid rows are saved to a CSV with the failure reason attached |
| **Aggregate** | Optional group-by with sum / count / avg / min / max                                                                                     |
| **Load**      | Write to Postgres, MySQL, SQLite, CSV, Excel, Parquet, or a REST API — any combination, in one job                                       |

Every job gets a structured JSON-lines log file and a job ID you can poll for status.

---

## Quick start

### With Docker (recommended — no Python needed)

```bash
# 1. Copy the env template
cp .env.example .env
# Edit .env — at minimum change SECRET_KEY

# 2. Start the API
docker compose up -d

# 3. Open the docs
open http://localhost:8000/docs
```

### Without Docker (local dev with uv)

```bash
uv sync
cp .env.example .env
uv run dev        # auto-reload dev server on :8000
```

---

## Docker usage

TinyTeemo connects to **your existing databases** — it does not own or manage them. The Docker setup only runs the ETL API itself.

```bash
# Start the API
docker compose up -d

# View logs
docker compose logs -f app

# Stop (data in named volumes is preserved)
docker compose down

# Run the test suite inside Docker
docker compose run --rm test
docker compose run --rm test -k test_migration   # filter tests
docker compose run --rm test --tb=long           # verbose output

# ── Local dev databases (optional) ──────────────────────────────────────────
# If you don't have a Postgres or MySQL running locally and want just for
# testing, you can spin up lightweight containers on demand:

sudo docker compose -f docker-compose-db.yml up -d

# Connection details for the Databases containers:
#   Postgres 1 → host: localhost  port: 5433, db: test_db_1, user: test_user_1, password: test_pass_1
#   Postgres 2 → host: localhost  port: 5434, db: test_db_2, user: test_user_2, password: test_pass_2
#   MySQL 1   → host: localhost  port: 3307, db: test_db_1, user: test_user_1, password: test_pass_1, root_password: root_pass_1
#   MySQL 2   → host: localhost  port: 3308, db: test_db_2, user: test_user_2, password: test_pass_2, root_password: root_pass_2

# Please look into docker-compose-db.yml file for more details about the Test Databases
```

### Why no databases in the default compose?

TinyTeemo is a tool, not a database host. Your production Postgres or MySQL lives on your own server or cloud — TinyTeemo just connects to it via credentials you supply in the API request. Starting extra DB containers by default would imply those are the databases you migrate to/from, which is wrong.

<!-- The `local-db` profile exists purely as a convenience for local development and testing. -->

The `docker-compose-db.yml` exists purely as a convenience for local development and testing.

---

## Running tests

```bash
# On the host (fastest)
uv run test

# Filter to specific tests
uv run test -k test_migration
uv run test -k "TestSQLiteConnector or TestRowFilter"

# Inside Docker (same environment as production)
docker compose run --rm test
```

---

## API overview

All routes live under `/v1`. Interactive docs at `http://localhost:8000/docs`.

### Files

| Method   | Path                                     | Description                        |
| -------- | ---------------------------------------- | ---------------------------------- |
| `POST`   | `/v1/files/upload`                       | Upload CSV / Excel / Parquet       |
| `GET`    | `/v1/files/list`                         | List uploaded files                |
| `GET`    | `/v1/files/info/{file_id}`               | Column metadata + type suggestions |
| `GET`    | `/v1/files/column-stats/{file_id}/{col}` | Detailed stats for one column      |
| `DELETE` | `/v1/files/{file_id}`                    | Delete an uploaded file            |

### ETL Jobs

| Method | Path                            | Description                               |
| ------ | ------------------------------- | ----------------------------------------- |
| `POST` | `/v1/etl/run`                   | Run a full ETL job (waits for result)     |
| `POST` | `/v1/etl/run-async`             | Queue a job, returns `job_id` immediately |
| `GET`  | `/v1/etl/status/{job_id}`       | Poll job status / get final result        |
| `GET`  | `/v1/etl/jobs`                  | List all jobs this session                |
| `GET`  | `/v1/etl/logs/{job_id}`         | Structured log events for a job           |
| `GET`  | `/v1/etl/invalid-rows/{job_id}` | Download invalid-rows CSV                 |

### DB Migration

| Method | Path                    | Description                                         |
| ------ | ----------------------- | --------------------------------------------------- |
| `POST` | `/v1/migrate/preview`   | Inspect source table/query schema — no data fetched |
| `POST` | `/v1/migrate/tables`    | List all tables in a source database                |
| `POST` | `/v1/migrate/run`       | Migrate DB→DB (synchronous)                         |
| `POST` | `/v1/migrate/run-async` | Migrate DB→DB (background, poll for result)         |

### Databases

| Method | Path                                | Description                         |
| ------ | ----------------------------------- | ----------------------------------- |
| `POST` | `/v1/databases/test-connection`     | Test a database connection          |
| `GET`  | `/v1/databases/supported-types`     | List supported DB engines           |
| `POST` | `/v1/databases/upload`              | Upload a file directly into a table |
| `POST` | `/v1/databases/summary`             | Full DB summary with table previews |
| `POST` | `/v1/databases/table-exists/{name}` | Check if a table exists             |

### Utilities

| Method | Path                             | Description                              |
| ------ | -------------------------------- | ---------------------------------------- |
| `GET`  | `/v1/utilities/data-types`       | Supported types with format requirements |
| `GET`  | `/v1/utilities/date-formats`     | Date format strings with examples        |
| `GET`  | `/v1/utilities/datetime-formats` | Datetime format strings with examples    |

---

## Examples

### Migrate a table between databases

```jsonc
// POST /v1/migrate/run
{
  "source": {
    "connection": {
      "db_type": "postgresql",
      "host": "old-server.example.com",
      "database": "legacy_db",
      "username": "reader",
      "password": "...",
    },
    "table_name": "orders",
    // or use "query": "SELECT * FROM orders WHERE created_at > '2024-01-01'"
  },
  "db_destination": {
    "connection": {
      "db_type": "mysql",
      "host": "new-server.example.com",
      "database": "new_db",
      "username": "writer",
      "password": "...",
    },
    "table_name": "orders",
    "if_exists": "replace",
  },
}
```

Column types are inferred automatically. Supply `column_mappings` only when you want to cast, rename, or transform specific columns.

### Load a CSV into Postgres with validation

```jsonc
// POST /v1/etl/run
{
  "file_id": "20240101_abc12345_sales.csv", // from POST /v1/files/upload

  "column_mappings": [
    {
      "column_name": "order_id",
      "source_dtype": "int64",
      "target_dtype": "integer",
      "is_primary_key": true,
    },
    {
      "column_name": "amount",
      "source_dtype": "object",
      "target_dtype": "decimal",
    }, // strips $,
    {
      "column_name": "status",
      "source_dtype": "object",
      "target_dtype": "string",
      "rename_to": "order_status",
    },
    {
      "column_name": "created_at",
      "source_dtype": "object",
      "target_dtype": "date",
      "date_format": "DD/MM/YYYY",
    },
  ],

  "filters": [{ "column": "amount", "operator": "gt", "value": 0 }],

  "validation_rules": [
    { "column": "order_id", "rule_type": "not_null" },
    { "column": "order_id", "rule_type": "unique" },
    { "column": "amount", "rule_type": "min_value", "params": { "min": 0.01 } },
  ],

  "db_destination": {
    "connection": {
      "db_type": "postgresql",
      "host": "localhost",
      "database": "mydb",
      "username": "user",
      "password": "secret",
    },
    "table_name": "orders",
    "if_exists": "replace",
  },

  "batch_size": 10000,
  "max_retries": 3,
}
```

### Check source schema before migrating

```jsonc
// POST /v1/migrate/preview
{
  "connection": {
    "db_type": "postgresql",
    "host": "old-server.example.com",
    "database": "legacy_db",
    "username": "reader",
    "password": "..."
  },
  "table_name": "orders"
}

// Response
{
  "success": true,
  "column_count": 8,
  "columns": [
    { "column_name": "order_id",   "source_dtype": "int64",   "suggested_target_dtype": "bigint" },
    { "column_name": "customer",   "source_dtype": "object",  "suggested_target_dtype": "text" },
    { "column_name": "amount",     "source_dtype": "float64", "suggested_target_dtype": "float" },
    ...
  ]
}
```

---

## Configuration (`.env`)

| Variable                 | Default          | Description                                        |
| ------------------------ | ---------------- | -------------------------------------------------- |
| `SECRET_KEY`             | _(required)_     | App secret — change in production                  |
| `APP_PORT`               | `8000`           | Host port to expose the API on                     |
| `UPLOAD_DIR`             | `uploaded_files` | Where uploaded files are stored                    |
| `LOG_DIR`                | `logs`           | Where per-job `.jsonl` log files go                |
| `INVALID_ROWS_DIR`       | `invalid_rows`   | Where invalid-row CSVs are saved                   |
| `DEFAULT_BATCH_SIZE`     | `10000`          | Default rows per DB insert batch                   |
| `MAX_RETRIES`            | `3`              | Default retry attempts on DB upload failure        |
| `RETRY_DELAY_SECONDS`    | `2.0`            | Base delay between retries (multiplied by attempt) |
| `POSTGRES_*` / `MYSQL_*` | —                | Only needed if using the `local-db` Docker profile |

---

## Project layout

```
tinyteemo/
├── main.py                          # FastAPI app + lifespan
├── Dockerfile                       # 3-stage: builder / runtime / test
├── docker-compose.yml               # app (default) + test + local-db (opt-in)
├── pyproject.toml                   # deps + uv run scripts
├── .env.example
│
├── app/
│   ├── api/v1/endpoints/
│   │   ├── files.py                 # file upload/management
│   │   ├── database.py              # DB connection + simple upload
│   │   ├── etl.py                   # ETL job run/status/logs
│   │   ├── migrate.py               # DB-to-DB migration
│   │   └── utilities.py             # data types, date formats
│   │
│   ├── core/
│   │   ├── config.py                # settings (pydantic-settings)
│   │   └── constants.py             # enums, limits
│   │
│   ├── database/connectors/
│   │   ├── base.py                  # abstract base + read_dataframe + upload_dataframe
│   │   ├── sqlite.py
│   │   ├── postgres.py
│   │   └── mysql.py
│   │
│   ├── models/schemas.py            # all Pydantic models
│   │
│   └── services/
│       ├── db_reader.py             # extract from source DB → DataFrame
│       ├── file_processor.py        # read CSV/Excel/Parquet
│       ├── schema_mapper.py         # transform, filter, validate, aggregate
│       ├── etl_runner.py            # full pipeline + job store
│       ├── etl_logger.py            # structured per-job logging
│       ├── file_writer.py           # write DataFrame → file
│       └── api_writer.py            # write DataFrame → REST API
│
└── tests/                           # 127 tests, 0 warnings
    ├── conftest.py
    ├── test_file_processor.py
    ├── test_transformations.py
    ├── test_connectors.py
    ├── test_etl.py
    └── test_migration.py
```

---

## Extending

**New file format** — add a branch in `FileProcessor._read_file()` and register the extension in `ALLOWED_EXTENSIONS`.

**New database** — subclass `BaseDatabaseConnector`, implement the abstract methods, add to the `DatabaseType` enum and the `_get_connector()` factory in `etl_runner.py`.

**New validation rule** — add a value to `ValidationRuleType` and a branch in `DataValidator.validate()` in `schema_mapper.py`.

**Persistent job store** — replace the in-memory `JOB_STORE` dict in `etl_runner.py` with a Redis client or a SQLAlchemy model. The interface (`get_job_status`, `list_jobs`) stays the same.
