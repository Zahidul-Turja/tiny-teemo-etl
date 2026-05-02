# 🐾 TinyTeemo ETL

**A production-grade ETL system** — extract from files, databases, or REST APIs; transform with type casting, filtering, and validation; load into any database — all with live WebSocket progress streaming and background task processing.

> Built with **FastAPI + Celery + Redis** · Containerized with Docker

---

## What It Does

| Feature             | Details                                                                                                                                  |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **Extract**         | CSV, Excel (.xls/.xlsx), Parquet, PostgreSQL, MySQL, SQLite, paginated REST APIs                                                         |
| **Transform**       | Type casting, column rename/drop, row filtering (12 operators), data validation (11 rule types), aggregation (SUM, COUNT, AVG, MIN, MAX) |
| **Load**            | PostgreSQL, MySQL, SQLite, file output (CSV/Excel/Parquet), REST API endpoints                                                           |
| **DB Migration**    | Any-to-any: PG→MySQL, MySQL→PG, SQLite→PG, etc. — with full transform pipeline                                                           |
| **Live Progress**   | WebSocket streaming — clients receive real-time stage updates (0% → 100%)                                                                |
| **Background Jobs** | Celery workers — jobs survive server restarts, run concurrently                                                                          |
| **Idempotency**     | SHA-256 request hashing — identical submissions return the existing job, no duplicate work                                               |
| **Retry + DLQ**     | Exponential backoff (2s → 4s → 8s…), dead-letter queue for permanently failed jobs                                                       |
| **Job Persistence** | Redis-backed store with 24h TTL — survives restarts unlike in-memory dicts                                                               |
| **Dashboard**       | Single-file HTML/JS SPA — upload files, inspect schemas, configure DB targets, watch live progress                                       |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Client / Dashboard                         │
│   Upload File  ─────►  POST /v1/etl/run-async  ──► job_id          │
│   WS Connect   ─────►  WS /v1/etl/ws/etl/{id}  ◄── live events     │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTP + WebSocket
┌────────────────────────────▼────────────────────────────────────────┐
│                         FastAPI App (:8000)                         │
│                                                                     │
│  /v1/files/*          File upload, metadata, column stats           │
│  /v1/etl/*            ETL jobs (sync + async) + WS endpoint         │
│  /v1/migrate/*        DB-to-DB migration (sync + async)             │
│  /v1/databases/*      Connection test, table browse                 │
│  /v1/utilities/*      Data types, filter operators, enums           │
│  /static/dashboard    Live progress dashboard (HTML)                │
└────────────┬───────────────────────────────┬───────────────────────┘
             │ Publish progress events        │ Enqueue tasks
             │                               ▼
┌────────────▼───────────────┐  ┌───────────────────────────────────┐
│        Redis               │  │       Celery Workers (:pool=4)    │
│  Broker (DB 0)             │  │                                   │
│  Result backend (DB 1)     │  │  run_etl_task                     │
│  Job store  job:<id>       │◄─┤  ├── Extract (file/DB/API)        │
│  Idempotency job:idem:<h>  │  │  ├── Filter rows                  │
│  Pub/Sub  etl:<id>  ───────┼──►  ├── Transform schema             │
│                            │  │  ├── Validate data                │
└────────────────────────────┘  │  ├── Aggregate                    │
                                │  └── Load to destination          │
                                │                                   │
                                │  etl_dead_letter (DLQ after       │
                                │  max retries exhausted)           │
                                └───────────────────────────────────┘
```

---

## Technical Highlights

### Idempotent Job Submission

Every `POST /run-async` request is fingerprinted with a SHA-256 hash of the entire body. Submitting the same job twice returns the existing `job_id` — no duplicate Celery tasks, no wasted worker time.

```python
req_hash = compute_request_hash(request_dict)   # SHA-256 of JSON body
existing = get_idempotent_job_id(req_hash)       # check Redis
if existing:
    return cached_result(existing)               # short-circuit
```

### Live WebSocket Progress via Redis Pub/Sub

The Celery worker publishes progress events to a Redis channel at each pipeline stage. The FastAPI WebSocket endpoint subscribes and forwards events to connected clients in real time — no polling needed.

```
Celery task  ──publish──►  Redis channel etl:<job_id>  ──subscribe──►  WS endpoint  ──►  browser
```

### Exponential Backoff + Dead Letter Queue

Tasks automatically retry on transient failures (network timeouts, DB unavailable). After `MAX_RETRIES`, they route to a dedicated DLQ for alerting/inspection — not silently dropped.

```python
@celery_app.task(
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,        # 2s → 4s → 8s
    retry_jitter=True,         # avoid thundering herd
    dont_autoretry_for=(ValueError, FileNotFoundError),  # data errors don't retry
)
```

### Database Query Optimisation (from production work)

Inspired by real-world work reducing DB queries from **180 → 44** at EWN using `select_related` and `prefetch_related`. TinyTeemo applies the same principle: batch inserts, chunked reads, and strategic connection pooling.

---

## Running Locally

**Prerequisites:** Docker + Docker Compose

```bash
git clone https://github.com/Zahidul-Turja/tiny-teemo-etl
cd tiny-teemo-etl

cp .env.example .env

# Build images (first time: resolves all dependencies)
docker compose build

# Start Redis + FastAPI + Celery worker + Flower monitor
docker compose up -d

# Open the dashboard
open http://localhost:8000/static/dashboard.html

# Swagger API docs
open http://localhost:8000/docs

# Celery task monitor (Flower)
open http://localhost:5555
```

**Tear down:**

```bash
docker compose down -v
```

---

## API Reference

### Files

| Method   | Endpoint                   | Description                                        |
| -------- | -------------------------- | -------------------------------------------------- |
| `POST`   | `/v1/files/upload`         | Upload CSV/Excel/Parquet, returns schema + preview |
| `GET`    | `/v1/files/list`           | List all uploaded files                            |
| `GET`    | `/v1/files/info/{file_id}` | Metadata + column stats for one file               |
| `DELETE` | `/v1/files/{file_id}`      | Delete uploaded file                               |

### ETL Jobs

| Method | Endpoint                        | Description                                 |
| ------ | ------------------------------- | ------------------------------------------- |
| `POST` | `/v1/etl/run`                   | Run ETL synchronously (small datasets)      |
| `POST` | `/v1/etl/run-async`             | Queue ETL job, returns `job_id` immediately |
| `GET`  | `/v1/etl/status/{job_id}`       | Poll job status / final result              |
| `GET`  | `/v1/etl/jobs`                  | List all jobs                               |
| `WS`   | `/v1/etl/ws/etl/{job_id}`       | Live progress stream                        |
| `GET`  | `/v1/etl/logs/{job_id}`         | Structured log events                       |
| `GET`  | `/v1/etl/invalid-rows/{job_id}` | Download invalid-rows CSV                   |

### DB Migration

| Method | Endpoint                | Description                             |
| ------ | ----------------------- | --------------------------------------- |
| `POST` | `/v1/migrate/tables`    | List tables in a source database        |
| `POST` | `/v1/migrate/preview`   | Inspect source schema (no data fetched) |
| `POST` | `/v1/migrate/run`       | Synchronous DB-to-DB migration          |
| `POST` | `/v1/migrate/run-async` | Background DB-to-DB migration           |

### Databases

| Method | Endpoint                        | Description                            |
| ------ | ------------------------------- | -------------------------------------- |
| `POST` | `/v1/databases/test-connection` | Test DB connectivity                   |
| `POST` | `/v1/databases/summary`         | Full schema browse with table previews |
| `GET`  | `/v1/databases/supported-types` | List supported DB engines              |

---

## Example: File → Database

```bash
# 1. Upload a file
curl -X POST http://localhost:8000/v1/files/upload \
  -F "file=@sales_data.csv"
# → {"data": {"file_id": "sales_data_abc123.csv", "row_count": 50000, ...}}

# 2. Queue async ETL job
curl -X POST http://localhost:8000/v1/etl/run-async \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "sales_data_abc123.csv",
    "column_mappings": [
      {"column_name": "id",     "source_dtype": "int64",  "target_dtype": "integer", "is_primary_key": true},
      {"column_name": "amount", "source_dtype": "float64","target_dtype": "decimal"},
      {"column_name": "date",   "source_dtype": "object", "target_dtype": "date"}
    ],
    "db_destination": {
      "connection": {"db_type": "postgresql", "host": "localhost", "database": "mydb", "username": "user", "password": "pass"},
      "table_name": "sales",
      "if_exists": "replace"
    }
  }'
# → {"job_id": "d4f2a...", "ws_url": "/v1/etl/ws/etl/d4f2a..."}

# 3. Connect WebSocket for live updates
wscat -c ws://localhost:8000/v1/etl/ws/etl/d4f2a...
# {"stage":"extract","progress":20,"message":"Extracted 50000 rows"}
# {"stage":"transform","progress":55,"message":"Schema mapping complete"}
# {"stage":"load","progress":90,"message":"Writing to DB table 'sales'"}
# {"stage":"done","progress":100,"result":{...}}
```

## Example: Database Migration

```bash
curl -X POST http://localhost:8000/v1/migrate/run-async \
  -H "Content-Type: application/json" \
  -d '{
    "source": {
      "connection": {"db_type": "postgresql", "host": "old-server", "database": "legacy", "username": "reader", "password": "..."},
      "table_name": "customers"
    },
    "db_destination": {
      "connection": {"db_type": "mysql", "host": "new-server", "database": "modern", "username": "writer", "password": "..."},
      "table_name": "customers_migrated",
      "if_exists": "replace"
    },
    "batch_size": 5000
  }'
```

---

## Project Structure

```
tiny-teemo-etl/
├── app/
│   ├── api/v1/endpoints/
│   │   ├── etl.py          # ETL jobs + WebSocket endpoint
│   │   ├── migrate.py      # DB-to-DB migration
│   │   ├── files.py        # File upload/metadata
│   │   ├── database.py     # Connection test + browse
│   │   └── utilities.py    # Enums/types reference
│   ├── worker/
│   │   ├── celery_app.py   # Celery config + queue definitions
│   │   ├── tasks.py        # ETL task with retry + DLQ
│   │   └── job_store.py    # Redis job CRUD + idempotency + pub/sub
│   ├── services/
│   │   ├── file_processor.py   # CSV/Excel/Parquet reader
│   │   ├── schema_mapper.py    # Type casting, filtering, validation
│   │   ├── db_reader.py        # Database source reader
│   │   ├── api_writer.py       # REST API destination writer
│   │   ├── file_writer.py      # File output writer
│   │   └── etl_logger.py       # Structured per-job logger
│   ├── database/connectors/
│   │   ├── base.py         # Abstract connector (upload_dataframe, create_index…)
│   │   ├── postgres.py     # psycopg2-based
│   │   ├── mysql.py        # PyMySQL-based (pure Python, cross-platform)
│   │   └── sqlite.py       # sqlite3-based
│   ├── models/schemas.py   # All Pydantic v2 request/response models
│   └── core/
│       ├── config.py       # Settings (pydantic-settings, .env)
│       └── constants.py    # Enums, type maps, limits
├── static/
│   └── dashboard.html      # Live dashboard SPA (zero dependencies)
├── tests/                  # pytest test suite
├── docker-compose.yml      # app + worker + redis + flower
├── Dockerfile              # Multi-stage: builder → runtime → test
├── pyproject.toml          # Dependencies (uv)
└── .env.example
```

---

## Tech Stack

| Layer            | Technology                                 |
| ---------------- | ------------------------------------------ |
| API Framework    | FastAPI 0.115 + Uvicorn (ASGI)             |
| Task Queue       | Celery 5.3 with Redis broker               |
| Real-time        | WebSocket (native FastAPI) + Redis Pub/Sub |
| Data Processing  | Pandas 2.0 + PyArrow                       |
| Databases        | psycopg2 (PostgreSQL) · PyMySQL · sqlite3  |
| Validation       | Pydantic v2                                |
| Containerisation | Docker + Docker Compose                    |
| Monitoring       | Flower (Celery task dashboard)             |
| Package Manager  | uv                                         |

---

## Environment Variables

```env
SECRET_KEY=your-secret-here
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
UPLOAD_DIR=uploaded_files
LOG_DIR=logs
INVALID_ROWS_DIR=invalid_rows
MAX_RETRIES=3
DEFAULT_BATCH_SIZE=10000
```

---

_Built by [Zahidul Islam Turja](https://zahidul-turja.vercel.app) · [LinkedIn](https://linkedin.com/in/zahidul-turja) · [GitHub](https://github.com/Zahidul-Turja)_
