# ────────────────────────────────────────────────────────────────────────────
# Stage 1 — dependency builder
#   Installs ALL deps (including dev/test) into /app/.venv.
#   The runtime stage re-uses this venv but without test files.
#   The test stage adds test files on top.
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /usr/local/bin/

WORKDIR /app

# Layer cache: only re-run uv sync when pyproject.toml / uv.lock changes
COPY pyproject.toml uv.lock* ./

# Install ALL deps (prod + dev group) — test stage needs pytest
RUN uv sync --frozen --no-editable

# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image  (production)
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 teemo \
 && useradd  --uid 1001 --gid teemo --shell /bin/bash --create-home teemo

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY --chown=teemo:teemo main.py  ./
COPY --chown=teemo:teemo app/     ./app/

RUN mkdir -p uploaded_files logs invalid_rows \
 && chown -R teemo:teemo uploaded_files logs invalid_rows

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UPLOAD_DIR=uploaded_files \
    LOG_DIR=logs \
    INVALID_ROWS_DIR=invalid_rows

USER teemo
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "4", "--no-access-log"]

# ────────────────────────────────────────────────────────────────────────────
# Stage 3 — test image
#   Inherits the full venv (prod + dev deps) from the builder.
#   Adds test files. Uses tmp directories so nothing leaks between runs.
#   No PORT exposed — this image is never deployed.
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS test

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Full venv (prod + dev) from builder
COPY --from=builder /app/.venv /app/.venv

# Application source
COPY main.py  ./
COPY app/     ./app/

# Test files — intentionally excluded from runtime stage
COPY tests/   ./tests/

# Writable temp dirs for uploads/logs/invalid_rows created during tests
RUN mkdir -p /tmp/teemo/uploads /tmp/teemo/logs /tmp/teemo/invalid_rows

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UPLOAD_DIR=/tmp/teemo/uploads \
    LOG_DIR=/tmp/teemo/logs \
    INVALID_ROWS_DIR=/tmp/teemo/invalid_rows \
    # Dummy secret so pydantic-settings doesn't error without a .env file
    SECRET_KEY=test-secret-key

CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]