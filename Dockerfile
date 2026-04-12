# ────────────────────────────────────────────────────────────────────────────
# Stage 1 — dependency builder
#   Uses uv to resolve + install all packages into /app/.venv so the final
#   image only copies the pre-built venv, keeping it lean and cache-friendly.
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Install uv (pinned for reproducibility)
COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy dependency manifests first — Docker caches this layer until they change
COPY pyproject.toml uv.lock* ./

# Install production deps only into a local venv
RUN uv sync --frozen --no-dev --no-editable

# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# System packages needed at runtime:
#   libpq-dev  → psycopg2 (PostgreSQL)
#   default-libmysqlclient-dev → mysql-connector fallback (optional)
#   curl       → healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --gid 1001 teemo \
 && useradd  --uid 1001 --gid teemo --shell /bin/bash --create-home teemo

WORKDIR /app

# Copy the pre-built venv from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application source
COPY --chown=teemo:teemo main.py         ./
COPY --chown=teemo:teemo app/            ./app/

# Directories that will be bind-mounted in compose; create them so the
# container works standalone too (e.g. docker run without -v)
RUN mkdir -p uploaded_files logs invalid_rows \
 && chown -R teemo:teemo uploaded_files logs invalid_rows

# Make the venv the default Python / pip
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # pydantic-settings picks up .env automatically; we override dirs here
    UPLOAD_DIR=uploaded_files \
    LOG_DIR=logs \
    INVALID_ROWS_DIR=invalid_rows

USER teemo

EXPOSE 8000

# Graceful shutdown: uvicorn handles SIGTERM cleanly
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "4", "--no-access-log"]