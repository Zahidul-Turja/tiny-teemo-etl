# ────────────────────────────────────────────────────────────────────────────
# Stage 1 — dependency builder
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync --no-editable 2>/dev/null || uv sync --no-editable --no-cache

# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# gosu is a minimal correct setuid helper for privilege dropping on Debian/Ubuntu.
# We stay on root until the entrypoint script drops privileges.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        curl \
        gosu \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 teemo \
 && useradd  --uid 1001 --gid teemo --shell /bin/bash --create-home teemo

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY --chown=teemo:teemo main.py  ./
COPY --chown=teemo:teemo app/     ./app/
COPY --chown=teemo:teemo static/  ./static/

# Copy entrypoint — must be executable and owned by root so it can chown volumes
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UPLOAD_DIR=/data/uploads \
    LOG_DIR=/data/logs \
    INVALID_ROWS_DIR=/data/invalid_rows

# Run as root — entrypoint drops to teemo after fixing volume ownership
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "4", "--no-access-log", \
     "--timeout-keep-alive", "120"]

# ────────────────────────────────────────────────────────────────────────────
# Stage 3 — test image
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim AS test

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY main.py  ./
COPY app/     ./app/
COPY tests/   ./tests/

RUN mkdir -p /tmp/teemo/uploads /tmp/teemo/logs /tmp/teemo/invalid_rows

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UPLOAD_DIR=/tmp/teemo/uploads \
    LOG_DIR=/tmp/teemo/logs \
    INVALID_ROWS_DIR=/tmp/teemo/invalid_rows \
    SECRET_KEY=test-secret-key

CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]
