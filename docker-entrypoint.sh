#!/bin/sh
# docker-entrypoint.sh
#
# Runs as root first to fix ownership of Docker-mounted volumes,
# then drops to the 'teemo' user (uid 1001) to run the actual process.
#
# Why this is needed:
#   Docker named volumes are created as root. The Dockerfile's RUN chown
#   only affects the image layer — it does NOT carry over to the volume
#   mount point at runtime. This script corrects ownership each startup.

set -e

# Directories that may be mounted as Docker volumes
DATA_DIRS="${UPLOAD_DIR:-/data/uploads} ${LOG_DIR:-/data/logs} ${INVALID_ROWS_DIR:-/data/invalid_rows}"

for dir in $DATA_DIRS; do
    mkdir -p "$dir"
    # Only chown if we're currently root (won't fail in non-root environments)
    if [ "$(id -u)" = "0" ]; then
        chown -R teemo:teemo "$dir"
    fi
done

# Drop to app user and exec the CMD passed in (uvicorn or celery)
if [ "$(id -u)" = "0" ]; then
    exec gosu teemo "$@"
else
    exec "$@"
fi
