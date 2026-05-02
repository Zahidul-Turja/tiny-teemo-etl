import hashlib
import json
from datetime import timedelta
from typing import Any, Dict, List, Optional

import redis

from app.core.config import settings
from app.models.schemas import ETLJobResult

_TTL = timedelta(hours=24)

# ── low-level client (sync, used from Celery tasks & regular code) ───────────


def _client() -> redis.Redis:
    return redis.Redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)


# ── job CRUD ─────────────────────────────────────────────────────────────────


def save_job(result: ETLJobResult) -> None:
    r = _client()
    r.set(
        f"job:{result.job_id}", result.model_dump_json(), ex=int(_TTL.total_seconds())
    )


def get_job(job_id: str) -> Optional[ETLJobResult]:
    r = _client()
    raw = r.get(f"job:{job_id}")
    if not raw:
        return None
    return ETLJobResult.model_validate_json(raw)


def list_jobs() -> List[ETLJobResult]:
    r = _client()
    keys = r.keys("job:*")
    # Exclude idempotency keys
    job_keys = [k for k in keys if not k.startswith("job:idem:")]
    if not job_keys:
        return []
    raws = r.mget(job_keys)
    results = []
    for raw in raws:
        if raw:
            try:
                results.append(ETLJobResult.model_validate_json(raw))
            except Exception:
                pass
    return results


# ── idempotency ───────────────────────────────────────────────────────────────


def compute_request_hash(request_dict: Dict[str, Any]) -> str:
    """Stable SHA-256 of the serialised request (sorted keys)."""
    canonical = json.dumps(request_dict, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def get_idempotent_job_id(request_hash: str) -> Optional[str]:
    """Return existing job_id for this hash, or None if not seen before."""
    r = _client()
    return r.get(f"job:idem:{request_hash}")


def set_idempotent_job_id(request_hash: str, job_id: str) -> None:
    r = _client()
    r.set(f"job:idem:{request_hash}", job_id, ex=int(_TTL.total_seconds()))


# ── progress pub/sub ──────────────────────────────────────────────────────────


def _channel(job_id: str) -> str:
    return f"etl:{job_id}"


def publish_progress(job_id: str, event: Dict[str, Any]) -> None:
    """Push a progress event to the job's Redis pub/sub channel."""
    r = _client()
    r.publish(_channel(job_id), json.dumps(event))


def subscribe_to_job(job_id: str):
    """
    Return a pubsub object already subscribed to the job channel.
    Caller is responsible for unsubscribing / closing.
    """
    r = _client()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(_channel(job_id))
    return ps
