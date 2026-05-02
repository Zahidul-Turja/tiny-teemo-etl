from celery import Celery
from app.core.config import settings

celery_app = Celery("tinyteemo")

celery_app.conf.update(
    # ── broker / backend ────────────────────────────────────────────────────
    broker_url=settings.CELERY_BROKER_URL,
    result_backend=settings.CELERY_RESULT_BACKEND,
    # ── serialisation ────────────────────────────────────────────────────────
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # ── task routing ─────────────────────────────────────────────────────────
    task_routes={
        "app.worker.tasks.run_etl_task": {"queue": "etl.default"},
        "app.worker.tasks.etl_dead_letter": {"queue": "etl.dlq"},
    },
    task_queues={
        "etl.default": {},
        "etl.dlq": {},
    },
    # ── retry / reliability ───────────────────────────────────────────────────
    task_acks_late=True,  # only ack after success/failure (safe retries)
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # don't prefetch — ensures fair distribution
    # ── result expiry ────────────────────────────────────────────────────────
    result_expires=86_400,  # 24 h
    # ── timezone ─────────────────────────────────────────────────────────────
    timezone="UTC",
    enable_utc=True,
)

# Auto-discover tasks in app/worker/tasks.py
celery_app.autodiscover_tasks(["app.worker"])
