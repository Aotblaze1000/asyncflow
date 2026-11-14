"""Celery application factory.

Configuration choices that matter for correctness:

* ``task_acks_late`` keeps a message unacknowledged until the task finishes, so
  a worker that dies mid-job does not lose the work.
* ``task_reject_on_worker_lost`` requeues a message when the worker process is
  killed, instead of silently dropping it.
* ``worker_prefetch_multiplier = 1`` stops a single worker from hoarding
  messages, which keeps long jobs from starving the queue.
"""

from __future__ import annotations

from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "asyncflow",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    task_soft_time_limit=300,
    task_time_limit=360,
    task_default_queue="asyncflow",
)
