import logging

from celery import Celery
from celery.signals import worker_ready

from community_manager.settings import community_manager_settings


logger = logging.getLogger(__name__)


def create_app() -> Celery:
    _app = Celery()
    _app.conf.update(
        {
            "broker_url": community_manager_settings.broker_url,
            "result_backend": community_manager_settings.broker_url,
            "result_expires": 300,  # 5 minutes
            "include": ["community_manager.tasks"],
            "worker_concurrency": community_manager_settings.worker_concurrency,
        }
    )
    return _app


app = create_app()


@worker_ready.connect
def log_queue_stats_on_startup(sender, **kwargs):
    """Log Celery queue statistics when worker starts"""
    try:
        inspect = app.control.inspect()

        reserved = inspect.reserved() or {}
        reserved_count = sum(len(tasks) for tasks in reserved.values())

        active = inspect.active() or {}
        active_count = sum(len(tasks) for tasks in active.values())

        scheduled = inspect.scheduled() or {}
        scheduled_count = sum(len(tasks) for tasks in scheduled.values())

        logger.info(
            f"Celery queue stats - Reserved: {reserved_count}, "
            f"Active: {active_count}, Scheduled: {scheduled_count}"
        )
    except Exception as e:
        logger.warning(f"Could not fetch Celery queue stats: {e}")
