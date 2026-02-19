"""Celery application configuration for QueryTorque async job processing."""

from celery import Celery

from qt_shared.config import get_settings


def create_celery_app() -> Celery:
    """Create and configure the Celery application."""
    settings = get_settings()

    app = Celery(
        "querytorque",
        broker=settings.celery_broker_url,
        backend=settings.celery_result_backend,
    )

    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_soft_time_limit=600,
        task_time_limit=900,
        result_expires=86400,
        task_routes={
            "qt_sql.tasks.optimize_query": {"queue": "optimization"},
            "qt_sql.tasks.optimize_batch": {"queue": "optimization"},
            "qt_sql.tasks.fleet_survey": {"queue": "fleet"},
            "qt_sql.tasks.process_github_pr": {"queue": "github"},
        },
    )

    app.autodiscover_tasks(["qt_sql"])

    return app


celery_app = create_celery_app()
