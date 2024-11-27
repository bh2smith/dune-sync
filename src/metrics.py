"""Handle submitting metrics, logs and other interesting details about jobs."""

from os import getenv as env
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway


def log_job_metrics(job_metrics: dict[str, Any]) -> None:
    """Log metrics about a job to a prometheus pushgateway."""
    if not (prometheus_pushgateway_url := env("PROMETHEUS_PUSHGATEWAY_URL")):
        return

    registry = CollectorRegistry()

    job_success_timestamp = Gauge(
        name="job_last_success_unixtime",
        documentation="Unix timestamp of job end",
        registry=registry,
    )
    job_success_timestamp.set_to_current_time()

    job_failure_counter = Counter(
        name="job_failure_count",
        documentation="Number of failed jobs",
        registry=registry,
    )
    job_failure_counter.inc(int(not job_metrics["success"]))

    job_duration_metric = Gauge(
        name="job_last_success_duration",
        documentation="How long did the job take to run (in seconds)",
        registry=registry,
    )
    job_duration_metric.set(job_metrics["duration"])
    push_to_gateway(
        gateway=prometheus_pushgateway_url,
        job=f'dune-sync-{job_metrics["job"].name}',
        registry=registry,
    )
