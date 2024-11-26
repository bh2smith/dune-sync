"""Handle submitting metrics, logs and other interesting details about jobs."""

from typing import Any

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


def log_job_metrics(job_metrics: dict[str, Any]) -> None:
    """Log metrics about a job to a prometheus pushgateway."""
    registry = CollectorRegistry()

    job_success_timestamp = Gauge(
        name="job_last_success_unixtime",
        documentation="Unix timestamp of job end",
        registry=registry,
        # labelnames=['runid']
    )
    # job_success_timestamp.labels(runid=job_metrics['run_id'])
    # .set_to_current_time()
    job_success_timestamp.set_to_current_time()

    job_duration_metric = Gauge(
        name="job_last_success_duration",
        documentation="How long did the job take to run (in seconds)",
        registry=registry,
        # labelnames=['runid']
    )
    # job_duration_metric.labels(runid=job_metrics['run_id'])
    # .set(job_metrics["duration"])
    job_duration_metric.set(job_metrics["duration"])
    push_to_gateway(
        # TODO take this from the config file or env var
        gateway="localhost:9091",
        job=f'dune-sync-{job_metrics["job"].name}',
        registry=registry,
    )
