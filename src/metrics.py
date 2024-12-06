"""Handle submitting metrics, logs and other interesting details about jobs."""

import uuid
from functools import wraps
from os import getenv as env
from time import perf_counter
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

# MARKER: pylint-bug
from src import Callable, Iterable, Mapping

# MARKER: pylint-bug end
from src.interfaces import Named
from src.logger import log


def log_job_metrics(prometheus_url: str, job_metrics: dict[str, Any]) -> None:
    """Log metrics about a job to a prometheus pushgateway."""
    registry = CollectorRegistry()
    log.info("Pushing metrics to Prometheus")

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
        gateway=prometheus_url,
        job=f'dune-sync-{job_metrics["name"]}',
        registry=registry,
    )


def collect_metrics(
    func: Callable,
) -> Callable:
    """Collect and submit metrics about a Job if a pushgateway is configured."""

    @wraps(func)
    async def wrapper(
        self: Named, *args: Iterable[Any], **kwargs: Mapping[Any, Any]
    ) -> Any:
        if not (prometheus_url := env("PROMETHEUS_PUSHGATEWAY_URL")):
            return await func(self, *args, **kwargs)

        run_id = uuid.uuid4().hex
        start = perf_counter()
        success = False

        try:
            result = await func(self, *args, **kwargs)
            success = True
            return result
        except Exception:
            success = False
            raise
        finally:
            duration = perf_counter() - start
            metrics = {
                "duration": duration,
                "name": self.name,
                "run_id": run_id,
                "success": success,
            }
            log_job_metrics(prometheus_url, metrics)

    return wrapper
