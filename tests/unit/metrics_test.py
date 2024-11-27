import unittest
from unittest.mock import MagicMock, patch

from src.metrics import log_job_metrics


class TestMetrics(unittest.TestCase):
    @patch("src.metrics.push_to_gateway")
    def test_log_job_metrics(self, mock_push):
        job = MagicMock()
        job.name = "mock-job"

        log_job_metrics(
            "https://localhost:9090",
            {"duration": 1, "job": job, "success": False, "name": job.name},
        )
        self.assertEqual(1, mock_push.call_count)
        self.assertEqual(
            "https://localhost:9090", mock_push.mock_calls[0].kwargs["gateway"]
        )
        self.assertEqual("dune-sync-mock-job", mock_push.mock_calls[0].kwargs["job"])
