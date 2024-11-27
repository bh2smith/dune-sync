import unittest
from unittest.mock import MagicMock, patch

from src.metrics import log_job_metrics


class TestMetrics(unittest.TestCase):
    @patch("src.metrics.push_to_gateway")
    def test_log_job_metrics(self, mock_push):
        job = MagicMock()
        job.name = "mock-job"

        with patch("src.metrics.env", return_value=None):
            log_job_metrics({"job": job, "duration": 2, "success": True})
            mock_push.assert_not_called()

        mock_push.reset_mock()

        with patch("src.metrics.env", return_value="https://localhost:9090"):
            log_job_metrics({"duration": 1, "job": job, "success": False})
            self.assertEqual(1, mock_push.call_count)
            self.assertEqual(
                "https://localhost:9090", mock_push.mock_calls[0].kwargs["gateway"]
            )
            self.assertEqual(
                "dune-sync-mock-job", mock_push.mock_calls[0].kwargs["job"]
            )
