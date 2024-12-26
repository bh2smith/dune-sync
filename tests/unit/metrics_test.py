import unittest
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from src.metrics import log_job_metrics, validate_prometheus_url


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

    def test_validate_prometheus_url(self):
        url = "http://prometheus:9091"

        # Test successful connection
        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200  # SUCCESS_STATUS
            mock_get.return_value = mock_response
            
            # Should not raise any exception
            validate_prometheus_url(url)
            mock_get.assert_called_once_with(url, timeout=5)

        # Test failed status code
        with patch("requests.get") as mock_get, patch("src.metrics.log") as mock_log:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.reason = "Not Found"
            mock_get.return_value = mock_response
            
            with pytest.raises(ConnectionError, match=f"Failed to connect to Prometheus Pushgateway at {url}"):
                validate_prometheus_url(url)
            
            mock_log.error.assert_called_once_with(
                "Failed to connect to Prometheus Pushgateway: %s %s",
                404,
                "Not Found"
            )

        # Test request exception
        with patch("requests.get") as mock_get, patch("src.metrics.log") as mock_log:
            mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")
            
            with pytest.raises(ConnectionError, match=f"Failed to connect to Prometheus Pushgateway at {url}"):
                validate_prometheus_url(url)
            
            mock_log.error.assert_called_once_with(
                "Error connecting to Prometheus Pushgateway: %s",
                "Connection refused"
            )