import pytest
import os
import json
import base64
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Set API_KEY before importing main to avoid RuntimeError
os.environ["API_KEY"] = "test_api_key"
os.environ["PROJECT_ID"] = "test-project"

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import fetch_fmp_data, run_fmp_ingestion_impl, run_fmp_ingestion


class TestFetchFmpData:
    """Test suite for fetch_fmp_data function"""

    @patch("main.requests.get")
    def test_fetch_fmp_data_success(self, mock_get):
        """Test successful API call with valid data"""
        mock_response = Mock()
        mock_response.json.return_value = [{"symbol": "AAPL", "revenue": 1000000}]
        mock_get.return_value = mock_response

        result = fetch_fmp_data("income-statement", "AAPL", "test_key")

        assert result == [{"symbol": "AAPL", "revenue": 1000000}]
        mock_get.assert_called_once()
        assert "AAPL" in mock_get.call_args[0][0]

    @patch("main.requests.get")
    def test_fetch_fmp_data_empty_list(self, mock_get):
        """Test API call that returns empty list"""
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        result = fetch_fmp_data("income-statement", "INVALID", "test_key")

        assert result is None

    @patch("main.requests.get")
    def test_fetch_fmp_data_http_error(self, mock_get):
        """Test handling of HTTP errors"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("401 Unauthorized")
        mock_get.return_value = mock_response

        result = fetch_fmp_data("income-statement", "AAPL", "invalid_key")

        assert result is None

    @patch("main.requests.get")
    def test_fetch_fmp_data_timeout(self, mock_get):
        """Test handling of request timeout"""
        mock_get.side_effect = Exception("Request timeout")

        result = fetch_fmp_data("income-statement", "AAPL", "test_key")

        assert result is None
        # Verify timeout was set
        assert mock_get.call_args[1].get("timeout") == 10

    @patch("main.requests.get")
    def test_fetch_fmp_data_with_opt_args(self, mock_get):
        """Test API call with optional arguments"""
        mock_response = Mock()
        mock_response.json.return_value = [{"data": "test"}]
        mock_get.return_value = mock_response

        fetch_fmp_data("income-statement", "AAPL", "test_key", opt_args="&limit=1")

        call_url = mock_get.call_args[0][0]
        assert "&limit=1" in call_url


class TestRunFmpIngestionImpl:
    """Test suite for run_fmp_ingestion_impl function"""

    @patch("main.pandas_gbq.to_gbq")
    @patch("main.fetch_fmp_data")
    def test_run_fmp_ingestion_impl_success(self, mock_fetch, mock_gbq):
        """Test successful ingestion with mocked data"""
        # Mock successful API responses
        # Each ticker makes 4 API calls (income, balance sheet, cash flow, profile)
        # 24 tickers * 4 calls = 96 calls
        mock_response = [{"symbol": "TEST", "data": "test"}]
        mock_fetch.return_value = mock_response

        result = run_fmp_ingestion_impl()

        assert result["status"] == "success"
        assert result["code"] == 200
        assert "Loaded" in result["message"]
        # Verify BigQuery was called
        assert mock_gbq.call_count >= 4

    @patch("main.pandas_gbq.to_gbq")
    @patch("main.fetch_fmp_data")
    def test_run_fmp_ingestion_impl_no_data(self, mock_fetch, mock_gbq):
        """Test ingestion when API returns no data"""
        mock_fetch.return_value = None

        result = run_fmp_ingestion_impl()

        # Should still return success even if no data
        assert result["status"] == "success"
        assert result["code"] == 200

    @patch("main.pandas_gbq.to_gbq")
    @patch("main.fetch_fmp_data")
    def test_run_fmp_ingestion_impl_bigquery_error(self, mock_fetch, mock_gbq):
        """Test handling of BigQuery errors"""
        mock_fetch.return_value = [{"symbol": "AAPL", "data": "test"}]
        mock_gbq.side_effect = Exception("BigQuery connection failed")

        result = run_fmp_ingestion_impl()

        assert result["status"] == "error"
        assert result["code"] == 500
        assert "BigQuery" in result["message"]

    @patch("main.pandas_gbq.to_gbq")
    @patch("main.fetch_fmp_data")
    def test_run_fmp_ingestion_impl_partial_data(self, mock_fetch, mock_gbq):
        """Test ingestion with some calls returning data and others returning None"""
        # Return data every other time, None for the rest
        mock_fetch.side_effect = [
            [{"symbol": "TEST", "data": "test"}],
            None,
            [{"symbol": "TEST", "data": "test"}],
            None,
        ] * 50  # Enough for 24 tickers with 4 calls each

        result = run_fmp_ingestion_impl()

        assert result["status"] == "success"
        assert mock_gbq.call_count > 0


class TestRunFmpIngestion:
    """Test suite for Cloud Functions entry point"""

    @patch("main.run_fmp_ingestion_impl")
    def test_run_fmp_ingestion_cloud_event_success(self, mock_impl):
        """Test Cloud Functions handler with successful execution"""
        mock_impl.return_value = {"status": "success", "code": 200, "message": "OK"}

        # Create mock cloud event
        mock_event = Mock()
        mock_event.data = None

        result = run_fmp_ingestion(mock_event)

        assert result["status"] == "success"
        assert mock_impl.called

    @patch("main.run_fmp_ingestion_impl")
    def test_run_fmp_ingestion_cloud_event_with_pubsub_message(self, mock_impl):
        """Test Cloud Functions handler with Pub/Sub message data"""
        mock_impl.return_value = {"status": "success", "code": 200, "message": "OK"}

        # Create mock cloud event with Pub/Sub message
        mock_event = Mock()
        message_payload = json.dumps({"action": "ingest"}).encode()
        encoded_payload = base64.b64encode(message_payload)

        mock_event.data = {
            "message": {
                "data": encoded_payload.decode(),
            }
        }

        result = run_fmp_ingestion(mock_event)

        assert result["status"] == "success"
        assert mock_impl.called

    @patch("main.run_fmp_ingestion_impl")
    def test_run_fmp_ingestion_cloud_event_error(self, mock_impl):
        """Test Cloud Functions handler with execution error"""
        mock_impl.side_effect = Exception("Unexpected error")

        mock_event = Mock()
        mock_event.data = None

        result = run_fmp_ingestion(mock_event)

        assert result["status"] == "error"
        assert result["code"] == 500

    @patch("main.run_fmp_ingestion_impl")
    def test_run_fmp_ingestion_cloud_event_empty_data(self, mock_impl):
        """Test Cloud Functions handler with empty cloud event data"""
        mock_impl.return_value = {"status": "success", "code": 200, "message": "OK"}

        mock_event = Mock()
        mock_event.data = {}

        result = run_fmp_ingestion(mock_event)

        assert result["status"] == "success"
        assert mock_impl.called


class TestApiKeyValidation:
    """Test suite for API_KEY validation"""

    def test_api_key_is_set(self):
        """Test that API_KEY is configured"""
        from main import API_KEY

        assert API_KEY is not None
        assert API_KEY == "test_api_key"


class TestTargetTickers:
    """Test suite for TARGET_TICKERS configuration"""

    def test_target_tickers_exists(self):
        """Test that TARGET_TICKERS list is populated"""
        from main import TARGET_TICKERS

        assert len(TARGET_TICKERS) > 0
        assert "AAPL" in TARGET_TICKERS
        assert "MSFT" in TARGET_TICKERS

    def test_target_tickers_format(self):
        """Test that all tickers are valid strings"""
        from main import TARGET_TICKERS

        for ticker in TARGET_TICKERS:
            assert isinstance(ticker, str)
            assert len(ticker) > 0
