import pytest
import os
import json
from unittest.mock import Mock, patch
import pandas as pd

# Set API_KEY before importing main to avoid RuntimeError
os.environ["API_KEY"] = "test_api_key"
os.environ["PROJECT_ID"] = "test-project"

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import (
    get_data,
    run_alphavantage_ingestion_impl,
    run_alphavantage_ingestion,
    TARGET_TICKERS,
)


class TestGetData:
    """Test suite for get_data function"""

    @patch("main.requests.get")
    def test_get_data_success(self, mock_get):
        """Test successful API call with valid quote data"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Global Quote": {
                "01. symbol": "AAPL",
                "02. open": "150.00",
                "03. high": "152.00",
                "04. low": "149.00",
            }
        }
        mock_get.return_value = mock_response

        result = get_data("AAPL", "test_key")

        assert result is not None
        assert result["01. symbol"] == "AAPL"
        mock_get.assert_called_once()
        assert "AAPL" in mock_get.call_args[0][0]

    @patch("main.requests.get")
    def test_get_data_no_quote(self, mock_get):
        """Test API call that returns no Global Quote"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Error Message": "Invalid API call"}
        mock_get.return_value = mock_response

        result = get_data("INVALID", "test_key")

        assert result is None

    @patch("main.requests.get")
    def test_get_data_http_error(self, mock_get):
        """Test handling of HTTP errors"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.json.return_value = {"Error Message": "Server error"}
        mock_get.return_value = mock_response

        result = get_data("AAPL", "test_key")

        # Should return None when no Global Quote in response
        assert result is None

    @patch("main.requests.get")
    def test_get_data_request_exception(self, mock_get):
        """Test handling of request exceptions"""
        mock_get.side_effect = Exception("Connection timeout")

        result = get_data("AAPL", "test_key")

        assert result is None

    @patch("main.requests.get")
    def test_get_data_with_timeout(self, mock_get):
        """Test that timeout is set on requests"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"Global Quote": {"01. symbol": "AAPL"}}
        mock_get.return_value = mock_response

        get_data("AAPL", "test_key")

        # Verify timeout was set
        assert mock_get.call_args[1].get("timeout") == 10


class TestRunAlphaVantageIngestionImpl:
    """Test suite for run_alphavantage_ingestion_impl function"""

    @patch("main.time.sleep")
    @patch("main.get_data")
    def test_run_alphavantage_ingestion_impl_success(self, mock_get_data, mock_sleep):
        """Test successful ingestion with mocked data"""
        mock_get_data.return_value = {
            "01. symbol": "TEST",
            "02. open": "100.00",
            "03. high": "105.00",
            "04. low": "99.00",
            "05. price": "102.00",
            "06. volume": "1000000",
        }

        # Mock DataFrame.to_gbq to avoid actual BigQuery calls
        with patch.object(pd.DataFrame, "to_gbq", return_value=None):
            result = run_alphavantage_ingestion_impl()

        assert result["status"] == "success"
        assert result["code"] == 200
        assert "Loaded" in result["message"]

    @patch("main.time.sleep")
    @patch("main.get_data")
    def test_run_alphavantage_ingestion_impl_no_data(self, mock_get_data, mock_sleep):
        """Test ingestion when API returns no data"""
        mock_get_data.return_value = None

        result = run_alphavantage_ingestion_impl()

        assert result["status"] == "error"
        assert result["code"] == 500
        assert "No data extracted" in result["message"]

    @patch("main.time.sleep")
    @patch("main.get_data")
    def test_run_alphavantage_ingestion_impl_partial_data(
        self, mock_get_data, mock_sleep
    ):
        """Test ingestion with some tickers having data and others not"""
        mock_get_data.side_effect = [
            {"01. symbol": "AAPL", "02. open": "100"},
            None,
            {"01. symbol": "GOOGL", "02. open": "200"},
            None,
        ] * 10

        # Mock DataFrame.to_gbq to avoid actual BigQuery calls
        with patch.object(pd.DataFrame, "to_gbq", return_value=None):
            result = run_alphavantage_ingestion_impl()

        assert result["status"] == "success"
        assert int(result["message"].split()[1]) > 0

    @patch("main.time.sleep")
    @patch("main.get_data")
    def test_run_alphavantage_ingestion_impl_bigquery_error(
        self, mock_get_data, mock_sleep
    ):
        """Test handling of BigQuery errors"""
        mock_get_data.return_value = {
            "01. symbol": "AAPL",
            "02. open": "100.00",
        }

        # Patch DataFrame.to_gbq to fail
        with patch.object(
            pd.DataFrame, "to_gbq", side_effect=Exception("BigQuery error")
        ):
            result = run_alphavantage_ingestion_impl()

        assert result["status"] == "error"
        assert result["code"] == 500
        assert "BigQuery" in result["message"]


class TestRunAlphaVantageIngestion:
    """Test suite for Cloud Functions HTTP entry point"""

    @patch("main.run_alphavantage_ingestion_impl")
    def test_run_alphavantage_ingestion_http_success(self, mock_impl):
        """Test HTTP handler with successful execution"""
        mock_impl.return_value = {"status": "success", "code": 200, "message": "OK"}

        mock_request = Mock()

        response_body, status_code, headers = run_alphavantage_ingestion(mock_request)

        assert response_body["status"] == "success"
        assert status_code == 200
        assert mock_impl.called

    @patch("main.run_alphavantage_ingestion_impl")
    def test_run_alphavantage_ingestion_http_with_request_data(self, mock_impl):
        """Test HTTP handler with request data"""
        mock_impl.return_value = {"status": "success", "code": 200, "message": "OK"}

        mock_request = Mock()
        mock_request.get_json.return_value = {"action": "ingest"}

        response_body, status_code, headers = run_alphavantage_ingestion(mock_request)

        assert response_body["status"] == "success"
        assert status_code == 200
        assert mock_impl.called

    @patch("main.run_alphavantage_ingestion_impl")
    def test_run_alphavantage_ingestion_http_error(self, mock_impl):
        """Test HTTP handler with execution error"""
        mock_impl.side_effect = Exception("Unexpected error")

        mock_request = Mock()

        response_body, status_code, headers = run_alphavantage_ingestion(mock_request)

        assert response_body["status"] == "error"
        assert status_code == 500


class TestDataCleaning:
    """Test suite for Alpha Vantage data cleaning logic"""

    def test_key_cleaning(self):
        """Test cleaning of Alpha Vantage keys"""
        raw_data = {
            "01. symbol": "AAPL",
            "02. open": "150.00",
            "03. high": "152.00",
        }

        # Simulate the cleaning logic
        clean_row = {k.split(". ")[1].replace(" ", "_"): v for k, v in raw_data.items()}

        assert "symbol" in clean_row
        assert "01" not in clean_row
        assert clean_row["symbol"] == "AAPL"

    def test_dataframe_with_timestamp(self):
        """Test adding timestamp to quote data"""
        data = [
            {"symbol": "AAPL", "open": "150.00"},
            {"symbol": "MSFT", "open": "200.00"},
        ]
        df = pd.DataFrame(data)
        df["_ingested_at"] = pd.Timestamp.utcnow()

        assert "_ingested_at" in df.columns
        assert len(df) == 2
        assert pd.notna(df["_ingested_at"][0])


class TestApiKeyValidation:
    """Test suite for API_KEY validation"""

    def test_api_key_is_set(self):
        """Test that API_KEY is configured"""
        from main import API_KEY

        assert API_KEY is not None
        assert API_KEY == "test_api_key"


class TestTargetTickersConfiguration:
    """Test suite for TARGET_TICKERS configuration"""

    def test_target_tickers_exists(self):
        """Test that TARGET_TICKERS list is populated"""
        assert len(TARGET_TICKERS) > 0
        assert "AAPL" in TARGET_TICKERS
        assert "BTC-USD" in TARGET_TICKERS

    def test_target_tickers_format(self):
        """Test that all tickers are valid strings"""
        for ticker in TARGET_TICKERS:
            assert isinstance(ticker, str)
            assert len(ticker) > 0
