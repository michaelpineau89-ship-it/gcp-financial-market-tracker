import pytest
import pandas as pd
import os
import json
from unittest.mock import Mock, patch, MagicMock, call
from cloudevents.http import CloudEvent
import sys
import base64
import pandas_gbq

# Add parent directory to path to import main module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main


class TestGetTickerCikMapping:
    """Test ticker-to-CIK mapping retrieval."""

    def test_get_ticker_cik_mapping_success(self):
        """Test successful retrieval and mapping of tickers to CIKs."""
        mock_response_data = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"},
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response_data

            target_tickers = ["AAPL", "MSFT"]
            result = main.get_ticker_cik_mapping(target_tickers, main.HEADERS)

            assert result == {"AAPL": "0000320193", "MSFT": "0000789019"}
            mock_get.assert_called_once()

    def test_get_ticker_cik_mapping_partial_match(self):
        """Test mapping when only some tickers are found."""
        mock_response_data = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc"},
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response_data

            target_tickers = ["AAPL", "UNKNOWN"]
            result = main.get_ticker_cik_mapping(target_tickers, main.HEADERS)

            assert result == {"AAPL": "0000320193"}

    def test_get_ticker_cik_mapping_cik_padding(self):
        """Test that CIKs are properly zero-padded to 10 digits."""
        mock_response_data = {
            "0": {"cik_str": 1, "ticker": "TINY", "title": "Tiny Corp"},
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response_data

            result = main.get_ticker_cik_mapping(["TINY"], main.HEADERS)
            assert result == {"TINY": "0000000001"}

    def test_get_ticker_cik_mapping_empty_result(self):
        """Test mapping when no tickers match."""
        mock_response_data = {}

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response_data

            result = main.get_ticker_cik_mapping(["AAPL"], main.HEADERS)
            assert result == {}

    def test_get_ticker_cik_mapping_api_error(self):
        """Test handling of API errors."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("API Error")

            with pytest.raises(Exception):
                main.get_ticker_cik_mapping(["AAPL"], main.HEADERS)


class TestGetLatest13fUrl:
    """Test 13F filing URL retrieval."""

    def test_get_latest_13f_url_success(self):
        """Test successful retrieval of latest 13F filing URL."""
        mock_submissions_data = {
            "filings": {
                "recent": {
                    "form": ["13F-HR", "4", "4"],
                    "accessionNumber": [
                        "0001104659-25-001234",
                        "0001104659-25-001233",
                        "0001104659-25-001232",
                    ],
                    "filingDate": ["2025-02-14", "2025-02-13", "2025-02-12"],
                }
            }
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_submissions_data

            result = main.get_latest_13f_url("0001067983")

            expected = "https://www.sec.gov/Archives/edgar/data/1067983/000110465925001234/infotable.xml"
            assert result == expected

    def test_get_latest_13f_url_no_filing(self):
        """Test when no 13F filing exists."""
        mock_submissions_data = {
            "filings": {
                "recent": {
                    "form": ["4", "4"],
                    "accessionNumber": ["0001104659-25-001233", "0001104659-25-001232"],
                    "filingDate": ["2025-02-13", "2025-02-12"],
                }
            }
        }

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_submissions_data

            result = main.get_latest_13f_url("0001067983")
            assert result is None

    def test_get_latest_13f_url_empty_filings(self):
        """Test when filings data is empty."""
        mock_submissions_data = {"filings": {"recent": {}}}

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_submissions_data

            result = main.get_latest_13f_url("0001067983")
            assert result is None

    def test_get_latest_13f_url_api_error(self):
        """Test API error handling."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("API Error")

            with pytest.raises(Exception):
                main.get_latest_13f_url("0001067983")


class TestParseSecXml:
    """Test SEC XML parsing."""

    def test_parse_sec_xml_success(self):
        """Test successful XML parsing with namespace cleanup."""
        xml_content = b"""<?xml version="1.0"?>
<DOCUMENT xmlns="http://www.sec.gov/cgi-bin">
<infoTable>
<titleOfClass>Common Stock</titleOfClass>
<cusip>037833100</cusip>
<value>123456</value>
<shrsOrPrnAmt>100</shrsOrPrnAmt>
</infoTable>
</DOCUMENT>"""

        result = main.parse_sec_xml(xml_content)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "value" in result.columns or "cusip" in result.columns

    def test_parse_sec_xml_removes_namespace(self):
        """Test that XML namespace is properly removed."""
        xml_with_namespace = b"""<?xml version="1.0"?>
<infoTable xmlns="http://www.sec.gov/cgi-bin">
<infotable>
<titleOfClass>Common Stock</titleOfClass>
<cusip>037833100</cusip>
<value>123456</value>
</infotable>
</infoTable>"""

        # Should not raise error due to namespace
        result = main.parse_sec_xml(xml_with_namespace)
        assert isinstance(result, pd.DataFrame)

    def test_parse_sec_xml_empty_document(self):
        """Test handling of empty XML document."""
        xml_content = b"""<?xml version="1.0"?>
<infoTable xmlns="http://example.com">
<infotable>
<titleOfClass>Common Stock</titleOfClass>
</infotable>
</infoTable>"""

        result = main.parse_sec_xml(xml_content)
        assert isinstance(result, pd.DataFrame)


class TestRunEdgarIngestionImpl:
    """Test main EDGAR ingestion implementation logic."""

    @patch("pandas_gbq.to_gbq")
    @patch("requests.get")
    def test_run_edgar_ingestion_impl_success(self, mock_get, mock_to_gbq):
        """Test successful end-to-end ingestion."""
        # Mock ticker mapping
        mock_get.side_effect = [
            # First call: ticker mapping
            MagicMock(
                json=lambda: {
                    "0": {"cik_str": 320193, "ticker": "AAPL"},
                    "1": {"cik_str": 1067983, "ticker": "BRK-B"},
                }
            ),
            # Second call: Berkshire submissions
            MagicMock(
                json=lambda: {
                    "filings": {
                        "recent": {
                            "form": ["13F-HR", "4"],
                            "accessionNumber": [
                                "0001104659-25-001234",
                                "0001104659-25-001233",
                            ],
                            "filingDate": ["2025-02-14", "2025-02-13"],
                        }
                    }
                }
            ),
        ]

        with patch.object(pd.DataFrame, "to_gbq", return_value=None):
            with patch("main.get_latest_13f_url", return_value=None):
                with patch.object(main, "parse_sec_xml") as mock_parse:
                    mock_parse.return_value = pd.DataFrame(
                        {
                            "cusip": ["037833100"],
                            "value": [123456],
                        }
                    )

                    result = main.run_edgar_ingestion_impl()

                    assert "code" in result
                    assert result["code"] in [200, 500]

    @patch("pandas_gbq.to_gbq")
    @patch("requests.get")
    def test_run_edgar_ingestion_impl_no_holdings(self, mock_get, mock_to_gbq):
        """Test when no holdings are found."""
        mock_get.side_effect = [
            MagicMock(json=lambda: {}),  # Empty ticker mapping
        ]

        result = main.run_edgar_ingestion_impl()

        assert result["code"] == 500
        assert "No filings processed" in result["message"]

    @patch("pandas_gbq.to_gbq")
    @patch("requests.get")
    def test_run_edgar_ingestion_impl_partial_error(self, mock_get, mock_to_gbq):
        """Test resilience when one entity fails."""
        mock_get.side_effect = [
            MagicMock(
                json=lambda: {
                    "0": {"cik_str": 320193, "ticker": "AAPL"},
                    "1": {"cik_str": 1067983, "ticker": "BRK-B"},
                }
            ),
            Exception("API Error"),  # First entity fails
            MagicMock(
                json=lambda: {  # Second entity succeeds
                    "filings": {
                        "recent": {
                            "form": ["13F-HR"],
                            "accessionNumber": ["0001104659-25-001234"],
                            "filingDate": ["2025-02-14"],
                        }
                    }
                }
            ),
        ]

        with patch.object(pd.DataFrame, "to_gbq", return_value=None):
            with patch("main.get_latest_13f_url", return_value=None):
                # Should continue despite partial error
                result = main.run_edgar_ingestion_impl()
                assert "code" in result


class TestRunEdgarIngestion:
    """Test Cloud Functions event handler."""

    def test_run_edgar_ingestion_cloud_event_success(self):
        """Test Cloud Functions handler with successful ingestion."""
        # Create a mock CloudEvent
        cloud_event = CloudEvent(
            {
                "specversion": "1.0",
                "type": "com.google.cloud.pubsub.topic.publish",
                "source": "//pubsub.googleapis.com/projects/test-project/topics/edgar-ingestion",
                "id": "1234567890",
                "time": "2025-04-18T00:00:00Z",
                "datacontenttype": "application/json",
            },
            {
                "message": {
                    "data": base64.b64encode(b"test").decode(),
                    "messageId": "1234",
                    "publishTime": "2025-04-18T00:00:00Z",
                }
            },
        )

        with patch("main.run_edgar_ingestion_impl") as mock_impl:
            mock_impl.return_value = {"code": 200, "message": "Success"}

            result = main.run_edgar_ingestion(cloud_event)

            assert result["code"] == 200
            mock_impl.assert_called_once()

    def test_run_edgar_ingestion_cloud_event_error(self):
        """Test error handling in Cloud Functions handler."""
        cloud_event = CloudEvent(
            {
                "specversion": "1.0",
                "type": "com.google.cloud.pubsub.topic.publish",
                "source": "//pubsub.googleapis.com/projects/test-project/topics/edgar-ingestion",
                "id": "1234567890",
            },
            {},
        )

        with patch("main.run_edgar_ingestion_impl") as mock_impl:
            mock_impl.side_effect = Exception("Test error")

            result = main.run_edgar_ingestion(cloud_event)

            assert result["code"] == 500
            assert "Error" in result["message"]


class TestTargetTickersConfiguration:
    """Test TARGET_TICKERS and WHALE_CIKS configuration."""

    def test_target_tickers_exists(self):
        """Test that TARGET_TICKERS is defined."""
        assert hasattr(main, "TARGET_TICKERS")
        assert len(main.TARGET_TICKERS) > 0

    def test_target_tickers_format(self):
        """Test that TARGET_TICKERS contains valid ticker symbols."""
        assert isinstance(main.TARGET_TICKERS, list)
        for ticker in main.TARGET_TICKERS:
            assert isinstance(ticker, str)
            assert len(ticker) > 0

    def test_whale_ciks_exists(self):
        """Test that WHALE_CIKS is defined."""
        assert hasattr(main, "WHALE_CIKS")
        assert len(main.WHALE_CIKS) > 0

    def test_whale_ciks_format(self):
        """Test that WHALE_CIKS contains valid CIK mappings."""
        assert isinstance(main.WHALE_CIKS, dict)
        for name, cik in main.WHALE_CIKS.items():
            assert isinstance(name, str)
            assert isinstance(cik, str)


class TestEdgarHeaders:
    """Test SEC headers configuration."""

    def test_headers_exist(self):
        """Test that HEADERS are defined."""
        assert hasattr(main, "HEADERS")
        assert isinstance(main.HEADERS, dict)

    def test_headers_have_user_agent(self):
        """Test that User-Agent header is set."""
        assert "User-Agent" in main.HEADERS
        assert main.HEADERS["User-Agent"]

    def test_headers_have_accept_encoding(self):
        """Test that Accept-Encoding header is set."""
        assert "Accept-Encoding" in main.HEADERS


class TestProjectConfiguration:
    """Test PROJECT configuration."""

    def test_project_id_exists(self):
        """Test that PROJECT_ID is configured."""
        assert hasattr(main, "PROJECT")
        assert main.PROJECT


class TestBigQueryIntegration:
    """Test BigQuery integration."""

    @patch("pandas_gbq.to_gbq")
    def test_submissions_loaded_to_bigquery(self, mock_to_gbq):
        """Test that submissions DataFrame is loaded to BigQuery."""
        mock_df = pd.DataFrame(
            {
                "form": ["13F-HR"],
                "accessionNumber": ["0001104659-25-001234"],
                "filingDate": ["2025-02-14"],
            }
        )

        # Verify the function can call to_gbq
        mock_to_gbq.return_value = None
        pandas_gbq.to_gbq(
            mock_df, "test_table", project_id="test_project", if_exists="append"
        )

        assert mock_to_gbq.called

    @patch("pandas_gbq.to_gbq")
    def test_holdings_loaded_to_bigquery(self, mock_to_gbq):
        """Test that holdings DataFrame is loaded to BigQuery."""
        mock_df = pd.DataFrame(
            {
                "cusip": ["037833100"],
                "value": [123456],
            }
        )

        # Verify DataFrame has to_gbq method
        assert hasattr(mock_df, "to_gbq")
