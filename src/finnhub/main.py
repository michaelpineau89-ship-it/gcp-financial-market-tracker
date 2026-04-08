"""
Finnhub Data Ingestion Service

Fetches market data from Finnhub API and stores it in Google BigQuery.
Supports: company news, recommendation trends, basic financials, and insider sentiment.

Environment Variables:
    API_KEY: Finnhub API key
    START: Start date (YYYY-MM-DD)
    END: End date (YYYY-MM-DD)
    TICKERS: Comma-separated list of stock tickers
    PORT: Flask server port (default: 8080)
    PROJECT: GCP project ID (default: mike-personal-portfolio)
"""

import finnhub
import os
import pandas as pd
import datetime
from flask import Flask

# CICD Test: v1.0.1

import logging
from time import sleep
import pandas_gbq

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration - loaded from environment variables
API_KEY = os.environ.get("API_KEY")  # Finnhub API authentication key
DATE_START = os.environ.get("START")  # Start date for data retrieval (YYYY-MM-DD)
DATE_END = os.environ.get("END")  # End date for data retrieval (YYYY-MM-DD)
TICKERS_STR = os.environ.get("TICKERS", "")  # Comma-separated ticker symbols
PORT = os.environ.get("PORT", "8080")  # Flask server port
PROJECT = os.environ.get("PROJECT", "mike-personal-portfolio")  # GCP BigQuery project

# Parse ticker string into a list, filtering empty entries
TICKERS = [t.strip() for t in TICKERS_STR.split(",") if t.strip()]

# Log initialization
logging.info("=" * 60)
logging.info("Finnhub Data Ingestion Service Initialized")
logging.info(f"Project: {PROJECT} | API Key configured: {bool(API_KEY)}")
logging.info(f"Date range: {DATE_START} to {DATE_END}")
logging.info(
    f"Monitoring {len(TICKERS)} tickers: {', '.join(TICKERS[:5])}{'...' if len(TICKERS) > 5 else ''}"
)
logging.info(f"Server port: {PORT}")
logging.info("=" * 60)


def make_api_call(client, method, *args, max_retries=3, **kwargs):
    """
    Wrapper for Finnhub API calls with retry logic and error handling.

    Handles rate limiting (429) with automatic retries, re-raises auth errors (401),
    and returns None on failure after max retries exhausted.

    Args:
        client: Finnhub client instance
        method: API method to call (e.g., client.company_news)
        *args: Positional arguments for the API method
        max_retries: Maximum retry attempts for rate-limited requests (default: 3)
        **kwargs: Keyword arguments for the API method

    Returns:
        API response data, or None if all retries exhausted
    """
    for attempt in range(max_retries):
        try:
            return method(*args, **kwargs)
        except finnhub.exceptions.FinnhubAPIException as e:
            # 429 = Rate limit exceeded - wait and retry
            if e.status_code == 429:
                if attempt < max_retries - 1:
                    logging.warning(
                        f"Rate limited, retrying in 60s (attempt {attempt + 1}/{max_retries})"
                    )
                    sleep(60)
                    continue
                logging.error("Max retries exceeded for rate limit")
                return None
            # 401 = Invalid API key - fail immediately
            elif e.status_code == 401:
                logging.error("Invalid API key")
                raise
            # Other API errors - fail immediately
            else:
                logging.error(f"API error {e.status_code}: {e}")
                raise
        # Catch-all for unexpected errors
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
    return None


@app.route("/", methods=["POST"])
def run():
    """
    Main entry point for the Flask app - fetches and stores Finnhub data.

    Makes API calls for each ticker and stores results in BigQuery:
    - Company news (bronze_finnhub_news)
    - Recommendation trends (bronze_finnhub_recommendations)
    - Basic financials (bronze_finnhub_financials)
    - Insider sentiment (bronze_finnhub_insider)

    Returns:
        JSON response with status "success" on completion
    """
    logging.info("=" * 60)
    logging.info("Starting Finnhub Ingestion Batch")
    logging.info(f"Processing {len(TICKERS)} tickers")
    logging.info("=" * 60)
    client = finnhub.Client(api_key=API_KEY)

    # Initialize empty DataFrames for each data type
    news = pd.DataFrame()
    recommendation = pd.DataFrame()
    financials = pd.DataFrame()
    insider = pd.DataFrame()

    # Iterate through each ticker symbol
    for idx, ticker in enumerate(TICKERS, start=1):
        logging.info(f"[{idx}/{len(TICKERS)}] Processing {ticker}...")
        try:
            # 1. NEWS: Company news from Finnhub
            # Returns a list of news items - manually inject symbol for BigQuery identification
            news_raw = make_api_call(
                client, client.company_news, ticker, DATE_START, DATE_END
            )
            if news_raw:
                news_q = pd.DataFrame(news_raw)
                news_q["symbol"] = (
                    ticker  # Inject the symbol so BQ knows who the news is about
                )
                news = pd.concat([news, news_q], ignore_index=True)

            # 2. RECOMMENDATIONS: Analyst recommendation trends
            # Returns a list - already contains the symbol field
            rec_raw = make_api_call(client, client.recommendation_trends, ticker)
            if rec_raw:
                recommendation = pd.concat(
                    [recommendation, pd.DataFrame(rec_raw)], ignore_index=True
                )

            # 3. FINANCIALS: Basic financial metrics
            # Returns a nested dict with 'metric' key - pd.DataFrame flattens it automatically
            fin_raw = make_api_call(
                client, client.company_basic_financials, ticker, "all"
            )
            if fin_raw and "metric" in fin_raw:
                fin_q = pd.DataFrame(fin_raw)
                financials = pd.concat([financials, fin_q], ignore_index=True)

            # 4. INSIDER SENTIMENT: Insider trading sentiment data
            # Returns a dict with 'data' array - extract the array before creating DataFrame
            insider_raw = make_api_call(
                client, client.stock_insider_sentiment, ticker, DATE_START, DATE_END
            )
            if insider_raw and "data" in insider_raw and len(insider_raw["data"]) > 0:
                insider_q = pd.DataFrame(insider_raw["data"])
                insider = pd.concat([insider, insider_q], ignore_index=True)

        except Exception as e:
            # Log error and continue with next ticker rather than failing entirely
            logging.error(f"Failed to process ticker {ticker}: {e}")
            continue

    # Write all DataFrames to BigQuery (only if non-empty)
    # Using if_exists="append" to preserve historical data
    logging.info("=" * 60)
    logging.info("Loading collected data to BigQuery...")
    logging.info(
        f"News records: {len(news)} | Recommendations: {len(recommendation)} | Financials: {len(financials)} | Insider: {len(insider)}"
    )

    if not news.empty:
        news["_ingested_at"] = pd.Timestamp.utcnow()
        pandas_gbq.to_gbq(
            news,
            "market_tracker.bronze_finnhub_news",
            project_id=PROJECT,
            if_exists="append",
        )
        logging.info(f"✓ Loaded {len(news)} news records to bronze_finnhub_news")
    if not recommendation.empty:
        recommendation["_ingested_at"] = pd.Timestamp.utcnow()
        pandas_gbq.to_gbq(
            recommendation,
            "market_tracker.bronze_finnhub_recommendations",
            project_id=PROJECT,
            if_exists="append",
        )
        logging.info(
            f"✓ Loaded {len(recommendation)} recommendation records to bronze_finnhub_recommendations"
        )
    if not financials.empty:
        financials["_ingested_at"] = pd.Timestamp.utcnow()
        pandas_gbq.to_gbq(
            financials,
            "market_tracker.bronze_finnhub_financials",
            project_id=PROJECT,
            if_exists="append",
        )
        logging.info(
            f"✓ Loaded {len(financials)} financial records to bronze_finnhub_financials"
        )
    if not insider.empty:
        insider["_ingested_at"] = pd.Timestamp.utcnow()
        pandas_gbq.to_gbq(
            insider,
            "market_tracker.bronze_finnhub_insider",
            project_id=PROJECT,
            if_exists="append",
        )
        logging.info(
            f"✓ Loaded {len(insider)} insider records to bronze_finnhub_insider"
        )

    logging.info("=" * 60)
    logging.info("✓ Finnhub Ingestion Complete")
    logging.info("=" * 60)
    return {"status": "success"}, 200


# Entry point - starts the Flask development server\nif __name__ == \"__main__\":\n    logging.info(f\"🚀 Starting Finnhub Data Ingestion Service on port {PORT}\")\n    app.run(\"0.0.0.0\", int(PORT))
