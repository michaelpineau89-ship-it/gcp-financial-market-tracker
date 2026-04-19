"""
Finnhub Data Ingestion Service

Cloud Functions entry point for fetching market data from Finnhub API and storing it in Google BigQuery.
Supports: company news, recommendation trends, basic financials, and insider sentiment.

Environment Variables:
    API_KEY: Finnhub API key (loaded from Secret Manager)
    PROJECT: GCP project ID (default: mike-personal-portfolio)
"""

import finnhub
import os
import pandas as pd
import datetime
import logging
import functions_framework
import base64
import json
from time import sleep
import pandas_gbq

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# CICD Test: v2.0.0 (Cloud Functions)

# Configuration - loaded from environment variables
API_KEY = os.environ.get("API_KEY")
PROJECT = os.environ.get("PROJECT_ID", "mike-personal-portfolio")

# Fail fast if API_KEY is missing
if not API_KEY:
    logging.error("FATAL: API_KEY environment variable not set from Secret Manager")
    raise RuntimeError("API_KEY must be configured in Secret Manager and mapped to env var")

DATE_START = datetime.date.today() - datetime.timedelta(days=14)
DATE_END = datetime.date.today()

TARGET_TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "AMD",
    "AVGO",
    "PLTR",
    "JPM",
    "BRK-B",
    "BNS",
    "BMO",
    "RY",
    "HOOD",
    "TSLA",
    "TM",
    "GM",
    "JNJ",
    "LLY",
    "GEN",
    "BTC-USD",
    "ETH-USD",
    "SPY",
    "QQQ",
]

# Log initialization
logging.info("=" * 60)
logging.info("Finnhub Data Ingestion Service Initialized (Cloud Functions)")
logging.info(f"Project: {PROJECT} | API Key configured: {bool(API_KEY)}")
logging.info(f"Date range: {DATE_START} to {DATE_END}")
logging.info(
    f"Monitoring {len(TARGET_TICKERS)} tickers: {', '.join(TARGET_TICKERS[:5])}{'...' if len(TARGET_TICKERS) > 5 else ''}"
)
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


def run_finnhub_ingestion_impl():
    """
    Core ingestion logic for fetching and storing Finnhub data.

    Makes API calls for each ticker and stores results in BigQuery:
    - Company news (bronze_finnhub_news)
    - Recommendation trends (bronze_finnhub_recommendations)
    - Basic financials (bronze_finnhub_financials)
    - Insider sentiment (bronze_finnhub_insider)

    Returns:
        dict: Status response with code for Cloud Functions or Flask
    """
    logging.info("=" * 60)
    logging.info("Starting Finnhub Ingestion Batch")
    logging.info(f"Processing {len(TARGET_TICKERS)} tickers")
    logging.info("=" * 60)
    client = finnhub.Client(api_key=API_KEY)

    # Initialize empty DataFrames for each data type
    news = pd.DataFrame()
    recommendation = pd.DataFrame()
    financials = pd.DataFrame()
    insider = pd.DataFrame()

    # Iterate through each ticker symbol
    for idx, ticker in enumerate(TARGET_TICKERS, start=1):
        logging.info(f"[{idx}/{len(TARGET_TICKERS)}] Processing {ticker}...")
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

    try:
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
        return {"status": "success", "code": 200, "message": "Finnhub ingestion complete"}

    except Exception as e:
        logging.error(f"BigQuery Load Failed: {e}")
        logging.error("=" * 60)
        return {"status": "error", "message": str(e), "code": 500}


# Cloud Functions entry point (Pub/Sub triggered)
@functions_framework.cloud_event
def run_finnhub_ingestion(cloud_event):
    """
    Cloud Functions entry point triggered by Pub/Sub.
    When a message is published to the topic, this function is invoked.
    """
    logging.info("Cloud Event received from Pub/Sub")

    try:
        # Parse the Pub/Sub message (optional - useful for debugging)
        if cloud_event.data:
            pubsub_message = cloud_event.data
            if isinstance(pubsub_message, dict) and "message" in pubsub_message:
                message_data = pubsub_message["message"].get("data")
                if message_data:
                    decoded_message = base64.b64decode(message_data).decode()
                    logging.info(f"Pub/Sub message: {decoded_message}")

        result = run_finnhub_ingestion_impl()
        logging.info(f"Result: {result}")
        return result

    except Exception as e:
        logging.error(f"Unexpected error in Cloud Function: {e}")
        return {"status": "error", "message": str(e), "code": 500}
