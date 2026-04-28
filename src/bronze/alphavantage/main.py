import os
import time
import requests
import pandas as pd
import logging
import functions_framework
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# CICD Test: v3.0.7 (Cloud Functions)

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

PROJECT_ID = os.environ.get("PROJECT_ID", "mike-personal-portfolio")
API_KEY = os.environ.get("API_KEY")

# Fail fast if API_KEY is missing
if not API_KEY:
    logger.error("FATAL: API_KEY environment variable not set from Secret Manager.")
    raise RuntimeError(
        "API_KEY must be configured in Secret Manager and mapped to env var"
    )

logger.info(f"Initialized Alpha Vantage (Cloud Functions) | Project: {PROJECT_ID}")
logger.info(f"API Key configured: {bool(API_KEY)}")


def get_data(ticker, key):
    """Fetch quote data from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}"
    try:
        r = requests.get(url, timeout=10)

        if r.status_code == 200:
            logging.debug(f"Query Success | Ticker: {ticker} | Response: 200")
        elif r.status_code == 429:
            logging.warning(
                f"Rate limit hit | Ticker: {ticker} | Status_Code: 429 | Response: {r.text}"
            )
            raise requests.exceptions.HTTPError("429 Too Many Requests")
        else:
            logging.warning(
                f"Query failed | Ticker: {ticker} | Status_Code: {r.status_code} | Response: {r.text}"
            )

        data = r.json()
        # Safe extraction in case the API rate-limits us and returns an Error/Information message
        return data.get("Global Quote", None)
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error fetching data for {ticker}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None


def run_alphavantage_ingestion_impl():
    """Core ingestion logic for Alpha Vantage quotes"""
    logging.info("=" * 60)
    logging.info("Starting Alpha Vantage Quote Ingestion...")
    logging.info(f"Processing {len(TARGET_TICKERS)} tickers")
    logging.info("=" * 60)

    master_data_list = []

    for count, ticker in enumerate(TARGET_TICKERS, start=1):
        logging.info(f"[{count}/{len(TARGET_TICKERS)}] Fetching {ticker}...")
        quote_data = get_data(ticker, API_KEY)

        if quote_data:
            master_data_list.append(quote_data)
            logging.info(f"✓ Successfully fetched {ticker}")
        else:
            logging.warning(f"Failed to fetch data for {ticker}. Check API limits.")

        if count < len(TARGET_TICKERS):
            logging.info("Waiting 20 seconds due to rate limits")
            time.sleep(20)

    logging.info("=" * 60)
    logging.info("Loading Data to BigQuery...")

    try:
        # Batch Load to BigQuery
        if master_data_list:
            logging.info(
                f"Collected {len(master_data_list)} records. Processing and cleaning data..."
            )

            # Clean the Alpha Vantage keys (they look like "01. symbol", which BigQuery hates)
            clean_data = []
            for row in master_data_list:
                clean_row = {
                    k.split(". ")[1].replace(" ", "_"): v for k, v in row.items()
                }
                clean_data.append(clean_row)

            df = pd.DataFrame(clean_data)
            df["_ingested_at"] = pd.Timestamp.utcnow()
            logging.info(
                f"Data cleaned. Uploading {len(df)} rows to BigQuery table: market_tracker.bronze_alpha_quotes"
            )
            # Write to BQ
            df.to_gbq(
                destination_table="market_tracker.bronze_alpha_quotes",
                project_id=PROJECT_ID,
                if_exists="append",
            )
            logging.info(f"✓ Successfully loaded {len(df)} rows to BigQuery.")
            logging.info("=" * 60)
            logging.info("✓ Alpha Vantage Ingestion Complete")
            logging.info("=" * 60)

            return {
                "status": "success",
                "message": f"Loaded {len(df)} records",
                "code": 200,
            }
        else:
            logging.error("No data extracted from Alpha Vantage API")
            return {
                "status": "error",
                "message": "No data extracted",
                "code": 500,
            }
    except Exception as e:
        logging.error(f"BigQuery Load Failed: {e}")
        logging.error("=" * 60)
        return {"status": "error", "message": str(e), "code": 500}


# Cloud Functions entry point (HTTP triggered)
@functions_framework.http
def run_alphavantage_ingestion(request):
    """
    Cloud Functions entry point triggered by HTTP.
    Can be invoked by Cloud Scheduler or direct HTTP requests.
    """
    logging.info("HTTP request received")

    try:
        result = run_alphavantage_ingestion_impl()
        logging.info(f"Result: {result}")
        return (
            {"status": "success", "data": result},
            200,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        logging.error(f"Unexpected error in Cloud Function: {e}")
        return (
            {"status": "error", "message": str(e), "code": 500},
            500,
            {"Content-Type": "application/json"},
        )
