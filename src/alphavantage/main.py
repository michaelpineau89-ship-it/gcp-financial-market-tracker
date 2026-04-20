import os
import time
import requests
import pandas as pd
import logging
import functions_framework
import base64
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# CICD Test: v2.0.0 (Cloud Functions)

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
    logger.error("FATAL: API_KEY environment variable not set from Secret Manager")
    raise RuntimeError(
        "API_KEY must be configured in Secret Manager and mapped to env var"
    )

logger.info(f"Initialized Alpha Vantage (Cloud Functions) | Project: {PROJECT_ID}")
logger.info(f"API Key configured: {bool(API_KEY)}.")


def get_data(ticker, key):
    """Fetch quote data from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}"
    try:
        r = requests.get(url, timeout=10)

        if r.status_code == 200:
            logging.debug(f"Query Success | Ticker: {ticker} | Response: 200")
        else:
            logging.warning(
                f"Query failed | Ticker: {ticker} | Status_Code: {r.status_code} | Response: {r.text}"
            )

        data = r.json()
        # Safe extraction in case the API rate-limits us and returns an Error/Information message
        return data.get("Global Quote", None)
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
            logging.info("Waiting 15 seconds due to rate limits")
            time.sleep(15)

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


# Cloud Functions entry point (Pub/Sub triggered)
@functions_framework.cloud_event
def run_alphavantage_ingestion(cloud_event):
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

        result = run_alphavantage_ingestion_impl()
        logging.info(f"Result: {result}")
        return result

    except Exception as e:
        logging.error(f"Unexpected error in Cloud Function: {e}")
        return {"status": "error", "message": str(e), "code": 500}
