import os
import time
import requests
import pandas as pd
from flask import Flask, request
import logging

# Initialize the Flask app for Cloud Run
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)

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

logger.info(f"Initialized Alpha Vantage with Project: {PROJECT_ID}")
logger.info(f"API Key configured: {bool(API_KEY)}")


def get_data(ticker, key):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}"
    r = requests.get(url)

    if r.status_code == 200:
        logging.debug(f"Query Success | Ticker: {ticker} | Response: 200")
    else:
        logging.warning(
            f"Query failed | Ticker: {ticker} | Status_Code: {r.status_code} | Response: {r.text} "
        )

    data = r.json()

    # Safe extraction in case the API rate-limits us and returns an Error/Information message instead
    return data.get("Global Quote", None)


@app.route("/", methods=["POST"])
def run_ingestion():
    logging.info("Starting Daily Alpha Vantage Ingestion...")
    logging.info(f"Processing {len(TARGET_TICKERS)} tickers")

    master_data_list = []

    for count, ticker in enumerate(TARGET_TICKERS, start=1):
        logging.info(f"[{count}/{len(TARGET_TICKERS)}] Fetching {ticker}...")
        quote_data = get_data(ticker, API_KEY)

        if quote_data:
            master_data_list.append(quote_data)
            logging.info(f"Successfully fetched {ticker}")

        else:
            logging.warning(f"Failed to fetch data for {ticker}. Check API limits.")

        if count < len(TARGET_TICKERS):
            logging.info(
                "Waiting 15 seconds due to rate limits and concurrency requirements"
            )
            time.sleep(15)

    logging.info("Loading Data to BQ")
    # Batch Load to BigQuery
    if master_data_list:
        logging.info(f"Collected {len(master_data_list)} records. Processing and cleaning data...")

        # Clean the Alpha Vantage keys (they look like "01. symbol", which BigQuery hates)
        clean_data = []
        for row in master_data_list:
            clean_row = {k.split(". ")[1].replace(" ", "_"): v for k, v in row.items()}
            clean_data.append(clean_row)

        df = pd.DataFrame(clean_data)
        df["_ingested_at"] = pd.Timestamp.utcnow()
        logging.info(f"Data cleaned. Uploading {len(df)} rows to BigQuery table: market_tracker.bronze_alpha_quotes")
        # Write to BQ once!
        df.to_gbq(
            destination_table="market_tracker.bronze_alpha_quotes",
            project_id=PROJECT_ID,
            if_exists="append",
        )
        logging.info(f"✓ Successfully loaded {len(df)} rows to BigQuery.")
        logging.info("Alpha Vantage Ingestion Complete")
        return f"Loaded {len(df)} records", 200
    else:
        logging.error("No data extracted from Alpha Vantage API")
        return "No data extracted", 500


# Required for local testing and Cloud Run port binding
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"🚀 Starting Alpha Vantage Ingestion Service on port {port}")
    app.run(host="0.0.0.0", port=port)
