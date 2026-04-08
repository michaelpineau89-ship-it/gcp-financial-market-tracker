import os
import requests
import pandas as pd
from flask import Flask
import logging
import pandas_gbq

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

API_KEY = os.environ.get("API_KEY")  # Make sure to set this locally!
PROJECT = os.environ.get("PROJECT_ID", "mike-personal-portfolio")
PORT = int(os.environ.get("PORT", 8080))

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


def fetch_fmp_data(endpoint, ticker, key, opt_args=""):
    """Generic fetcher for FMP endpoints"""
    url = f"https://financialmodelingprep.com/stable/{endpoint}?symbol={ticker}{opt_args}&apikey={key}"
    try:
        r = requests.get(url)
        r.raise_for_status()  # Catches 401s and 500s
        data = r.json()
        # FMP returns an empty list [] if the ticker is invalid or unsupported (like crypto)
        return data if isinstance(data, list) and len(data) > 0 else None
    except Exception as e:
        logging.error(f"Error fetching {endpoint} for {ticker}: {e}")
        return None


@app.route("/", methods=["POST"])
def run_fmp_ingestion():
    logging.info("Starting FMP Fundamental Ingestion...")

    income_master = []
    balance_master = []
    cashflow_master = []
    profile_master = []
    ownership_master = []

    for ticker in TARGET_TICKERS:
        logging.info(f"Fetching fundamentals for {ticker}...")

        # 1. Income Statement
        inc_data = fetch_fmp_data(
            "income-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if inc_data:
            income_master.extend(inc_data)

        # 2. Balance Sheet
        bal_data = fetch_fmp_data(
            "balance-sheet-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if bal_data:
            balance_master.extend(bal_data)

        # 3. Cash Flow Statement
        cf_data = fetch_fmp_data(
            "cash-flow-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if cf_data:
            cashflow_master.extend(cf_data)

        pr_data = fetch_fmp_data("profile", ticker, API_KEY)
        if pr_data:
            profile_master.extend(pr_data)
    # Batch Load to BigQuery
    try:
        if income_master:
            df_inc = pd.DataFrame(income_master).assign(
                _ingested_at=pd.Timestamp.utcnow()
            )
            pandas_gbq.to_gbq(
                df_inc,
                "market_tracker.bronze_fmp_income",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(f"Loaded {len(df_inc)} rows to Income table.")

        if balance_master:
            df_bal = pd.DataFrame(balance_master).assign(
                _ingested_at=pd.Timestamp.utcnow()
            )
            pandas_gbq.to_gbq(
                df_bal,
                "market_tracker.bronze_fmp_balance",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(f"Loaded {len(df_bal)} rows to Balance table.")

        if cashflow_master:
            df_cf = pd.DataFrame(cashflow_master).assign(
                _ingested_at=pd.Timestamp.utcnow()
            )
            pandas_gbq.to_gbq(
                df_cf,
                "market_tracker.bronze_fmp_cashflow",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(f"Loaded {len(df_cf)} rows to Cash Flow table.")

        if profile_master:
            df_pr = pd.DataFrame(profile_master).assign(
                _ingested_at=pd.Timestamp.utcnow()
            )
            pandas_gbq.to_gbq(
                df_pr,
                "market_tracker.bronze_fmp_profile",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(f"Loaded {len(df_pr)} rows to profile table.")

        return "FMP Ingestion Complete", 200

    except Exception as e:
        logging.error(f"BigQuery Load Failed: {e}")
        return "Database Error", 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
