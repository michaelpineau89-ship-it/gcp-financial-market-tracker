import os
import requests
import pandas as pd
import logging
import pandas_gbq
import functions_framework

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# CICD Test: v3.0.2 (Cloud Functions)

API_KEY = os.environ.get("API_KEY")
PROJECT = os.environ.get("PROJECT_ID", "mike-personal-portfolio")

# Fail fast if API_KEY is missing
if not API_KEY:
    logger.error("FATAL: API_KEY environment variable not set from Secret Manager")
    raise RuntimeError(
        "API_KEY must be configured in Secret Manager and mapped to env var"
    )

logger.info(f"Initialized FMP Ingestion | Project: {PROJECT}")
logger.info(f"API Key configured: {bool(API_KEY)}")

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
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        # FMP returns an empty list [] if the ticker is invalid or unsupported (like crypto)
        return data if isinstance(data, list) and len(data) > 0 else None
    except Exception as e:
        logging.error(f"Error fetching {endpoint} for {ticker}: {e}")
        return None


def run_fmp_ingestion_impl():
    """Core ingestion logic for Cloud Functions"""
    logging.info("=" * 60)
    logging.info("Starting FMP Fundamental Ingestion...")
    logging.info(f"Processing {len(TARGET_TICKERS)} tickers")
    logging.info("=" * 60)

    income_master = []
    balance_master = []
    cashflow_master = []
    profile_master = []

    for idx, ticker in enumerate(TARGET_TICKERS, start=1):
        logging.info(
            f"[{idx}/{len(TARGET_TICKERS)}] Fetching fundamentals for {ticker}..."
        )

        # 1. Income Statement
        logging.debug(f"  Fetching income statement for {ticker}")
        inc_data = fetch_fmp_data(
            "income-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if inc_data:
            income_master.extend(inc_data)
            logging.debug(f"  ✓ Found {len(inc_data)} income statement records")

        # 2. Balance Sheet
        logging.debug(f"  Fetching balance sheet for {ticker}")
        bal_data = fetch_fmp_data(
            "balance-sheet-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if bal_data:
            balance_master.extend(bal_data)
            logging.debug(f"  ✓ Found {len(bal_data)} balance sheet records")

        # 3. Cash Flow Statement
        logging.debug(f"  Fetching cash flow statement for {ticker}")
        cf_data = fetch_fmp_data(
            "cash-flow-statement", ticker, API_KEY, opt_args="&limit=1"
        )
        if cf_data:
            cashflow_master.extend(cf_data)
            logging.debug(f"  ✓ Found {len(cf_data)} cash flow records")

        logging.debug(f"  Fetching company profile for {ticker}")
        pr_data = fetch_fmp_data("profile", ticker, API_KEY)
        if pr_data:
            profile_master.extend(pr_data)
            logging.debug(f"  ✓ Found profile for {ticker}")

        logging.info(f"✓ Completed {ticker}")
    # Batch Load to BigQuery
    logging.info("=" * 60)
    logging.info("Loading data to BigQuery...")
    logging.info(
        f"Income records: {len(income_master)} | Balance: {len(balance_master)} | Cash Flow: {len(cashflow_master)} | Profile: {len(profile_master)}"
    )
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
            logging.info(f"✓ Loaded {len(df_inc)} rows to bronze_fmp_income")

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
            logging.info(f"✓ Loaded {len(df_bal)} rows to bronze_fmp_balance")

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
            logging.info(f"✓ Loaded {len(df_cf)} rows to bronze_fmp_cashflow")

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
            logging.info(f"✓ Loaded {len(df_pr)} rows to bronze_fmp_profile")

        logging.info("=" * 60)
        logging.info("✓ FMP Ingestion Complete")
        logging.info("=" * 60)

        return {
            "status": "success",
            "message": f"Loaded {len(income_master) + len(balance_master) + len(cashflow_master) + len(profile_master)} records",
            "code": 200,
        }

    except Exception as e:
        logging.error(f"BigQuery Load Failed: {e}")
        logging.error("=" * 60)
        return {"status": "error", "message": str(e), "code": 500}


# Cloud Functions entry point (HTTP triggered)
@functions_framework.http
def run_fmp_ingestion(request):
    """
    Cloud Functions entry point triggered by HTTP.
    Can be invoked by Cloud Scheduler or direct HTTP requests.
    """
    logging.info("HTTP request received")

    try:
        result = run_fmp_ingestion_impl()
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
