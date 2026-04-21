import os
import requests
import pandas as pd
import io
import re
import logging
import pandas_gbq
import functions_framework

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT = os.environ.get("PROJECT_ID", "mike-personal-portfolio")

# CICD Test: v3.0.3 (Cloud Functions)

# The SEC strictly requires you to declare your identity
HEADERS = {
    "User-Agent": "Freelance Data Solutions mike@freelancedatasolutions.com",
    "Accept-Encoding": "gzip, deflate",
}

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
    "HOOD",
    "TSLA",
    "TM",
    "GM",
    "JNJ",
    "LLY",
    "GEN",
    # Note: Crypto (BTC/ETH), ETFs (SPY/QQQ), and Canadian dual-lists (BNS, RY)
    # generally do not file standard SEC 10-Ks, so we omit them here.
]

WHALE_CIKS = {
    "Berkshire Hathaway": "0001067983",
    "Scion Asset Management": "0001649339",
    "Pershing Square": "0001336528",
    "Bridgewater": "0001350694",
}


def get_ticker_cik_mapping(target_tickers, headers):
    """
    Downloads the SEC's master ticker list and returns a clean dictionary
    mapping your specific target tickers to their 10-digit padded CIKs..
    """
    url = "https://www.sec.gov/files/company_tickers.json"

    logging.info("Fetching SEC Ticker-to-CIK master list...")
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    sec_data = r.json()

    ticker_mapping = {}

    # The SEC JSON is structured as {"0": {"cik_str": 320193, "ticker": "AAPL", ...}}
    for entry in sec_data.values():
        ticker = entry["ticker"]
        if ticker in target_tickers:
            # The Submissions API strictly requires a 10-digit CIK padded with zeros
            raw_cik = str(entry["cik_str"])
            ticker_mapping[ticker] = raw_cik.zfill(10)

    return ticker_mapping


def get_latest_13f_url(cik):
    """Finds the raw XML URL for the most recent 13F filing."""
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"

    r = requests.get(submissions_url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()

    recent_filings = data.get("filings", {}).get("recent", {})

    if not recent_filings:
        return None

    # Zip the separate arrays into a single list of dictionaries
    filings_df = pd.DataFrame(recent_filings)

    if filings_df.empty or "form" not in filings_df.columns:
        return None

    # Filter for the newest 13F-HR (Holdings Report)
    thirteen_fs = filings_df[filings_df["form"] == "13F-HR"]

    if thirteen_fs.empty:
        return None

    latest_filing = thirteen_fs.iloc[0]
    accession_no_raw = latest_filing["accessionNumber"]

    # The SEC file directory removes the dashes from the accession number
    accession_no_clean = accession_no_raw.replace("-", "")

    # Construct the URL to the raw XML document
    xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_no_clean}/infotable.xml"
    return xml_url


def parse_sec_xml(xml_content):
    """Cleans SEC XML namespaces and parses into a Pandas DataFrame."""
    # SEC XML namespaces are notoriously messy and break standard parsers.
    # The safest ELT approach is to strip the namespace declaration before parsing.
    clean_xml = re.sub(r'\sxmlns="[^"]+"', "", xml_content.decode("utf-8"), count=1)

    # Pandas can read the clean XML directly into a DataFrame
    df = pd.read_xml(io.StringIO(clean_xml))
    return df


def run_edgar_ingestion_impl():

    # 1. Dynamically fetch the CIKs for your standard tickers
    logging.info("Fetching CIK mappings for standard tickers...")
    dynamic_ciks = get_ticker_cik_mapping(TARGET_TICKERS, HEADERS)
    logging.info(f"✓ Retrieved CIK mappings for {len(dynamic_ciks)} tickers")

    # 2. Merge them into one master targeting dictionary
    # This results in: {"AAPL": "0000320193", "Berkshire Hathaway": "0001067983", ...}
    master_targets = {**dynamic_ciks, **WHALE_CIKS}
    logging.info(f"✓ Total entities to process: {len(master_targets)}")

    master_submissions = []
    master_holdings = []

    # 3. Proceed with the loop we built earlier!
    for idx, (entity_name, cik) in enumerate(master_targets.items(), start=1):
        logging.info(
            f"[{idx}/{len(master_targets)}] Querying SEC EDGAR for {entity_name} (CIK: {cik})..."
        )

        try:
            # 1. Fetch the raw Submissions JSON
            submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            r = requests.get(submissions_url, headers=HEADERS)
            data = r.json()

            # 2. INGEST THE RAW SUBMISSIONS LOG (All Forms)
            recent_filings = data.get("filings", {}).get("recent", {})
            submissions_df = pd.DataFrame(recent_filings)

            # Inject metadata
            submissions_df["entity_name"] = entity_name
            submissions_df["cik"] = cik
            submissions_df = submissions_df.assign(_ingested_at=pd.Timestamp.utcnow())

            # Load ALL filings to the Submissions Bronze table
            pandas_gbq.to_gbq(
                submissions_df,
                "market_tracker.bronze_edgar_submissions",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(f"✓ Loaded {len(submissions_df)} filings for {entity_name}")

            # 3. CHECK FOR RECENT 13Fs TO PARSE THE XML
            thirteen_fs = submissions_df[submissions_df["form"] == "13F-HR"]

            if not thirteen_fs.empty:
                latest_filing = thirteen_fs.iloc[0]
                filing_date = latest_filing["filingDate"]
                accession_no_clean = latest_filing["accessionNumber"].replace("-", "")
                logging.info(
                    f"  Found 13F-HR filing from {filing_date}. Parsing holdings..."
                )

                # Download and parse the raw XML
                xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_no_clean}/infotable.xml"
                xml_response = requests.get(xml_url, headers=HEADERS)

                fund_df = parse_sec_xml(xml_response.content)
                fund_df["entity_name"] = entity_name
                fund_df["cik"] = cik
                logging.info(f"  ✓ Parsed {len(fund_df)} holdings from 13F filing")

                master_holdings.append(fund_df)
            else:
                logging.debug(f"  No 13F-HR filings found for {entity_name}")

        except Exception as e:
            logging.error(f"Failed to process {entity_name}: {e}")
            continue

    logging.info("=" * 60)
    logging.info("Loading data to BigQuery...")
    if master_holdings:
        try:
            # Combine all funds into one massive DataFrame and inject the system timestamp
            final_df = pd.concat(master_holdings, ignore_index=True)
            final_df = final_df.assign(_ingested_at=pd.Timestamp.utcnow())
            logging.info(
                f"Total holdings to upload: {len(final_df)} records from {len(master_holdings)} filings"
            )

            pandas_gbq.to_gbq(
                final_df,
                "market_tracker.bronze_edgar_13f",
                project_id=PROJECT,
                if_exists="append",
            )
            logging.info(
                f"✓ Successfully loaded {len(final_df)} institutional holdings to BigQuery"
            )
            logging.info("=" * 60)
            logging.info("✓ SEC EDGAR Ingestion Complete")
            logging.info("=" * 60)
            return {"code": 200, "message": f"Loaded {len(final_df)} records"}
        except Exception as e:
            logging.error(f"BigQuery Load Failed: {e}")
            return {"code": 500, "message": f"Database Error: {e}"}
    else:
        return {"code": 500, "message": "No filings processed"}


@functions_framework.http
def run_edgar_ingestion(request):
    """Cloud Functions entry point for SEC EDGAR ingestion.

    Triggered by HTTP request from Cloud Scheduler or direct invocation.
    """
    logging.info("=" * 60)
    logging.info("Starting SEC EDGAR Ingestion Pipeline (Cloud Functions)...")
    logging.info(f"Project: {PROJECT}")
    logging.info(
        f"Processing {len(TARGET_TICKERS)} standard tickers + {len(WHALE_CIKS)} institutional funds"
    )
    logging.info("=" * 60)

    try:
        result = run_edgar_ingestion_impl()
        return (
            {"status": "success", "data": result},
            200,
            {"Content-Type": "application/json"},
        )
    except Exception as e:
        logging.error(f"Cloud Functions handler error: {e}")
        return (
            {"status": "error", "message": f"Error: {e}", "code": 500},
            500,
            {"Content-Type": "application/json"},
        )
