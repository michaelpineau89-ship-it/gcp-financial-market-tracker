import os
import time
import requests
import pandas as pd
from flask import Flask, request
import logging

# Initialize the Flask app for Cloud Run
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

TARGET_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "AMD", "AVGO", "PLTR",
    "JPM", "BRK-B", "BNS", "BMO", "RY", "HOOD", 
    "TSLA", "TM", "GM",
    "JNJ", "LLY", "GEN",
    "BTC-USD", "ETH-USD",
    "SPY", "QQQ"
]

PROJECT_ID = os.environ.get("PROJECT_ID", "mike-personal-portfolio")
API_KEY = os.environ.get("API_KEY")

def get_data(ticker, key):
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()
    
    # Safe extraction in case the API rate-limits us and returns an Error/Information message instead
    return data.get('Global Quote', None)

@app.route('/', methods=['POST'])
def run_ingestion():
    logging.info("Starting Daily Alpha Vantage Ingestion...")
    
    master_data_list = []
    
    for count, ticker in enumerate(TARGET_TICKERS, start=1):
        logging.info(f"Fetching {ticker}...")
        quote_data = get_data(ticker, API_KEY)
        
        if quote_data:
            master_data_list.append(quote_data)
            
        else:
            logging.warning(f"Failed to fetch data for {ticker}. Check API limits.")

        if count < len(TARGET_TICKERS):
            logging.info('Waiting 15 seconds due to rate limits and concurrency requirements')
            time.sleep(15)


    # Batch Load to BigQuery
    if master_data_list:
        logging.info("Batching complete. Loading to BigQuery...")
        
        # Clean the Alpha Vantage keys (they look like "01. symbol", which BigQuery hates)
        clean_data = []
        for row in master_data_list:
            clean_row = {k.split('. ')[1].replace(' ', '_'): v for k, v in row.items()}
            clean_data.append(clean_row)
            
        df = pd.DataFrame(clean_data)
        
        # Write to BQ once!
        df.to_gbq(
            destination_table="market_tracker.bronze_alphavantage",
            project_id=PROJECT_ID,
            if_exists="append"
        )
        logging.info(f"Successfully loaded {len(df)} rows to BigQuery.")
        return f"Loaded {len(df)} records", 200
    else:
        return "No data extracted", 500

# Required for local testing and Cloud Run port binding
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)