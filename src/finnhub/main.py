import finnhub
import os
import pandas as pd
import datetime
from flask import Flask

import logging
from time import sleep
import pandas_gbq

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_KEY = os.environ.get("API_KEY")
DATE_START = os.environ.get("START")
DATE_END = os.environ.get("END")
TICKERS_STR = os.environ.get("TICKERS", "")
PORT = os.environ.get("PORT", "8080")
PROJECT = os.environ.get("PROJECT", "mike-personal-portfolio")

TICKERS = [t.strip() for t in TICKERS_STR.split(",") if t.strip()]


def make_api_call(client, method, *args, max_retries=3, **kwargs):
    """Wrapper to make API calls with retry logic and consistent error handling."""
    for attempt in range(max_retries):
        try:
            return method(*args, **kwargs)
        except finnhub.exceptions.FinnhubAPIException as e:
            if e.status_code == 429:
                if attempt < max_retries - 1:
                    logging.warning(f"Rate limited, retrying in 60s (attempt {attempt + 1}/{max_retries})")
                    sleep(60)
                    continue
                logging.error("Max retries exceeded for rate limit")
                return None
            elif e.status_code == 401:
                logging.error("Invalid API key")
                raise
            else:
                logging.error(f"API error {e.status_code}: {e}")
                raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise
    return None


@app.route('/', methods=['POST'])
def run():
    """Main entry point for the Flask app - fetches and stores Finnhub data."""
    logging.info("Starting Finnhub Ingestion")
    client = finnhub.Client(api_key=API_KEY)

    news = pd.DataFrame()
    recommendation = pd.DataFrame()
    financials = pd.DataFrame()
    insider = pd.DataFrame()

    for ticker in TICKERS:
        try:
            news_q = pd.DataFrame(make_api_call(client, client.company_news, ticker, DATE_START, DATE_END))
            news = pd.concat([news, news_q], ignore_index=True)

            recommendation_q = pd.DataFrame(make_api_call(client, client.recommendation_trends, ticker))
            recommendation = pd.concat([recommendation, recommendation_q], ignore_index=True)

            financials_q = pd.DataFrame(make_api_call(client, client.company_basic_financials, ticker, 'all'))
            financials = pd.concat([financials, financials_q], ignore_index=True)

            insider_q = pd.DataFrame(make_api_call(client, client.stock_insider_sentiment, ticker, DATE_START, DATE_END))
            insider = pd.concat([insider, insider_q], ignore_index=True)
        except Exception as e:
            logging.error(f"Failed to process ticker {ticker}: {e}")
            continue

    if not news.empty:
        pandas_gbq.to_gbq(news, "market_tracker.bronze_finnhub_news", project_id=PROJECT, if_exists="append")
    if not recommendation.empty:
        pandas_gbq.to_gbq(recommendation, "market_tracker.bronze_finnhub_recommendations", project_id=PROJECT, if_exists="append")
    if not financials.empty:
        pandas_gbq.to_gbq(financials, "market_tracker.bronze_finnhub_financials",project_id= PROJECT, if_exists="append")
    if not insider.empty:
        pandas_gbq.to_gbq(insider, "market_tracker.bronze_finnhub_insider", project_id= PROJECT, if_exists="append")

    logging.info("Finnhub Ingestion Complete")
    return {"status": "success"}, 200


if __name__ == "__main__":
    logging.info(f"Starting Flask on port: {PORT}")
    app.run("0.0.0.0", int(PORT))