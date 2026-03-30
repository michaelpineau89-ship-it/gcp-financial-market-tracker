import requests
from time import sleep
import pandas_gbq
import pandas as pd
import os

TARGET_TICKERS = [
    # Mega-Cap Tech & AI
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "AMD", "AVGO", "PLTR",
    # Financials & Canadian Banks
    "JPM", "BRK-B", "BNS", "BMO", "RY", "HOOD", 
    # Auto & Manufacturing
    "TSLA", "TM", "GM",
    # Healthcare & Cybersecurity
    "JNJ", "LLY", "GEN",
    # Crypto
    "BTC-USD", "ETH-USD",
    # Market Benchmarks
    "SPY", "QQQ"
]

api_key = os.environ.get("API_KEY")

def get_data(ticker, key):
    url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    return pd.Dataframe(r.json()['Global Quote'])

def run():
    itter = 1
    output_data = pd.DataFrame()

    for ticker in TARGET_TICKERS:
        data = get_data(ticker, api_key)
        
        itter += 1

        if itter % 5 == 0:
            sleep(60)

if __name__ == "__main__":
    run()