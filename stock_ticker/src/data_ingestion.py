import yfinance as yf
import pandas as pd
import feedparser
import numpy as np
import logging
from datetime import datetime, timedelta
import requests
import io

try:
    from src.utils import save_to_parquet, save_to_csv
except ImportError:
    from utils import save_to_parquet, save_to_csv

logging.basicConfig(level=logging.INFO)

class DataIngestor:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.stocks_file = f"{data_dir}/equity_master.csv"

    def get_nse_equity_list(self):
        """Fetches the list of all active equity stocks from NSE."""
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        try:
            # Using a user-agent to avoid being blocked
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                # Clean up and just keep the symbol
                df = df[['SYMBOL', 'NAME OF COMPANY', ' ISIN NUMBER']]
                df['Ticker'] = df['SYMBOL'] + ".NS" # Format for yfinance
                save_to_csv(df, self.stocks_file)
                logging.info(f"Fetched {len(df)} stocks from NSE")
                return df
            else:
                logging.warning("Failed to fetch from NSE website, status code: " + str(response.status_code))
                return self._fallback_stock_list()
        except Exception as e:
            logging.error(f"Error fetching NSE list: {e}")
            return self._fallback_stock_list()

    def _fallback_stock_list(self):
        # A small fallback list for testing/offline
        logging.info("Using fallback NIFTY 50 list")
        symbols = [
            "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "LT", "SBIN", "AXISBANK", "ITC", "HINDUNILVR"
        ]
        df = pd.DataFrame({'SYMBOL': symbols, 'Ticker': [s + ".NS" for s in symbols]})
        return df

    def fetch_stock_history(self, ticker, period="2y"):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            if not hist.empty:
                # Save to parquet
                path = f"{self.data_dir}/history/{ticker}.parquet"
                save_to_parquet(hist, path)
                return hist
        except Exception as e:
            logging.error(f"Error fetching history for {ticker}: {e}")
        return None

    def fetch_fundamentals(self, ticker):
        """Fetches key fundamentals. Note: This can be slow for many stocks."""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Return the full dictionary so Analyzer has all keys (Graham, Piotroski, etc.)
            # Add Ticker explicitly
            info['Ticker'] = ticker
            return info
        except Exception as e:
            logging.error(f"Error fetching fundamentals for {ticker}: {e}")
            return {}

    def fetch_news(self, query):
        """Fetches news titles for sentiment analysis."""
        encoded_query = query.replace(" ", "+")
        url = f"https://news.google.com/rss/search?q={encoded_query}+stock+india&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            feed = feedparser.parse(url)
            headlines = [entry.title for entry in feed.entries[:10]]
            return headlines
        except Exception as e:
            logging.error(f"Error fetching news for {query}: {e}")
            return []

if __name__ == "__main__":
    ingestor = DataIngestor()
    stocks = ingestor.get_nse_equity_list()
    print(stocks.head())
    
    # Test fetch for one
    if not stocks.empty:
        ticker = stocks.iloc[0]['Ticker']
        print(f"Fetching data for {ticker}...")
        ingestor.fetch_stock_history(ticker)
        print(ingestor.fetch_fundamentals(ticker))
        print(ingestor.fetch_news(ticker))
