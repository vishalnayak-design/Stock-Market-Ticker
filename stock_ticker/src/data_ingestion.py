import yfinance as yf
import feedparser
import logging
from datetime import datetime, timedelta
import requests
import io
import csv

try:
    from src.utils import save_to_csv
except ImportError:
    from utils import save_to_csv

# logging.basicConfig(level=logging.INFO) # REMOVED: Managed by main.py

class DataIngestor:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.stocks_file = f"{data_dir}/equity_master.csv"

    def get_nse_equity_list(self):
        """Fetches the list of all active equity stocks using nsepython or fallback."""
        stocks = []
        try:
            # 1. Try nsepython (Best Source)
            logging.info("Attempting to fetch via nsepython...")
            from nsepython import nse_eq_symbols
            symbols = nse_eq_symbols() # Returns list of symbols
            if symbols and len(symbols) > 500:
                logging.info(f"nsepython returned {len(symbols)} stocks.")
                stocks = [{'Ticker': f"{s}.NS", 'Name': s} for s in symbols]
                save_to_csv(stocks, self.stocks_file)
                return stocks
        except Exception as e:
            logging.warning(f"nsepython fetch failed: {e}")

        # 2. Fallback to Requests (NSE Website)
        try:
            logging.info("Attempting direct CSV download...")
            url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                csv_reader = csv.DictReader(io.StringIO(content))
                stocks = []
                for row in csv_reader:
                    symbol = row.get('SYMBOL')
                    if symbol:
                        stocks.append({'Ticker': f"{symbol}.NS", 'Name': row.get('NAME OF COMPANY', symbol)})
                
                if len(stocks) > 500:
                     save_to_csv(stocks, self.stocks_file)
                     logging.info(f"Direct download fetched {len(stocks)} stocks.")
                     return stocks
        except Exception as e:
            logging.error(f"Direct download failed: {e}")

        # 3. Last Resort Fallback
        return self._fallback_stock_list()

    def _fallback_stock_list(self):
        logging.info("Using fallback NIFTY 50 list + ETFs")
        symbols = [
            "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "LT", "SBIN", "AXISBANK", "ITC", "HINDUNILVR",
            "NIFTYBEES", "BANKBEES", "GOLDBEES", "LIQUIDBEES", "SILVERBEES" # ETFs
        ]
        return [{'Ticker': s + ".NS", 'Name': s} for s in symbols]

    def _get_stealth_session(self):
        """Creates a session with rotating headers to avoid blocks."""
        import random
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        ]
        session = requests.Session()
        session.headers.update({'User-Agent': random.choice(user_agents)})
        return session

    def fetch_stock_history(self, ticker, period="2y"):
        try:
            # Let YF handle session/headers natively to avoid curl_cffi conflict
            stock = yf.Ticker(ticker)
            
            # Use Pandas efficiency
            hist = stock.history(period=period)
            
            if not hist.empty:
                hist = hist.reset_index()
                # Ensure Date is string for JSON/CSV compatibility
                hist['Date'] = hist['Date'].astype(str)
                return hist.to_dict('records')
        except Exception as e:
            # Rate Limit Detection
            if "429" in str(e):
                logging.warning(f"Rate Limit Hit on {ticker}. Pausing...")
                import time
                time.sleep(5) 
            logging.error(f"Error fetching history for {ticker}: {e}")
        return []

    def fetch_fundamentals(self, ticker):
        """Fetches key fundamentals."""
        try:
            # Let YF handle session
            stock = yf.Ticker(ticker)
            info = stock.info
            info['Ticker'] = ticker
            return info
        except Exception as e:
            logging.error(f"Error fetching fundamentals for {ticker}: {e}")
            return {}

    def fetch_news(self, query):
        """Fetches news via Google RSS."""
        encoded_query = query.replace(" ", "+")
        url = f"https://news.google.com/rss/search?q={encoded_query}+stock+india&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            feed = feedparser.parse(url)
            return [entry.title for entry in feed.entries[:5]]
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
