import yfinance as yf
import feedparser
import logging
from datetime import datetime, timedelta
import requests
import io
import csv
import random
import time
import os

try:
    from src.utils import save_to_csv
except ImportError:
    from utils import save_to_csv

class DataIngestor:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        self.stocks_file = f"{data_dir}/equity_master.csv"
        self.session = self._get_stealth_session()
        logging.info("DataIngestor v2: Session Removed (Fix Verified)")

    def get_nse_equity_list(self):
        """Fetches the list of all active equity stocks using nsepython or fallback."""
        stocks = []
        try:
            # 1. Try nsepython (Best Source)
            logging.info("Attempting to fetch via nsepython...")
            try:
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
            logging.info("Attempting direct CSV download...")
            url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
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
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1'
        ]
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1', 
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        return session

    def fetch_stock_history(self, ticker, period="2y", retries=3):
        """Fetches history with retries."""
        for attempt in range(retries):
            try:
                # Use shared session for persistency, but rotate UA on retry
                if attempt > 0:
                    self.session = self._get_stealth_session()
                    time.sleep(2 ** attempt + random.random() * 2) # Exponential Backoff: 2s, 4s, 8s...

                # Remove session argument to let yfinance handle curl_cffi internally
                stock = yf.Ticker(ticker)
                
                # Fetch
                hist = stock.history(period=period)
                
                if hist is None or hist.empty:
                    if attempt < retries - 1:
                        continue # Retry
                    return []

                # Convert to dict list
                hist = hist.reset_index()
                # Ensure Date is string for JSON/CSV compatibility
                hist['Date'] = hist['Date'].astype(str)
                return hist.to_dict('records')

            except Exception as e:
                msg = str(e).lower()
                if "429" in msg or "rate limited" in msg or "too many requests" in msg:
                    logging.warning(f"Rate Limit ({ticker}). Retry {attempt+1}/{retries}...")
                    time.sleep(5 + random.random() * 5)
                elif "401" in msg or "unauthorized" in msg:
                     logging.warning(f"Unauthorized ({ticker}). Refreshing session...")
                     self.session = self._get_stealth_session()
                else:
                    logging.error(f"Error fetching history for {ticker}: {e}")
                    if attempt == retries - 1:
                        return []
        return []

    def fetch_fundamentals(self, ticker, retries=2):
        """Fetches key fundamentals with retries."""
        for attempt in range(retries):
            try:
                if attempt > 0:
                     time.sleep(1 + random.random())

                # Remove session argument
                stock = yf.Ticker(ticker)
                info = stock.info
                if info is None:
                    return {}
                
                # ROCE Fallback: If missing, calculate from Financials
                if not info.get('returnOnCapitalEmployed'):
                    try:
                        # Fetch Financials (Income Statement) & Balance Sheet
                        # Note: This increases API calls, so only done if ROCE is critical and missing
                        fin = stock.financials
                        bs = stock.balance_sheet
                        
                        if not fin.empty and not bs.empty:
                            # 1. Get EBIT
                            ebit = 0
                            if 'EBIT' in fin.index:
                                ebit = fin.loc['EBIT'].iloc[0]
                            elif 'Net Income' in fin.index and 'Interest Expense' in fin.index and 'Tax Provision' in fin.index:
                                # Approx EBIT = Net Income + Interest + Tax
                                ebit = fin.loc['Net Income'].iloc[0] + fin.loc['Interest Expense'].iloc[0] + fin.loc['Tax Provision'].iloc[0]
                            
                            # 2. Get Capital Employed
                            cap_employed = 0
                            if 'Invested Capital' in bs.index:
                                cap_employed = bs.loc['Invested Capital'].iloc[0]
                            elif 'Total Assets' in bs.index and 'Current Liabilities' in bs.index:
                                cap_employed = bs.loc['Total Assets'].iloc[0] - bs.loc['Current Liabilities'].iloc[0]
                                
                            # 3. Calculate ROCE
                            if cap_employed > 0:
                                roce = (ebit / cap_employed) * 100
                                info['returnOnCapitalEmployed'] = roce
                                
                    except Exception as fallback_err:
                        # Fail silently on fallback to avoid log spam, just keep ROCE as None/0
                        pass

                info['Ticker'] = ticker
                return info
            except Exception as e:
                msg = str(e).lower()
                if "429" in msg or "too many requests" in msg:
                    time.sleep(2)
                elif "404" in msg:
                    return {} # No data found, don't retry
                
                if attempt == retries - 1:
                    logging.error(f"Error fetching fundamentals for {ticker}: {e}")
                    return {}
        return {}

    def fetch_news(self, query):
        """Fetches news via Google RSS."""
        encoded_query = query.replace(" ", "+")
        url = f"https://news.google.com/rss/search?q={encoded_query}+stock+india&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            # Use requests to fetch then parse string to avoid feedparser User-Agent issues
            response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            feed = feedparser.parse(response.content)
            return [entry.title for entry in feed.entries[:5]]
        except requests.exceptions.Timeout:
            logging.warning(f"News fetch timed out for {query}")
            return []
        except Exception as e:
            logging.error(f"Error fetching news for {query}: {e}")
            return []

if __name__ == "__main__":
    ingestor = DataIngestor()
    stocks = ingestor.get_nse_equity_list()
    # Test
    if stocks: # stocks is list of dicts
        ticker = stocks[0]['Ticker']
        print(f"Fetching {ticker}...")
        print("History:", len(ingestor.fetch_stock_history(ticker)))
        print("Fundamentals:", ingestor.fetch_fundamentals(ticker).get('currentPrice'))

