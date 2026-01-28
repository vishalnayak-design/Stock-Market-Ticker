import requests
import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import gzip
import shutil

class UpstoxDataClient:
    """
    Client for Upstox API v3 (Historical Data).
    Docs: https://upstox.com/developer/api-documentation/v3/get-historical-candle-data
    """
    BASE_URL = "https://api.upstox.com/v3"
    MASTER_URL_NSE = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    
    def __init__(self, access_token=None):
        self.access_token = access_token
        self.master_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "nse_master.csv")
        self.symbol_map = {}
        
        # Try loading from file if not provided
        if not self.access_token:
            self._load_token_from_file()
            
        # Load master list if exists, else lazy load later
        if os.path.exists(self.master_file):
            self._load_master_map()
            
    def _load_token_from_file(self):
        token_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "upstox_token.json")
        if os.path.exists(token_path):
            try:
                with open(token_path, "r") as f:
                    data = json.load(f)
                    self.access_token = data.get("access_token")
            except Exception as e:
                logging.error(f"Failed to load Upstox token: {e}")
    
    def get_headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }

    def _download_master_list(self):
        """Downloads and unzips the NSE Master list."""
        try:
            logging.info("Downloading Upstox NSE Master List...")
            response = requests.get(self.MASTER_URL_NSE, stream=True)
            if response.status_code == 200:
                # Save compressed
                gz_path = self.master_file + ".gz"
                with open(gz_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
                
                # Unzip
                with gzip.open(gz_path, 'rb') as f_in:
                    with open(self.master_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # Cleanup
                os.remove(gz_path)
                logging.info("Master list downloaded.")
                self._load_master_map()
            else:
                logging.error(f"Failed to download seed data: {response.status_code}")
        except Exception as e:
            logging.error(f"Error downloading master list: {e}")

    def _load_master_map(self):
        """Loads symbol -> instrument_key map into memory."""
        try:
            df = pd.read_csv(self.master_file)
            # Filter for NSE_EQ only to avoid noise
            df = df[df['exchange'] == 'NSE_EQ'] 
            # Structure: instrument_key, tradingsymbol, name, ...
            self.symbol_map = pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()
        except Exception as e:
            logging.error(f"Error loading master map: {e}")

    def get_instrument_key_by_symbol(self, symbol):
        """
        Maps symbol (e.g. RELIANCE) to Upstox Key (NSE_EQ|INE...).
        Autodownloads master list if missing.
        """
        if not self.symbol_map:
            self._download_master_list()
            
        return self.symbol_map.get(symbol)

    def get_instrument_key(self, isin, exchange="NSE_EQ"):
        """
        Formats ISIN to Upstox Instrument Key.
        Example: INE848E01016 -> NSE_EQ|INE848E01016
        """
        return f"{exchange}|{isin}"

    def fetch_historical_candles(self, instrument_key, from_date, to_date=None, interval="day"):
        """
        Fetches historical candles.
        interval: 1minute, 30minute, day, week, month (Upstox uses 'day' for 1D)
        """
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")
            
        # Upstox V3 Endpoint: /historical-candle/{instrumentKey}/{interval}/{to_date}/{from_date}
        # Ref: https://upstox.com/developer/api-documentation/v3/get-historical-candle-data
        # Interval format is actually split: /days/1 or /minutes/1
        
        upstox_interval = interval
        if interval == "day":
            upstox_interval = "days/1" # Default to 1 day
        elif interval == "minute":
            upstox_interval = "minutes/1" 
            
        url = f"{self.BASE_URL}/historical-candle/{instrument_key}/{upstox_interval}/{to_date}/{from_date}"
        
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "success" and data.get("data", {}).get("candles"):
                candles = data["data"]["candles"]
                # Columns: Timestamp, Open, High, Low, Close, Volume, Open Interest
                df = pd.DataFrame(candles, columns=["Date", "Open", "High", "Low", "Close", "Volume", "OI"])
                
                # Upstox returns formatted string "2025-01-01T00:00:00+05:30"
                # Parse to datetime
                df["Date"] = pd.to_datetime(df["Date"]).dt.date
                
                # Sort ascending (API might return descending)
                df = df.sort_values("Date").reset_index(drop=True)
                return df
                
            else:
                logging.warning(f"Upstox: No data found for {instrument_key}")
                return pd.DataFrame()
                
        except requests.exceptions.HTTPError as e:
            logging.error(f"Upstox API Error: {e.response.text}")
            return None
        except Exception as e:
            logging.error(f"Upstox Client Error: {e}")
            return None
