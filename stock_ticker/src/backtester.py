import os
import pandas as pd
import yfinance as yf
import logging
from src.medium_term_strategy import MediumTermEngine

class Backtester:
    def __init__(self, data_dir):
        self.snapshots_dir = os.path.join(data_dir, "daily_snapshots")
        self.engine = MediumTermEngine()

    def get_available_snapshots(self):
        """Returns a list of available snapshot dates."""
        if not os.path.exists(self.snapshots_dir):
            return []
        
        files = os.listdir(self.snapshots_dir)
        # Expected format: analysis_YYYY-MM-DD.csv
        dates = []
        for f in files:
            if f.startswith("analysis_") and f.endswith(".csv"):
                date_str = f.replace("analysis_", "").replace(".csv", "")
                dates.append(date_str)
        
        return sorted(list(set(dates)), reverse=True)

    def run_backtest(self, date_str, amount=100000, strategy="Big Bets"):
        """
        Loads the snapshot from date_str, runs the strategy, 
        and calculates ROI using today's prices.
        """
        snapshot_file = os.path.join(self.snapshots_dir, f"analysis_{date_str}.csv")
        if not os.path.exists(snapshot_file):
            raise FileNotFoundError(f"Snapshot not found for {date_str}")
            
        # 1. Load Point-in-Time Data
        df = pd.read_csv(snapshot_file)
        if df.empty:
            raise ValueError("Snapshot is empty.")
            
        # 2. Run Strategy to get historical picks
        if strategy == "Big Bets":
            # Big Bets expects DataFrame
            top_picks, _, _ = self.engine.run_analysis(df, amount=amount, duration_months=12)
        else:
            raise NotImplementedError("Only Big Bets strategy is supported for backtesting right now.")
            
        if not top_picks:
            return []
            
        # 3. Fetch Current Prices and Calculate ROI
        results = []
        for pick in top_picks:
            ticker = pick['Ticker']
            buy_price = pick['CMP']
            allocation = pick['Allocation']
            qty = int(allocation // buy_price) if buy_price > 0 else 0
            actual_invested = qty * buy_price
            
            # Fetch today's price
            try:
                stock = yf.Ticker(ticker)
                hist = stock.history(period="1d")
                if not hist.empty:
                    current_price = float(hist['Close'].iloc[-1])
                else:
                    current_price = buy_price # Fallback if delisted or no data
            except Exception as e:
                logging.warning(f"Failed to fetch current price for {ticker}: {e}")
                current_price = buy_price
                
            current_value = qty * current_price
            profit_loss = current_value - actual_invested
            roi_pct = (profit_loss / actual_invested) * 100 if actual_invested > 0 else 0
            
            pick_result = {
                "Ticker": ticker,
                "Name": pick['Name'],
                "Buy_Date": date_str,
                "Buy_Price": buy_price,
                "Current_Price": current_price,
                "Shares": qty,
                "Invested": actual_invested,
                "Current_Value": current_value,
                "P/L": profit_loss,
                "ROI_%": roi_pct
            }
            results.append(pick_result)
            
        return results
