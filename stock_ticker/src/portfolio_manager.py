import pandas as pd
import os
import logging
from datetime import datetime
from src.utils import save_to_csv

class PortfolioManager:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.history_file = os.path.join(data_dir, "portfolio_history.csv")
        self.daily_log_dir = os.path.join(data_dir, "daily_snapshots")
        
        if not os.path.exists(self.daily_log_dir):
            os.makedirs(self.daily_log_dir)

    def load_history(self):
        if os.path.exists(self.history_file):
            return pd.read_csv(self.history_file)
        return pd.DataFrame(columns=["Date", "Ticker", "Name", "Action", "Rank", "Price", "Score"])

    def save_daily_snapshot(self, full_df, rec_df):
        """Saves the full analysis for the day."""
        today = datetime.now().strftime('%Y-%m-%d')
        # Save full analysis - Use pandas to_csv if it's a DataFrame
        if isinstance(full_df, pd.DataFrame):
            full_df.to_csv(os.path.join(self.daily_log_dir, f"analysis_{today}.csv"), index=False)
        else:
            save_to_csv(full_df, os.path.join(self.daily_log_dir, f"analysis_{today}.csv"))
            
        # Save recommendations
        if isinstance(rec_df, pd.DataFrame):
             rec_df.to_csv(os.path.join(self.daily_log_dir, f"recommendations_{today}.csv"), index=False)
        else:
             save_to_csv(rec_df, os.path.join(self.daily_log_dir, f"recommendations_{today}.csv"))

    def update_portfolio(self, current_rec_df):
        """Compares current recommendations with history to detect changes."""
        # Convert list to DataFrame if necessary
        if isinstance(current_rec_df, list):
            current_rec_df = pd.DataFrame(current_rec_df)
            
        today = datetime.now().strftime('%Y-%m-%d')
        history_df = self.load_history()
        
        # Get last entry date
        if not history_df.empty:
            last_date = history_df['Date'].max()
            last_recs = history_df[history_df['Date'] == last_date]
            previous_tickers = set(last_recs[last_recs['Action'] == 'HOLD']['Ticker'])
        else:
            previous_tickers = set()

        current_tickers = set(current_rec_df['Ticker'])
        
        new_entries = []
        
        # Detect New Entries
        for _, row in current_rec_df.iterrows():
            ticker = row['Ticker']
            action = "HOLD"
            if ticker not in previous_tickers:
                action = "NEW_ENTRY"
            
            new_entries.append({
                "Date": today,
                "Ticker": ticker,
                "Name": row['Name'],
                "Action": action,
                "Rank": row.name + 1 if isinstance(row.name, int) else 0, # Assuming sorted
                "Price": row['Close'],
                "Score": row['Final_Score']
            })

        # Detect Dropouts (Stocks that were in top list but are gone now)
        for tick in previous_tickers:
            if tick not in current_tickers:
                # We need to look up details, typically from previous history or just log it
                prev_row = history_df[(history_df['Date'] == last_date) & (history_df['Ticker'] == tick)].iloc[0]
                new_entries.append({
                    "Date": today,
                    "Ticker": tick,
                    "Name": prev_row['Name'],
                    "Action": "DROPOUT",
                    "Rank": -1,
                    "Price": prev_row['Price'], # Ideally current price, but for now using last known
                    "Score": 0
                })

        new_df = pd.DataFrame(new_entries)
        
        # Append and Save
        updated_history = pd.concat([history_df, new_df], ignore_index=True)
        updated_history.to_csv(self.history_file, index=False)
        
        return new_df # Return today's changes for notification
