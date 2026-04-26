from src.upstox_client import UpstoxDataClient
import logging

# Setup basic logging to see output
logging.basicConfig(level=logging.INFO)

def test_fetch():
    print("\n--- Testing Upstox Data Fetch ---")
    
    client = UpstoxDataClient()
    if not client.access_token:
        print("❌ No Access Token found. Please run 'python stock_ticker/upstox_auth.py' first.")
        return

    # Test with Reliance (ISIN: INE002A01018)
    symbol = "RELIANCE"
    isin = "INE002A01018"
    print(f"Fetching data for {symbol} ({isin})...")
    
    instrument_key = client.get_instrument_key(isin)
    print(f"Instrument Key: {instrument_key}")
    
    # Fetch last 30 days
    from datetime import datetime, timedelta
    from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    df = client.fetch_historical_candles(instrument_key, from_date=from_date)
    
    if df is not None and not df.empty:
        print("\n✅ Data Fetched Successfully!")
        print(f"Rows: {len(df)}")
        print("Last 5 records:")
        print(df.tail())
        print("\nColumns:", df.columns.tolist())
    else:
        print("\n❌ Failed to fetch data (or empty). Check logs.")

if __name__ == "__main__":
    test_fetch()
