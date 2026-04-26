from src.data_ingestion import DataIngestor
import logging

logging.basicConfig(level=logging.INFO)

def test_ingestion():
    print("\n--- Testing DataIngestor with Upstox Integration ---")
    
    ingestor = DataIngestor()
    
    # Check if Upstox is ready
    if ingestor.upstox.access_token:
        print("✅ Upstox Client Initialized & Authenticated")
    else:
        print("⚠️ Upstox Not Authenticated (will use fallback)")

    # Fetch
    print("Fetching history for RELIANCE.NS...")
    import time
    start = time.time()
    data = ingestor.fetch_stock_history("RELIANCE.NS", period="1mo")
    end = time.time()
    
    print(f"Time taken: {end-start:.2f}s")
    if data:
        print(f"✅ Records fetched: {len(data)}")
        print("First Record:", data[0])
        print("Last Record:", data[-1])
        
        # Verify columns
        keys = data[0].keys()
        if 'Date' in keys and 'Close' in keys:
            print("✅ Structure Valid")
        else:
            print("❌ Invalid Structure:", keys)
    else:
        print("❌ No Data Fetched")

if __name__ == "__main__":
    test_ingestion()
