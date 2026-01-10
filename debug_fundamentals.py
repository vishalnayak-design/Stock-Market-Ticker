import yfinance as yf
import json

tickers = ["TCS.NS", "RELIANCE.NS", "INFY.NS", "ZOMATO.NS"]

for ticker in tickers:
    print(f"\n--- {ticker} ---")
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Check specific keys used in our analysis
        keys_to_check = [
            'trailingPE', 'forwardPE', 'pegRatio', 
            'returnOnEquity', 'debtToEquity', 'priceToBook',
            'trailingEps', 'bookValue', 'currentPrice', 'previousClose',
            'returnOnAssets', 'operatingCashflow', 'currentRatio'
        ]
        
        found_data = {}
        for k in keys_to_check:
            val = info.get(k)
            found_data[k] = val
            print(f"{k}: {val}")
            
    except Exception as e:
        print(f"Error: {e}")
