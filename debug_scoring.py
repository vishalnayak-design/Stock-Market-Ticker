import sys
import os
sys.path.append(os.getcwd())
from stock_ticker.src.analysis import Analyzer
import yfinance as yf
import pandas as pd

def debug_stock(ticker):
    print(f"\n--- DEBUGGING {ticker} ---")
    stock = yf.Ticker(ticker)
    try:
        info = stock.info
        hist = stock.history(period="1y")
        
        print(f"Info Keys Found: {len(info)}")
        
        # Extract key metrics used in scoring
        print(f"Trailing PE: {info.get('trailingPE')}")
        print(f"Return on Equity: {info.get('returnOnEquity')}")
        print(f"Debt to Equity: {info.get('debtToEquity')}")
        print(f"Price to Book: {info.get('priceToBook')}")
        print(f"Market Cap: {info.get('marketCap')}")
        
        # Run Analyzer
        analyzer = Analyzer()
        fund_score = analyzer.score_fundamental(info)
        print(f"Calculated Fund_Score: {fund_score}")
        
    except Exception as e:
        print(f"Error fetching/processing {ticker}: {e}")

if __name__ == "__main__":
    # Test with stocks user reported having 0 score
    debug_stock("20MICRONS.NS")
    debug_stock("360ONE.NS")
    debug_stock("RELIANCE.NS") # Control 
