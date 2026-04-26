import yfinance as yf
import pandas as pd

ticker = "RELIANCE.NS"
stock = yf.Ticker(ticker)

print(f"--- Debugging {ticker} ---")
try:
    fin = stock.financials
    print("Financials Shape:", fin.shape)
    if not fin.empty:
        print("Financial Indices:", fin.index.tolist())
        if 'Total Revenue' in fin.index:
            revs = fin.loc['Total Revenue']
            print("\nTotal Revenue Data:")
            print(revs)
            
            if len(revs) >= 4:
                cagr = ((revs.iloc[0] / revs.iloc[3])**(1/3)) - 1
                print(f"\nCalculated SalesGrowth3Y: {cagr * 100}")
            else:
                print(f"Not enough data points ({len(revs)}) for 3Y CAGR")
        else:
            print("'Total Revenue' NOT found in financials index.")
    else:
        print("Financials DataFrame is empty.")

except Exception as e:
    print(f"Error: {e}")
