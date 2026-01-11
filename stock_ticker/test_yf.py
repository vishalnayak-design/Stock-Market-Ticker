import yfinance as yf
ticker = "RELIANCE.NS"
stock = yf.Ticker(ticker)

print("\n--- Income Statement (Financials) ---")
try:
    fin = stock.financials
    if 'EBIT' in fin.index:
        # Get most recent
        ebit = fin.loc['EBIT'].iloc[0]
        print(f"EBIT: {ebit}")
    else:
        print("EBIT not found in financials")
except Exception as e:
    print(f"Error fetching financials: {e}")

print("\n--- Balance Sheet ---")
try:
    bs = stock.balance_sheet
    if 'Invested Capital' in bs.index:
        ic = bs.loc['Invested Capital'].iloc[0]
        print(f"Invested Capital: {ic}")
    else:
        print("Invested Capital not found")
        
    if 'Total Assets' in bs.index and 'Current Liabilities' in bs.index:
        ta = bs.loc['Total Assets'].iloc[0]
        cl = bs.loc['Current Liabilities'].iloc[0]
        print(f"Total Assets - Current Liab: {ta - cl}")
    else:
        print("Total Assets or Current Liabilities not found")
        
except Exception as e:
    print(f"Error fetching balance sheet: {e}")
