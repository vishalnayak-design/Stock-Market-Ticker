import pandas as pd
import os
import sys

# Mock MediumTermEngine parts
class MediumTermEngine:
    def __init__(self):
        self.feature_cols = ["SalesGrowth3Y"]
    
    def clean_columns(self, cols):
        import re
        cleaned = []
        for c in cols:
            c = str(c)
            c = c.replace("\xa0", " ")
            c = re.sub(r"\s+", " ", c)
            c = c.strip()
            cleaned.append(c)
        return cleaned

    def validate_columns(self, df):
        critical = ["SalesGrowth3Y"]
        missing = [c for c in critical if c not in df.columns]
        return missing

    def preprocess_data(self, df):
        df.columns = self.clean_columns(df.columns)
        return df

print("--- Verifying Data Loading ---")
data_path = "stock_ticker/data/full_analysis.csv"
if not os.path.exists(data_path):
    print(f"Error: {data_path} not found")
    sys.exit(1)

df = pd.read_csv(data_path)
print("Original Columns:", df.columns.tolist())

engine = MediumTermEngine()
df = engine.preprocess_data(df)
print("Cleaned Columns:", df.columns.tolist())

missing = engine.validate_columns(df)
if missing:
    print("MISSING:", missing)
else:
    print("All critical columns found.")
