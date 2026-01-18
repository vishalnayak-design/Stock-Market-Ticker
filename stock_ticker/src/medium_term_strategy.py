import pandas as pd
import numpy as np
import re
from datetime import datetime
from sklearn.linear_model import LogisticRegression
import logging

class MediumTermEngine:
    def __init__(self):
        self.feature_cols = [
            "ROCE", "ROE", "OPM", "FreeCashFlow",
            "DebtToEquity", "InterestCoverage",
            "PromoterHolding", "PromoterHoldingChange3Y",
            "SalesGrowth3Y", "ProfitGrowth3Y",
            "QtrSalesGrowth", "QtrProfitGrowth",
            "DMA_200", "RSI"
        ]

    def clean_columns(self, cols):
        cleaned = []
        for c in cols:
            c = str(c)
            c = c.replace("\xa0", " ")
            c = re.sub(r"\s+", " ", c)
            c = c.replace("%", "")
            c = c.replace(".", "")
            c = c.replace("/", "")
            c = c.strip()
            cleaned.append(c)
        return cleaned

    def preprocess_data(self, df):
        # 1. Basic Clean
        df.columns = self.clean_columns(df.columns)
        
        # 2. Smart Mapping (Keyword Based) to handle variations
        # Target: [Keywords to match]
        smart_map = {
            "SNo": ["sno", "srno", "serial"],
            "Name": ["name", "company", "stock"],
            "CMP": ["cmp", "currentprice", "price", "ltp", "close"],
            "PE": ["pe", "priceearnings"],
            "MarketCap": ["marcap", "marketcap", "mcap"],
            "ROCE": ["roce", "returnoncapital"],
            "ROE": ["roe", "returnonequity"],
            "DebtToEquity": ["debteq", "debtoequity", "debttoequity", "gearing"],
            "SalesGrowth3Y": ["salesvar3yr", "salesgrowth3yr", "salescagr3yr", "sales var 3"],
            "ProfitGrowth3Y": ["profitvar3yr", "profitgrowth3yr", "profitcagr3yr", "profit var 3"],
            "QtrSalesGrowth": ["qtrsales", "quartersales", "qtr sales", "quarter sales"],
            "QtrProfitGrowth": ["qtrprofit", "quarterprofit", "qtr profit", "quarter profit"],
            "OPM": ["opm", "operatingmargin"],
            "InterestCoverage": ["intcoverage", "interestcoverage"],
            "PromoterHolding": ["promhold", "promoterhold"],
            "PromoterHoldingChange3Y": ["chginprom", "promchange", "chg in prom"],
            "FreeCashFlow": ["freecash", "fcf"],
            "DMA_200": ["200dma", "dma200", "200 dma"],
            "RSI": ["rsi"],
            # Generic matches last to avoid greedy collision
            "Sales": ["sales", "revenue"]
        }
        
        # Existing columns in dataframe (lowercase for matching)
        lower_cols = {c.lower(): c for c in df.columns}
        
        final_rename = {}
        for target, keywords in smart_map.items():
            # If target already exists properly, skip
            if target in df.columns:
                continue
                
            # Check keywords
            found = False
            for k in keywords:
                # Direct match
                if k in lower_cols:
                    final_rename[lower_cols[k]] = target
                    found = True
                    break
                # Partial match (careful here, but useful for 'CMP Rs.' -> 'cmp' handled by clean, but 'Market Cap' -> 'marcap')
                # The clean_columns function already removes spaces, so 'Market Cap' becomes 'MarketCap'. 
                # We check if cleaned column contains keyword
                for actual_col in df.columns:
                    if k in actual_col.lower():
                        final_rename[actual_col] = target
                        found = True
                        break
                if found: break
        
        if final_rename:
            df = df.rename(columns=final_rename)
            
        # 3. Numeric Safety
        for col in self.feature_cols + ['CMP', 'ROI_6to12_Score']:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                 
        return df

    def validate_columns(self, df):
        """Returns list of missing critical columns."""
        # Critical columns for scoring
        critical = [
            "ROCE", "ROE", "OPM", "FreeCashFlow", 
            "SalesGrowth3Y", "ProfitGrowth3Y",
            "QtrSalesGrowth", "QtrProfitGrowth"
        ]
        missing = [c for c in critical if c not in df.columns]
        return missing

    def quality_score(self, row):
        score = 0
        # Quality
        if row.get("ROCE", 0) > 15: score += 1
        if row.get("ROE", 0) > 15: score += 1
        if row.get("OPM", 0) > 12: score += 1
        if row.get("FreeCashFlow", 0) > 0: score += 1
        # Safety
        if row.get("DebtToEquity", 1) < 0.5 or row.get("InterestCoverage", 0) > 5: score += 1
        if row.get("PromoterHolding", 0) > 50: score += 1
        if row.get("PromoterHoldingChange3Y", 0) >= 0: score += 1
        # Growth
        if row.get("SalesGrowth3Y", 0) > 10: score += 1
        if row.get("ProfitGrowth3Y", 0) > 12: score += 1
        # Trend
        if row.get("CMP", 0) > row.get("DMA_200", 0): score += 1
        if 45 <= row.get("RSI", 50) <= 70: score += 1
        return score

    def roi_6to12_score(self, row):
        score = 0
        # Minimum business quality
        if row.get("ROCE", 0) > 15: score += 1
        if row.get("ROE", 0) > 15: score += 1
        if row.get("OPM", 0) > 12: score += 1
        if row.get("FreeCashFlow", 0) > 0: score += 1
        # Financial safety
        if row.get("DebtToEquity", 1) < 0.5 or row.get("InterestCoverage", 0) > 5: score += 1
        if row.get("PromoterHolding", 0) > 50: score += 1
        if row.get("PromoterHoldingChange3Y", 0) >= 0: score += 1
        # Growth (re-rating fuel)
        if row.get("SalesGrowth3Y", 0) > 10: score += 1
        if row.get("ProfitGrowth3Y", 0) > 12: score += 1
        # Earnings trigger (MOST IMPORTANT)
        if row.get("QtrProfitGrowth", 0) > 15: score += 2
        if row.get("QtrSalesGrowth", 0) > 10: score += 1
        # Trend confirmation
        if row.get("CMP", 0) > row.get("DMA_200", 0): score += 1
        if 45 <= row.get("RSI", 50) <= 70: score += 1
        return score

    def train_and_predict(self, df):
        """Trains Logistic Regression and calculates Win Probability."""
        try:
            # 1. Target
            df["Target"] = (df["ROI_6to12_Score"] >= 9).astype(int)
            
            # Use only available features
            cols = [c for c in self.feature_cols if c in df.columns]
            
            if len(df) < 20 or df['Target'].sum() < 2:
                # Not enough data to train
                df["WinProbability"] = 0.0
                return df

            X = df[cols].fillna(0)
            y = df["Target"]

            # Train
            model = LogisticRegression(max_iter=2000)
            model.fit(X, y)

            # Predict
            df["WinProbability"] = model.predict_proba(X)[:, 1]
            return df
        except Exception as e:
            logging.error(f"Training failed: {e}")
            df["WinProbability"] = 0.5 # Default neutral
            return df

    def run_analysis(self, df_input, amount, duration_months):
        """Main entry point. df_input can be DataFrame or list."""
        
        # Ensure DataFrame
        if isinstance(df_input, list):
            df = pd.DataFrame(df_input)
        else:
            df = df_input.copy()
            
        df = self.preprocess_data(df)
        
        # Validation
        missing_cols = self.validate_columns(df)
        
        # Calculate Scores
        df["QualityScore"] = df.apply(self.quality_score, axis=1)
        df["ROI_6to12_Score"] = df.apply(self.roi_6to12_score, axis=1)
        
        # Train & Predict
        df = self.train_and_predict(df)
        
        # Filter & Select Top 3
        candidates = df[df["ROI_6to12_Score"] >= 9].copy()
        
        if candidates.empty:
             # Fallback if strict criteria not met
             candidates = df.sort_values("ROI_6to12_Score", ascending=False).head(10)

        # Sort by ROI Score then Win Prob
        candidates = candidates.sort_values(["ROI_6to12_Score", "WinProbability"], ascending=[False, False])
        
        # Reason Generation (Vectorized for safety)
        def get_reason(row):
            reasons = []
            if row.get('QtrProfitGrowth', 0) > 15: reasons.append("Earnings Breakout")
            if row.get('WinProbability', 0) > 0.7: reasons.append("High ML Conviction")
            if row.get('CMP', 0) > row.get('DMA_200', 0): reasons.append("Momentum")
            return " + ".join(reasons) if reasons else "Good Fundamental Score"

        candidates["Reason"] = candidates.apply(get_reason, axis=1)
        
        # Expected Return
        candidates["ExpectedReturn"] = candidates["ROI_6to12_Score"].apply(lambda s: f"{12 + (s-9)*3:.0f}-{18 + (s-9)*3:.0f}%")
        
        # Slice Top 3
        top_3 = candidates.head(3).copy()
        
        # Allocation
        weights = [0.40, 0.35, 0.25]
        
        final_recs = []
        for i, (idx, row) in enumerate(top_3.iterrows()):
             if i < 3:
                 alloc = amount * weights[i]
                 rec = {
                     "Rank": i + 1,
                     "Name": row.get("Name", "Unknown"),
                     "Ticker": row.get("Ticker", idx), # Fallback
                     "CMP": row.get("CMP"),
                     "Allocation": alloc,
                     "Expected_Return": row["ExpectedReturn"],
                     "Reason": row["Reason"],
                     "ROI_Score": row["ROI_6to12_Score"],
                     "Quality_Score": row["QualityScore"],
                     "Win_Prob": row.get("WinProbability", 0)
                 }
                 final_recs.append(rec)
                 
        return final_recs, candidates, missing_cols # Return missing_cols
