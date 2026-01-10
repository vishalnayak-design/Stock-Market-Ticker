import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import logging

# Ensure VADER lexicon is downloaded
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

class Analyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()

    def calculate_technicals(self, df):
        """Calculates RSI, MACD, and SMAs."""
        if df.empty or len(df) < 50:
            return None
        
        # Copy to avoid SettingWithCopy warnings
        df = df.copy()

        # RSI
        rsi = RSIIndicator(close=df['Close'], window=14)
        df['RSI'] = rsi.rsi()

        # MACD
        macd = MACD(close=df['Close'])
        df['MACD'] = macd.macd()
        df['MACD_Signal'] = macd.macd_signal()
        df['MACD_Diff'] = macd.macd_diff()

        # SMA
        df['SMA_50'] = SMAIndicator(close=df['Close'], window=50).sma_indicator()
        df['SMA_200'] = SMAIndicator(close=df['Close'], window=200).sma_indicator()

        return df

    def get_piotroski_score(self, info):
        """Calculates Piotroski F-Score (0-9)."""
        score = 0
        try:
            # Profitability
            roa = info.get('returnOnAssets', 0)
            score += 1 if roa and roa > 0 else 0
            
            ocf = info.get('operatingCashflow', 0)
            score += 1 if ocf and ocf > 0 else 0
            
            # Operating Efficiency
            # Asset Turnover (approximated)
            asset_turnover = info.get('revenuePerShare', 0) # Proxy
            if asset_turnover > 0: score += 1

            # Leverage/Liquidity
            current_ratio = info.get('currentRatio', 0)
            if current_ratio and current_ratio > 1: score += 1
            
            debt_equity = info.get('debtToEquity', 0)
            if debt_equity and debt_equity < 1: score += 1
            
            # Gross Margin (hard to get from summary, skipping or approximating)
        except:
            pass
            
        # Scaling to 9 (since we are missing some precise financial statement diffs)
        # We will normalize our 5 checks to 9
        return int((score / 5) * 9)

    def calculate_graham_number(self, info):
        """Calculates Graham Number: Sqrt(22.5 * EPS * BVPS)."""
        eps = info.get('trailingEps')
        bvps = info.get('bookValue')
        
        if eps and bvps and eps > 0 and bvps > 0:
            return np.sqrt(22.5 * eps * bvps)
        return 0

    def score_fundamental(self, info):
        """Advanced Fundamental Scoring (Calibrated for NSE)."""
        if not info:
            return 0
        
        score = 0
        checks = 0

        # 1. Graham Value (Deep Value Bonus) - Weight 2
        graham_num = self.calculate_graham_number(info)
        current_price = info.get('currentPrice', info.get('previousClose', 0))
        if graham_num and current_price and current_price < graham_num:
            score += 2
        checks += 2

        # 2. Piotroski F-Score (Financial Strength) - Weight 2
        f_score = self.get_piotroski_score(info)
        if f_score >= 7:
            score += 2
        elif f_score >= 5:
            score += 1
        checks += 2

        # 3. PE Valuation (Growth Market adjusted) - Weight 1
        pe = info.get('trailingPE')
        if pe:
            if 0 < pe < 40: # Relaxed from 25 for Indian Growth stocks
                score += 1
            checks += 1
        # If PE is missing (loss making?), we penalize by adding checks but no score
        elif info.get('trailingEps', 0) < 0:
             checks += 1 # Penalize negative earnings

        # 4. PEG (Growth at fair price) - Weight 1
        peg = info.get('pegRatio')
        if peg is not None:
            if 0 < peg < 2.0: # Relaxed from 1.5
                score += 1
            checks += 1
        # If PEG missing, ignore completely (don't add to checks)

        # 5. ROE (Efficiency) - Weight 1
        roe = info.get('returnOnEquity')
        if roe:
            if roe > 0.10: # Relaxed from 15%
                score += 1
            checks += 1

        # 6. D/E (Safety) - Weight 1
        de = info.get('debtToEquity')
        if de is not None:
             # yfinance returns % (e.g. 35.6 = 0.35). 
             # We want D/E < 1.0 (100%) or up to 2.0 (200%) for capital intensive
             if de < 100: 
                 score += 1
             checks += 1
        
        return score / checks if checks > 0 else 0

    def get_investment_thesis(self, row, info):
        """Generates a natural language explanation."""
        reasons = []
        
        # Tech Reason
        if row['Tech_Score'] > 0.7:
            reasons.append("Strong technical uptrend (RSI/MACD bullish)")
        
        # Fund Reason
        graham = self.calculate_graham_number(info)
        price = row['Close']
        if graham > price:
            upside = ((graham - price) / price) * 100
            reasons.append(f"Undervalued by ~{int(upside)}% (Graham)")
            
        f_score = self.get_piotroski_score(info)
        if f_score >= 7:
            reasons.append(f"High Financial Strength (F-Score {f_score}/9)")
            
        # Sentiment
        if row['Sent_Score'] > 0.6:
            reasons.append("Positive Market Sentiment")
            
        if not reasons:
            return "Balanced moderate growth candidate."
            
        return ". ".join(reasons) + "."

    def get_technical_score(self, df):
        """Returns a normalized score 0-1 based on technicals."""
        if df is None or df.empty:
            return 0
        
        last_row = df.iloc[-1]
        score = 0
        checks = 0

        # RSI Strategy
        rsi = last_row['RSI']
        if 40 < rsi < 70:
            score += 1 
        elif rsi <= 30:
            score += 2 # Ultra value
        checks += 2

        # MACD
        if last_row['MACD_Diff'] > 0:
            score += 1
        checks += 1

        # Golden Cross
        if last_row['SMA_50'] > last_row['SMA_200']:
            score += 1
        checks += 1
        
        # Trend
        if last_row['Close'] > last_row['SMA_200']:
             score += 1
        checks += 1

        return score / checks if checks > 0 else 0

    def analyze_sentiment(self, headlines):
        if not headlines:
            return 0.5
        scores = [self.sia.polarity_scores(h)['compound'] for h in headlines]
        avg_score = np.mean(scores)
        return (avg_score + 1) / 2
