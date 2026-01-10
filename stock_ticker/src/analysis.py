import numpy as np
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import logging
import math

# Ensure VADER lexicon is downloaded
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

class Analyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()

    def _calculate_sma(self, prices, window):
        """Calculates Simple Moving Average."""
        smas = []
        for i in range(len(prices)):
            if i < window - 1:
                smas.append(None)
            else:
                window_slice = prices[i - window + 1 : i + 1]
                smas.append(sum(window_slice) / window)
        return smas

    def _calculate_ema(self, prices, window):
        """Calculates Exponential Moving Average."""
        emas = []
        multiplier = 2 / (window + 1)
        for i, price in enumerate(prices):
            if i == 0:
                emas.append(price)
            else:
                ema = (price - emas[-1]) * multiplier + emas[-1]
                emas.append(ema)
        return emas

    def _calculate_rsi(self, prices, window=14):
        """Calculates RSI."""
        deltas = np.diff(prices)
        seed = deltas[:window+1]
        up = seed[seed >= 0].sum()/window
        down = -seed[seed < 0].sum()/window
        rs = up/down if down != 0 else 0
        rsi = np.zeros_like(prices)
        rsi[:window] = 50. # Neutral for initial
        
        # We need simpler loop for consistency with list logic if we want pure python, 
        # but numpy is cleaner for this specific calc.
        # Let's use a standard iterative approach for the rest
        # To match previous logic, we can stick to simple loop
        
        rsis = [50] * len(prices) # Default
        if len(prices) > window:
            gains = []
            losses = []
            
            # First average
            for i in range(1, window+1):
                delta = prices[i] - prices[i-1]
                if delta > 0:
                    gains.append(delta)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(delta))
            
            avg_gain = sum(gains) / window
            avg_loss = sum(losses) / window
            
            # Subsequent
            for i in range(window + 1, len(prices)):
                delta = prices[i] - prices[i-1]
                gain = delta if delta > 0 else 0
                loss = abs(delta) if delta < 0 else 0
                
                avg_gain = (avg_gain * (window - 1) + gain) / window
                avg_loss = (avg_loss * (window - 1) + loss) / window
                
                if avg_loss == 0:
                    rsis[i] = 100
                else:
                    rs = avg_gain / avg_loss
                    rsis[i] = 100 - (100 / (1 + rs))
                    
        return rsis

    def calculate_technicals(self, history_list):
        """Calculates RSI, MACD, and SMAs on a List of Dicts."""
        if not history_list or len(history_list) < 50:
            return None
        
        # Extract Closing Prices
        closes = [float(item['Close']) for item in history_list]
        
        # SMA
        sma_50 = self._calculate_sma(closes, 50)
        sma_200 = self._calculate_sma(closes, 200)
        
        # RSI
        rsi = self._calculate_rsi(closes)
        
        # MACD (EMA12 - EMA26)
        ema_12 = self._calculate_ema(closes, 12)
        ema_26 = self._calculate_ema(closes, 26)
        macd_line = [e12 - e26 for e12, e26 in zip(ema_12, ema_26)]
        
        # Signal Line (EMA9 of MACD)
        signal_line = self._calculate_ema(macd_line, 9)
        macd_diff = [m - s for m, s in zip(macd_line, signal_line)]
        
        # Enrich List
        for i, item in enumerate(history_list):
            item['RSI'] = rsi[i]
            item['SMA_50'] = sma_50[i]
            item['SMA_200'] = sma_200[i]
            item['MACD_Diff'] = macd_diff[i]
            
        return history_list

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
            asset_turnover = info.get('revenuePerShare', 0) # Proxy
            if asset_turnover > 0: score += 1

            # Leverage/Liquidity
            current_ratio = info.get('currentRatio', 0)
            if current_ratio and current_ratio > 1: score += 1
            
            debt_equity = info.get('debtToEquity', 0)
            if debt_equity and debt_equity < 1: score += 1
            
        except:
            pass
            
        return int((score / 5) * 9)

    def calculate_graham_number(self, info):
        """Calculates Graham Number."""
        eps = info.get('trailingEps')
        bvps = info.get('bookValue')
        
        if eps and bvps and eps > 0 and bvps > 0:
            return math.sqrt(22.5 * eps * bvps)
        return 0

    def score_fundamental(self, info):
        """Advanced Fundamental Scoring (Calibrated for NSE)."""
        if not info:
            return 0
        
        score = 0
        checks = 0

        # 1. Graham Value
        graham_num = self.calculate_graham_number(info)
        current_price = info.get('currentPrice', info.get('previousClose', 0))
        if graham_num and current_price and current_price < graham_num:
            score += 2
        checks += 2

        # 2. Piotroski F-Score
        f_score = self.get_piotroski_score(info)
        if f_score >= 7:
            score += 2
        elif f_score >= 5:
            score += 1
        checks += 2

        # 3. PE Valuation
        pe = info.get('trailingPE')
        if pe:
            if 0 < pe < 40:
                score += 1
            checks += 1
        elif info.get('trailingEps', 0) < 0:
             checks += 1

        # 4. PEG
        peg = info.get('pegRatio')
        if peg is not None:
            if 0 < peg < 2.0:
                score += 1
            checks += 1

        # 5. ROE
        roe = info.get('returnOnEquity')
        if roe:
            if roe > 0.10:
                score += 1
            checks += 1

        # 6. D/E
        de = info.get('debtToEquity')
        if de is not None:
             if de < 100: 
                 score += 1
             checks += 1
        
        return score / checks if checks > 0 else 0

    def get_investment_thesis(self, row, info):
        """Generates a natural language explanation."""
        reasons = []
        
        # Tech Reason
        if row.get('Tech_Score', 0) > 0.7:
            reasons.append("Strong technical uptrend (RSI/MACD bullish)")
        
        # Fund Reason
        graham = self.calculate_graham_number(info)
        price = row.get('Close', 0)
        if graham > price:
            upside = ((graham - price) / price) * 100
            reasons.append(f"Undervalued by ~{int(upside)}% (Graham)")
            
        f_score = self.get_piotroski_score(info)
        if f_score >= 7:
            reasons.append(f"High Financial Strength (F-Score {f_score}/9)")
            
        # Sentiment
        if row.get('Sent_Score', 0) > 0.6:
            reasons.append("Positive Market Sentiment")
            
        if not reasons:
            return "Balanced moderate growth candidate."
            
        return ". ".join(reasons) + "."

    def get_technical_score(self, history_list):
        """Returns a normalized score 0-1 based on technicals."""
        if not history_list:
            return 0
        
        last_row = history_list[-1]
        score = 0
        checks = 0

        # RSI Strategy
        rsi = last_row.get('RSI', 50)
        if 40 < rsi < 70:
            score += 1 
        elif rsi <= 30:
            score += 2
        checks += 2

        # MACD
        if last_row.get('MACD_Diff', 0) > 0:
            score += 1
        checks += 1

        # Golden Cross
        sma50 = last_row.get('SMA_50')
        sma200 = last_row.get('SMA_200')
        if sma50 and sma200 and sma50 > sma200:
            score += 1
        checks += 1
        
        # Trend
        close = last_row.get('Close')
        if close and sma200 and close > sma200:
             score += 1
        checks += 1

        return score / checks if checks > 0 else 0

    def analyze_sentiment(self, headlines):
        if not headlines:
            return 0.5
        scores = [self.sia.polarity_scores(h)['compound'] for h in headlines]
        avg_score = np.mean(scores)
        return (avg_score + 1) / 2
