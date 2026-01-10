import pandas as pd
import logging
from src.data_ingestion import DataIngestor
from src.analysis import Analyzer
from src.models import Forecaster
from src.utils import save_to_csv
from tqdm import tqdm
import os

class RecommendationEngine:
    def __init__(self, config):
        self.config = config
        self.ingestor = DataIngestor(data_dir=config['data_dir'])
        self.analyzer = Analyzer()
        self.forecaster = Forecaster(lookback=config['lookback_days'])
        self.weights = config['weights']

    def run_full_analysis(self, limit=None):
        """Runs the E2E analysis pipeline."""
        logging.info("Starting Analysis...")
        
        # 1. Get List
        stocks = self.ingestor.get_nse_equity_list()
        if limit:
            stocks = stocks.head(limit)
        
        results = []
        
        # 2. Iterate
        # Use tqdm for progress bar, file is used for dashboard status
        total = len(stocks)
        logging.info(f"Scanning {total} stocks...")
        
        for index, row in tqdm(stocks.iterrows(), total=total, desc="Analyzing Stocks"):
            ticker = row['Ticker']
            name = row.get('NAME OF COMPANY', ticker)
            
            # Log for Watchdog (every 5 stocks to reduce IO spam)
            if index % 5 == 0:
                logging.info(f"[{index}/{total}] Analyzing {ticker}...")

            # Fetch Data
            hist = self.ingestor.fetch_stock_history(ticker, period=self.config['history_period'])
            funds = self.ingestor.fetch_fundamentals(ticker)
            news = self.ingestor.fetch_news(name) 

            if hist is None or hist.empty:
                continue

            # Analyze
            df_tech = self.analyzer.calculate_technicals(hist)
            tech_score = self.analyzer.get_technical_score(df_tech)
            try:
                fund_score = self.analyzer.score_fundamental(funds)
            except:
                fund_score = 0
            sent_score = self.analyzer.analyze_sentiment(news)

            # Pre-calculate partial score
            pre_score = (self.weights['technical'] * tech_score + 
                         self.weights['fundamental'] * fund_score + 
                         self.weights['sentiment'] * sent_score)

            # Investment Thesis
            reason = self.analyzer.get_investment_thesis({"Tech_Score": tech_score, "Sent_Score": sent_score, "Close": hist['Close'].iloc[-1]}, funds)
            
            result_row = {
                "Ticker": ticker,
                "Name": name,
                "Close": round(hist['Close'].iloc[-1], 2),
                
                # Scores
                "Final_Score": pre_score, # Placeholder
                "Tech_Score": tech_score,
                "Fund_Score": fund_score,
                "Sent_Score": sent_score,
                "Pre_Score": pre_score,
                "Forecast_Score": 0,
                
                # Raw Metrics (For User Visibility & Blue Chip Strategy)
                "PE_Ratio": funds.get('trailingPE', 0),
                "ROE": funds.get('returnOnEquity', 0),
                "Debt_to_Equity": funds.get('debtToEquity', 0), # in %
                "PEG_Ratio": funds.get('pegRatio', 0),
                "Market_Cap": funds.get('marketCap', 0),
                "Div_Yield": funds.get('dividendYield', 0),
                
                "Reason": reason
            }
            results.append(result_row)
            
            # INCREMENTAL SAVE (Every 10 stocks)
            if len(results) % 10 == 0:
                temp_df = pd.DataFrame(results)
                save_to_csv(temp_df, f"{self.config['data_dir']}/full_analysis.csv")
                
        # Final Save of Pre-Analysis
        save_to_csv(pd.DataFrame(results), f"{self.config['data_dir']}/full_analysis.csv")

        # 3. Filter Top Candidates for LSTM
        df_results = pd.DataFrame(results)
        if df_results.empty:
            return df_results
        
        df_results = df_results.sort_values(by="Pre_Score", ascending=False)
        top_candidates = df_results.head(20) # Top 20 for LSTM

        # 4. Run Forecasting on Top Candidates
        final_results = []
        for _, row in top_candidates.iterrows():
            ticker = row['Ticker']
            hist = self.ingestor.fetch_stock_history(ticker) # Re-fetch or load from cache
            
            # Forecast
            forecast_score = self.forecaster.get_forecast_score(hist)
            
            # Final Score
            final_score = row['Pre_Score'] + (self.weights['forecast'] * forecast_score)
            
            row['Forecast_Score'] = forecast_score
            row['Final_Score'] = final_score
            final_results.append(row)

        df_final = pd.DataFrame(final_results)
        df_final = df_final.sort_values(by="Final_Score", ascending=False)
        
        # 5. Allocation
        top_n = df_final.head(self.config['top_n_stocks'])
        budget = self.config['monthly_budget']
        
        # Simple Equal Weight Allocation
        if not top_n.empty:
            top_n['Allocation'] = budget / len(top_n)
            top_n['Qty'] = (top_n['Allocation'] / top_n['Close']).astype(int)
        
        # Save results
        save_to_csv(top_n, f"{self.config['data_dir']}/recommendations.csv")
        save_to_csv(df_final, f"{self.config['data_dir']}/full_analysis.csv")
        
        return top_n
