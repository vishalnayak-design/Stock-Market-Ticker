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

    def process_stock(self, row):
        """Helper to process a single stock (for threading)."""
        try:
            ticker = row.get('Ticker')
            name = row.get('NAME OF COMPANY', ticker)
            
            # Fetch Data
            hist = self.ingestor.fetch_stock_history(ticker, period=self.config['history_period'])
            funds = self.ingestor.fetch_fundamentals(ticker)
            news = self.ingestor.fetch_news(name) 

            if not hist:
                return None

            # Analyze
            hist_tech = self.analyzer.calculate_technicals(hist) 
            if not hist_tech:
                 return None
                 
            tech_score = self.analyzer.get_technical_score(hist_tech)
            try:
                fund_score = self.analyzer.score_fundamental(funds)
            except:
                fund_score = 0
            sent_score = self.analyzer.analyze_sentiment(news)
            
            # Buffett DCF Valuation
            intrinsic_val, margin_safety = self.analyzer.calculate_intrinsic_value(funds)

            # Pre-calculate partial score
            pre_score = (self.weights['technical'] * tech_score + 
                         self.weights['fundamental'] * fund_score + 
                         self.weights['sentiment'] * sent_score)

            # Investment Thesis
            last_price = float(hist_tech[-1]['Close'])
            reason = self.analyzer.get_investment_thesis({"Tech_Score": tech_score, "Sent_Score": sent_score, "Close": last_price}, funds)
            
            # Add Buffett Thesis
            if margin_safety > 30:
                reason = f"DCF Undervalued by {margin_safety}%. {reason}"
            elif margin_safety < -20:
                reason = f"Overvalued by {abs(margin_safety)}%. {reason}"
                
            return {
                "Ticker": ticker,
                "Name": name,
                "Close": round(last_price, 2),
                
                # Scores
                "Final_Score": pre_score, # Placeholder
                "Tech_Score": tech_score,
                "Fund_Score": fund_score,
                "Sent_Score": sent_score,
                "Pre_Score": pre_score,
                "Forecast_Score": 0,
                
                # Buffett / Fundamental Metrics
                "Intrinsic_Value": intrinsic_val,
                "Margin_Safety": margin_safety,
                "PE_Ratio": funds.get('trailingPE', 0),
                "ROE": funds.get('returnOnEquity', 0),
                "Debt_to_Equity": funds.get('debtToEquity', 0), 
                "PEG_Ratio": funds.get('pegRatio', 0),
                "Market_Cap": funds.get('marketCap', 0),
                "Div_Yield": funds.get('dividendYield', 0),
                
                "Reason": reason
            }
        except Exception as e:
            logging.error(f"Error processing {row.get('Ticker')}: {e}")
            return None

    def run_fetch_only(self, limit=None):
        """Downloads data only."""
        logging.info("Starting Data Download...")
        stocks_list = self.ingestor.get_nse_equity_list()
        if limit:
            stocks_list = stocks_list[:limit]
            
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for row in stocks_list:
                futures.append(executor.submit(self.ingestor.fetch_stock_history, row['Ticker'], "2y"))
                futures.append(executor.submit(self.ingestor.fetch_fundamentals, row['Ticker']))
            
            count = 0
            total = len(futures)
            
            # State Manager Import
            try:
                import src.state_manager as sm
            except ImportError:
                 sm = None
                 
            for _ in tqdm(concurrent.futures.as_completed(futures), total=total, desc="Downloading Data"):
                count += 1
                
                # Heartbeat
                if sm: sm.update_heartbeat(count=count)
                
                if count % 20 == 0:
                    logging.info(f"Downloading [{count}/{total}]...")
        logging.info("Download Complete.")

    def run_full_analysis(self, limit=None, skip_fetch=False):
        """Runs the E2E analysis pipeline with Parallel Processing."""
        logging.info("Starting Analysis...")
        
        import concurrent.futures
        
        # 1. Get List
        stocks_list = self.ingestor.get_nse_equity_list()
        if limit:
            stocks_list = stocks_list[:limit]
        
        results = []
        
        # 2. Parallel Execution
        total = len(stocks_list)
        logging.info(f"Scanning {total} stocks (Parallel Mode)...")
        
        # Use ThreadPoolExecutor for I/O bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Map returns iterator in order
            futures = {executor.submit(self.process_stock, row): row for row in stocks_list}
            
            count = 0
            # Import State Manager safely
            try:
                import src.state_manager as sm
            except ImportError:
                sm = None

            for future in tqdm(concurrent.futures.as_completed(futures), total=total, desc="Analyzing Stocks"):
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    logging.error(f"Analysis Crash for a stock: {e}")
                
                count += 1
                
                # Heartbeat every item
                if sm:
                     sm.update_heartbeat(count=count)
                     
                if count % 10 == 0:
                     logging.info(f"[{count}/{total}] Processed...")
                     # Incremental save (optional, but good for safety)
                     if len(results) > 0 and len(results) % 50 == 0:
                         save_to_csv(results, f"{self.config['data_dir']}/full_analysis.csv")

        # Final Save of Pre-Analysis
        save_to_csv(results, f"{self.config['data_dir']}/full_analysis.csv")

        # 3. Filter Top Candidates for LSTM
        if not results:
            return []
        
        # Sort by Pre_Score Descending
        results.sort(key=lambda x: x['Pre_Score'], reverse=True)
        top_candidates = results[:20] 

        # 4. Run Forecasting on Top Candidates
        logging.info(f"Running Forecasting on Top {len(top_candidates)}...")
        
        updates = {}
        for row in top_candidates:
            ticker = row['Ticker']
            hist = self.ingestor.fetch_stock_history(ticker)
            
            # Forecast
            forecast_score = self.forecaster.get_forecast_score(hist)
            
            # Final Score
            final_score = row['Pre_Score'] + (self.weights.get('forecast', 0) * forecast_score)
            
            # Score Boosting
            if final_score > 0.5:
                # curve towards 0.99
                final_score = min(0.99, final_score * 1.2) 
            
            updates[ticker] = {
                'Forecast_Score': forecast_score,
                'Final_Score': final_score
            }

        # 5. Merge Updates back into Results
        for row in results:
            if row['Ticker'] in updates:
                row['Forecast_Score'] = updates[row['Ticker']]['Forecast_Score']
                row['Final_Score'] = updates[row['Ticker']]['Final_Score']
            else:
                row['Final_Score'] = row['Pre_Score']

        # Resort by Final Score
        results.sort(key=lambda x: x['Final_Score'], reverse=True)
        
        # 6. Allocation (Top N)
        top_n = results[:self.config['top_n_stocks']]
        budget = self.config['monthly_budget']
        
        # Calculate Allocation manually
        # Avoid setting reference, make copy if needed (dicts are mutable so new dict OK)
        final_recommendations = []
        if top_n:
            allocation_per_stock = budget / len(top_n)
            for item in top_n:
                # Create a copy for recs to avoid polluting full analysis with allocation data if not desired
                # But user wants full analysis. Let's add keys to top_n dicts
                rec_item = item.copy()
                rec_item['Allocation'] = allocation_per_stock
                price = item['Close']
                if price > 0:
                    rec_item['Qty'] = int(allocation_per_stock / price)
                else:
                    rec_item['Qty'] = 0
                final_recommendations.append(rec_item)
        
        # Save results
        save_to_csv(final_recommendations, f"{self.config['data_dir']}/recommendations.csv")
        save_to_csv(results, f"{self.config['data_dir']}/full_analysis.csv")
        
        return final_recommendations
