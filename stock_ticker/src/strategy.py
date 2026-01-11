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
                "Sector": funds.get('sector', 'Unknown'),
                "Industry": funds.get('industry', 'Unknown'),
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
                "Market_Cap": funds.get('marketCap', 0),
                "Div_Yield": funds.get('dividendYield', 0),
                
                # Big Bets Fields (Mapped)
                "ROCE": funds.get('returnOnCapitalEmployed', 0), 
                "OPM": funds.get('operatingMargins', 0) * 100 if funds.get('operatingMargins') else 0,
                "FreeCashFlow": funds.get('freeCashflow', 0) / 10000000 if funds.get('freeCashflow') else 0, # Convert to Cr
                "SalesGrowth3Y": 0, # Not avaialble in standard yF info
                "ProfitGrowth3Y": 0, # Not available in standard yF info
                "QtrSalesGrowth": funds.get('revenueGrowth', 0) * 100 if funds.get('revenueGrowth') else 0,
                "QtrProfitGrowth": funds.get('earningsGrowth', 0) * 100 if funds.get('earningsGrowth') else 0,
                
                "Reason": reason
            }
        except Exception as e:
            logging.error(f"Error processing {row.get('Ticker')}: {e}")
            return None

    def run_fetch_only(self, limit=None):
        """Downloads data and performs basic analysis (Reuses full pipeline)."""
        logging.info("Starting Data Download & Basic Scan...")
        # Use full analysis but skip the heavy forecast step
        self.run_full_analysis(limit=limit, skip_forecast=True)

    def run_full_analysis(self, limit=None, skip_fetch=False, skip_forecast=False):
        """Runs the E2E analysis pipeline with Parallel Processing."""
        logging.info("Starting Analysis...")
        
        import concurrent.futures
        from src.utils import read_csv_to_list
        
        # 1. Get List
        stocks_list = self.ingestor.get_nse_equity_list()
        
        # Shuffle to ensure diverse partial results
        import random
        random.shuffle(stocks_list)
        
        if limit:
            stocks_list = stocks_list[:limit]
        
        # --- RESUME CAPABILITY ---
        results = []
        analysis_path = f"{self.config['data_dir']}/full_analysis.csv"
        processed_tickers = set()

        if os.path.exists(analysis_path):
            logging.info("Found existing analysis file. Resuming...")
            try:
                results = read_csv_to_list(analysis_path)
                processed_tickers = {row['Ticker'] for row in results if row.get('Ticker')}
                logging.info(f"Already processed {len(processed_tickers)} stocks.")
            except Exception as e:
                logging.warning(f"Failed to read existing analysis: {e}. Starting fresh.")
        
        # Filter stocks to process
        stocks_to_process = [s for s in stocks_list if s['Ticker'] not in processed_tickers]
        
        if not stocks_to_process and not skip_fetch:
            logging.info("All stocks already processed. Skipping analysis phase.")
        else:
            # 2. Parallel Execution
            total_to_process = len(stocks_to_process)
            logging.info(f"Scanning {total_to_process} remaining stocks (Parallel Mode)...")
            
            # Use ThreadPoolExecutor for I/O bound tasks
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # Map returns iterator in order
                futures = {executor.submit(self.process_stock, row): row for row in stocks_to_process}
                
                count = 0 
                # Import State Manager safely
                try:
                    import src.state_manager as sm
                except ImportError:
                    sm = None
                
                # Update total scanned in state for UI
                current_total = len(processed_tickers)

                for future in tqdm(concurrent.futures.as_completed(futures), total=total_to_process, desc="Analyzing Stocks"):
                    try:
                        res = future.result()
                        if res:
                            results.append(res)
                            current_total += 1
                    except Exception as e:
                        logging.error(f"Analysis Crash for a stock: {e}")
                    
                    count += 1
                    
                    # Heartbeat every item
                    if sm:
                         sm.update_heartbeat(count=current_total)
                         
                    if count % 10 == 0:
                         logging.info(f"[{count}/{total_to_process}] Processed in this batch... Total: {current_total}")
                         # Incremental save
                         if len(results) > 0:
                             save_to_csv(results, analysis_path)

        # Final Save of Pre-Analysis
        save_to_csv(results, analysis_path)

        # 3. Filter Top Candidates for LSTM
        if not results:
            return []
        
        # Sort by Pre_Score Descending (Convert to float first to be safe)
        for r in results:
            try:
                if 'Pre_Score' in r: r['Pre_Score'] = float(r['Pre_Score'])
            except: pass

        results.sort(key=lambda x: x.get('Pre_Score', 0), reverse=True)
        top_candidates = results[:20] 

        # 4. Run Forecasting on Top Candidates (Optional)
        if not skip_forecast:
            logging.info(f"Running Forecasting on Top {len(top_candidates)}...")
            
            updates = {}
            for row in top_candidates:
                ticker = row['Ticker']
                # Fetch history again for pure fresh forecast (or use cached if feasible, but history needs to be fresh)
                # Forecaster internally uses standard logic.
                # We already fetched history in process_stock but didn't save it to CSV to save space.
                # So we fetch again.
                hist = self.ingestor.fetch_stock_history(ticker)
                
                # Forecast
                forecast_score = self.forecaster.get_forecast_score(hist)
                
                # Final Score
                pre = row.get('Pre_Score', 0)
                final_score = pre + (self.weights.get('forecast', 0) * forecast_score)
                
                # Score Boosting
                if final_score > 0.5:
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
                    row['Final_Score'] = row.get('Pre_Score', 0)

            # Resort by Final Score
            results.sort(key=lambda x: x.get('Final_Score', 0), reverse=True)
        
        # 6. Allocation (Top N)
        top_n = results[:self.config['top_n_stocks']]
        budget = self.config['monthly_budget']
        
        # Calculate Allocation manually
        final_recommendations = []
        if top_n:
            allocation_per_stock = budget / len(top_n)
            for item in top_n:
                rec_item = item.copy()
                rec_item['Allocation'] = allocation_per_stock
                price = float(item.get('Close', 1))
                if price > 0:
                    rec_item['Qty'] = int(allocation_per_stock / price)
                else:
                    rec_item['Qty'] = 0
                final_recommendations.append(rec_item)
        
        # Save results
        save_to_csv(final_recommendations, f"{self.config['data_dir']}/recommendations.csv")
        save_to_csv(results, analysis_path)
        
        return final_recommendations
