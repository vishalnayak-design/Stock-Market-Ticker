import yaml
import logging
import sys
import os
import argparse
import pandas as pd
from src.strategy import RecommendationEngine
from src.notifications import Notifier
from src.portfolio_manager import PortfolioManager
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--limit", type=int, help="Limit number of stocks to analyze")
    args = parser.parse_args()

    config = load_config()
    
    # Ensure data_dir is absolute
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(config['data_dir']):
        config['data_dir'] = os.path.join(base_dir, config['data_dir'])
    
    engine = RecommendationEngine(config)
    pm = PortfolioManager(config['data_dir'])
    notifier = Notifier(config)

    logging.info("Starting Daily Analysis Pipeline...")
    
    # Determine limit
    limit = args.limit if args.limit else (5 if args.test else None)
    
    # 1. Run Analysis
    rec_df = engine.run_full_analysis(limit=limit)
    full_df = pd.read_csv(os.path.join(config['data_dir'], "full_analysis.csv"))
    
    # 2. Save Snapshots
    pm.save_daily_snapshot(full_df, rec_df)

    if not rec_df.empty:
        logging.info("Analysis Complete. Updating Portfolio History...")
        
        # 3. Update History & Detect Changes
        changes_df = pm.update_portfolio(rec_df)
        
        print("\n\nTOP RECOMMENDATIONS:")
        print(rec_df[['Name', 'Ticker', 'Final_Score', 'Allocation']])
        
        # 4. Notify
        notifier.send_recommendation(rec_df)
    else:
        logging.warning("No recommendations found.")

if __name__ == "__main__":
    main()
