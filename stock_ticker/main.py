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
# Debugging Start
import sys
import os
print(f"DEBUG: main.py starting. CWD={os.getcwd()}", file=sys.stderr)

try:
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    log_file = os.path.join(log_dir, "app_activity.log")
    
    # FORCE File Logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if root_logger.handlers: root_logger.handlers = []

    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root_logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    root_logger.addHandler(stream_handler)
         
    logging.info("Logging Initialized. Writing to: " + log_file)
except Exception as e:
    print(f"CRITICAL: Logging Setup Failed: {e}", file=sys.stderr)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--limit", type=int, help="Limit number of stocks to analyze")
    parser.add_argument("--fetch-only", action="store_true", help="Only download data")
    parser.add_argument("--analyze-only", action="store_true", help="Run analysis on existing data")
    args = parser.parse_args()

    config = load_config()
    
    # Ensure data_dir is absolute
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(config['data_dir']):
        config['data_dir'] = os.path.join(base_dir, config['data_dir'])
    
    engine = RecommendationEngine(config)
    pm = PortfolioManager(config['data_dir'])
    notifier = Notifier(config)

    # State Manager Init
    import src.state_manager as sm
    sm.set_status(sm.STATUS_RUNNING, stage="STARTUP")
    sm.update_heartbeat(pid=os.getpid())

    logging.info("Starting Daily Analysis Pipeline...")
    
    # Determine limit
    limit = args.limit if args.limit else (5 if args.test else None)
    
    try:
        # 1. Run Analysis or Fetch
        if args.fetch_only:
            logging.info("Mode: Fetch Only")
            sm.set_status(sm.STATUS_RUNNING, stage=sm.STAGE_FETCH)
            engine.run_fetch_only(limit=limit)
            # Mark complete if successful
            sm.mark_flag("fetch_complete", True)
            sm.set_status(sm.STATUS_COMPLETED, stage=sm.STAGE_FETCH)
            return
    
        logging.info(f"Mode: {'Analysis Only' if args.analyze_only else 'Full Run'}")
        
        # Run Analysis (Skip fetch if analyze_only)
        sm.set_status(sm.STATUS_RUNNING, stage=sm.STAGE_MODEL)
        rec_df = engine.run_full_analysis(limit=limit, skip_fetch=args.analyze_only)
        sm.mark_flag("model_complete", True)
        
        # If we just ran fetch, we returned early. If we are here, we have analysis results.
        if not rec_df:
            logging.warning("No recommendations generated.")
            sm.set_status(sm.STATUS_COMPLETED, stage="NO_RESULTS")
            return
    except Exception as e:
        logging.error(f"Pipeline Crash: {e}")
        sm.set_status(sm.STATUS_FAILED)
        raise e
        
    sm.set_status(sm.STATUS_COMPLETED)

    full_df = pd.read_csv(os.path.join(config['data_dir'], "full_analysis.csv"))
    
    # 2. Save Snapshots
    pm.save_daily_snapshot(full_df, rec_df)

    logging.info("Analysis Complete. Updating Portfolio History...")
        
    # 3. Update History & Detect Changes
    changes_df = pm.update_portfolio(rec_df)
    
    print("\n\nTOP RECOMMENDATIONS:")
    # print(rec_df[['Name', 'Ticker', 'Final_Score', 'Allocation']]) # rec_df is list of dicts now
    # Just log success
    
    # 4. Notify
    notifier.send_recommendation(rec_df)

    # 5. Cloud Sync
    try:
        from src.utils import push_to_github
        push_to_github(f"Auto-update: {len(rec_df)} recommendations")
    except Exception as e:
        logging.warning(f"Cloud sync module failed: {e}")

if __name__ == "__main__":
    main()
