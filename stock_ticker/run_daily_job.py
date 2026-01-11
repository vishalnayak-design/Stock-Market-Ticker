import os
import sys
import logging
import subprocess
import pandas as pd

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - JOB - %(message)s')

def run_step(command):
    logging.info(f"Running: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        logging.error(f"Command failed: {command}")
        sys.exit(1)

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(base_dir)
    
    # 1. Fetch Data
    logging.info("STEP 1: Fetching Market Data...")
    run_step(f'python "{os.path.join(base_dir, "main.py")}" --fetch-only')
    
    # 2. Run Main Analysis
    logging.info("STEP 2: Running Main Model...")
    run_step(f'python "{os.path.join(base_dir, "main.py")}" --analyze-only')
    
    # 3. Big Bets Analysis
    logging.info("STEP 3: Running Big Bets Logic...")
    try:
        from src.medium_term_strategy import MediumTermEngine
        data_dir = os.path.join(base_dir, "data")
        input_path = os.path.join(data_dir, "full_analysis.csv")
        output_path = os.path.join(data_dir, "big_bets_results.csv")
        
        if os.path.exists(input_path):
            df = pd.read_csv(input_path)
            engine = MediumTermEngine()
            top_picks, full_results, _ = engine.run_analysis(df, amount=200000, duration_months=12)
            full_results.to_csv(output_path, index=False)
            logging.info(f"Big Bets saved to {output_path}")
        else:
            logging.warning("Skipped Big Bets: full_analysis.csv not found.")
            
    except Exception as e:
        logging.error(f"Big Bets Failed: {e}")
        sys.exit(1)
        
    logging.info("Daily Job Completed Successfully.")

if __name__ == "__main__":
    main()
