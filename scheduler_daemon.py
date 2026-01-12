import schedule
import time
import subprocess
import os
import logging
from datetime import datetime

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SCHEDULER] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stock_ticker/data/scheduler.log", mode='a')
    ]
)

def run_analysis():
    logging.info("⏰ Time to run! Starting daily analysis...")
    try:
        # Run main.py using the same python interpreter
        start_time = datetime.now()
        result = subprocess.run(["python", "stock_ticker/main.py"], capture_output=True, text=True)
        end_time = datetime.now()
        duration = end_time - start_time
        
        if result.returncode == 0:
            logging.info(f"✅ Analysis Completed Successfully in {duration}.")
        else:
            logging.error(f"❌ Analysis Failed. \nOutput: {result.stdout}\nError: {result.stderr}")
            
    except Exception as e:
        logging.error(f"❌ Scheduler crashed while launching analysis: {e}")

def main():
    logging.info("🚀 Stock Analysis Scheduler Started.")
    logging.info("📅 Schedule set for daily at 12:00 PM.")
    
    # Schedule the job
    schedule.every().day.at("12:00").do(run_analysis)
    
    # Also run once specifically on startup if needed (Optional, user might prefer wait)
    # logging.info("Running initial check...")
    # run_analysis() 

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
