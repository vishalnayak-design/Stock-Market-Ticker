import time
import os
import sys
import subprocess
import logging
from datetime import datetime

# Setup paths
sys.path.append(os.path.dirname(__file__))
import src.state_manager as sm
from src.medium_term_strategy import MediumTermEngine
from src.utils import push_to_github
import pandas as pd

# Config
PYTHON_EXE = sys.executable
MAIN_SCRIPT = os.path.join(os.path.dirname(__file__), "main.py")
LOG_FILE = os.path.join(os.path.dirname(__file__), "data", "auto_pilot.log")

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - AUTO_PILOT - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() # For dev
    ]
)

def run_main(flags=[]):
    """Runs the main script with flags."""
    cmd = [PYTHON_EXE, MAIN_SCRIPT] + flags
    logging.info(f"Starting process: {cmd}")
    # Run detached/background? No, blocking for simplicity in this daemon loop
    # actually, daemon should supervise.
    subprocess.Popen(cmd)

def kill_process(pid):
    try:
        if pid:
            import psutil
            p = psutil.Process(pid)
            p.kill()
            logging.warning(f"Killed stuck process {pid}")
    except Exception as e:
        logging.error(f"Failed to kill {pid}: {e}")

def start_new_day():
    """Resets the state to trigger a new pipeline run."""
    logging.info("Midnight Triggered! Starting new day...")
    try:
        state = sm.load_state()
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Reset Logic
        state['run_date'] = current_date
        state['flags'] = {'fetch_complete': False, 'model_complete': False, 'big_bets_complete': False}
        state['total_scanned'] = 0
        state['status'] = sm.STATUS_IDLE
        sm.save_state(state)
        logging.info(f"State reset for {current_date}. Pipeline should pick this up shortly.")
        
    except Exception as e:
        logging.error(f"Failed to reset state at midnight: {e}")

def run_big_bets_task():
    """Runs the Big Bets Analysis and marks it complete."""
    try:
        logging.info("Starting Big Bets Analysis...")
        
        # Paths
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        input_path = os.path.join(data_dir, "full_analysis.csv")
        output_path = os.path.join(data_dir, "big_bets_results.csv")
        
        if not os.path.exists(input_path):
            logging.error("Big Bets Skipped: full_analysis.csv not found.")
            return

        # Load Data
        df = pd.read_csv(input_path)
        
        # Run Engine
        engine = MediumTermEngine()
        # Using default settings: 2L investment, 12 months duration (Value+Growth)
        top_picks, full_results, _ = engine.run_analysis(df, amount=200000, duration_months=12)
        
        # Save Results
        full_results.to_csv(output_path, index=False)
        logging.info(f"Big Bets Results Saved to {output_path}")
        
        # Update State Flag
        state = sm.load_state()
        state['flags']['big_bets_complete'] = True
        sm.save_state(state)
        
        # Auto-Push Data (Since this is backend automation, we can push results to repo)
        try:
            push_to_github("Auto-Pilot: Daily Data Sync (All Files)")
            logging.info("Pushed All Data results to GitHub.")
        except Exception as ge:
            logging.warning(f"Git Push Failed: {ge}")
            
    except Exception as e:
        logging.error(f"Big Bets Task Failed: {e}")

def main_loop():
    logging.info("Auto-Pilot Started. Schedule: Daily at 12:00 AM (Midnight).")
    
    # 1. Schedule the Reset
    import schedule
    schedule.every().day.at("00:00").do(start_new_day)
    
    logging.info(f"Next run scheduled for: {schedule.next_run()}")
    
    while True:
        try:
            # Run pending schedule jobs
            schedule.run_pending()
            
            # Existing Pipeline Supervision Logic (Stays active to monitor the job once started)
            state = sm.load_state()
            status = state['status']
            
            # WATCHDOG
            is_stuck, stuck_pid = sm.check_stuck(timeout_seconds=1800)
            if is_stuck:
                logging.error(f"Watchdog Triggered! Process {stuck_pid} stuck.")
                kill_process(stuck_pid)
                sm.set_status(sm.STATUS_FAILED)
                status = sm.STATUS_FAILED 

            # PIPELINE EXECUTION LOGIC
            # Only act if we have work to do (flags are False)
            fetch_done = state['flags'].get('fetch_complete', False)
            model_done = state['flags'].get('model_complete', False)
            big_bets_done = state['flags'].get('big_bets_complete', False)
            
            # Check if we should be running
            # If date matches today (meaning start_new_day ran), proceed.
            # If date is old, we DO NOT Run (Strict Midnight requirement)
            current_date = datetime.now().strftime("%Y-%m-%d")
            run_date = state.get('run_date', '')
            
            if run_date == current_date:
                # We are in the active day
                if status in [sm.STATUS_IDLE, sm.STATUS_COMPLETED, sm.STATUS_FAILED]:
                     if not fetch_done:
                         logging.info("Triggering FETCH...")
                         run_main(["--fetch-only"])
                         time.sleep(60) 
                     
                     elif not model_done:
                         logging.info("Triggering MODEL...")
                         run_main(["--analyze-only"])
                         time.sleep(60)

                     elif not big_bets_done:
                         logging.info("Triggering BIG BETS...")
                         run_big_bets_task()
                         time.sleep(60)
            
            time.sleep(60) # Check every minute
            
        except Exception as e:
            logging.error(f"Auto-Pilot Loop Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
