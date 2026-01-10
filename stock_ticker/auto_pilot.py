import time
import os
import sys
import subprocess
import logging
from datetime import datetime

# Setup paths
sys.path.append(os.path.dirname(__file__))
import src.state_manager as sm

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

def main_loop():
    logging.info("Auto-Pilot Started. Monitoring pipeline...")
    
    while True:
        try:
            state = sm.load_state()
            status = state['status']
            stage = state.get('stage')
            
            # 1. DAILY RESET Check
            current_date = datetime.now().strftime("%Y-%m-%d")
            saved_date = state.get('run_date', '')
            
            if current_date != saved_date:
                logging.info(f"New Day Detected! Resetting flags. ({saved_date} -> {current_date})")
                state['run_date'] = current_date
                state['flags'] = {'fetch_complete': False, 'model_complete': False}
                state['total_scanned'] = 0
                state['status'] = sm.STATUS_IDLE
                sm.save_state(state)
                status = sm.STATUS_IDLE # Update local var
            
            # 2. WATCHDOG: Check if stuck
            is_stuck, stuck_pid = sm.check_stuck(timeout_seconds=1800) # 30 mins
            if is_stuck:
                logging.error(f"Watchdog Triggered! Process {stuck_pid} stuck for >30m.")
                kill_process(stuck_pid)
                sm.set_status(sm.STATUS_FAILED)
                status = sm.STATUS_FAILED 
            
            # 3. AUTO-RESTART / NEXT STAGE logic
            if status in [sm.STATUS_IDLE, sm.STATUS_COMPLETED, sm.STATUS_FAILED]:
                
                # Logic: Fetch -> Model
                fetch_done = state['flags'].get('fetch_complete', False)
                model_done = state['flags'].get('model_complete', False)
                total_scanned = state.get('total_scanned', 0)
                
                # If Fetch not done OR count is suspicious (low count but marked done?)
                # Actually, user said 1800+
                if total_scanned > 1800:
                    if not fetch_done: sm.mark_flag("fetch_complete", True)
                    fetch_done = True
                
                if not fetch_done:
                    logging.info("Triggering FETCH...")
                    run_main(["--fetch-only"])
                    time.sleep(60) # Wait for startup
                
                elif not model_done:
                    logging.info("Triggering MODEL...")
                    run_main(["--analyze-only"])
                    time.sleep(60)
                    
                else:
                    # All Done
                    # Maybe check time? Reset flags next day?
                    # For now, just idle if done.
                    pass

            time.sleep(60) # Check every minute
            
        except Exception as e:
            logging.error(f"Auto-Pilot Loop Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
