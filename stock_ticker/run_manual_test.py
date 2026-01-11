import sys
import os
sys.path.append(os.path.dirname(__file__))
import logging

# Configure logging to stdout
logging.basicConfig(level=logging.INFO)

try:
    from auto_pilot import run_big_bets_task
    print("Running Manual Big Bets Test...")
    run_big_bets_task()
    print("Done.")
except Exception as e:
    print(f"Error: {e}")
