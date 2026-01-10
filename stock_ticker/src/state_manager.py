import json
import os
import time
import logging

STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "pipeline_state.json")

# Status Constants
STATUS_IDLE = "IDLE"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"

STAGE_FETCH = "FETCH"
STAGE_MODEL = "MODEL"
STAGE_ALLOCATION = "ALLOCATION"

def _get_default_state():
    return {
        "status": STATUS_IDLE,
        "stage": None,
        "last_heartbeat": 0,
        "total_scanned": 0,
        "pid": None,
        "flags": {
            "fetch_complete": False,
            "model_complete": False
        }
    }

def load_state():
    if not os.path.exists(STATE_FILE):
        return _get_default_state()
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        # Prevent recursion by returning DEFAULT directly instead of calling load_state
        return _get_default_state()

def save_state(state):
    try:
        # Ensure dir exists
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        logging.error(f"Failed to save state: {e}")

def update_heartbeat(count=None, pid=None):
    """Updates the heartbeat timestamp to prove we are alive."""
    state = load_state()
    state['last_heartbeat'] = time.time()
    if count is not None:
        state['total_scanned'] = count
    if pid is not None:
        state['pid'] = pid
    save_state(state)

def set_status(status, stage=None):
    state = load_state()
    state['status'] = status
    if stage:
        state['stage'] = stage
    state['last_heartbeat'] = time.time()
    save_state(state)

def check_stuck(timeout_seconds=1800): # 30 mins default
    state = load_state()
    if state['status'] == STATUS_RUNNING:
        elapsed = time.time() - state.get('last_heartbeat', 0)
        if elapsed > timeout_seconds:
            return True, state.get('pid')
    return False, None

def mark_flag(flag_name, value=True):
    state = load_state()
    state['flags'][flag_name] = value
    save_state(state)
