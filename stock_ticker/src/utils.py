import pandas as pd
import os
import logging

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def save_to_parquet(df, path):
    try:
        ensure_dir(os.path.dirname(path))
        df.to_parquet(path, engine='pyarrow')
        logging.info(f"Saved data to {path}")
    except Exception as e:
        logging.error(f"Failed to save {path}: {e}")

def load_from_parquet(path):
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None

def save_to_csv(df, path):
    try:
        ensure_dir(os.path.dirname(path))
        df.to_csv(path, index=False)
        logging.info(f"Saved data to {path}")
    except Exception as e:
        logging.error(f"Failed to save {path}: {e}")
