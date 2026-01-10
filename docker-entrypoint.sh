#!/bin/bash

# 1. Start Streamlit Dashboard in the background
echo "Starting Dashboard..."
streamlit run stock_ticker/dashboard.py --server.port 8501 --server.address 0.0.0.0 &

# 2. Start the Scheduler (Python Daemon)
echo "Starting Scheduler (9:30 AM Daily)..."
python scheduler_daemon.py
