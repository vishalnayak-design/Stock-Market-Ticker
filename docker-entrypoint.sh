#!/bin/bash

# 1. Start Streamlit Dashboard in the background
echo "Starting Dashboard..."
streamlit run stock_ticker/dashboard.py --server.port 8501 --server.address 0.0.0.0 &

# 2. Start the Smart Auto-Pilot (Daemon)
echo "Starting Auto-Pilot..."

# Install runtime deps if missing (Hot-Fix)
pip install watchdog nsepython --no-cache-dir || true

# Run with auto-restart on file change (if watchdog available)
if command -v watchmedo &> /dev/null; then
    echo "Running with Watchdog for Hot-Reloading..."
    watchmedo auto-restart --directory=./stock_ticker --pattern=*.py --recursive -- python stock_ticker/auto_pilot.py
else
    echo "Running Standard Python..."
    python stock_ticker/auto_pilot.py
fi
