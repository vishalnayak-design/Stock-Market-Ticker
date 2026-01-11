import streamlit as st
import yaml
import os
import sys
import time
import subprocess
import numpy as np
from datetime import datetime
import io
import re
import xlsxwriter
import logging
import math

# --- Setup Logging to Stdout for Docker Debugging ---
logging.basicConfig(level=logging.INFO)
logging.info("Dashboard script started...")

st.set_page_config(page_title="Stock SIP Dashboard", layout="wide")

# --- Robust Imports ---
try:
    # Ensure 'src' is importable
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Try importing from 'src.utils' (standard)
    try:
        from src.utils import read_csv_to_list, push_to_github
        logging.info("Imported read_csv_to_list from src.utils")
    except ImportError:
        # If 'src' not found, maybe we are inside 'src' or 'stock_ticker' is not in path?
        # Add current dir to path to find 'src' if it is a subdir here
        if current_dir not in sys.path:
            sys.path.append(current_dir)
        
        from src.utils import read_csv_to_list, push_to_github
        logging.info("Imported read_csv_to_list after path fix")

except ImportError as e:
    st.error(f"CRITICAL IMPORT ERROR: {e}")
    logging.error(f"Import failed: {e}")
    st.stop()

# --- Helpers ---
def load_config():
    logging.info("Loading config...")
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

@st.cache_data(ttl=3600) 
def load_data(data_dir, cache_buster):
    logging.info(f"Loading data from {data_dir}...")
    rec_path = os.path.join(data_dir, "recommendations.csv")
    full_path = os.path.join(data_dir, "full_analysis.csv")
    hist_path = os.path.join(data_dir, "portfolio_history.csv")
    
    # DEBUG: Return empty first to test UI
    # return [], [], []
    
    try:
        logging.info("Reading CSVs...")
        rec_data = read_csv_to_list(rec_path)
        full_data = read_csv_to_list(full_path)
        hist_data = read_csv_to_list(hist_path)
        logging.info(f"Read {len(full_data)} rows.")

        # Convert numeric fields
        logging.info("Converting numerics...")
        # Optimization: REMOVED LIMIT as per user request for 1800+ stocks
        # if len(full_data) > 500:
        #      full_data = full_data[:500] 
        
        for row in rec_data + full_data:
            for k, v in row.items():
                try:
                    # Basic check to avoid crashing on complex strings
                    if k not in ['Ticker', 'Name', 'Reason', 'Date'] and v and isinstance(v, (str, int, float)):
                        row[k] = float(v)
                except:
                    pass
        logging.info("Data loaded successfully.")       
        return rec_data, full_data, hist_data
    except Exception as e:
        logging.error(f"Data Load Error: {e}")
        return [], [], []

def get_pipeline_status(data_dir):
    """Reads the centralized log to show what the system is doing."""
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Use centralized log
    # Try multiple locations just in case
    log_path = os.path.join(root_dir, "stock_ticker", "data", "app_activity.log")
    if not os.path.exists(log_path):
        log_path = os.path.join(root_dir, "data", "app_activity.log") # Fallback

    
    if not os.path.exists(log_path):
        return "Waiting for logs...", 0, []
    
    with open(log_path, "r", encoding='utf-8') as f:
        lines = f.readlines()
    
    last_line = lines[-1].strip() if lines else "Waiting..."
    
    # Parse Progress
    progress = 0
    for line in reversed(lines[-20:]):
        match = re.search(r"\[(\d+)/(\d+)\]", line)
        if match:
            current, total = map(int, match.groups())
            if total > 0:
                progress = int((current / total) * 100)
            break
            
    return last_line, progress, lines[-10:]

def run_pipeline_script(flags=[]):
    """Triggers the pipeline with optional flags."""
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    cmd = ["python", os.path.join(root_dir, "stock_ticker", "main.py")] + flags
    
    if os.name == 'nt':
        # On Windows, using subprocess directly for better control
        # Or us the watchdog shell script if preferred, but direct python is cleaner for flags
        subprocess.Popen(cmd, cwd=root_dir)
    else:
        subprocess.Popen(cmd, cwd=root_dir)

def calculate_future_wealth(monthly_investment, years, cagr):
    months = years * 12
    rate = cagr / 100 / 12
    future_value = monthly_investment * ((((1 + rate) ** months) - 1) * (1 + rate)) / rate
    total_invested = monthly_investment * months
    return total_invested, future_value

import math 

# ... (Previous imports)

def to_excel_bytes(data_list):
    """Generates Excel bytes for download using xlsxwriter."""
    try:
        if not data_list:
            logging.warning("to_excel_bytes: No data list provided.")
            return None
            
        logging.info(f"Generating Excel for {len(data_list)} rows...")
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet('Picks')
        
        headers = list(data_list[0].keys())
        
        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        num_fmt = workbook.add_format({'num_format': '#,##0.00'})
        
        # Write Headers
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_fmt)
            
            # Write Data and Format
        # Define Color Rules: (Column Name, 'high_good' or 'low_good', Threshold Green, Threshold Red)
        rules = [
            ('Final_Score', 'high_good', 0.7, 0.4),
            ('Pre_Score', 'high_good', 0.7, 0.4), # Added
            ('ROI_6to12_Score', 'high_good', 12, 6), # Big Bets
            ('QualityScore', 'high_good', 8, 4), # Big Bets
            ('WinProbability', 'high_good', 0.7, 0.4), # Big Bets
            ('Fund_Score', 'high_good', 0.6, 0.3),
            ('Tech_Score', 'high_good', 0.7, 0.4),
            ('Sent_Score', 'high_good', 0.5, 0.2),
            ('Forecast_Score', 'high_good', 0.5, 0.0),
            ('Intrinsic_Value', 'high_good', 0, 0), 
            ('Margin_Safety', 'high_good', 20, -10),
            ('PE_Ratio', 'low_good', 25, 50),
            ('ROE', 'high_good', 0.15, 0.05),
            ('Debt_to_Equity', 'low_good', 50, 200),
            ('PEG_Ratio', 'low_good', 1.0, 2.0),
            ('Div_Yield', 'high_good', 0.02, 0.0),
            ('Market_Cap', 'high_good', 50000000000, 5000000000) # 5k Cr Green, 500Cr Red
        ]
        
        # Map headers to indices
        col_indices = {key: headers.index(key) for key in rules if key in headers}

        for row_num, row_data in enumerate(data_list, start=1):
            for col_num, key in enumerate(headers):
                val = row_data.get(key)
                
                # Write with format if number
                cell_fmt = None
                if isinstance(val, (int, float)):
                     if math.isnan(val) or math.isinf(val):
                         worksheet.write(row_num, col_num, "N/A")
                         continue
                     else:
                         cell_fmt = num_fmt
                         
                     # Check Rules
                     for rule_key, rule_type, thresh_good, thresh_bad in rules:
                         # Use lower case comparison to be safe
                         if key.lower() == rule_key.lower():
                             if rule_type == 'high_good':
                                 if val >= thresh_good: cell_fmt = green_fmt
                                 elif val <= thresh_bad: cell_fmt = red_fmt
                             elif rule_type == 'low_good':
                                 if val <= thresh_good: cell_fmt = green_fmt
                                 elif val >= thresh_bad: cell_fmt = red_fmt
                             break
                     
                     worksheet.write(row_num, col_num, val, cell_fmt)
                else:
                     worksheet.write(row_num, col_num, str(val) if val is not None else "")
    
        workbook.close()
        size = output.tell()
        logging.info(f"Excel generated. Size: {size} bytes")
        return output.getvalue()
        
    except Exception as e:
        logging.error(f"Excel Generation Failed: {e}", exc_info=True)
        return None

# --- Main ---
def main():
    logging.info("Entering main()...")
    config = load_config()
    logging.info("Config loaded.")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(config['data_dir']):
        config['data_dir'] = os.path.join(base_dir, config['data_dir'])
        
    st.title("📈 Smart Stock SIP System")
    logging.info("Title rendered.")

    # --- STATE MANAGER Check ---
    try:
        import src.state_manager as sm
        pipeline_state = sm.load_state()
    except ImportError:
        pipeline_state = {}

    status = pipeline_state.get('status', 'IDLE')
    stage = pipeline_state.get('stage', '')
    is_running = status == 'RUNNING'
    
    # Calculate Heartbeat Age
    last_hb = pipeline_state.get('last_heartbeat', 0)
    hb_age = time.time() - last_hb if last_hb else 0
    
    # --- SIDEBAR: CONTROL PANEL ---
    with st.sidebar:
        st.header("🎮 Control Panel")
        
        # 1. STATUS & ACTIVITY
        st.subheader("📡 System Status")
        
        # Status Card
        if is_running:
            st.info(f"🤖 Auto-Pilot: {stage}\n\nLast Beat: {int(hb_age)}s ago")
        else:
            if status == 'FAILED':
                st.error("⚠️ Pipeline Failed (Check Logs)")
            elif status == 'COMPLETED':
                st.success("✅ Cycle Completed")
            else:
                st.success("🟢 System Idle")


        st.divider()
        
        # Progress Bar (from State)
        count = pipeline_state.get('total_scanned', 0)
        # Assuming ~1900 total
        progress_val = min(100, int((count / 1900) * 100))
        
        if is_running:
            st.progress(progress_val, text=f"{stage}: {count} Stocks Scanned")
            
        # Clear Cache Button

        # Clear Cache Button
        if st.button("Cleared Cached Data", help="Click if data looks stuck"):
            st.cache_data.clear()
            st.toast("Cache Cleared!")
            st.rerun()

        # Activity Log (Last 3 Events)
        with st.expander("📜 Recent Activity", expanded=is_running):
            # Read logs directly
            log_path = os.path.join(config['data_dir'], "app_activity.log")
            logs = []
            if os.path.exists(log_path):
                with open(log_path, "r") as f:
                    logs = f.readlines()[-20:]
            else:
                logs = ["Waiting for logs..."]

            # Simple parser for recent "Started" or "Completed" events
            events = [line for line in logs if "Started" in line or "Complete" in line or "Error" in line or "Downloading" in line or "Processed" in line]
            if events:
                for event in reversed(events[-3:]): # Show last 3
                     st.caption(event.split(' - ')[-1] if ' - ' in event else event)
            else:
                st.caption("No recent significant events.")

        st.divider()

        # 2. ACTIONS (IMPORT)
        st.subheader("⚡ Actions")
        
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔄 Fetch", disabled=is_running, help="Download latest data"):
                run_pipeline_script(["--fetch-only"])
                st.toast("Fetching Started...")
                time.sleep(1)
                st.rerun()
                
        with c2:
            if st.button("🧠 Model", disabled=is_running, help="Run AI Analysis"):
                run_pipeline_script(["--analyze-only"])
                st.toast("Analysis Started...")
                time.sleep(1)
                st.rerun()

        # Allocation / Budget
        st.markdown("### 💰 Fund Allocation")
        user_budget = st.number_input("Monthly Investment (₹)", min_value=1000, value=config['monthly_budget'], step=500, disabled=is_running)
        
        if st.button("Sync Allocation", disabled=is_running, use_container_width=True):
             st.toast("Allocation Synced!")
             st.rerun()

        st.divider()

        # 3. STRATEGY SELECTION
        st.subheader("🎯 Strategy")
        
        strat_map = {
            "AI": "AI Growth (Aggressive)",
            "Buffet": "Buffet Value (Deep Value)",
            "BlueChip": "Blue Chip (Stability 🛡️)"
        }
        strat_options = list(strat_map.values())
        
        # Get from URL
        saved_strat_key = st.query_params.get("strategy", "AI")
        default_strat = strat_map.get(saved_strat_key, strat_options[0])
        
        # Widget
        strategy = st.radio("Choose Strategy:", strat_options, index=strat_options.index(default_strat))
        
        # Sync to URL
        new_strat_key = next((k for k, v in strat_map.items() if v == strategy), "AI")
        if new_strat_key != saved_strat_key:
            st.query_params["strategy"] = new_strat_key
            st.rerun()



    # --- MAIN CONTENT ---
    
    # Data Freshness (Top of Main)
    # Data Freshness Check
    data_date = "Unknown"
    data_date = "N/A"
    full_path = os.path.join(config['data_dir'], "full_analysis.csv")
    if os.path.exists(full_path):
        mtime = os.path.getmtime(full_path)
        dt_obj = datetime.fromtimestamp(mtime)
        # Assuming system is UTC, add 5.5 hours for IST rough fix if needed, 
        # but better to show relative time
        time_diff = datetime.now() - dt_obj
        hours = int(time_diff.total_seconds() // 3600)
        minutes = int((time_diff.total_seconds() % 3600) // 60)
        
        rel_str = f"{hours}h {minutes}m ago" if hours > 0 else f"{minutes}m ago"
        data_date = f"{dt_obj.strftime('%Y-%m-%d %H:%M')} ({rel_str})"
        
    st.caption(f"📅 Last Data Update: {data_date} | ⏳ Auto-Schedule: Daily 12:00 AM (Midnight)")

    # --- DATA LOADING ---
    # Trigger cache invalidation if file time changed
    rec_list, full_list, hist_list = load_data(config['data_dir'], mtime if 'mtime' in locals() else 0)

    # --- HELPER: ALLOCATION ---
    def apply_allocation(stock_list, budget, score_key):
        if not stock_list: return []
        
        # Calculate scores sum (Use Exponential Weighting to differentiate)
        scores = [x.get(score_key, 0) ** 5 for x in stock_list]
        total = sum(scores)
        
        res = []
        for item in stock_list:
            new_item = item.copy()
            score = item.get(score_key, 0)
            
            # Exaggerate differences
            weight = (score ** 5) / total if total > 0 else 0
            
            # Weighted Allocation
            raw_alloc = budget * weight
            new_item['Allocation'] = int(round(raw_alloc / 10) * 10)
            
            price = new_item.get('Close', 1)
            new_item['Qty'] = int(new_item['Allocation'] / price) if price > 0 else 0
            
            # Round floats
            for k, v in new_item.items():
                    if isinstance(v, float):
                        new_item[k] = round(v, 2)
            res.append(new_item)
        return res

    # --- TAB NAVIGATION (Persistent) ---
    # --- TAB NAVIGATION (Persistent) ---
    tabs_map = {
        "Recommendations": "🚀 Recommendations",
        "BigBets": "🎯 Big Bets",
        "RawData": "📥 Raw Data"
    }
    tabs = list(tabs_map.values())
    
    # Get active tab from URL (expecting clean key)
    url_key = st.query_params.get("tab", "Recommendations")
    
    # Resolve to display label
    active_tab = tabs_map.get(url_key, tabs[0])
    
    selected_tab = st.radio("Navigation", tabs, index=tabs.index(active_tab), horizontal=True, label_visibility="collapsed")
    
    # Update URL if changed (save clean key)
    current_key = next((k for k, v in tabs_map.items() if v == selected_tab), "Recommendations")
    if current_key != url_key:
        st.query_params["tab"] = current_key
        st.rerun()

    # --- CONTENT ROUTING ---
    if selected_tab == "🚀 Recommendations":
        # Placeholder for AI logic below
        pass
    else:
        # Placeholder for Raw Data logic
        pass

    # --- TAB 1: RECOMMENDATIONS ---
    if selected_tab == "🚀 Recommendations":
        # Rename legacy tab1 var for diff compatibility
        tab1_dummy = True
    
    # Pre-Calculate ALL Strategies
    ai_top = []
    buffet_top = []
    blue_top = []
    
    if full_list:
        # 1. AI Picks (Growth + Value + Tech)
        ai_data = sorted(full_list, key=lambda x: x.get('Final_Score', 0), reverse=True)[:5]
        ai_top = apply_allocation(ai_data, user_budget, 'Final_Score')
        
        # 2. Buffett Picks (Deep Value ONLY)
        # Filter: Must be undervalued (Margin > 0)
        # Handle potential string values safely
        def safe_float(v):
            try: return float(v)
            except: return 0.0
            
        value_stocks = [x for x in full_list if safe_float(x.get('Margin_Safety', 0)) > 0]
        # If no value stocks, fallback to top Fund_Score
        if not value_stocks:
             value_stocks = full_list
        
        buff_data = sorted(value_stocks, key=lambda x: x.get('Fund_Score', 0), reverse=True)[:5]
        buffet_top = apply_allocation(buff_data, user_budget, 'Fund_Score')
        
        # 3. Blue Chip
        blue_data = []
        if 'Market_Cap' in full_list[0]:
            caps = [x.get('Market_Cap', 0) for x in full_list]
            threshold = np.percentile(caps, 70) if caps else 0
            large_caps = [x for x in full_list if x.get('Market_Cap', 0) >= threshold]
            blue_data = sorted(large_caps, key=lambda x: x.get('Fund_Score', 0), reverse=True)[:5]
        else:
             blue_data = sorted(full_list, key=lambda x: (x.get('Fund_Score', 0), x.get('Close', 0)), reverse=True)[:5]
        blue_top = apply_allocation(blue_data, user_budget, 'Fund_Score')

    if selected_tab == "🚀 Recommendations":
        st.markdown(f"### Results: {strategy}")
        
        if full_list:
            # Selector Logic
            active_display = []
            score_key = 'Final_Score'
            
            if "AI" in strategy:
                active_display = ai_top
                score_key = 'Final_Score'
            elif "Buffet" in strategy:
                active_display = buffet_top
                score_key = 'Fund_Score'
            else:
                active_display = blue_top
                score_key = 'Fund_Score'
            
            if active_display:
                # Metrics
                scores = [x.get(score_key, 0) for x in active_display]
                avg_score = sum(scores)/len(scores) if scores else 0
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Investment", f"₹{user_budget}")
                c2.metric("Score", f"{avg_score:.2f}/1.0")
                with c3:
                    st.caption("Top Pick")
                    st.markdown(f"**{active_display[0]['Name']}**")
                
                # Table
                display_cols = ['Name', 'Ticker', 'Sector', 'Close', 'Intrinsic_Value', 'Margin_Safety', 'Qty', 'Allocation', 'Reason']
                table_data = [{k: row.get(k) for k in display_cols if k in row} for row in active_display]
                st.dataframe(table_data, use_container_width=True)
            else:
                st.warning("No stocks found for this strategy.")




    # --- TAB: BIG BETS (NEW) ---
    if selected_tab == "🎯 Big Bets":
        st.subheader("🦁 Big Bets (Medium Term Momentum)")
        import pandas as pd # Required for data loading
        st.info("Upload your Screener.in CSV export to find high-conviction medium-term picks (4-24 Months).")

        # Inputs
        c1, c2 = st.columns(2)
        with c1:
             big_budget = st.number_input("Investment Amount (₹)", min_value=100000, value=200000, step=10000)
        with c2:
             duration = st.selectbox("Duration", ["4-6 Months (Momentum)", "6-12 Months (Value+Growth)", "12-24 Months (Structural)"])
        
        duration_map = {"4-6 Months (Momentum)": 6, "6-12 Months (Value+Growth)": 12, "12-24 Months (Structural)": 24}

        # Data Source Selection
        data_source = st.radio("Data Source", ["📂 Upload File", "💽 Use Recent System Scan"], horizontal=True)
        
        df_raw = None
        
        if data_source == "📂 Upload File":
            uploaded_file = st.file_uploader("Upload Data (CSV/Excel)", type=["csv", "xlsx"])
            if uploaded_file:
                if uploaded_file.name.endswith('.csv'):
                     try:
                        df_raw = pd.read_csv(uploaded_file)
                     except:
                        # Fallback for bad encoding
                        import io
                        stringio = io.StringIO(uploaded_file.getvalue().decode("latin1"))
                        df_raw = pd.read_csv(stringio)
                else:
                     df_raw = pd.read_excel(uploaded_file)
        else:
            # Use System Data
            system_path = os.path.join(config['data_dir'], "full_analysis.csv")
            if os.path.exists(system_path):
                st.info(f"Using system data from: {system_path}")
                try:
                    df_raw = pd.read_csv(system_path)
                except Exception as e:
                    st.error(f"Error reading system data: {e}")
            else:
                st.warning("No system scan data found. Please run a fetch or upload a file.")

        
        # --- PERSISTENCE LOGIC START ---
        # Check if previous results exist and load them automatically
        saved_results_path = os.path.join(config['data_dir'], "big_bets_results.csv")
        loaded_results = None
        
        if os.path.exists(saved_results_path):
             try:
                 loaded_results = pd.read_csv(saved_results_path)
                 mtime = os.path.getmtime(saved_results_path)
                 dt_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
                 st.caption(f"📜 Showing cached analysis from: {dt_str}")
             except Exception as e:
                 st.warning(f"Failed to load cached results: {e}")

        # If we have loaded results and NO new run is triggered, use them
        # We use session state to track if a new run just happened
        if 'big_bets_run' not in st.session_state:
             st.session_state.big_bets_run = False

        # Prepare variables for display
        display_results = None
        display_picks = None
        
        # LOGIC: Run New Analysis OR Use Cached
        if df_raw is not None and st.button("Run Big Bet Model 🚀"):
             st.session_state.big_bets_run = True
             try:
                 import importlib
                 import src.medium_term_strategy
                 importlib.reload(src.medium_term_strategy)
                 from src.medium_term_strategy import MediumTermEngine
                 
                 engine = MediumTermEngine()
                 
                 with st.spinner("Training Neural/ML Model & Scoring Candidates..."):
                     top_picks, full_results, missing_cols = engine.run_analysis(df_raw, big_budget, duration_map[duration])
                 
                 # 1. Missing Column Feedback
                 if missing_cols:
                     st.warning(f"⚠️ **Missing Data Warning**: The following columns were not found in your data. The model assumed 0/Neutral for them.\n\n`{', '.join(missing_cols)}`\n\n*Please add these columns to your CSV for better accuracy.*")
                 else:
                     st.success("Analysis Complete! All required data points found.")
                 
                 # Save Results for Persistence
                 save_path = os.path.join(config['data_dir'], "big_bets_results.csv")
                 full_results.to_csv(save_path, index=False)
                 st.toast(f"✅ Analysis Saved!")
                 
                 
                 display_results = full_results
                 display_picks = top_picks

                 
             except Exception as e:
                 st.error(f"Model Execution Failed: {e}")
                 logging.error(f"Big Bets Error: {e}", exc_info=True)
                 
        elif loaded_results is not None:
             # Use Cached Data
             display_results = loaded_results
             # Re-construct Top Picks from the DF (Top 3 by Rank)
             if 'Rank' in display_results.columns:
                 top_df = display_results.sort_values("Rank").head(3)
                 display_picks = top_df.to_dict('records')
        
        # --- SHARED DISPLAY BLOCK ---
        if display_results is not None:
             
             # FILTER UI (User Request)
             use_strict = st.checkbox("💎 Filter: Super High Conviction Only (Quality>8, ROI>10, Win>0.88)", value=False)
             
             final_display_df = display_results.copy()
             
             if use_strict:
                 # Apply Filters
                 mask = (final_display_df['QualityScore'] > 8) & (final_display_df['ROI_6to12_Score'] > 10) & (final_display_df['WinProbability'] > 0.88)
                 final_display_df = final_display_df[mask]
                 if final_display_df.empty:
                     st.warning("No stocks found matching these strict criteria.")
                 else:
                     st.success(f"Found {len(final_display_df)} Super High Conviction stocks!")

             if not final_display_df.empty:
                 # Download Button Removed from here as per user request
                 # It will be handled in Sidebar
                 pass

                 # Display Top 3 Cards
                 st.markdown("### 🏆 Top 3 High Conviction Picks")
                 
                 cols = st.columns(3)
                 for i, pick in enumerate(display_picks):
                     with cols[i]:
                         st.markdown(f"""
                         <div style="padding: 20px; border-radius: 10px; background-color: {'#0E1117' if i==0 else '#262730'}; border: 1px solid {'#4CAF50' if i==0 else '#444'}; color: white;">
                            <h3 style="color: white; margin-bottom: 0px;">#{pick.get('Rank')} {pick.get('Name')}</h3>
                            <h2 style="color: #4CAF50; margin-top: 5px;">₹{pick.get('Allocation', 0):,}</h2>
                            <p style="color: #e0e0e0;"><b>Target:</b> {pick.get('Expected_Return')}</p>
                            <p style="color: #cccccc;"><i>"{pick.get('Reason')}"</i></p>
                            <small style="color: #888;">ROI Score: {pick.get('ROI_Score')} | Win Prob: {float(pick.get('Win_Prob', 0)):.2f}</small>
                         </div>
                         """, unsafe_allow_html=True)
                 
                 # Detailed Table
                 st.markdown("### 📋 Detailed Analysis")
                 
                 # Defensive Column Selection
                 target_cols = ['Name', 'CMP', 'ROI_6to12_Score', 'QualityScore', 'WinProbability', 'Reason', 'ExpectedReturn']
                 # Add some input columns for context
                 context_cols = ['SalesGrowth3Y', 'ProfitGrowth3Y', 'ROCE', 'PE']
                 feature_cols = [c for c in context_cols if c in final_display_df.columns]
                 
                 final_cols = target_cols + feature_cols
                 available_cols = [c for c in final_cols if c in final_display_df.columns]
                 
                 st.dataframe(final_display_df[available_cols].head(20), use_container_width=True)



    if selected_tab == "📥 Raw Data":
        st.subheader(f"Full Market Scan Data")
        
        # Count Metric
        if full_list:
             st.metric("Total Stocks Scanned", f"{len(full_list)}")
        else:
             st.metric("Total Stocks Scanned", "0")
        
        with st.expander("ℹ️ Data Glossary"):
             st.markdown("""
            **Fundamental Metrics:**
            - **PE_Ratio**: Price to Earnings. Lower is "Cheaper". (Under 30 is good for Value).
            - **ROE**: Return on Equity. Higher is "Efficient". (>15% is great).
            - **Debt_to_Equity**: Debt %. Lower is Safer. (<100% is safe).
            - **PEG_Ratio**: Price/Earnings to Growth. <1.0 is Undervalued Growth.
            - **Div_Yield**: Dividend %. Cash back to you.
            
            **Scores (0.0 - 1.0)**:
            - **Tech**: Chart patterns.
            - **Fund**: Financial health.
            - **Sent**: News sentiment.
            - **Forecast**: AI Prediction.
            """)
            
        if full_list:
            # --- PERSISTENT COLUMN VIEW ---
            # 1. Get all available columns
            all_cols = list(full_list[0].keys())
            
            # 2. Get saved columns from URL
            saved_cols_str = st.query_params.get("cols", "")
            default_cols = saved_cols_str.split(",") if saved_cols_str else all_cols
            # Filter out invalid columns just in case data changed
            valid_defaults = [c for c in default_cols if c in all_cols]
            if not valid_defaults: valid_defaults = all_cols
            
            # 3. Widget
            selected_cols = st.multiselect(
                "Select Columns to View (Saved on Refresh)", 
                options=all_cols, 
                default=valid_defaults,
                key="raw_data_cols"
            )
            
            # 4. Sync to URL if changed
            new_cols_str = ",".join(selected_cols)
            if new_cols_str != saved_cols_str:
                st.query_params["cols"] = new_cols_str
                # No rerun needed strictly, but helps updates
            
            # 5. Filter Data
            st.caption(f"Showing all {len(full_list)} scanned stocks.")
            
            # Create subset list of dicts for safety
            display_data = [{k: row.get(k) for k in selected_cols} for row in full_list]
            st.dataframe(display_data, use_container_width=True)
        else:
            st.warning("No data found.")

    # --- LATE RENDER: EXPORTS ---
    # We render this last so 'full_list' and 'ai_top' are available
    if full_list:
        with st.sidebar:
            st.divider()
            st.subheader("📤 Export Reports")
            date_str = datetime.now().strftime("%Y-%m-%d")
            
            export_options = [
                "Select a Report...",
                "🤖 The AI Picks", 
                "🍔 The Buffett Picks", 
                "💎 The Blue Chip Picks", 
                "🦁 Big Bets Analysis",
                "📊 Full Market Scan (Raw)"
            ]
            
            selected_export = st.selectbox("Choose Report to Download", export_options)
            
            if selected_export != "Select a Report...":
                data_to_export = None
                file_label = "Report"
                
                if selected_export == "🤖 The AI Picks":
                    data_to_export = ai_top
                    file_label = "Picks_AI"
                elif selected_export == "🍔 The Buffett Picks":
                    data_to_export = buffet_top
                    file_label = "Picks_Buffett"
                elif selected_export == "💎 The Blue Chip Picks":
                    data_to_export = blue_top
                    file_label = "Picks_BlueChip"
                elif selected_export == "📊 Full Market Scan (Raw)":
                    data_to_export = full_list
                    file_label = "Raw_Market_Data"
                elif selected_export == "🦁 Big Bets Analysis":
                    # Smart Logic: Use filtered data if available on screen
                    if 'final_display_df' in locals() and final_display_df is not None and not final_display_df.empty:
                         data_to_export = final_display_df.to_dict('records')
                         file_label = "Big_Bets_Filtered" if 'use_strict' in locals() and use_strict else "Big_Bets_Analysis"
                    else:
                        # Fallback: Load from disk
                        bb_path = os.path.join(config['data_dir'], "big_bets_results.csv")
                        if os.path.exists(bb_path):
                            try:
                                import pandas as pd
                                df_load = pd.read_csv(bb_path)
                                data_to_export = df_load.to_dict('records')
                                file_label = "Big_Bets_Analysis"
                            except:
                                st.error("Failed to load Big Bets data.")
                        else:
                            st.warning("No Big Bets analysis found. Run the model first.")
                
                if data_to_export:
                    excel_data = to_excel_bytes(data_to_export)
                    if excel_data:
                        st.download_button(
                            label=f"⬇️ Download {file_label}.xlsx",
                            data=excel_data,
                            file_name=f"{file_label}_{date_str}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

    # --- AUTO-REFRESH FOR DYNAMIC LOGS ---
    if is_running:
        time.sleep(3)
        st.rerun()

logging.info(f"Script reached end. Name is {__name__}")
try:
    logging.info("Calling main() directly...")
    main()
except Exception as e:
    st.error(f"An error occurred: {e}")
    logging.error(f"Dashboard Crash: {e}", exc_info=True)
