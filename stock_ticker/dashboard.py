import streamlit as st
import pandas as pd
import yaml
import os
import time
import subprocess
import numpy as np
from datetime import datetime
import plotly.express as px
import re

st.set_page_config(page_title="Stock SIP Dashboard", layout="wide")

# --- Helpers ---
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def load_data(data_dir):
    rec_path = os.path.join(data_dir, "recommendations.csv")
    full_path = os.path.join(data_dir, "full_analysis.csv")
    hist_path = os.path.join(data_dir, "portfolio_history.csv")
    
    rec_df = pd.read_csv(rec_path) if os.path.exists(rec_path) else pd.DataFrame()
    full_df = pd.read_csv(full_path) if os.path.exists(full_path) else pd.DataFrame()
    hist_df = pd.read_csv(hist_path) if os.path.exists(hist_path) else pd.DataFrame()
    return rec_df, full_df, hist_df

def get_pipeline_status(data_dir):
    """Reads the watchdog log to get status and progress."""
    log_path = os.path.join(data_dir, "watchdog_log.txt")
    if not os.path.exists(log_path):
        return "Not Started", 0, []
    
    with open(log_path, "r") as f:
        lines = f.readlines()
    
    last_line = lines[-1].strip() if lines else "Waiting..."
    
    # Parse Progress
    progress = 0
    # Look for [10/1800] pattern in recent lines (reverse search)
    for line in reversed(lines[-20:]):
        match = re.search(r"\[(\d+)/(\d+)\]", line)
        if match:
            current, total = map(int, match.groups())
            if total > 0:
                progress = int((current / total) * 100)
            break
            
    return last_line, progress, lines[-10:]

def run_pipeline_script():
    """Triggers the watchdog script."""
    # We call the wrapper batch file or powershell directly
    # Assuming execution from root
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(root_dir, "watchdog.ps1")
    subprocess.Popen(["powershell", "-ExecutionPolicy", "Bypass", "-File", script], cwd=root_dir)

def calculate_future_wealth(monthly_investment, years, cagr):
    months = years * 12
    rate = cagr / 100 / 12
    future_value = monthly_investment * ((((1 + rate) ** months) - 1) * (1 + rate)) / rate
    total_invested = monthly_investment * months
    return total_invested, future_value

# --- Main ---
def main():
    config = load_config()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if not os.path.isabs(config['data_dir']):
        config['data_dir'] = os.path.join(base_dir, config['data_dir'])
        
    st.title("📈 Smart Stock SIP System")

    # --- ACTION BAR (Top) ---
    col_btn, col_budget, col_status = st.columns([1, 2, 3])
    
    with col_btn:
        if st.button("▶ Run Analysis Now"):
            run_pipeline_script()
            st.toast("Pipeline started! core 'CTO' monitoring active.")
    
    with col_budget:
         user_budget = st.number_input("Monthly SIP (₹)", min_value=1000, value=config['monthly_budget'], step=500, label_visibility="collapsed")

    with col_status:
        status, progress, logs = get_pipeline_status(config['data_dir'])
        if "Executing" in status or "Analyzing" in status:
            st.info(f"⚙️ Running: {status}")
            st.progress(progress if progress > 0 else 5, text=f"{progress}% Complete")
        elif "SUCCESS" in status or "Completed" in status:
            st.success(f"✅ Ready. Last: {status}")
            st.progress(100)
        else:
            st.warning(f"⚠️ {status}")

    # --- DATA LOADING ---
    rec_df, full_df, hist_df = load_data(config['data_dir'])

    tab1, tab2, tab3, tab4 = st.tabs(["🚀 Top Recommendations", "🔮 Portfolio Growth", "📉 Daily Changes", "📥 Raw Data (1800+ Stocks)"])

    # --- TAB 1: RECOMMENDATIONS ---
    with tab1:
        st.markdown("### 🎯 Investment Strategy")
        
        # Strategy Explainer
        with st.expander("ℹ️ Understand the Columns & Strategies"):
            st.markdown("""
            - **Close**: Current Stock Price.
            - **Tech_Score** (Technical): Price trend. High = Uptrend (Bullish).
            - **Fund_Score** (Fundamental): Financial health. High = Strong Balance Sheet (Buffet style).
            - **Sent_Score** (Sentiment): News positivity. High = Good News.
            - **Final_Score**: Weighted average of all above + AI Forecast.
            - **Strategies**:
                - **🚀 AI Growth**: Aggressive. High Tech + Sentiment + Forecast. (High Risk/Reward).
                - **🎩 Buffet Value**: Conservative. High Fundamentals (P/E, Debt, ROE). (Long Term).
                - **🛡️ Blue Chip Stability**: Safe. Large Market Cap + Dividends + Stability. (Wealth Preservation).
            """)

        if not full_df.empty:
            if 'Reason' not in full_df.columns: full_df['Reason'] = "Pending..."
            
            # Strategies
            ai_picks = full_df.sort_values(by='Final_Score', ascending=False).head(5)
            buffet_picks = full_df.sort_values(by='Fund_Score', ascending=False).head(5)
            
            # Blue Chip Logic: Top 30% Market Cap + Positive Fund Score
            # If Market Cap is missing (0), we fall back to Fund Score filtering
            if 'Market_Cap' in full_df.columns:
                 blue_chips = full_df[full_df['Market_Cap'] > full_df['Market_Cap'].quantile(0.7)]
                 blue_chips = blue_chips.sort_values(by='Fund_Score', ascending=False).head(5)
            else:
                 # Fallback if no market cap data yet
                 blue_chips = full_df.sort_values(by=['Fund_Score', 'Close'], ascending=[False, False]).head(5)

            # Selector
            strategy = st.radio("Select Strategy:", 
                                ["AI Growth (Aggressive)", "Buffet Value (Deep Value)", "Blue Chip (Stability 🛡️)"], 
                                horizontal=True)
            
            if "AI" in strategy:
                active_df = ai_picks
            elif "Buffet" in strategy:
                active_df = buffet_picks
            else:
                active_df = blue_chips
            
            # Calculations
            if not active_df.empty:
                active_df['Allocation'] = user_budget / len(active_df)
                active_df['Qty'] = (active_df['Allocation'] / active_df['Close']).replace([np.inf, -np.inf], 0).fillna(0).astype(int)
                avg_score = active_df['Final_Score'].mean() if "AI" in strategy else active_df['Fund_Score'].mean()
                
                # Metrics
                c1, c2, c3 = st.columns(3)
                c1.metric("Investment Required", f"₹{user_budget}")
                c2.metric("Strategy Score", f"{avg_score:.2f}/1.0")
                c3.metric("Top Pick", active_df.iloc[0]['Name'])
                
                # Table
                display_cols = ['Name', 'Ticker', 'Close', 'Qty', 'Allocation', 'Reason']
                if "AI" in strategy: display_cols.append('Final_Score')
                else: display_cols.append('Fund_Score')
                    
                st.dataframe(active_df[display_cols].style.highlight_max(axis=0))
                
                # Download
                csv = active_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(f"📥 Download {strategy.split()[0]} Picks", csv, "picks.csv", "text/csv")
            else:
                st.warning(f"⚠️ No stocks found matching '{strategy}'. Wait for more data analysis to complete.")
            
        else:
            st.info("👋 Welcome! Click 'Run Analysis Now' to start scanning the market.")

    # --- TAB 2: WEALTH SIMULATOR ---
    with tab2:
        st.header("Projected Returns")
        if not rec_df.empty:
            c1, c2 = st.columns([1, 2])
            with c1:
                years = st.slider("Duration (Years)", 5, 30, 10)
                cagr_default = 18 if np.random.random() > 0.5 else 18 
                cagr = st.slider("Expected CAGR (%)", 10, 30, cagr_default, help="AI Picks target ~18%+. Value picks target ~15%.")
            
            with c2:
                invested, value = calculate_future_wealth(user_budget, years, cagr)
                st.metric("Future Value", f"₹{value:,.0f}", delta=f"{(value-invested)/invested*100:.0f}% Gain")
                
                chart_data = pd.DataFrame({
                    "Year": range(1, years + 1),
                    "Invested": [user_budget * 12 * i for i in range(1, years + 1)],
                    "Portfolio Value": [calculate_future_wealth(user_budget, i, cagr)[1] for i in range(1, years + 1)]
                })
                st.area_chart(chart_data.set_index("Year"))
        else:
            st.warning("Run analysis first to get specific projections.")

    # --- TAB 3: HISTORY ---
    with tab3:
        st.caption("Daily Log of Entries & Exits")
        if not hist_df.empty:
            st.dataframe(hist_df.sort_values(by="Date", ascending=False))
        else:
            st.info("No history yet. This tab tracks changes over time.")

    # --- TAB 4: RAW DATA ---
    with tab4:
        st.subheader(f"Full Market Scan Data")
        st.markdown("This table contains the raw analysis for **every single stock** scanned today.")
        
        if not full_df.empty:
            # Reorder columns for better readability
            cols = ['Ticker', 'Name', 'Close', 'Final_Score', 'Fund_Score', 'Tech_Score', 'Reason']
            # Add missing if not present
            cols = [c for c in cols if c in full_df.columns]
            remaining = [c for c in full_df.columns if c not in cols]
            
            st.dataframe(full_df[cols + remaining])
            
            csv_full = full_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full Dataset (CSV)",
                data=csv_full,
                file_name=f"full_market_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                type="primary"
            )
        else:
            st.warning("No data found. Please run the analysis.")

if __name__ == "__main__":
    main()
