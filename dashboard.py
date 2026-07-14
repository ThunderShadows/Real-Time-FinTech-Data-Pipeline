import streamlit as pd_stream
import pandas as pd
import plotly.express as px
import os
import time
import glob
from datetime import datetime  
import numpy as np

pd_stream.set_page_config(page_title="Fintech Real-Time Fraud Monitor ", layout="wide")
pd_stream.title("Fintech Real-Time Fraud Detection Dashboard")
pd_stream.markdown("Monitoring streaming high-velocity transaction bursts from live crypto/UPI event feeds (coinbase).")

placeholder = pd_stream.empty()

def load_streaming_parquet_data(folder_path):
    # 1. Fallback UI Mock Logic for Cloud Deployments or empty directory starts
    if not os.path.exists(folder_path):
        now = datetime.utcnow()
        mock_data = {
            'alert_window_start': [datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") for _ in range(5)],
            'card_number': ['BTC-USD', 'ETH-USD', 'SOL-USD', 'BTC-USD', 'ADA-USD'],
            'transaction_count': [np.random.randint(100, 500),
                                  np.random.randint(50, 300),
                                  np.random.randint(10, 150),
                                  np.random.randint(200, 600),
                                  np.random.randint(1, 15)],
            'alert_triggered_at': [now for _ in range(5)]
        }
        return pd.DataFrame(mock_data)
    
    # 2. Real Parquet Directory Pipeline Reader (Added missing logic)
    parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    
    # Filter out empty delta/crc files that Spark is currently lock-writing
    valid_files = [f for f in parquet_files if os.path.getsize(f) > 0]
    if not valid_files:
        return pd.DataFrame()
        
    try:
        df_list = [pd.read_parquet(f, engine='pyarrow') for f in valid_files]
        return pd.concat(df_list, ignore_index=True)
    except Exception:
        return pd.DataFrame()
    
while True:
    try:
        df = load_streaming_parquet_data("output/delta_fraud_alerts")
        
        iteration_suffix = str(int(time.time() * 1000))

        with placeholder.container():
            if df is None or df.empty:
                pd_stream.warning("Waiting for Spark Streaming pipeline to output batches...")
            else:
                df["alert_triggered_at"] = pd.to_datetime(df['alert_triggered_at'])
                df["transaction_count"] = pd.to_numeric(df['transaction_count'])

                clean_df = df.dropna(subset=['card_number', 'alert_window_start', 'transaction_count'])
                failed_checks_count = len(df) - len(clean_df)

                # KPI'S
                kpi1, kpi2, kpi3 = pd_stream.columns(3)
                kpi1.metric(label="Total Alerts Flagged", value=len(clean_df))
                kpi2.metric(label="Highest Transaction Count/Min", value=int(clean_df['transaction_count'].max()) if len(clean_df) > 0 else 0)
                kpi3.metric(label="Data Quality Anomalies", value=failed_checks_count)

                # Chart 1 - Real-Time Fraud Burst Volume Over Time
                pd_stream.subheader("Fraud Alert Velocity Timeline")
                timeline_df = clean_df.sort_values(by="alert_triggered_at")
                fig_timeline = px.line(
                    timeline_df,
                    x="alert_triggered_at",
                    y="transaction_count",
                    color='card_number',
                    labels={"alert_triggered_at": "Timestamp (IST)", "transaction_count": "Swipes per minute"},
                    markers=True
                )
                
                pd_stream.plotly_chart(
                    fig_timeline, 
                    use_container_width=True, 
                    key=f"fraud_velocity_timeline_chart_{iteration_suffix}"
                )

                col1, col2 = pd_stream.columns(2)

                with col1:
                    pd_stream.subheader("Alerts Share by Asset")
                    pie_fig = px.pie(clean_df, names='card_number', values='transaction_count', hole=0.4)
                    
                    pd_stream.plotly_chart(
                        pie_fig, 
                        use_container_width=True, 
                        key=f"asset_share_pie_chart_{iteration_suffix}"
                    )
                
                with col2:
                    pd_stream.subheader("Recent Log Commits")
                    pd_stream.dataframe(clean_df.tail(10)[['alert_window_start', 'card_number', 'transaction_count']], use_container_width=True)

    except Exception as e:
        pd_stream.error(f"Error accessing the Parquet Stream Layers: {str(e)}")
    
    time.sleep(2)