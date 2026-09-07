"""Streamlit dashboard that live-monitors the synthetic-card velocity
alerts written by consumer_fraud_detection.py.

Only ever shows real data read from the alert Parquet output - there is no
mock/demo fallback. If the pipeline hasn't produced any alerts yet, the
dashboard says so explicitly rather than fabricating numbers.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "output/fraud_alerts")
POLL_INTERVAL_SECONDS = 2

st.set_page_config(page_title="Fintech Real-Time Fraud Monitor", layout="wide")
st.title("Fintech Real-Time Fraud Detection Dashboard")
st.markdown(
    "Monitoring streaming synthetic-card velocity bursts from a live crypto event feed (Coinbase). "
    "See README.md “Simulated Fraud-Detection Methodology” for what this is - and isn't - demonstrating."
)


def load_new_parquet_files(
    folder_path: str, already_loaded: dict[str, tuple[float, int]]
) -> tuple[pd.DataFrame, dict[str, tuple[float, int]]]:
    """Scan `folder_path` for *.parquet files and return only the rows from
    files that are new or changed since `already_loaded` was captured, plus
    an updated tracking dict keyed by file path -> (mtime, size).

    Zero-byte files (still being written by Spark) and files that fail to
    read (partial/corrupt) are skipped and deliberately left OUT of the
    returned tracking dict, so they're retried on the next call instead of
    being permanently skipped.
    """
    updated = dict(already_loaded)
    new_frames: list[pd.DataFrame] = []

    folder = Path(folder_path)
    if not folder.exists():
        return pd.DataFrame(), updated

    for file_path in sorted(folder.glob("*.parquet")):
        try:
            stat = file_path.stat()
        except OSError as exc:
            logger.warning("Could not stat %s: %s", file_path, exc)
            continue

        if stat.st_size == 0:
            continue  # Spark is still writing this file - retry next poll

        fingerprint = (stat.st_mtime, stat.st_size)
        key = str(file_path)
        if updated.get(key) == fingerprint:
            continue  # unchanged since the last successful read

        try:
            new_frames.append(pd.read_parquet(file_path, engine="pyarrow"))
        except Exception as exc:
            logger.warning("Skipping unreadable parquet file %s: %s", file_path, exc)
            continue  # not marked as loaded - retried next poll

        updated[key] = fingerprint

    if not new_frames:
        return pd.DataFrame(), updated

    return pd.concat(new_frames, ignore_index=True), updated


def _init_session_state() -> None:
    if "fraud_alerts_df" not in st.session_state:
        st.session_state.fraud_alerts_df = pd.DataFrame()
    if "loaded_files" not in st.session_state:
        st.session_state.loaded_files = {}


def _render_charts(clean_df: pd.DataFrame, failed_checks_count: int) -> None:
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="Total Alerts Flagged", value=len(clean_df))
    kpi2.metric(label="Highest Transaction Count/Min", value=int(clean_df["transaction_count"].max()))
    kpi3.metric(label="Data Quality Anomalies", value=failed_checks_count)

    st.subheader("Fraud Alert Velocity Timeline")
    timeline_df = clean_df.sort_values(by="alert_triggered_at")
    fig_timeline = px.line(
        timeline_df,
        x="alert_triggered_at",
        y="transaction_count",
        color="card_number",
        labels={
            "alert_triggered_at": "Timestamp",
            "transaction_count": "Transactions per minute",
            "card_number": "Card",
        },
        markers=True,
    )
    st.plotly_chart(fig_timeline, use_container_width=True, key="fraud_velocity_timeline_chart")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Alerts Share by Card")
        pie_fig = px.pie(clean_df, names="card_number", values="transaction_count", hole=0.4)
        st.plotly_chart(pie_fig, use_container_width=True, key="card_share_pie_chart")

    with col2:
        st.subheader("Recent Alerts")
        display_cols = [
            c
            for c in ["alert_window_start", "card_number", "transaction_count", "assets_involved"]
            if c in clean_df.columns
        ]
        st.dataframe(clean_df.tail(10)[display_cols], use_container_width=True)

    if "assets_involved" in clean_df.columns:
        exploded = clean_df.explode("assets_involved").dropna(subset=["assets_involved"])
        if not exploded.empty:
            st.subheader("Assets Touched by Flagged Cards")
            st.caption(
                "Demonstrates that the velocity signal is independent of which asset was traded - "
                "see README.md “Simulated Fraud-Detection Methodology”."
            )
            asset_counts = exploded["assets_involved"].value_counts().reset_index()
            asset_counts.columns = ["asset", "alert_count"]
            fig_assets = px.bar(asset_counts, x="asset", y="alert_count")
            st.plotly_chart(fig_assets, use_container_width=True, key="assets_involved_bar_chart")


@st.fragment(run_every=POLL_INTERVAL_SECONDS)
def render_dashboard() -> None:
    _init_session_state()

    try:
        new_rows, st.session_state.loaded_files = load_new_parquet_files(
            OUTPUT_PATH, st.session_state.loaded_files
        )
    except Exception as exc:
        logger.exception("Failed to poll parquet output directory %s", OUTPUT_PATH)
        st.error(f"Error accessing the Parquet output directory: {exc}")
        return

    if not new_rows.empty:
        st.session_state.fraud_alerts_df = pd.concat(
            [st.session_state.fraud_alerts_df, new_rows], ignore_index=True
        )

    df = st.session_state.fraud_alerts_df
    if df.empty:
        st.info(
            "Waiting for real fraud-alert data from the Spark pipeline... "
            "No demo or fabricated data is ever shown here."
        )
        return

    df = df.copy()
    df["alert_triggered_at"] = pd.to_datetime(df["alert_triggered_at"], errors="coerce")
    df["transaction_count"] = pd.to_numeric(df["transaction_count"], errors="coerce")

    clean_df = df.dropna(subset=["card_number", "alert_window_start", "transaction_count"])
    failed_checks_count = len(df) - len(clean_df)

    if clean_df.empty:
        st.info("Waiting for real fraud-alert data from the Spark pipeline...")
        return

    _render_charts(clean_df, failed_checks_count)


render_dashboard()
