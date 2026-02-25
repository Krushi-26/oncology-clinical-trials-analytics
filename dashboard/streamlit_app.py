import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
from datetime import datetime
from config.db_config import DB_CONFIG

# ---------------- PAGE CONFIG ---------------- #

st.set_page_config(
    page_title="Oncology Clinical Trials Intelligence",
    layout="wide"
)

# ---------------- DATA LOADING ---------------- #

@st.cache_data
def load_trials():
    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql("SELECT * FROM clinical_trials", conn)
    conn.close()
    return df

@st.cache_data
def load_etl_logs():
    conn = mysql.connector.connect(**DB_CONFIG)
    df = pd.read_sql(
        "SELECT * FROM etl_run_logs ORDER BY run_timestamp DESC",
        conn
    )
    conn.close()
    return df


df = load_trials()

if df.empty:
    st.warning("No clinical trial data available.")
    st.stop()

# ---------------- TABS ---------------- #

tab1, tab2 = st.tabs(["📊 Analytics Dashboard", "⚙️ ETL Monitoring"])

# ============================================================
# ===================== ANALYTICS TAB ========================
# ============================================================

with tab1:

    st.title("🧬 Oncology Clinical Trials Intelligence Dashboard")
    st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.divider()

    # ---------------- SIDEBAR FILTERS ---------------- #

    st.sidebar.header("🔎 Filter Controls")

    selected_phase = st.sidebar.multiselect(
        "Phase",
        options=sorted(df["phase"].dropna().unique()),
        default=sorted(df["phase"].dropna().unique())
    )

    selected_status = st.sidebar.multiselect(
        "Status",
        options=sorted(df["status"].dropna().unique()),
        default=sorted(df["status"].dropna().unique())
    )

    filtered_df = df[
        (df["phase"].isin(selected_phase)) &
        (df["status"].isin(selected_status))
    ].copy()

    if filtered_df.empty:
        st.error("No trials match the selected filters.")
        st.stop()

    # ---------------- DATASET OVERVIEW ---------------- #

    st.subheader("📊 Dataset Overview")

    st.info(
        f"Analyzing {len(filtered_df):,} oncology trials "
        f"across {filtered_df['phase'].nunique()} phases "
        f"and {filtered_df['state'].nunique()} US states."
    )

    # ---------------- KPI SECTION ---------------- #

    st.subheader("📌 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Trials", f"{len(filtered_df):,}")
    col2.metric("Recruiting Trials", f"{len(filtered_df[filtered_df['status']=='RECRUITING']):,}")
    col3.metric("Completed Trials", f"{len(filtered_df[filtered_df['status']=='COMPLETED']):,}")

    avg_enrollment = filtered_df["enrollment"].mean()
    if pd.notna(avg_enrollment):
        col4.metric("Avg Enrollment", f"{int(avg_enrollment):,}")
    else:
        col4.metric("Avg Enrollment", "N/A")

    st.divider()

    # ---------------- PHASE DISTRIBUTION ---------------- #

    st.subheader("📊 Trial Distribution by Phase")

    phase_counts = filtered_df["phase"].value_counts().reset_index()
    phase_counts.columns = ["Phase", "Count"]

    fig_phase = px.bar(
        phase_counts,
        x="Phase",
        y="Count",
        text="Count"
    )

    st.plotly_chart(fig_phase, width="stretch")

    # ---------------- STATUS DISTRIBUTION ---------------- #

    st.subheader("📈 Trial Distribution by Status")

    status_counts = filtered_df["status"].value_counts().reset_index()
    status_counts.columns = ["Status", "Count"]

    if len(status_counts) > 1:
        fig_status = px.pie(
            status_counts,
            names="Status",
            values="Count",
            hole=0.4
        )
        fig_status.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_status, width="stretch")
    elif len(status_counts) == 1:
        st.info(f"All selected trials are '{status_counts.iloc[0]['Status']}'")
    else:
        st.warning("No status data available.")

    # ---------------- TIME TREND ---------------- #

    st.subheader("📅 Trials Started Per Year")

    filtered_df["start_date"] = pd.to_datetime(
        filtered_df["start_date"],
        errors="coerce"
    )

    trend_df = filtered_df.dropna(subset=["start_date"]).copy()

    if trend_df.empty:
        st.warning("Selected filters contain no trials with valid start dates.")
    else:
        trend_df["Year"] = trend_df["start_date"].dt.year

        trend_summary = (
            trend_df.groupby("Year")
            .size()
            .reset_index(name="Trials")
        )

        fig_trend = px.line(
            trend_summary,
            x="Year",
            y="Trials",
            markers=True
        )

        st.plotly_chart(fig_trend, width="stretch")

        # Growth Chart
        trend_summary["Growth %"] = trend_summary["Trials"].pct_change() * 100

        fig_growth = px.bar(
            trend_summary,
            x="Year",
            y="Growth %",
            title="Year-over-Year Growth Rate"
        )

        st.plotly_chart(fig_growth, width="stretch")

    # ---------------- GEOGRAPHIC MAP ---------------- #

    st.subheader("🗺️ Geographic Distribution (US States)")

    state_counts = (
        filtered_df["state"]
        .dropna()
        .value_counts()
        .reset_index()
    )

    state_counts.columns = ["State", "Count"]

    if state_counts.empty:
        st.warning("Selected filters contain no geographic data.")
    else:
        fig_map = px.choropleth(
            state_counts,
            locations="State",
            locationmode="USA-states",
            color="Count",
            scope="usa"
        )
        st.plotly_chart(fig_map, width="stretch")

    # ---------------- ENROLLMENT DISTRIBUTION ---------------- #

    st.subheader("📊 Enrollment Distribution")

    if not filtered_df["enrollment"].dropna().empty:
        fig_enroll = px.histogram(
            filtered_df,
            x="enrollment",
            nbins=30
        )
        st.plotly_chart(fig_enroll, width="stretch")
    else:
        st.warning("No enrollment data available.")

    # ---------------- PHASE vs STATUS MATRIX ---------------- #

    st.subheader("🔥 Phase vs Status Matrix")

    matrix_df = (
        filtered_df
        .groupby(["phase", "status"])
        .size()
        .reset_index(name="Count")
    )

    if not matrix_df.empty:
        fig_matrix = px.density_heatmap(
            matrix_df,
            x="phase",
            y="status",
            z="Count",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_matrix, width="stretch")

    # ---------------- TOP STATES ---------------- #

    st.subheader("🏆 Top 10 States by Trial Volume")

    top_states = (
        filtered_df["state"]
        .dropna()
        .value_counts()
        .head(10)
        .reset_index()
    )

    top_states.columns = ["State", "Count"]

    if not top_states.empty:
        fig_states = px.bar(
            top_states,
            x="State",
            y="Count",
            text="Count"
        )
        st.plotly_chart(fig_states, width="stretch")

# ============================================================
# ===================== ETL MONITORING TAB ===================
# ============================================================

with tab2:

    st.title("⚙️ ETL Pipeline Monitoring")

    logs_df = load_etl_logs()

    if logs_df.empty:
        st.warning("No ETL runs logged yet.")
    else:

        st.subheader("📋 Recent ETL Runs")
        st.dataframe(logs_df)

        st.subheader("⏱ Pipeline Duration Trend")

        duration_df = logs_df.sort_values("run_timestamp")

        fig_duration = px.line(
            duration_df,
            x="run_timestamp",
            y="duration_seconds",
            markers=True
        )

        st.plotly_chart(fig_duration, width="stretch")

        st.subheader("📦 Records Loaded Per Run")

        fig_loaded = px.bar(
            duration_df,
            x="run_timestamp",
            y="records_loaded"
        )

        st.plotly_chart(fig_loaded, width="stretch")