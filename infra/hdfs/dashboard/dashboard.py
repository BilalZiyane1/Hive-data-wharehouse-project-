"""Lightweight dashboard for the PostgreSQL Gold transaction metrics."""

import os

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(page_title="Realtime Warehouse", page_icon="📊", layout="wide")
st.title("Realtime Transaction Warehouse")
st.caption("Gold metrics served from PostgreSQL after Spark processing")

st_autorefresh(interval=2000, key="warehouse-auto-refresh")


@st.cache_data(ttl=2)
def load_metrics() -> pd.DataFrame:
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "realtime_warehouse"),
        user=os.getenv("POSTGRES_USER", "warehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "warehouse_dev"),
    )
    try:
        return pd.read_sql_query(
            """
            SELECT transaction_date, currency, status, transaction_type,
                   transaction_count, gross_amount, average_amount
            FROM gold.daily_transaction_metrics
            ORDER BY transaction_date, currency, status, transaction_type
            """,
            connection,
        )
    finally:
        connection.close()


@st.cache_data(ttl=2)
def load_live_metrics() -> pd.DataFrame:
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "realtime_warehouse"),
        user=os.getenv("POSTGRES_USER", "warehouse"),
        password=os.getenv("POSTGRES_PASSWORD", "warehouse_dev"),
    )
    try:
        return pd.read_sql_query(
            """
            SELECT COUNT(*) AS transaction_count,
                   COALESCE(SUM(amount), 0) AS gross_amount,
                   MAX(created_at) AS latest_transaction
            FROM public.transactions
            """,
            connection,
        )
    finally:
        connection.close()


try:
    metrics = load_metrics()
    live = load_live_metrics()
except psycopg2.Error as error:
    st.error("Unable to load Gold metrics from PostgreSQL.")
    st.exception(error)
    st.stop()

if metrics.empty:
    st.warning("No Gold metrics are available yet. Run the batch loader first.")
    st.stop()

live_row = live.iloc[0]
live_count = int(live_row["transaction_count"])
live_amount = float(live_row["gross_amount"])
live_time = live_row["latest_transaction"]

st.subheader("Live PostgreSQL source")
live_1, live_2, live_3 = st.columns(3)
live_1.metric("Live source transactions", f"{live_count:,}")
live_2.metric("Live source gross amount", f"{live_amount:,.2f}")
live_3.metric("Latest transaction", str(live_time))

total_transactions = int(metrics["transaction_count"].sum())
total_amount = float(metrics["gross_amount"].sum())
average_amount = float(
    metrics["gross_amount"].sum() / metrics["transaction_count"].sum()
)

first, second, third = st.columns(3)
first.metric("Transactions", f"{total_transactions:,}")
second.metric("Gross amount", f"{total_amount:,.2f}")
third.metric("Average amount", f"{average_amount:,.2f}")

daily = (
    metrics.groupby("transaction_date", as_index=True)["transaction_count"]
    .sum()
    .sort_index()
)
st.subheader("Transactions by day")
st.line_chart(daily)

left, right = st.columns(2)
with left:
    st.subheader("Transactions by currency")
    st.bar_chart(metrics.groupby("currency")["transaction_count"].sum())
with right:
    st.subheader("Transactions by status")
    st.bar_chart(metrics.groupby("status")["transaction_count"].sum())

st.subheader("Gold dataset")
st.dataframe(metrics, use_container_width=True, hide_index=True)
