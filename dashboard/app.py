"""
Client Analytics Dashboard — Streamlit app.

Shows live KPIs pulled from BigQuery (or local DuckDB fallback for dev).
Auto-refreshes every 5 minutes via st.rerun().

Run:
    streamlit run dashboard/app.py

For local dev without BigQuery, set USE_MOCK_DATA=true in your .env.
"""

import os
import time
from datetime import date, timedelta

import pandas as pd
import streamlit as st

# ------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------

st.set_page_config(
    page_title="Client Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------
# Data loading
# ------------------------------------------------------------------

USE_MOCK = os.environ.get("USE_MOCK_DATA", "false").lower() == "true"


@st.cache_data(ttl=300)   # refresh every 5 minutes
def load_revenue_data() -> pd.DataFrame:
    """Load daily revenue from BigQuery or mock data for local dev."""
    if USE_MOCK:
        return _mock_revenue()

    from google.cloud import bigquery
    client = bigquery.Client(project=os.environ["BIGQUERY_PROJECT"])

    query = """
        select
            payment_date,
            currency,
            acquisition_channel,
            count(payment_id)           as payment_count,
            sum(amount)                 as gross_revenue,
            sum(net_revenue)            as net_revenue,
            sum(mrr_contribution)       as mrr
        from `{project}.marts.mart_revenue`
        where payment_date >= date_sub(current_date(), interval 30 day)
          and status = 'succeeded'
        group by 1, 2, 3
        order by 1 desc
    """.format(project=os.environ["BIGQUERY_PROJECT"])

    return client.query(query).to_dataframe()


@st.cache_data(ttl=300)
def load_customer_summary() -> pd.DataFrame:
    """Load customer counts by lifecycle stage."""
    if USE_MOCK:
        return _mock_customers()

    from google.cloud import bigquery
    client = bigquery.Client(project=os.environ["BIGQUERY_PROJECT"])

    query = """
        select
            lifecycle_stage,
            acquisition_channel,
            count(customer_id) as customer_count
        from `{project}.marts.dim_customers_scd2`
        where is_current = true
        group by 1, 2
    """.format(project=os.environ["BIGQUERY_PROJECT"])

    return client.query(query).to_dataframe()


# ------------------------------------------------------------------
# Mock data for local development
# ------------------------------------------------------------------

def _mock_revenue() -> pd.DataFrame:
    import numpy as np
    rng = pd.date_range(end=date.today(), periods=30, freq="D")
    channels = ["organic_search", "paid_search", "email", "referral", "direct"]
    rows = []
    for d in rng:
        for ch in channels:
            rev = abs(np.random.normal(2000, 500))
            rows.append({
                "payment_date": d.date(),
                "currency": "USD",
                "acquisition_channel": ch,
                "payment_count": int(abs(np.random.normal(15, 5))),
                "gross_revenue": round(rev, 2),
                "net_revenue": round(rev * 0.95, 2),
                "mrr": round(rev * 0.95, 2),
            })
    return pd.DataFrame(rows)


def _mock_customers() -> pd.DataFrame:
    return pd.DataFrame([
        {"lifecycle_stage": "customer",              "acquisition_channel": "organic_search", "customer_count": 342},
        {"lifecycle_stage": "customer",              "acquisition_channel": "paid_search",    "customer_count": 218},
        {"lifecycle_stage": "salesqualifiedlead",   "acquisition_channel": "email",          "customer_count": 156},
        {"lifecycle_stage": "lead",                  "acquisition_channel": "referral",       "customer_count": 89},
        {"lifecycle_stage": "marketingqualifiedlead","acquisition_channel": "direct",         "customer_count": 74},
        {"lifecycle_stage": "subscriber",            "acquisition_channel": "social_media",   "customer_count": 201},
    ])


# ------------------------------------------------------------------
# Dashboard layout
# ------------------------------------------------------------------

df_rev = load_revenue_data()
df_cust = load_customer_summary()

# Header
st.title("📊 Client Analytics Platform")
st.caption(f"Data refreshed: {date.today()} · Next refresh in 5 min · {'🟡 Mock data' if USE_MOCK else '🟢 Live — BigQuery'}")

st.divider()

# ------------------------------------------------------------------
# KPI cards — top row
# ------------------------------------------------------------------

last_30_days = df_rev[df_rev["payment_date"] >= (date.today() - timedelta(days=30))]
prev_30_days = df_rev[
    (df_rev["payment_date"] >= (date.today() - timedelta(days=60))) &
    (df_rev["payment_date"] < (date.today() - timedelta(days=30)))
]

total_revenue = last_30_days["net_revenue"].sum()
prev_revenue = prev_30_days["net_revenue"].sum()
revenue_delta = ((total_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue else 0

total_mrr = last_30_days.groupby("payment_date")["mrr"].sum().mean()
total_customers = df_cust["customer_count"].sum()
paying_customers = df_cust[df_cust["lifecycle_stage"] == "customer"]["customer_count"].sum()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Net Revenue (30d)",
        f"${total_revenue:,.0f}",
        f"{revenue_delta:+.1f}% vs prior period",
    )
with col2:
    st.metric("Avg Daily MRR", f"${total_mrr:,.0f}")
with col3:
    st.metric("Total Contacts", f"{total_customers:,}")
with col4:
    conversion = (paying_customers / total_customers * 100) if total_customers else 0
    st.metric("Paying Customers", f"{paying_customers:,}", f"{conversion:.1f}% conversion")

st.divider()

# ------------------------------------------------------------------
# Revenue trend chart
# ------------------------------------------------------------------

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("Daily net revenue — last 30 days")
    daily = (
        last_30_days.groupby("payment_date")["net_revenue"]
        .sum()
        .reset_index()
        .sort_values("payment_date")
    )
    daily.columns = ["Date", "Net Revenue ($)"]
    st.line_chart(daily.set_index("Date"), height=280)

with col_right:
    st.subheader("Revenue by channel")
    by_channel = (
        last_30_days.groupby("acquisition_channel")["net_revenue"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    by_channel.columns = ["Channel", "Revenue ($)"]
    st.bar_chart(by_channel.set_index("Channel"), height=280)

st.divider()

# ------------------------------------------------------------------
# Customer funnel
# ------------------------------------------------------------------

st.subheader("Customer lifecycle funnel")

funnel_order = [
    "subscriber", "lead", "marketingqualifiedlead",
    "salesqualifiedlead", "opportunity", "customer",
]
funnel = (
    df_cust.groupby("lifecycle_stage")["customer_count"]
    .sum()
    .reindex(funnel_order, fill_value=0)
    .reset_index()
)
funnel.columns = ["Stage", "Count"]
funnel["Stage"] = funnel["Stage"].str.replace("qualifiedlead", "QL").str.replace("marketing", "MQL").str.replace("sales", "SQL")
st.bar_chart(funnel.set_index("Stage"), height=220)

# ------------------------------------------------------------------
# Footer
# ------------------------------------------------------------------

st.divider()
st.caption(
    "Built with BigQuery · Apache Airflow · dbt Core · Streamlit | "
    "[GitHub](https://github.com/gavi-ai/client-analytics-platform)"
)
