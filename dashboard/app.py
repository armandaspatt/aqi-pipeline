"""AQI India — live dashboard over the dbt marts."""
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

PG_DSN = os.environ.get("PG_DSN", "postgresql://aqi:aqi@localhost/aqi")

st.set_page_config(page_title="AQI India", page_icon="🌫️", layout="wide")
st.title("🌫️ India Air Quality — Live Pipeline")

CATEGORY_COLORS = {
    "Good": "#2ecc71", "Satisfactory": "#a3d977", "Moderate": "#f1c40f",
    "Poor": "#e67e22", "Very Poor": "#e74c3c", "Severe": "#8e44ad",
}


@st.cache_data(ttl=300)
def load(query: str) -> pd.DataFrame:
    conn = psycopg2.connect(PG_DSN)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


try:
    latest = load("""
        select city, aqi, dominant_pollutant, category, hour
        from public_marts.mart_city_hourly_aqi
        where hour = (select max(hour) from public_marts.mart_city_hourly_aqi)
        order by aqi desc
    """)
except Exception:
    st.warning("Marts not built yet — trigger the `aqi_pipeline` DAG in Airflow first (localhost:8081).")
    st.stop()

if latest.empty:
    st.info("No data yet. Run the pipeline once.")
    st.stop()

st.caption(f"Latest hour: {latest['hour'].iloc[0]}")

# KPI cards
cols = st.columns(min(len(latest), 6))
for col, (_, row) in zip(cols, latest.iterrows()):
    col.metric(row["city"], int(row["aqi"]), row["category"])

left, right = st.columns([1, 1])

with left:
    st.subheader("Cities by AQI (latest hour)")
    fig = px.bar(
        latest, x="aqi", y="city", orientation="h", color="category",
        color_discrete_map=CATEGORY_COLORS, text="dominant_pollutant",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("24h trend")
    trend = load("""
        select city, hour, aqi from public_marts.mart_city_hourly_aqi
        where hour > now() - interval '24 hours'
        order by hour
    """)
    if not trend.empty:
        st.plotly_chart(
            px.line(trend, x="hour", y="aqi", color="city"),
            use_container_width=True,
        )

st.subheader("🚨 Active alerts (AQI > 300)")
alerts = load("select * from public_marts.mart_aqi_alerts order by aqi desc")
if alerts.empty:
    st.success("No cities above 'Very Poor' right now.")
else:
    st.dataframe(alerts, use_container_width=True)
