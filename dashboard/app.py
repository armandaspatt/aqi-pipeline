"""AQI India — live dashboard over the dbt marts."""
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    st.subheader("Last 24h by city")
    trend = load("""
        select city, hour, aqi from public_marts.mart_city_hourly_aqi
        where hour > now() - interval '24 hours'
        order by hour
    """)
    if trend.empty:
        st.info("Not enough history yet — check back after a few more hourly runs.")
    else:
        # A 10-line overlay has no readable trend here (each hourly poll is an
        # independent draw, not a continuous series) and is well past the
        # legible series count anyway. A heatmap shows the same worst-hour /
        # worst-city pattern without implying a false continuity between points.
        pivot = trend.pivot(index="city", columns="hour", values="aqi")
        pivot = pivot.reindex(pivot.iloc[:, -1].sort_values(ascending=False).index)

        # Discrete steps at the real CPCB band breakpoints (0/50/100/200/300/400/500),
        # using the same colors as the category bar chart above — not a generic ramp.
        aqi_colorscale = [
            [0.00, CATEGORY_COLORS["Good"]], [0.10, CATEGORY_COLORS["Good"]],
            [0.10, CATEGORY_COLORS["Satisfactory"]], [0.20, CATEGORY_COLORS["Satisfactory"]],
            [0.20, CATEGORY_COLORS["Moderate"]], [0.40, CATEGORY_COLORS["Moderate"]],
            [0.40, CATEGORY_COLORS["Poor"]], [0.60, CATEGORY_COLORS["Poor"]],
            [0.60, CATEGORY_COLORS["Very Poor"]], [0.80, CATEGORY_COLORS["Very Poor"]],
            [0.80, CATEGORY_COLORS["Severe"]], [1.00, CATEGORY_COLORS["Severe"]],
        ]
        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=pivot.columns.strftime("%a %H:%M"),
            y=pivot.index,
            zmin=0, zmax=500,
            colorscale=aqi_colorscale,
            xgap=2, ygap=2,
            colorbar=dict(
                title="AQI",
                tickvals=[25, 75, 150, 250, 350, 450],
                ticktext=list(CATEGORY_COLORS.keys()),
            ),
            hovertemplate="%{y} — %{x}<br>AQI %{z}<extra></extra>",
        ))
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View as table"):
            st.dataframe(pivot, use_container_width=True)

st.subheader("🚨 Active alerts (AQI > 300)")
alerts = load("select * from public_marts.mart_aqi_alerts order by aqi desc")
if alerts.empty:
    st.success("No cities above 'Very Poor' right now.")
else:
    st.dataframe(alerts, use_container_width=True)
