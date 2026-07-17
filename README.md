# AQI India Pipeline

Hourly ELT pipeline over CPCB real-time air quality data (data.gov.in), with a live Streamlit dashboard.

**Flow:** CPCB API → Kafka (Redpanda) → Postgres bronze → dbt (staging → marts) → Airflow-orchestrated, alerts + data-quality checks → Streamlit dashboard.

## Stack
- **Redpanda** — Kafka-compatible broker (topic: `cpcb-aqi-raw`)
- **Postgres 16** — bronze landing + dbt warehouse
- **dbt** — staging views, hourly city AQI mart, alerts mart, schema + freshness tests
- **Airflow 2.9** — hourly DAG: health check → poll → land → dbt run/test → alerts + DQ report
- **Streamlit + Plotly** — dashboard on `localhost:8501`
- **Redpanda Console** — inspect topics on `localhost:8080`

## Quickstart
```bash
cp .env.example .env          # mock mode works out of the box
docker compose up -d
# Airflow UI: http://localhost:8081  (user/pass printed in `docker compose logs airflow`)
# Trigger the `aqi_pipeline` DAG once, then open the dashboard:
# Dashboard: http://localhost:8501
```

## Real data
1. Register at https://data.gov.in and generate an API key.
2. Put it in `.env` as `DATA_GOV_API_KEY=...` and set `AQI_MOCK_MODE=0`.
3. `docker compose up -d --force-recreate airflow`

## Design notes
- **At-least-once landing:** the consumer commits Postgres before Kafka offsets; dedup happens in `stg_aqi_readings` via `row_number()` per (city, station, pollutant, hour).
- **AQI convention:** CPCB AQI = max pollutant sub-index; `mart_city_hourly_aqi` implements this with category bands (Good → Severe).
- **Freshness tests:** `dbt source freshness` fails loudly if bronze goes stale >4h — stations do go offline.
