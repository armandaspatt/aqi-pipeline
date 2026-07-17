"""Hourly AQI pipeline: poll CPCB -> Kafka -> bronze -> dbt -> alerts + DQ report."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PIPELINE = "/opt/pipeline"
DBT = f"cd {PIPELINE}/dbt/aqi && dbt"
DBT_FLAGS = "--profiles-dir . --no-use-colors"

default_args = {
    "owner": "arman",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="aqi_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["aqi", "cpcb"],
) as dag:

    check_api_health = BashOperator(
        task_id="check_api_health",
        bash_command=(
            'if [ "${AQI_MOCK_MODE:-0}" = "1" ]; then echo mock-mode; '
            "else curl -sf -o /dev/null --max-time 20 https://api.data.gov.in || exit 1; fi"
        ),
    )

    poll_and_produce = BashOperator(
        task_id="poll_and_produce",
        bash_command=f"python {PIPELINE}/producer/poll_cpcb.py",
    )

    land_bronze = BashOperator(
        task_id="land_bronze",
        bash_command=f"python {PIPELINE}/consumer/land_bronze.py",
    )

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps {DBT_FLAGS}")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run {DBT_FLAGS}")
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS} && {DBT} source freshness {DBT_FLAGS}",
    )

    def _check_alerts():
        import os
        import psycopg2

        conn = psycopg2.connect(os.environ["PG_DSN"])
        with conn.cursor() as cur:
            cur.execute("select city, aqi, category from public_marts.mart_aqi_alerts")
            rows = cur.fetchall()
        conn.close()
        if rows:
            # Swap print for a Slack webhook / email operator later
            for city, aqi, cat in rows:
                print(f"ALERT: {city} AQI={aqi} ({cat})")
        else:
            print("No cities above 'Very Poor' threshold.")

    check_severe_alerts = PythonOperator(
        task_id="check_severe_alerts", python_callable=_check_alerts
    )

    def _dq_report():
        import os
        import psycopg2

        conn = psycopg2.connect(os.environ["PG_DSN"])
        with conn.cursor() as cur:
            cur.execute(
                """select count(distinct payload->>'station')
                   from bronze.aqi_readings_raw
                   where ingested_at > now() - interval '1 hour'"""
            )
            (stations,) = cur.fetchone()
        conn.close()
        print(f"DQ: {stations} distinct stations reported in the last hour")

    data_quality_report = PythonOperator(
        task_id="data_quality_report", python_callable=_dq_report
    )

    (
        check_api_health
        >> poll_and_produce
        >> land_bronze
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> [check_severe_alerts, data_quality_report]
    )
