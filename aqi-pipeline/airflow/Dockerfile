FROM apache/airflow:2.9.2-python3.11

# dbt-postgres depends on the source "psycopg2" package (not psycopg2-binary),
# which needs pg_config/libpq headers to build; the base image doesn't have them.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
