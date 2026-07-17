"""Drain the cpcb-aqi-raw topic into Postgres bronze.aqi_readings_raw.

Runs as a bounded batch job (fits Airflow task semantics): consumes until the
topic is idle for IDLE_TIMEOUT seconds, commits offsets, exits.
"""
import json
import os
import sys

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer

TOPIC = os.environ.get("AQI_TOPIC", "cpcb-aqi-raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://aqi:aqi@localhost/aqi")
IDLE_TIMEOUT = float(os.environ.get("IDLE_TIMEOUT", "10"))
BATCH_SIZE = 500


def main() -> int:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "bronze-lander",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    total, batch = 0, []
    try:
        while True:
            msg = consumer.poll(IDLE_TIMEOUT)
            if msg is None:
                break  # topic idle -> batch complete
            if msg.error():
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue
            batch.append((msg.offset(), msg.partition(), msg.value().decode()))
            if len(batch) >= BATCH_SIZE:
                flush(cur, conn, consumer, batch)
                total += len(batch)
                batch = []
        if batch:
            flush(cur, conn, consumer, batch)
            total += len(batch)
    finally:
        consumer.close()
        conn.close()

    print(f"Landed {total} records into bronze.aqi_readings_raw")
    return 0


def flush(cur, conn, consumer, batch):
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO bronze.aqi_readings_raw (kafka_offset, kafka_partition, payload)
           VALUES %s""",
        [(o, p, v) for o, p, v in batch],
        template="(%s, %s, %s::jsonb)",
    )
    conn.commit()          # DB commit first...
    consumer.commit()      # ...then Kafka offsets (at-least-once semantics)


if __name__ == "__main__":
    sys.exit(main())
