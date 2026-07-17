CREATE SCHEMA IF NOT EXISTS bronze;

-- Raw landing table: one row per (station, pollutant, poll timestamp)
CREATE TABLE IF NOT EXISTS bronze.aqi_readings_raw (
    id            BIGSERIAL PRIMARY KEY,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_offset  BIGINT,
    kafka_partition INT,
    payload       JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_ingested ON bronze.aqi_readings_raw (ingested_at);
CREATE INDEX IF NOT EXISTS idx_bronze_payload_city ON bronze.aqi_readings_raw ((payload->>'city'));
