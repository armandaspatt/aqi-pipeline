-- Flatten JSONB payload into typed columns; dedupe on (station, pollutant, last_update)
with src as (
    select
        id,
        ingested_at,
        payload->>'state'          as state,
        payload->>'city'           as city,
        payload->>'station'        as station,
        payload->>'pollutant_id'   as pollutant,
        nullif(payload->>'avg_value','NA')::numeric as avg_value,
        nullif(payload->>'min_value','NA')::numeric as min_value,
        nullif(payload->>'max_value','NA')::numeric as max_value,
        nullif(payload->>'latitude','')::numeric    as latitude,
        nullif(payload->>'longitude','')::numeric   as longitude,
        (payload->>'polled_at')::timestamptz        as polled_at,
        payload->>'source'         as source
    from {{ source('bronze', 'aqi_readings_raw') }}
),
deduped as (
    select *,
        row_number() over (
            partition by city, station, pollutant, date_trunc('hour', polled_at)
            order by ingested_at desc
        ) as rn
    from src
)
select * from deduped where rn = 1
