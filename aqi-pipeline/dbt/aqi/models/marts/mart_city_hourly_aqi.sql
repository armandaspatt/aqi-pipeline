-- Hourly city-level worst-pollutant AQI (CPCB convention: AQI = max sub-index).
-- avg_value from the API is already the pollutant sub-index.
with hourly as (
    select
        city,
        date_trunc('hour', polled_at) as hour,
        pollutant,
        avg(avg_value) as sub_index
    from {{ ref('stg_aqi_readings') }}
    group by 1, 2, 3
)
select
    city,
    hour,
    max(sub_index)::int as aqi,
    (array_agg(pollutant order by sub_index desc))[1] as dominant_pollutant,
    case
        when max(sub_index) <= 50  then 'Good'
        when max(sub_index) <= 100 then 'Satisfactory'
        when max(sub_index) <= 200 then 'Moderate'
        when max(sub_index) <= 300 then 'Poor'
        when max(sub_index) <= 400 then 'Very Poor'
        else 'Severe'
    end as category
from hourly
group by 1, 2
