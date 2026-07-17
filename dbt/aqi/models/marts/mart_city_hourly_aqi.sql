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
),
-- CPCB minimum-data rule: a city-hour needs at least 3 distinct pollutants
-- reporting, and at least one of them must be PM2.5 or PM10, else no AQI
-- is computed for that city-hour.
eligible as (
    select
        city,
        hour
    from hourly
    group by 1, 2
    having
        count(distinct pollutant) >= 3
        and bool_or(pollutant in ('PM2.5', 'PM10'))
)
select
    h.city,
    h.hour,
    max(h.sub_index)::int as aqi,
    (array_agg(h.pollutant order by h.sub_index desc))[1] as dominant_pollutant,
    case
        when max(h.sub_index) <= 50  then 'Good'
        when max(h.sub_index) <= 100 then 'Satisfactory'
        when max(h.sub_index) <= 200 then 'Moderate'
        when max(h.sub_index) <= 300 then 'Poor'
        when max(h.sub_index) <= 400 then 'Very Poor'
        else 'Severe'
    end as category
from hourly h
join eligible e on h.city = e.city and h.hour = e.hour
group by 1, 2
