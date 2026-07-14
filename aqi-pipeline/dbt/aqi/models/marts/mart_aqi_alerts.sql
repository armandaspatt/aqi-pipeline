-- Cities currently in 'Very Poor' or worse in the latest completed hour
with latest as (
    select max(hour) as hour from {{ ref('mart_city_hourly_aqi') }}
)
select c.*
from {{ ref('mart_city_hourly_aqi') }} c
join latest l on c.hour = l.hour
where c.aqi > 300
