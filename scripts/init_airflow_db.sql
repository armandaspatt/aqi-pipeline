-- Separate database for Airflow's own metadata; keep it out of the aqi
-- application database so Airflow's internal tables never mix with pipeline data.
CREATE DATABASE airflow OWNER aqi;
