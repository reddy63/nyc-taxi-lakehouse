-- databricks/dbt/models/staging/stg_gold_hourly_demand.sql
-- CHANGE: delta_scan() -> {{ source('nyc_taxi_gold', 'hourly_demand') }}

{{ config(materialized='view') }}

SELECT
    trip_date,
    day_of_week,
    day_name,
    hour_of_day,
    time_of_day_bucket,
    trip_count,
    avg_fare,
    avg_duration_min,
    total_fare,
    _gold_computed_at
FROM {{ source('nyc_taxi_gold', 'hourly_demand') }}
