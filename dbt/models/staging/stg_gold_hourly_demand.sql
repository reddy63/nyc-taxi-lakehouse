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
FROM delta_scan('s3://nyc-taxi-gold/yellow/hourly_demand/')
