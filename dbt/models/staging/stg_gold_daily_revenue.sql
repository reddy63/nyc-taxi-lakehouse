-- staging/stg_gold_daily_revenue.sql
-- Thin view over the Gold daily_revenue Delta table.
-- dbt reads this via DuckDB (dev) or Spark Thrift (prod).

{{ config(materialized='view') }}

SELECT
    trip_date,
    total_trips,
    total_fare,
    total_tips,
    total_revenue,
    avg_fare,
    avg_distance_miles,
    avg_duration_min,
    avg_passengers,
    total_distance_miles,
    _gold_computed_at
FROM delta_scan('s3://nyc-taxi-gold/yellow/daily_revenue/')
