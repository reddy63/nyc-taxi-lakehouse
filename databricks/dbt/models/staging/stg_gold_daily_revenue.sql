-- databricks/dbt/models/staging/stg_gold_daily_revenue.sql
--
-- CHANGE vs local version:
--   delta_scan('s3://...')  removed
--   {{ source('nyc_taxi_gold', 'daily_revenue') }}  replaces it
--
-- dbt-databricks reads this via the Databricks SQL Warehouse connection
-- defined in profiles.yml.  The source table is the Unity Catalog external
-- table registered by aggregate_gold.py:  nyc_taxi.gold.daily_revenue

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
FROM {{ source('nyc_taxi_gold', 'daily_revenue') }}
