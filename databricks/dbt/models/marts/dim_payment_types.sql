-- databricks/dbt/models/marts/dim_payment_types.sql
-- CHANGE: delta_scan() -> {{ source('nyc_taxi_gold', 'payment_breakdown') }}
-- Everything else (ROUND, NULLIF, ORDER BY) is ANSI SQL — zero changes.

{{ config(
    materialized = 'table',
    tags         = ['dimension', 'payment']
) }}

SELECT
    payment_type,
    payment_type_desc,
    trip_count,
    total_fare,
    total_tips,
    total_revenue,
    avg_tip,
    trip_share_pct,
    ROUND(total_tips / NULLIF(total_fare, 0) * 100, 2) AS tip_rate_pct
FROM {{ source('nyc_taxi_gold', 'payment_breakdown') }}
ORDER BY trip_count DESC
