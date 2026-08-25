-- marts/dim_payment_types.sql
-- Payment dimension: revenue split, tip rate, trip share.

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
FROM delta_scan('s3://nyc-taxi-gold/yellow/payment_breakdown/')
ORDER BY trip_count DESC
