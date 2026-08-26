-- marts/fct_peak_hours.sql
-- Peak demand analysis: busiest hours ranked per day-of-week.

{{ config(
    materialized = 'table',
    tags         = ['demand', 'hourly']
) }}

WITH base AS (
    SELECT * FROM {{ ref('stg_gold_hourly_demand') }}
),

aggregated AS (
    SELECT
        day_of_week,
        day_name,
        hour_of_day,
        time_of_day_bucket,
        SUM(trip_count)               AS total_trips,
        ROUND(AVG(avg_fare),    2)    AS avg_fare,
        ROUND(AVG(avg_duration_min),2) AS avg_duration_min,
        ROUND(SUM(total_fare),  2)    AS total_revenue,
        COUNT(DISTINCT trip_date)     AS days_in_sample
    FROM base
    GROUP BY 1, 2, 3, 4
),

ranked AS (
    SELECT
        *,
        ROUND(total_trips * 1.0 / NULLIF(days_in_sample, 0), 1) AS avg_trips_per_day,
        RANK() OVER (
            PARTITION BY day_of_week
            ORDER BY total_trips DESC
        ) AS hour_rank_within_day
    FROM aggregated
)

SELECT
    day_of_week,
    day_name,
    hour_of_day,
    time_of_day_bucket,
    total_trips,
    avg_trips_per_day,
    avg_fare,
    avg_duration_min,
    total_revenue,
    hour_rank_within_day
FROM ranked
ORDER BY day_of_week, hour_of_day
