-- marts/fct_monthly_revenue.sql
-- Monthly revenue fact table with MoM growth.

{{ config(
    materialized = 'table',
    tags         = ['revenue', 'monthly']
) }}

WITH base AS (
    SELECT * FROM {{ ref('stg_gold_daily_revenue') }}
),

monthly AS (
    SELECT
        DATE_TRUNC('month', trip_date)          AS revenue_month,
        SUM(total_trips)                        AS total_trips,
        ROUND(SUM(total_fare),    2)            AS total_fare,
        ROUND(SUM(total_tips),    2)            AS total_tips,
        ROUND(SUM(total_revenue), 2)            AS total_revenue,
        ROUND(AVG(avg_fare),      2)            AS avg_daily_fare,
        ROUND(AVG(avg_distance_miles), 2)       AS avg_distance_miles,
        ROUND(SUM(total_distance_miles), 2)     AS total_distance_miles,
        ROUND(AVG(avg_passengers), 2)           AS avg_passengers
    FROM base
    GROUP BY 1
),

with_growth AS (
    SELECT
        *,
        LAG(total_revenue) OVER (ORDER BY revenue_month) AS prev_month_revenue,
        ROUND(
            (total_revenue - LAG(total_revenue) OVER (ORDER BY revenue_month))
            * 100.0
            / NULLIF(LAG(total_revenue) OVER (ORDER BY revenue_month), 0),
            2
        ) AS mom_revenue_growth_pct
    FROM monthly
)

SELECT
    revenue_month,
    total_trips,
    total_fare,
    total_tips,
    total_revenue,
    avg_daily_fare,
    avg_distance_miles,
    total_distance_miles,
    avg_passengers,
    COALESCE(prev_month_revenue, 0)         AS prev_month_revenue,
    COALESCE(mom_revenue_growth_pct, 0)     AS mom_revenue_growth_pct
FROM with_growth
ORDER BY revenue_month
