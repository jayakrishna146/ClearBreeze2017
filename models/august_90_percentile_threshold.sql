-- models/august_90_percentile_threshold.sql
{{ config(materialized='view') }}
SELECT 
    city,pollutant,
    approx_percentile(avg_value, 0.9) AS "90_p_value"
FROM 
    {{ ref('august_monthly_avg') }}
GROUP BY city,pollutant
