-- models/highest_polluted_cities.sql
{{ config(materialized='table') }}
SELECT 
    a.city,
    a.pollutant,
    a.avg_value,
    p."90_p_value" AS "90_p_value"
FROM 
    {{ ref('august_monthly_avg') }} a
JOIN 
    {{ ref('august_90_percentile_threshold') }} p 
ON 
    a.pollutant = p.pollutant
WHERE 
    a.avg_value >= p."90_p_value"
