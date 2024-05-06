-- models/calculate_aqi.sql

{{config(materialized='table')}}
WITH normalized_data AS (
    SELECT *
    FROM {{ ref('normalized_air_quality') }}
),
aqi AS (
    SELECT
        country,
        local,
        hour,
        AVG(normalized_value) AS average_aqi
    FROM normalized_data
    GROUP BY country, local, hour
)
SELECT
    country,
    local,
    hour,
    average_aqi,
    CASE
        WHEN average_aqi > 66 THEN 'High'
        WHEN average_aqi > 33 THEN 'Moderate'
        ELSE 'Low'
    END AS aqi_level,
    CURRENT_TIMESTAMP() AS loaded_at
FROM aqi
