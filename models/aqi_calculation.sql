-- models/aqi_calculation.sql

WITH normalized_data AS (
    SELECT *
    FROM {{ ref('normalized_air_quality') }}
),

aqi_scores AS (
    SELECT
        country,
        hour,
        (normalized_pm25 + normalized_so2 + normalized_co) / 3 AS aqi
    FROM normalized_data
)

SELECT
    country,
    hour,
    aqi,
    CASE
        WHEN aqi > 66 THEN 'High'
        WHEN aqi > 33 THEN 'Moderate'
        ELSE 'Low'
    END AS aqi_level
FROM aqi_scores
