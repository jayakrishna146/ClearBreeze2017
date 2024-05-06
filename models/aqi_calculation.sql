-- models/aqi_calculation.sql

WITH normalized_data AS (
    SELECT 
        country,
        hour,
        pollutant,
        AVG(normalized_value) AS normalized_avg  -- Aggregate average normalized values by pollutant
    FROM 
        {{ ref('normalized_air_quality') }}
    GROUP BY 
        country, hour, pollutant
),

pivot_data AS (
    SELECT
        country,
        hour,
        MAX(CASE WHEN pollutant = 'pm25' THEN normalized_avg END) AS normalized_pm25,
        MAX(CASE WHEN pollutant = 'so2' THEN normalized_avg END) AS normalized_so2,
        MAX(CASE WHEN pollutant = 'co' THEN normalized_avg END) AS normalized_co
    FROM 
        normalized_data
    GROUP BY 
        country, hour
),

aqi_scores AS (
    SELECT
        country,
        hour,
        (COALESCE(normalized_pm25, 0) + COALESCE(normalized_so2, 0) + COALESCE(normalized_co, 0)) / 
        (CASE WHEN normalized_pm25 IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN normalized_so2 IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN normalized_co IS NOT NULL THEN 1 ELSE 0 END) AS aqi
    FROM 
        pivot_data
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
FROM 
    aqi_scores
