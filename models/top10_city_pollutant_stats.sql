-- models/top10_city_pollutant_stats.sql

WITH top_cities AS (
    SELECT city, day
    FROM {{ ref('top_10_cities_hourly_pm25') }}
),
pollutant_values AS (
    SELECT
        a.city,
        a.pollutant,
        a.value
    FROM {{ ref('airquality') }} a
    JOIN top_cities ON a.city = top_cities.city AND a.day = top_cities.day
    WHERE a.pollutant IN ('co', 'so2')
),
value_counts AS (
    SELECT
        city,
        pollutant,
        value,
        COUNT(*) AS frequency
    FROM pollutant_values
    GROUP BY city, pollutant, value
),
max_frequency AS (
    SELECT
        city,
        pollutant,
        MAX(frequency) AS max_frequency
    FROM value_counts
    GROUP BY city, pollutant
),
mode_values AS (
    SELECT
        vc.city,
        vc.pollutant,
        vc.value AS mode
    FROM value_counts vc
    INNER JOIN max_frequency mf ON vc.city = mf.city AND vc.pollutant = mf.pollutant AND vc.frequency = mf.max_frequency
),
pollutant_aggregates AS (
    SELECT
        pv.city,
        pv.pollutant,
        AVG(pv.value) AS mean,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pv.value) AS median,
        mv.mode
    FROM pollutant_values pv
    JOIN mode_values mv ON pv.city = mv.city AND pv.pollutant = mv.pollutant
    GROUP BY pv.city, pv.pollutant, mv.mode
)
SELECT 
    city,
    MAX(CASE WHEN pollutant = 'co' THEN mean ELSE NULL END) AS co_mean,
    MAX(CASE WHEN pollutant = 'co' THEN median ELSE NULL END) AS co_median,
    MAX(CASE WHEN pollutant = 'co' THEN mode ELSE NULL END) AS co_mode,
    MAX(CASE WHEN pollutant = 'so2' THEN mean ELSE NULL END) AS so2_mean,
    MAX(CASE WHEN pollutant = 'so2' THEN median ELSE NULL END) AS so2_median,
    MAX(CASE WHEN pollutant = 'so2' THEN mode ELSE NULL END) AS so2_mode
FROM pollutant_aggregates
GROUP BY city
