--For any given hour, 
--find the top 10 cities with the highest daily average levels of particle air pollution (PM2.5) and provide the mean, median and mode measures of carbon monoxide (CO) and sulfur dioxide pollution (SO2) for those cities on that day.
-- models/top_10_cities_hourly_pm25.sql

{{ config(materialized='table') }}
WITH daily_averages AS 
(
    SELECT city,day,hour, AVG(value) AS pm25_daily_avg  
    FROM {{ ref('airquality') }} 
    WHERE pollutant = 'pm25'
    GROUP BY  city,day,hour
),
ranked_cities_per_day AS 
(
    SELECT 
        city, 
        pm25_daily_avg,
        day,
        hour,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY pm25_daily_avg DESC) as row_no
    FROM 
        daily_averages
)
SELECT 
    city,day
FROM 
    ranked_cities_per_day
WHERE 
    row_no <= 10





