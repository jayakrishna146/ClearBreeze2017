--For any given day find the top 5 cities globally 
--with the highest daily average levels of particle air pollution (PM2.5).
-- models/top_5_cities_pm25_avg.sql
{{ config(materialized='table') }}

WITH ranked_cities AS 
(
    SELECT city,AVG(value) AS pm25_avg,day,pollutant,
     ROW_NUMBER() OVER (ORDER BY AVG(value) DESC) as rank
    FROM  {{ ref('airquality') }}  
    WHERE day = '20' AND pollutant = 'pm25'
    GROUP BY city, day, pollutant
)
SELECT city
FROM ranked_cities
WHERE rank <=5
