{{ config(materialized='view') }}
WITH filtered_data AS 
(
    SELECT  city,pollutant,value
    FROM {{ ref('airquality') }} 
    WHERE month ='08'   
    AND pollutant IN ('co', 'so2')
)
SELECT city,pollutant,AVG(value) AS avg_value
FROM filtered_data
GROUP BY city,pollutant
