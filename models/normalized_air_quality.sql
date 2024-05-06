-- models/normalized_air_quality.sql

WITH x AS (
    SELECT local,country,hour,
    case
     when pollutant = 'pm25'   then value / 25.0 * 100 
     when pollutant = 'so2' then value / 20.0 * 100 
     when pollutant = 'co' then value / 10.0 * 100 
     ELSE NULL
     END AS normalized_value, 
     pollutant

    FROM {{ ref('airquality') }}
)
SELECT * FROM x
