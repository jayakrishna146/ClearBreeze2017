{{config(materialized='table')}}
WITH source_data AS 
(
    SELECT 
    extract(year from local) as year,
    extract(month from local) as month,
    extract(day from local)  as day,
    extract(hour from local) as hour,  
    local,location,city,country,
    parameter as pollutant,
    value,unit,latitude,longitude
    FROM {{ source('raw', 'source_raw') }} 
)
SELECT *, CURRENT_TIMESTAMP() AS loaded_at
FROM source_data
    