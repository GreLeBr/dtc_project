{{ config(materialized='table')}}

With bike_coords AS (
    SELECT nom_compteur, CAST(SPLIT(coordinates, ',')[OFFSET(0)] AS float64)  AS longitude,
CAST(SPLIT(coordinates, ',')[OFFSET(1)] AS float64) AS latitude , coordinates
FROM (
SELECT nom_compteur,coordinates
FROM {{ source('staging','traffic_bike_table') }} 
WHERE nom_compteur != ""
AND coordinates != ""
GROUP BY nom_compteur, coordinates)
),
car_coords AS (SELECT iu_ac,libelle, geo_point_2d,CAST(SPLIT(geo_point_2d, ',')[OFFSET(0)] as float64) AS longitude,
CAST(SPLIT(geo_point_2d, ',')[OFFSET(1)] as float64) AS latitude 
FROM (
SELECT iu_ac,geo_point_2d, libelle
FROM {{ source('staging','current_table') }}  
WHERE iu_ac != ""
AND geo_point_2d != ""
GROUP BY iu_ac, geo_point_2d, libelle)
)
SELECT * FROM (
    SELECT *, RANK() OVER(partition by nom_compteur order by distance) AS RANK FROM (
    SELECT nom_compteur,coordinates, libelle,geo_point_2d, distance FROM (SELECT bike_coords.nom_compteur, bike_coords.coordinates, car_coords.libelle, car_coords.geo_point_2d,
      ST_DISTANCE(ST_GEOGPOINT(bike_coords.longitude,bike_coords.latitude) , ST_GEOGPOINT(car_coords.longitude, car_coords.latitude)) AS distance
FROM bike_coords, car_coords)
)
)
WHERE RANK=1
AND distance < 50
-- ORDER BY distance
ORDER BY nom_compteur

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}