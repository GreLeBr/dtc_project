{{ config(materialized='table')}}


With all_trafic_coords AS (
SELECT label, CAST(SPLIT(coordonnees_geo, ',')[OFFSET(0)] AS float64) AS longitude,
CAST(SPLIT(coordonnees_geo, ',')[OFFSET(1)] AS float64) AS latitude, coordonnees_geo 
FROM (
SELECT label,coordonnees_geo
FROM {{ source('staging','traffic_allvehicle_table') }} 
WHERE label != ""
GROUP BY label, coordonnees_geo)
),
bike_coords AS (
    SELECT nom_compteur, CAST(SPLIT(coordinates, ',')[OFFSET(0)] AS float64)  AS longitude,
CAST(SPLIT(coordinates, ',')[OFFSET(1)] AS float64) AS latitude , coordinates
FROM (
SELECT nom_compteur,coordinates
FROM {{ source('staging','traffic_bike_table') }}
WHERE nom_compteur != ""
AND coordinates != ""
GROUP BY nom_compteur, coordinates)
)
SELECT * FROM (
    SELECT label, coordonnees_geo, nom_compteur, coordinates, distance, RANK() over(partition by label order by distance ) AS RANK 
FROM (
    SELECT all_trafic_coords.label,
     all_trafic_coords.coordonnees_geo,
       bike_coords.nom_compteur,bike_coords.coordinates,
       ST_DISTANCE(ST_GEOGPOINT(all_trafic_coords.longitude,
       all_trafic_coords.latitude),
 ST_GEOGPOINT(bike_coords.longitude, bike_coords.latitude)) AS distance
FROM all_trafic_coords, bike_coords)
 )
WHERE RANK<4
AND distance < 250
ORDER BY distance DESC



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}