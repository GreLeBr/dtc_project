{{ config(materialized='table')}}

With all_trafic_coords AS (
    SELECT label,coordonnees_geo,
     CAST(SPLIT(coordonnees_geo, ',')[OFFSET(0)] AS float64) AS longitude,
CAST(SPLIT(coordonnees_geo, ',')[OFFSET(1)] AS float64) AS latitude 
FROM (
SELECT label,coordonnees_geo
FROM {{ source('staging','traffic_allvehicle_table') }} 
WHERE label != ""
GROUP BY label, coordonnees_geo)
),
car_coords AS (
    SELECT iu_ac,libelle, geo_point_2d,
    CAST(SPLIT(geo_point_2d, ',')[OFFSET(0)] as float64) AS longitude,
CAST(SPLIT(geo_point_2d, ',')[OFFSET(1)] as float64) AS latitude 
FROM (
SELECT iu_ac, geo_point_2d, libelle
FROM {{ source('staging','current_table') }}  
WHERE iu_ac != ""
AND geo_point_2d != ""
GROUP BY iu_ac, geo_point_2d, libelle)
)
SELECT * FROM (
    SELECT *, RANK() OVER(partition by label order by distance) AS RANK FROM (
    SELECT label,coordonnees_geo,iu_ac, 
    libelle,geo_point_2d, distance FROM (
        SELECT all_trafic_coords.label, 
        all_trafic_coords.coordonnees_geo,
         car_coords.iu_ac,
          car_coords.libelle,
           car_coords.geo_point_2d,
      ST_DISTANCE(ST_GEOGPOINT(all_trafic_coords.longitude,all_trafic_coords.latitude) ,
       ST_GEOGPOINT(car_coords.longitude, car_coords.latitude)) AS distance
FROM all_trafic_coords, car_coords)
)
)
WHERE RANK=1
AND distance < 100
ORDER BY distance

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}