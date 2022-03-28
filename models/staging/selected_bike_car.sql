{{ config(materialized='view')}}


with close AS (SELECT DISTINCT nom_compteur FROM {{ source('distance_calculated','close_bike_car') }} ) 

SELECT RTRIM(nom_compteur, ARRAY_REVERSE(SPLIT(nom_compteur, ' '))[SAFE_OFFSET(0)]) as nom_compteur, date, sum(sum_counts) as sum_counts
FROM (

   
SELECT b.nom_compteur, date, sum_counts
FROM `dtc-proj.traffic_all.traffic_bike_table` b
JOIN close c
ON b.nom_compteur = c.nom_compteur
)

GROUP BY nom_compteur, date
-- ORDER BY date



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}

