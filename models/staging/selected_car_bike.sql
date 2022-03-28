{{ config(materialized='view')}}


with close AS (SELECT DISTINCT  libelle FROM {{ source('distance_calculated','close_bike_car') }} )


SELECT c.libelle,t_1h, q, k, etat_trafic, c.geo_point_2d
FROM {{ source('staging','current_table') }} c
JOIN close cl
ON c.libelle = cl.libelle
-- ORDER BY c.libelle, t_1H

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}