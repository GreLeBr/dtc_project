{{ config(materialized='view')}}

with close AS (SELECT DISTINCT nom_compteur FROM {{ source('distance_calculated','close_triple') }})


SELECT b.nom_compteur, date, sum_counts
FROM {{ source('staging','traffic_bike_table') }}  b
JOIN close c
ON b.nom_compteur = c.nom_compteur

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

