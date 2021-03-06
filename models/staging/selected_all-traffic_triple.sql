{{ config(materialized='view')}}

with close AS (SELECT DISTINCT label FROM {{ source('distance_calculated','close_triple') }} limit 1 )


SELECT t, a.label, mode, nb_usagers, voie
FROM {{ source('staging','traffic_allvehicle_table') }}  a
JOIN  close c
ON  a.label = c.label

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}