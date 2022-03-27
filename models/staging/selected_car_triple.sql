{{ config(materialized='view')}}


with close AS (SELECT DISTINCT iu_ac, libelle FROM {{ source('distance_calculated','close_triple') }})


SELECT c.iu_ac, c.libelle,t_1h, q, k, etat_trafic
FROM {{ source('staging','current_table') }} c
JOIN close cl
ON c.iu_ac = cl.iu_ac
ORDER BY iu_ac, t_1H

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}