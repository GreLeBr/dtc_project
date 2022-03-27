{{ config(materialized='table')}}


SELECT tb.label, tb.nom_compteur, libelle, iu_ac
FROM {{ source('distance_calculated','close_all-traffic_bike') }}  tb
JOIN {{ source('distance_calculated','close_all-traffic_car') }}  ac
ON tb.label = ac.label