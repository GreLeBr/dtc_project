{{ config(materialized='table') }}

WITH a AS (SELECT * FROM {{ref('selected_all-traffic_car')  }} ),
b AS (SELECT * FROM {{ref('selected_car_all-traffic')  }} )


SELECT a.t as date, a.label, a.mode as mode_of_transport, a.nb_usagers as multimod_users,
 b.libelle, b.q as car_debit, b.k as road_occupation, b.etat_trafic, b.geo_point_2d
FROM a
JOIN b
ON a.t = b.t_1h

 
ORDER BY a.t, a.label, a.mode, b.libelle, b.etat_trafic