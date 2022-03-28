{{ config(materialized='table') }}

WITH a AS (SELECT * FROM {{ref('selected_all-traffic_triple')  }} ),
b AS (SELECT * FROM {{ref('selected_car_triple')  }} ),
c AS (SELECT * FROM {{ref('selected_bike_triple')  }} )

SELECT a.t as date, a.label, a.mode as mode_of_transport, a.nb_usagers as multimod_users,
 b.libelle, b.q as car_debit, b.k as road_occupation, b.etat_trafic, c.nom_compteur, c.sum_counts as bike_users
FROM a
JOIN b
ON a.t = b.t_1h
JOIN c 
ON a.t = c.date
-- WHERE a.mode ='Trottinettes + vélos' AND b.libelle='Amsterdam' 
ORDER BY a.t, a.label, a.mode, b.libelle, b.etat_trafic, c.nom_compteur