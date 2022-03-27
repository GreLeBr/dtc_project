{{ config(materialized='table') }}

WITH a AS (SELECT * FROM {{ref('selected_all-traffic_triple')  }} ),
b AS (SELECT * FROM {{ref('selected_car_triple')  }} ),
c AS (SELECT * FROM {{ref('selected_bike_triple')  }} )

SELECT a.t, a.label, a.mode, a.nb_usagers, b.libelle, b.q, b.k, b.etat_trafic, c.nom_compteur, c.sum_counts
FROM a
JOIN b
ON a.t = b.t_1h
JOIN c 
ON a.t = c.date
-- WHERE a.mode ='Trottinettes + vélos' AND b.libelle='Amsterdam' 
ORDER BY a.t, a.label, a.mode, b.libelle, b.etat_trafic, c.nom_compteur