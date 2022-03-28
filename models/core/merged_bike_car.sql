{{ config(materialized='table') }}

WITH 
b AS (SELECT * FROM {{ref('selected_car_bike')  }} ),
c AS (SELECT * FROM {{ref('selected_bike_car')  }} )

SELECT 
 b.libelle, b.q as car_debit, b.k as road_occupation, b.etat_trafic, c.nom_compteur, c.sum_counts as bike_users
FROM b
JOIN c

ON b.t_1h = c.date
ORDER BY  b.libelle, b.etat_trafic, c.nom_compteur