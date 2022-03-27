with close AS (SELECT DISTINCT nom_compteur FROM `dtc-proj.traffic_all.close_counter`)


SELECT b.nom_compteur, date, sum_counts
FROM `dtc-proj.traffic_all.traffic_bike_table` b
JOIN close c
ON b.nom_compteur = c.nom_compteur