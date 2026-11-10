/* ---------------------------------------------------------------------------
   EJ 1 — Huecos en la transmisión  (LAG)
   Para cada equipo, encuentra los intervalos donde pasaron MÁS de 5 minutos
   entre una lectura y la siguiente (usa stg.readings_clean.reading_ts).
   Salida: feeder_id, prev_ts, reading_ts, gap_minutes.
   Después: top 10 equipos con más minutos perdidos en total.
   Pista: LAG(reading_ts) OVER (PARTITION BY ... ORDER BY ...)
   ------------------------------------------------------------------------- */

use IoTLakehouse;

SELECT Top(10) * from stg.readings_clean;

WITH consecutive_ts as (
	SELECT r.*, 
		LAG(r.reading_ts, 1) OVER (
			Partition By r.feeder_id
			order by r.reading_ts
		) as prev_ts
	FROM stg.readings_clean as r
),
gaps as (
	Select c.feeder_id, c.prev_ts, c.reading_ts, DATEDIFF(MINUTE, c.prev_ts, c.reading_ts) as gap_minutes
	FROM consecutive_ts as c
)
Select TOP(10) * from gaps
where gap_minutes > 5;


-- TOP10
WITH consecutive_ts as (
	SELECT r.*, 
		LAG(r.reading_ts, 1) OVER (
			Partition By r.feeder_id
			order by r.reading_ts
		) as prev_ts
	FROM stg.readings_clean as r
),
gaps as (
	Select c.feeder_id, c.prev_ts, c.reading_ts, DATEDIFF(MINUTE, c.prev_ts, c.reading_ts) as gap_minutes
	FROM consecutive_ts as c
)
Select TOP(10) * from gaps
where gap_minutes > 5
ORDER BY gap_minutes DESC;

-- "¿Cómo lo explicaría en una entrevista en 2 frases?"
-- Basicamente se pide una diferencia entre un valor actual y uno anterior, esto lo logro con LAG.
-- luego al restar estos valores obtengo la diferencia entre un ts anterior y uno actual, pudiendo notar 
-- si hubo o no algún retraso entre las transmisiones, ya que se requiere que esten espaciadas 5 min.
-- esto me permite saber en que intervalos de tiempo hubo fallas, e identificar por que fallaron.