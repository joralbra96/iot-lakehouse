/* ---------------------------------------------------------------------------
   EJ 5 — Sesiones de alimentación  (gaps and islands — muy de entrevista)
   Una "sesión" = lecturas consecutivas de un equipo con is_feeding = 1.
   Para cada sesión: feeder_id, session_start, session_end, duration_min.
   Pista: la diferencia entre dos ROW_NUMBER() (uno sobre todas las lecturas
   del equipo y otro solo sobre las de is_feeding = 1) es constante dentro de
   una misma isla.
   ------------------------------------------------------------------------- */

USE IoTLakehouse;
GO

SELECT TOP(1) * FROM dw.fact_readings;

WITH row_readings as (
	SELECT r.feeder_key, f.feeder_id, r.reading_ts, is_feeding,
		ROW_NUMBER() OVER (
			PARTITION BY f.feeder_id
			ORDER BY r.reading_ts
		) as rn_readings
	FROM dw.fact_readings as r
	JOIN dw.dim_feeder as f ON f.feeder_key = r.feeder_key
),
row_feedings as (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY feeder_id, is_feeding
			ORDER BY reading_ts
		) as rn_feed
	FROM row_readings
),
diff_rn as (
	SELECT *, rn_readings - rn_feed as dif_rn
	FROM row_feedings
),
sesion as (
	SELECT feeder_id, min(reading_ts) as session_start, max(reading_ts) as session_end FROM diff_rn
	WHERE is_feeding = 1
	GROUP BY feeder_id, dif_rn
)
SELECT *, DATEDIFF(MINUTE, session_start, session_end) as duration_min
FROM sesion
ORDER BY feeder_id, session_end;


-- “Es un problema de gaps and islands. Numero las lecturas de cada equipo con ROW_NUMBER
-- y lo resto de una secuencia que avanza de forma constante: dentro de una racha la diferencia 
-- no cambia, así que puedo agrupar por ella y sacar el inicio, el fin y la duración de cada sesión.
-- Si pueden faltar datos, uso como secuencia el número de intervalo de tiempo y no la posición de 
-- la fila; si no, un hueco uniría dos sesiones distintas.”