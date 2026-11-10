/* ---------------------------------------------------------------------------
   EJ 4 — Media móvil de batería de 1 hora  (marco de ventana)
   Para un equipo (p. ej. 'FD-0001'), media de battery_pct de las últimas 12
   lecturas (incluida la actual).
   Pista: AVG(...) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ? PRECEDING AND CURRENT ROW)
   Pregunta extra: ¿por qué ROWS y no RANGE? ¿Qué pasa con los huecos del EJ 1?
   ------------------------------------------------------------------------- */


USE IoTLakehouse;

SELECT TOP(1) * from dw.fact_readings;

SELECT * from dw.dim_feeder;


SELECT f.feeder_id, r.reading_ts, r.battery_pct,
		AVG(r.battery_pct) OVER (
			PARTITION BY f.feeder_id
			ORDER BY r.reading_ts
			ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
			) as media_movil
FROM dw.fact_readings as r
JOIN dw.dim_feeder as f ON f.feeder_key = r.feeder_key


-- “12 filas” no es lo mismo que “1 hora” cuando faltan datos: con un hueco de 40 minutos,
-- la ventana cubre unos 90 minutos, y en el caso extremo puede mezclar lecturas de días distintos. 
-- Lo detecto midiendo con LAG(reading_ts, 11) cuánto tiempo abarca cada ventana, y marco como no
-- fiables las que pasan de 55 minutos o tienen menos de 12 filas. Si el negocio necesita estrictamente
-- la última hora, uso una ventana por tiempo: en Spark con rangeBetween o en SQL Server con CROSS APPLY y un filtro de fechas.