/* ---------------------------------------------------------------------------
   EJ 8 — Índices y planes de ejecución
   a) Ejecuta la consulta del EJ 1 con SET STATISTICS IO ON, anota los
      logical reads.
   b) Crea un índice que la mejore, vuelve a medir y anota la diferencia.
   c) Explica "index seek" vs "index scan" y qué es un índice de cobertura.
   ------------------------------------------------------------------------- */

use IoTLakehouse;

SET STATISTICS IO ON

DROP INDEX IF EXISTS IX_readings_clean_feeder_ts ON stg.readings_clean;
CREATE NONCLUSTERED INDEX IX_readings_clean_feeder_ts
    ON stg.readings_clean (feeder_id, reading_ts DESC)
    INCLUDE (pond_id, voltage_v, current_a, battery_pct, is_feeding);
GO

DROP TABLE IF EXISTS #gaps_temp;
GO


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
Select * INTO #gaps_temp from gaps
where gap_minutes > 5;

SELECT * FROM #gaps_temp;

-- TOP10
-- TOP 10 equipos con más minutos perdidos
SELECT TOP (10)
    feeder_id,
    SUM(gap_minutes - 5) AS lost_minutes
FROM #gaps_temp
GROUP BY feeder_id
ORDER BY lost_minutes DESC;

-- index seek realiza una busqueda por paginas del indice primero utilizando la estructura del arbol, se usa cuando
-- se busca datos en especifico, como un alimentador en específico. Ahorra tiempo y procesamiento.
-- index scan, lee en suecuencia todas las paginas del index, lo que lo hace más lento, se usa para el Query busca muchos datos
-- esto lo determina automáticamente el Query Optimizer.{

-- Index seek: navega el árbol del índice (raíz → hojas) directo al rango que
-- necesita. Ideal para filtros selectivos (un equipo, un rango de fechas).
-- Index scan: lee todas las páginas del índice o de la tabla. No es malo si la
-- consulta necesita la mayoría de las filas (como esta, que recorre toda la tabla).
-- El seek solo es posible si el filtro usa la primera columna del índice y es
-- SARGable (sin funciones sobre la columna, ej. YEAR(ts) = 2026 impide el seek).
-- Índice de cobertura: contiene todas las columnas que pide la consulta (clave +
-- INCLUDE), así evita los Key Lookups a la tabla.
-- En este ejercicio el índice no evita el scan, pero es más estrecho (menos
-- páginas) y entrega los datos ya ordenados, eliminando el Sort del LAG.