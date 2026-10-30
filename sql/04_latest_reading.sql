/* =============================================================================
   04 — Última lectura por equipo (pregunta clásica de entrevista)
   -----------------------------------------------------------------------------
   Tres formas de resolverlo y un índice que las acelera.
   Para comparar: activar SET STATISTICS IO ON y mirar "logical reads",
   o ver el plan de ejecución (Ctrl+M en SSMS).
   ========================================================================== */
USE IoTLakehouse;
GO

/* Índice compuesto (feeder_id, reading_ts DESC) + INCLUDE:
   - Las columnas clave permiten un SEEK por equipo ya ordenado por fecha.
   - INCLUDE guarda columnas extra solo en las hojas del índice, así la
     consulta se responde sin volver a la tabla ("índice de cobertura").   */
DROP INDEX IF EXISTS IX_readings_clean_feeder_ts ON stg.readings_clean;
CREATE NONCLUSTERED INDEX IX_readings_clean_feeder_ts
    ON stg.readings_clean (feeder_id, reading_ts DESC)
    INCLUDE (pond_id, voltage_v, current_a, battery_pct, is_feeding);
GO

SET STATISTICS IO ON;

-- A) ROW_NUMBER: la más usada y la más fácil de explicar
WITH ranked AS (
    SELECT
        c.feeder_id, c.pond_id, c.reading_ts, c.voltage_v, c.battery_pct, c.is_feeding,
        ROW_NUMBER() OVER (PARTITION BY c.feeder_id ORDER BY c.reading_ts DESC) AS rn
    FROM stg.readings_clean AS c
)
SELECT feeder_id, pond_id, reading_ts, voltage_v, battery_pct, is_feeding
FROM ranked
WHERE rn = 1
ORDER BY feeder_id;

-- B) CROSS APPLY + TOP (1): por cada equipo, busca su última fila.
--    Con el índice de arriba es un seek por equipo; muy eficiente cuando hay
--    pocos equipos y muchas lecturas por equipo.
SELECT f.feeder_id, last_r.pond_id, last_r.reading_ts, last_r.voltage_v,
       last_r.battery_pct, last_r.is_feeding
FROM (SELECT DISTINCT feeder_id FROM stg.readings_clean) AS f
CROSS APPLY (
    SELECT TOP (1) c.pond_id, c.reading_ts, c.voltage_v, c.battery_pct, c.is_feeding
    FROM stg.readings_clean AS c
    WHERE c.feeder_id = f.feeder_id
    ORDER BY c.reading_ts DESC
) AS last_r
ORDER BY f.feeder_id;

-- C) MAX + JOIN: funciona, pero si hay empates en el MAX devuelve varias filas
SELECT c.feeder_id, c.pond_id, c.reading_ts, c.voltage_v, c.battery_pct, c.is_feeding
FROM stg.readings_clean AS c
JOIN (
    SELECT feeder_id, MAX(reading_ts) AS max_ts
    FROM stg.readings_clean
    GROUP BY feeder_id
) AS m
  ON m.feeder_id = c.feeder_id AND m.max_ts = c.reading_ts
ORDER BY c.feeder_id;

SET STATISTICS IO OFF;
GO

/* Uso práctico: alerta de equipos "silenciosos" (sin reportar en los últimos
   30 min respecto a la lectura más reciente del conjunto) y batería baja.   */
DECLARE @as_of datetime2(0) = (SELECT MAX(reading_ts) FROM stg.readings_clean);

WITH last_reading AS (
    SELECT feeder_id, pond_id, reading_ts, battery_pct,
           ROW_NUMBER() OVER (PARTITION BY feeder_id ORDER BY reading_ts DESC) AS rn
    FROM stg.readings_clean
)
SELECT feeder_id, pond_id, reading_ts, battery_pct,
       DATEDIFF(MINUTE, reading_ts, @as_of) AS minutes_silent
FROM last_reading
WHERE rn = 1
  AND (DATEDIFF(MINUTE, reading_ts, @as_of) > 30 OR battery_pct < 20)
ORDER BY minutes_silent DESC;
