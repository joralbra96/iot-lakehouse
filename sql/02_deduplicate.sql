/* =============================================================================
   02 — Deduplicación con ROW_NUMBER()
   -----------------------------------------------------------------------------
   Dos tipos de duplicado:
     a) Exactos: el mismo mensaje llegó dos veces (mismo message_id).
     b) Reenvíos: el equipo volvió a mandar la MISMA lectura como un mensaje
        nuevo (otro message_id, received_at más tarde). Clave de negocio:
        (feeder_id, device_ts).
   DISTINCT solo resuelve (a). ROW_NUMBER() resuelve ambos.
   ========================================================================== */
USE IoTLakehouse;
GO

-- a) Duplicados exactos
SELECT TOP (10) message_id, COUNT(*) AS copies
FROM stg.readings
GROUP BY message_id
HAVING COUNT(*) > 1
ORDER BY copies DESC;

-- Cuantos valores unicos hay
Select count(distinct message_id) from stg.readings;
-- Cuantos valores duplicados hay
select count(*) - count(distinct message_id) from stg.readings;

Select count(*)
from(
    Select message_id, Count(message_id) as copies
    from stg.readings
    GROUP BY message_id
    HAVING Count(message_id) > 1
) as sub;


-- b) Reenvíos por clave de negocio
SELECT TOP (10)
    feeder_id, device_ts,
    COUNT(*)         AS copies,
    MIN(received_at) AS first_received,
    MAX(received_at) AS last_received
FROM stg.readings
GROUP BY feeder_id, device_ts
HAVING COUNT(*) > 1
ORDER BY copies DESC;
GO

select * from stg.readings
where feeder_id = 'FD-0108' and device_ts = '2026-10-02 17:50:00';

/* --- ¿En qué horas aparecen los duplicados? -------------------------------


   CTE (WITH ...): una consulta con nombre que solo vive durante la sentencia
   siguiente. Hace legible una consulta por pasos (como variables en Python).

   ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...):
     - PARTITION BY = "agrupa" sin colapsar filas (a diferencia de GROUP BY)
     - ORDER BY     = dentro de cada grupo, numera 1, 2, 3...
   La fila con rn = 1 es la que nos quedamos; rn > 1 son duplicados.

   Caso borde: tras un reinicio de RTC el equipo reporta desde 2000-01-01,
   así que dos reinicios en días distintos producen el MISMO device_ts.
   Para esas filas se añade el día de recepción a la partición.             */

select TOP(10) * from stg.readings;

-- mi solución




WITH ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.feeder_id,
                         r.device_ts,
                         CASE WHEN r.device_ts < '2020-01-01' THEN CAST(r.received_at AS date) END
            ORDER BY r.received_at, r.row_id      -- nos quedamos con la primera que llegó
        ) AS rn
    FROM stg.readings AS r
)
SELECT
    DATEPART(HOUR, device_ts)                AS hour_of_day,
    SUM(CASE WHEN rn > 1 THEN    1 ELSE 0 END)  AS duplicates,
    COUNT(*)                                 AS total_rows
FROM ranked
GROUP BY DATEPART(HOUR, device_ts)
ORDER BY hour_of_day;
-- Esperado: muchos menos duplicados en 06–08, 11–13 y 16–18 (horas de alimentación).
GO

/* --- Tabla deduplicada -----------------------------------------------------
   En un pipeline se prefiere CREAR una tabla limpia en vez de borrar de la
   original: el paso es repetible (idempotente) y no se pierde la evidencia.  */
DROP TABLE IF EXISTS stg.readings_dedup;

WITH ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.feeder_id,
                         r.device_ts,
                         CASE WHEN r.device_ts < '2020-01-01' THEN CAST(r.received_at AS date) END
            ORDER BY r.received_at, r.row_id
        ) AS rn
    FROM stg.readings AS r
)
SELECT row_id, message_id, farm_id, pond_id, feeder_id, device_ts, received_at,
       voltage_v, current_a, battery_pct, is_feeding
INTO stg.readings_dedup
FROM ranked
WHERE rn = 1;
GO

SELECT count(*) from stg.readings_dedup;

/* Alternativa clásica (destructiva): borrar a través del CTE.
   SQL Server permite DELETE sobre un CTE que referencia una sola tabla.

WITH ranked AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY feeder_id, device_ts
                              ORDER BY received_at, row_id) AS rn
    FROM stg.readings
)
DELETE FROM ranked WHERE rn > 1;
*/

-- Verificación: antes vs después, y que no quede ningún duplicado
SELECT
    (SELECT COUNT(*) FROM stg.readings)       AS rows_before,
    (SELECT COUNT(*) FROM stg.readings_dedup) AS rows_after;

SELECT feeder_id, device_ts, COUNT(*) AS copies
FROM stg.readings_dedup
WHERE device_ts >= '2020-01-01'
GROUP BY feeder_id, device_ts
HAVING COUNT(*) > 1;   -- esperado: 0 filas
