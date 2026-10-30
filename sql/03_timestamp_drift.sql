/* =============================================================================
   03 — Deriva de reloj: detectar, marcar y corregir timestamps
   -----------------------------------------------------------------------------
   lag_s = received_at - device_ts
     · Equipo sano: lag_s ≈ latencia de red (1–30 s; a veces hasta 10 min).
     · Reloj atrasado/adelantado: lag_s crece hora a hora (+/- minutos).
     · Reinicio de RTC: device_ts ≈ 2000-01-01 → lag_s enorme.

   Idea de la corrección:
     Una fila suelta con lag alto puede ser solo un mensaje demorado. Lo que
     delata un reloj con deriva es que TODAS sus lecturas de esa hora tengan
     lag alto. Por eso se usa la MEDIANA del lag por equipo y hora (robusta
     frente a outliers) como estimación del desfase del reloj:

        reading_ts = redondear_a_5_min(device_ts + mediana_lag)

   Para reinicios de RTC device_ts no sirve: se usa received_at truncado a 5 min.
   ========================================================================== */
USE IoTLakehouse;
GO

-- Exploración: equipos con mayor desfase mediano (candidatos a reloj con deriva)
WITH lagged AS (
    SELECT feeder_id,
           CAST(received_at AS date)                   AS day_received,
           DATEDIFF(SECOND, device_ts, received_at)    AS lag_s
    FROM stg.readings_dedup
    WHERE device_ts >= '2020-01-01'
)
SELECT DISTINCT TOP (15)
    feeder_id,
    day_received,
    -- PERCENTILE_CONT como función de ventana: solo admite PARTITION BY
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lag_s)
        OVER (PARTITION BY feeder_id, day_received)  AS median_lag_s,
    MAX(lag_s) OVER (PARTITION BY feeder_id, day_received) AS max_lag_s
FROM lagged
ORDER BY max_lag_s DESC;
GO

/* --- Tabla limpia ------------------------------------------------------------
   CROSS APPLY (VALUES ...) se usa para "nombrar" expresiones calculadas y
   reutilizarlas sin repetir la fórmula (SQL no permite usar un alias del
   SELECT en el mismo SELECT).

   Redondeo a 5 minutos sin DATE_BUCKET (compatible con SQL Server 2019):
     segundos desde una época fija → dividir entre 300 → redondear → volver.
   En SQL Server 2022: DATE_BUCKET(MINUTE, 5, ts) trunca a 5 min.           */
DROP TABLE IF EXISTS stg.readings_clean;

WITH measured AS (
    SELECT
        d.*,
        DATEDIFF(SECOND, d.device_ts, d.received_at)          AS lag_s,
        CASE WHEN d.device_ts < '2020-01-01' THEN 1 ELSE 0 END AS is_rtc_reset
    FROM stg.readings_dedup AS d
),
with_median AS (
    SELECT
        m.*,
        -- Las filas con RTC reiniciado se excluyen de la mediana (NULL se ignora)
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN m.is_rtc_reset = 0 THEN m.lag_s END)
            OVER (PARTITION BY m.feeder_id,
                               CAST(m.received_at AS date),
                               DATEPART(HOUR, m.received_at)) AS median_lag_s
    FROM measured AS m
),
corrected AS (
    SELECT
        w.*,
        x.reading_ts,
        CASE
            WHEN w.is_rtc_reset = 1          THEN 'RTC_RESET'
            WHEN ABS(w.median_lag_s) > 60    THEN 'DRIFT'   -- reloj desfasado > 1 min
            WHEN w.lag_s > 120               THEN 'LATE'    -- reloj bien, mensaje demorado
            ELSE 'OK'
        END AS ts_quality,
        -- Si la corrección colisiona con otra lectura del mismo equipo, gana la "sana"
        ROW_NUMBER() OVER (
            PARTITION BY w.feeder_id, x.reading_ts
            ORDER BY w.is_rtc_reset, w.received_at
        ) AS rn
    FROM with_median AS w
    CROSS APPLY (VALUES (CAST('2000-01-01' AS datetime2(0)))) AS e(epoch)
    CROSS APPLY (VALUES (
        CASE
            WHEN w.is_rtc_reset = 1 OR w.median_lag_s IS NULL
                -- truncar received_at a 5 min
                THEN DATEADD(MINUTE, (DATEDIFF(MINUTE, e.epoch, w.received_at) / 5) * 5, e.epoch)
            -- redondear (device_ts + desfase estimado) a 5 min
            ELSE DATEADD(SECOND,
                         CAST(ROUND(DATEDIFF(SECOND, e.epoch,
                                             DATEADD(SECOND, CAST(w.median_lag_s AS int), w.device_ts))
                                    / 300.0, 0) AS int) * 300,
                         e.epoch)
        END
    )) AS x(reading_ts)
)
SELECT
    row_id, message_id, farm_id, pond_id, feeder_id,
    device_ts, received_at, reading_ts,
    lag_s, CAST(median_lag_s AS int) AS median_lag_s, ts_quality,
    voltage_v, current_a, battery_pct, is_feeding
INTO stg.readings_clean
FROM corrected
WHERE rn = 1;
GO

-- Resumen por calidad de timestamp
SELECT ts_quality, COUNT(*) AS readings, COUNT(DISTINCT feeder_id) AS feeders
FROM stg.readings_clean
GROUP BY ts_quality
ORDER BY readings DESC;

-- Antes / después para un equipo con deriva
SELECT TOP (20) feeder_id, device_ts, received_at, reading_ts, lag_s, median_lag_s, ts_quality
FROM stg.readings_clean
WHERE ts_quality = 'DRIFT'
ORDER BY ABS(median_lag_s) DESC;

-- Verificación: reading_ts siempre cae en múltiplos de 5 min y es único por equipo
SELECT
    SUM(CASE WHEN DATEPART(MINUTE, reading_ts) % 5 <> 0
               OR DATEPART(SECOND, reading_ts) <> 0 THEN 1 ELSE 0 END) AS not_aligned,
    COUNT(*) - COUNT(DISTINCT CONCAT(feeder_id, '|', reading_ts))       AS duplicates
FROM stg.readings_clean;   -- esperado: 0 y 0
