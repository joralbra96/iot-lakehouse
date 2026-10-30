/* =============================================================================
   07 — Carga de fact_readings con join "point-in-time" a dim_feeder (SCD2)
   -----------------------------------------------------------------------------
   Cada lectura debe apuntar a la VERSIÓN del equipo vigente en el momento de
   la lectura:  reading_ts >= valid_from AND reading_ts < valid_to
   Así, una lectura del 1-oct queda con el firmware viejo y una del 2-oct con
   el nuevo, aunque sean del mismo feeder_id.

   La piscina se toma de la versión del maestro (fuente de verdad de la
   ubicación), no del payload del equipo.
   ========================================================================== */
USE IoTLakehouse;
GO

-- 1) Antes de cargar: ¿hay lecturas sin versión de equipo? ("late arriving dimension")
--    En producción se suelen mandar a un "miembro desconocido" (feeder_key = -1)
--    o a una tabla de cuarentena en vez de perderlas en un INNER JOIN.
SELECT COUNT(*) AS orphan_readings
FROM stg.readings_clean AS c
LEFT JOIN dw.dim_feeder AS f
       ON f.feeder_id = c.feeder_id
      AND c.reading_ts >= f.valid_from
      AND c.reading_ts <  f.valid_to
WHERE f.feeder_key IS NULL;   -- esperado: 0
GO

-- 2) Carga (TRUNCATE + INSERT = recarga completa e idempotente)
TRUNCATE TABLE dw.fact_readings;

INSERT INTO dw.fact_readings (date_key, feeder_key, pond_key, reading_ts,
                              voltage_v, current_a, battery_pct, is_feeding, ts_quality)
SELECT
    CONVERT(int, CONVERT(char(8), c.reading_ts, 112)) AS date_key,
    f.feeder_key,
    p.pond_key,
    c.reading_ts,
    c.voltage_v,
    c.current_a,
    c.battery_pct,
    c.is_feeding,
    c.ts_quality
FROM stg.readings_clean AS c
JOIN dw.dim_feeder AS f
  ON f.feeder_id = c.feeder_id
 AND c.reading_ts >= f.valid_from
 AND c.reading_ts <  f.valid_to
JOIN dw.dim_pond AS p
  ON p.pond_id = f.pond_id;
GO

SELECT (SELECT COUNT(*) FROM stg.readings_clean) AS clean_rows,
       (SELECT COUNT(*) FROM dw.fact_readings)   AS fact_rows;   -- deben coincidir
GO

-- 3) Consultas de ejemplo sobre la estrella

-- Energía y minutos de alimentación por granja y día
SELECT d.full_date, p.farm_name,
       SUM(f.energy_wh)                     AS energy_wh,
       SUM(CAST(f.is_feeding AS int)) * 5   AS feeding_minutes
FROM dw.fact_readings AS f
JOIN dw.dim_date AS d ON d.date_key = f.date_key
JOIN dw.dim_pond AS p ON p.pond_key = f.pond_key
GROUP BY d.full_date, p.farm_name
ORDER BY d.full_date, p.farm_name;

-- ¿Consume distinto el firmware nuevo? Solo responde bien gracias a SCD2:
-- cada lectura queda asociada al firmware que tenía en ese momento.
SELECT df.firmware_version,
       COUNT(*)                         AS readings,
       AVG(CASE WHEN f.is_feeding = 1 THEN f.current_a END) AS avg_current_feeding_a,
       AVG(f.battery_pct)               AS avg_battery_pct
FROM dw.fact_readings AS f
JOIN dw.dim_feeder AS df ON df.feeder_key = f.feeder_key
GROUP BY df.firmware_version
ORDER BY df.firmware_version;
