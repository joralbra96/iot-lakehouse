/* ---------------------------------------------------------------------------
   EJ 6 — Consulta point-in-time sobre la SCD2
   Elige un equipo que cambió de firmware (ver final de 06_...sql).
   a) ¿Qué firmware tenía el 2026-10-01 a las 12:00?
   b) Cuenta sus lecturas en fact_readings por firmware_version.
   ------------------------------------------------------------------------- */

-- eligo el FD-0003 cambio de 1.4.2 a 1.6.0

SELECT TOP(1) * FROM dw.fact_readings;

USE IoTLakehouse;
GO


-- a
DECLARE @date_consult datetime2(0) = CONVERT(datetime2(0), '2026-10-01 12:00');

SELECT f.feeder_id, f.model, f.firmware_version
FROM dw.dim_feeder as f
WHERE  @date_consult >= f.valid_from and @date_consult < f.valid_to and f.feeder_id = 'FD-0003';

-- b
SELECT f.feeder_id, COUNT(r.feeder_key) as n_readings, f.firmware_version
FROM dw.fact_readings as r
JOIN dw.dim_feeder as f ON f.feeder_key = r.feeder_key
WHERE f.firmware_version = '1.4.2' and f.feeder_id = 'FD-0003'
GROUP BY f.feeder_id, f.firmware_version


