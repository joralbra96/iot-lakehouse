/* ---------------------------------------------------------------------------
   EJ 2 — Minutos de alimentación por piscina y día  (estrella + GROUP BY)
   Usando dw.fact_readings, dw.dim_pond y dw.dim_date: minutos alimentando
   por piscina y día (cada lectura con is_feeding = 1 son 5 min) y energía total.
   Pista: is_feeding es bit; SUM no acepta bit directamente.
   ------------------------------------------------------------------------- */


  USE IoTLakehouse;
  GO

SELECT * FROM dw.dim_pond;

SELECT
    p.pond_id,
    d.full_date,
    SUM(CAST(f.is_feeding AS int)) * 5 AS feeding_minutes,
    SUM(f.energy_wh)                   AS energy_wh
FROM dw.fact_readings AS f
JOIN dw.dim_pond AS p ON p.pond_key = f.pond_key
JOIN dw.dim_date AS d ON d.date_key = f.date_key
GROUP BY p.pond_id, d.full_date
ORDER BY p.pond_id, d.full_date;