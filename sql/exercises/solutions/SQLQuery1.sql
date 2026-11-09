/* ---------------------------------------------------------------------------
   EJ 1 — Huecos en la transmisión  (LAG)
   Para cada equipo, encuentra los intervalos donde pasaron MÁS de 5 minutos
   entre una lectura y la siguiente (usa stg.readings_clean.reading_ts).
   Salida: feeder_id, prev_ts, reading_ts, gap_minutes.
   Después: top 10 equipos con más minutos perdidos en total.
   Pista: LAG(reading_ts) OVER (PARTITION BY ... ORDER BY ...)
   ------------------------------------------------------------------------- */

   use IoTLakehouse;

