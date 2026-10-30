/* =============================================================================
   Ejercicios — Semana 1 (resolver SIN mirar soluciones en internet primero)
   -----------------------------------------------------------------------------
   Requisito: haber ejecutado sql/00 … sql/07.
   Guarda cada solución en sql/exercises/solutions/exNN_<nombre>.sql y haz
   un commit por ejercicio (practicas Git a la vez).

   Para cada ejercicio, escribe también en un comentario: "¿Cómo lo explicaría
   en una entrevista en 2 frases?"
   ========================================================================== */
USE IoTLakehouse;
GO

/* ---------------------------------------------------------------------------
   EJ 1 — Huecos en la transmisión  (LAG)
   Para cada equipo, encuentra los intervalos donde pasaron MÁS de 5 minutos
   entre una lectura y la siguiente (usa stg.readings_clean.reading_ts).
   Salida: feeder_id, prev_ts, reading_ts, gap_minutes.
   Después: top 10 equipos con más minutos perdidos en total.
   Pista: LAG(reading_ts) OVER (PARTITION BY ... ORDER BY ...)
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 2 — Minutos de alimentación por piscina y día  (estrella + GROUP BY)
   Usando dw.fact_readings, dw.dim_pond y dw.dim_date: minutos alimentando
   por piscina y día (cada lectura con is_feeding = 1 son 5 min) y energía total.
   Pista: is_feeding es bit; SUM no acepta bit directamente.
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 3 — Top 3 piscinas por energía en cada granja  (DENSE_RANK)
   Pregunta extra: ¿qué cambia si usas ROW_NUMBER, RANK o DENSE_RANK cuando
   dos piscinas empatan? Escribe la respuesta en un comentario.
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 4 — Media móvil de batería de 1 hora  (marco de ventana)
   Para un equipo (p. ej. 'FD-0001'), media de battery_pct de las últimas 12
   lecturas (incluida la actual).
   Pista: AVG(...) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ? PRECEDING AND CURRENT ROW)
   Pregunta extra: ¿por qué ROWS y no RANGE? ¿Qué pasa con los huecos del EJ 1?
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 5 — Sesiones de alimentación  (gaps and islands — muy de entrevista)
   Una "sesión" = lecturas consecutivas de un equipo con is_feeding = 1.
   Para cada sesión: feeder_id, session_start, session_end, duration_min.
   Pista: la diferencia entre dos ROW_NUMBER() (uno sobre todas las lecturas
   del equipo y otro solo sobre las de is_feeding = 1) es constante dentro de
   una misma isla.
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 6 — Consulta point-in-time sobre la SCD2
   Elige un equipo que cambió de firmware (ver final de 06_...sql).
   a) ¿Qué firmware tenía el 2026-10-01 a las 12:00?
   b) Cuenta sus lecturas en fact_readings por firmware_version.
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 7 — SCD tipo 1 vs tipo 2
   Escribe un MERGE para dw.dim_pond como SCD tipo 1 (si cambia area_ha,
   se sobrescribe). Pruébalo cambiando a mano un valor en stg.ponds.
   ¿Cuándo elegirías tipo 1 y cuándo tipo 2? (comentario)
   ------------------------------------------------------------------------- */


/* ---------------------------------------------------------------------------
   EJ 8 — Índices y planes de ejecución
   a) Ejecuta la consulta del EJ 1 con SET STATISTICS IO ON, anota los
      logical reads.
   b) Crea un índice que la mejore, vuelve a medir y anota la diferencia.
   c) Explica "index seek" vs "index scan" y qué es un índice de cobertura.
   ------------------------------------------------------------------------- */
