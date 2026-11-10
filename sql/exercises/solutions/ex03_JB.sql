/* ---------------------------------------------------------------------------
   EJ 3 — Top 3 piscinas por energía en cada granja  (DENSE_RANK)
   Pregunta extra: ¿qué cambia si usas ROW_NUMBER, RANK o DENSE_RANK cuando
   dos piscinas empatan? Escribe la respuesta en un comentario.
   ------------------------------------------------------------------------- */

   USE IoTLakehouse;

   SELECT * from dw.fact_readings;
   SELECT * from dw.dim_pond;

WITH energy AS (
    SELECT f.pond_key, SUM(f.energy_wh) AS total_energy_wh
    FROM dw.fact_readings AS f
    GROUP BY f.pond_key
),
ranked AS (
    SELECT p.farm_id, p.pond_id, e.total_energy_wh,
           DENSE_RANK() OVER (PARTITION BY p.farm_id
                              ORDER BY e.total_energy_wh DESC) AS energy_rank
    FROM energy AS e
    JOIN dw.dim_pond AS p ON p.pond_key = e.pond_key
)
SELECT farm_id, pond_id, total_energy_wh, energy_rank
FROM ranked
WHERE energy_rank <= 3
ORDER BY farm_id, energy_rank;

-- EN este caso en especifico no hubiera habido cambios, puesto que la diferencia radica en los casos en los que se encuente empate.
-- ROW_NUMBER sigue numerando aunque aparezan empates
-- RANK en el empate deja de contar, pero al terminar los empates salta al numero de la fila correspondiente.
-- DENSE RANK en cambio, en el empate dea de contar, pero al terminar los empates no salta sino que continua desde el número que se quedo.