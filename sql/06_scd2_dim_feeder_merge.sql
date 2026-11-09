/* =============================================================================
   06 — dim_feeder como SCD tipo 2 con MERGE
   -----------------------------------------------------------------------------
   SCD = Slowly Changing Dimension (atributos que cambian "de vez en cuando").
     Tipo 1: se sobrescribe el valor (se pierde la historia).
     Tipo 2: se cierra la fila vieja y se inserta una versión nueva
             → se puede saber qué firmware/piscina tenía el equipo en
               cualquier momento del pasado.

   MERGE compara una tabla origen con una destino en UNA sentencia:
     WHEN NOT MATCHED BY TARGET  → equipo nuevo           → INSERT
     WHEN MATCHED AND cambió     → cerrar versión vigente → UPDATE
     WHEN NOT MATCHED BY SOURCE  → equipo dado de baja    → UPDATE (cerrar)

   Un MERGE no puede insertar una fila Y actualizar la misma fila destino en
   la misma pasada, así que la versión nueva de los equipos que cambiaron se
   inserta después, usando lo que MERGE devolvió con OUTPUT.

   Ejecutar después de 05.
   ========================================================================== */
USE IoTLakehouse;
GO

-- Tabla de entrada del proceso: "el maestro tal como está hoy"
DROP TABLE IF EXISTS stg.feeder_master;
SELECT TOP (0) * INTO stg.feeder_master FROM stg.feeder_snapshot_v1;
GO

SELECT * from stg.feeder_master;
GO

CREATE OR ALTER PROCEDURE dw.usp_merge_dim_feeder
    @effective_from datetime2(0)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;   -- ante cualquier error, revierte toda la transacción

    DECLARE @changes TABLE (merge_action nvarchar(10) NOT NULL, feeder_id varchar(20) NOT NULL);

    BEGIN TRANSACTION;

    MERGE dw.dim_feeder WITH (HOLDLOCK) AS tgt   -- HOLDLOCK evita condiciones de carrera
    USING stg.feeder_master AS src
       ON tgt.feeder_id = src.feeder_id
      AND tgt.is_current = 1
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (feeder_id, pond_id, farm_id, model, firmware_version, install_date, valid_from)
        VALUES (src.feeder_id, src.pond_id, src.farm_id, src.model, src.firmware_version,
                src.install_date, @effective_from)
    -- "¿Cambió algún atributo?" con EXISTS + EXCEPT: compara varias columnas a la
    -- vez y trata NULL = NULL como iguales (con <> habría que manejar NULL a mano).
    WHEN MATCHED AND EXISTS (
            SELECT src.pond_id, src.farm_id, src.model, src.firmware_version, src.install_date
            EXCEPT
            SELECT tgt.pond_id, tgt.farm_id, tgt.model, tgt.firmware_version, tgt.install_date
         ) THEN
        UPDATE SET tgt.valid_to = @effective_from,
                   tgt.is_current = 0
    WHEN NOT MATCHED BY SOURCE AND tgt.is_current = 1 THEN
        UPDATE SET tgt.valid_to = @effective_from,
                   tgt.is_current = 0
    OUTPUT $action, inserted.feeder_id INTO @changes (merge_action, feeder_id);
    -- MERGE siempre debe terminar en punto y coma.

    -- Versión nueva para los equipos que cambiaron (los dados de baja no están en src)
    INSERT INTO dw.dim_feeder (feeder_id, pond_id, farm_id, model, firmware_version,
                               install_date, valid_from)
    SELECT s.feeder_id, s.pond_id, s.farm_id, s.model, s.firmware_version,
           s.install_date, @effective_from
    FROM stg.feeder_master AS s
    JOIN @changes AS c
      ON c.feeder_id = s.feeder_id
     AND c.merge_action = N'UPDATE';

    COMMIT TRANSACTION;

    SELECT merge_action, COUNT(*) AS feeders
    FROM @changes
    GROUP BY merge_action;
END;
GO

/* --- Carga inicial: maestro v1 (vigente desde 2026-09-30) ----------------- */
TRUNCATE TABLE stg.feeder_master;
INSERT INTO stg.feeder_master SELECT * FROM stg.feeder_snapshot_v1;
EXEC dw.usp_merge_dim_feeder @effective_from = '2026-09-30T00:00:00';   -- 300 INSERT

/* --- Cambios: maestro v2 (vigente desde 2026-10-02) ------------------------
   Trae actualizaciones de firmware y algunas reubicaciones de piscina.      */
TRUNCATE TABLE stg.feeder_master;
INSERT INTO stg.feeder_master SELECT * FROM stg.feeder_snapshot_v2;
EXEC dw.usp_merge_dim_feeder @effective_from = '2026-10-02T00:00:00';   -- 44 UPDATE con --seed 42

/* --- Idempotencia: volver a aplicar v2 no debe cambiar nada ----------------
   (Correr el mismo paso dos veces y obtener el mismo resultado es clave
   cuando un orquestador reintenta un job que falló a medias.)               */
EXEC dw.usp_merge_dim_feeder @effective_from = '2026-10-02T00:00:00';   -- sin filas
GO

-- Historia de los equipos que cambiaron
SELECT feeder_key, feeder_id, pond_id, firmware_version, valid_from, valid_to, is_current
FROM dw.dim_feeder
WHERE feeder_id IN (SELECT feeder_id FROM dw.dim_feeder GROUP BY feeder_id HAVING COUNT(*) > 1)
ORDER BY feeder_id, valid_from;

-- Comprobaciones: 300 versiones vigentes, ninguna con intervalo inválido
SELECT
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END)          AS current_versions,
    SUM(CASE WHEN valid_to <= valid_from THEN 1 ELSE 0 END)  AS invalid_intervals
FROM dw.dim_feeder;

SELECT 
    feeder_key,
    feeder_id,
    valid_from,
    valid_to,
    is_current,
    -- Identificar el error específico
    CASE 
        WHEN valid_from >= valid_to THEN 'Error: valid_from mayor o igual a valid_to'
        WHEN is_current = 1 AND valid_to <> '9999-12-31 00:00:00' THEN 'Error: Registro activo con valid_to cerrado'
        WHEN is_current = 0 AND valid_to = '9999-12-31 00:00:00' THEN 'Error: Registro inactivo con valid_to abierto'
        ELSE 'Ok'
    END AS tipo_inconsistencia
FROM dw.dim_feeder
WHERE valid_from >= valid_to
   OR (is_current = 1 AND valid_to <> '9999-12-31 00:00:00')
   OR (is_current = 0 AND valid_to = '9999-12-31 00:00:00');

Select count(*) from dw.dim_feeder;