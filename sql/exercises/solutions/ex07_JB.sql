/* ---------------------------------------------------------------------------
   EJ 7 — SCD tipo 1 vs tipo 2
   Escribe un MERGE para dw.dim_pond como SCD tipo 1 (si cambia area_ha,
   se sobrescribe). Pruébalo cambiando a mano un valor en stg.ponds.
   ¿Cuándo elegirías tipo 1 y cuándo tipo 2? (comentario)
   ------------------------------------------------------------------------- */


USE IoTLakehouse;
GO

SELECT * from stg.ponds

DROP TABLE IF EXISTS stg.ponds_master;
SELECT TOP(0) * INTO stg.ponds_master FROM stg.ponds;
GO

CREATE OR ALTER PROCEDURE dw.merge_ponds
AS
BEGIN
	SET NOCOUNT ON
	SET XACT_ABORT ON
	
	BEGIN TRANSACTION
	MERGE dw.dim_pond WITH (HOLDLOCK) as tgt
	USING stg.ponds_master as src
		ON src.pond_id = tgt.pond_id
	
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (pond_id, farm_id, farm_name, area_ha, species)
		VALUES (src.pond_id, src.farm_id, src.farm_name, src.area_ha, src.species)
	WHEN MATCHED AND EXISTS (
			SELECT src.farm_id, src.farm_name, src.area_ha, src.species
			EXCEPT
			SELECT tgt.farm_id, tgt.farm_name, tgt.area_ha, tgt.species
			) THEN
		UPDATE SET tgt.farm_id = src.farm_id, 
				tgt.farm_name = src.farm_name, 
				tgt.area_ha = src.area_ha, 
				tgt.species = src.species
		OUTPUT $action, inserted.pond_id, deleted.area_ha AS old_area_ha, inserted.area_ha AS new_area_ha;

	COMMIT TRANSACTION
END;
GO

TRUNCATE TABLE stg.ponds_master;
INSERT INTO stg.ponds_master SELECT * from stg.ponds;
EXEC dw.merge_ponds;
GO

UPDATE stg.ponds_master SET area_ha = 9.8 WHERE pond_id = 'FARM01-P01';

EXEC dw.merge_ponds;

SELECT * FROM dw.dim_pond WHERE pond_id = 'FARM01-P01';

-- Tipo 1 vs tipo 2: la decisión es por columna y depende de si el valor anterior
-- fue verdad y afecta el análisis de hechos pasados.
-- · Tipo 1 (sobrescribir): correcciones de errores (área mal digitada) o atributos
--   descriptivos que no influyen en métricas (nombre de la granja).
-- · Tipo 2 (historizar): cambios reales que afectan métricas. Ej.: firmware, que
--   cambia consumo y batería, o el área si la piscina se amplía (energía por hectárea).
-- Aquí uso tipo 1 para dim_pond asumiendo que los cambios son correcciones; si el
-- área pudiera cambiar de verdad, la trataría como tipo 2.