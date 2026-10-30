/* =============================================================================
   01 — Carga de los CSV a staging
   -----------------------------------------------------------------------------
   Patrón: aterrizar todo como TEXTO (stg.readings_raw) y convertir tipos en un
   segundo paso con TRY_CONVERT. Así una fila mal formada no rompe la carga
   completa: queda en NULL y se puede contar/reportar.

   ⚠ Ajustar @data_path. La cuenta del servicio de SQL Server debe poder leer
     esa carpeta (BULK INSERT lee el archivo desde el SERVIDOR, no desde SSMS).
   ========================================================================== */
USE IoTLakehouse;
GO

DROP TABLE IF EXISTS stg.readings_raw;
CREATE TABLE stg.readings_raw (
    message_id  varchar(50),
    farm_id     varchar(20),
    pond_id     varchar(20),
    feeder_id   varchar(20),
    device_ts   varchar(30),
    received_at varchar(30),
    voltage_v   varchar(20),
    current_a   varchar(20),
    battery_pct varchar(20),
    is_feeding  varchar(5)
);

DROP TABLE IF EXISTS stg.ponds;
CREATE TABLE stg.ponds (
    pond_id   varchar(20)  NOT NULL,
    farm_id   varchar(20)  NOT NULL,
    farm_name nvarchar(50) NOT NULL,
    area_ha   decimal(4,1) NOT NULL,
    species   nvarchar(50) NOT NULL
);

DROP TABLE IF EXISTS stg.feeder_snapshot_v1;
CREATE TABLE stg.feeder_snapshot_v1 (
    feeder_id        varchar(20) NOT NULL,
    pond_id          varchar(20) NOT NULL,
    farm_id          varchar(20) NOT NULL,
    model            varchar(20) NOT NULL,
    firmware_version varchar(20) NOT NULL,
    install_date     date        NOT NULL
);

DROP TABLE IF EXISTS stg.feeder_snapshot_v2;
SELECT TOP (0) * INTO stg.feeder_snapshot_v2 FROM stg.feeder_snapshot_v1;
GO

/* --- BULK INSERT con SQL dinámico ------------------------------------------
   BULK INSERT no acepta variables en la ruta, por eso se arma la sentencia
   como texto y se ejecuta con sp_executesql. (En código que reciba rutas de
   usuarios esto sería un riesgo de SQL injection; aquí la ruta es local.)    */
DECLARE @data_path  nvarchar(260) = N'C:\Users\jorge\Documents\Repositories\iot-lakehouse\data\raw\';
DECLARE @start_date date = '2026-10-01';
DECLARE @days       int  = 2;

DECLARE @opts nvarchar(300) = N' WITH (FORMAT = ''CSV'', FIRSTROW = 2, FIELDTERMINATOR = '','',
                                       ROWTERMINATOR = ''0x0a'', CODEPAGE = ''65001'', TABLOCK);';
DECLARE @i int = 0, @sql nvarchar(max);

WHILE @i < @days
BEGIN
    SET @sql = N'BULK INSERT stg.readings_raw FROM ''' + @data_path + N'readings_'
             + CONVERT(nchar(10), DATEADD(DAY, @i, @start_date), 23) + N'.csv''' + @opts;
    PRINT @sql;
    EXEC sys.sp_executesql @sql;
    SET @i += 1;
END;

SET @sql = N'BULK INSERT stg.ponds FROM ''' + @data_path + N'ponds.csv''' + @opts;
EXEC sys.sp_executesql @sql;
SET @sql = N'BULK INSERT stg.feeder_snapshot_v1 FROM ''' + @data_path + N'feeders_snapshot_v1.csv''' + @opts;
EXEC sys.sp_executesql @sql;
SET @sql = N'BULK INSERT stg.feeder_snapshot_v2 FROM ''' + @data_path + N'feeders_snapshot_v2.csv''' + @opts;
EXEC sys.sp_executesql @sql;
GO

/* --- Tipado ----------------------------------------------------------------
   Estilo 126 = ISO 8601 (yyyy-mm-ddThh:mi:ss).
   IDENTITY() en un SELECT ... INTO crea una columna autoincremental: sirve
   como "orden de carga" y para distinguir duplicados exactos.               */
DROP TABLE IF EXISTS stg.readings;

SELECT
    IDENTITY(bigint, 1, 1)                      AS row_id,
    TRY_CONVERT(uniqueidentifier, message_id)   AS message_id,
    farm_id,
    pond_id,
    feeder_id,
    TRY_CONVERT(datetime2(0), device_ts, 126)   AS device_ts,
    TRY_CONVERT(datetime2(0), received_at, 126) AS received_at,
    TRY_CONVERT(decimal(5,2), voltage_v)        AS voltage_v,
    TRY_CONVERT(decimal(6,3), current_a)        AS current_a,
    TRY_CONVERT(decimal(4,1), battery_pct)      AS battery_pct,
    TRY_CONVERT(bit, is_feeding)                AS is_feeding
INTO stg.readings
FROM stg.readings_raw;
GO

-- Control de calidad: filas cuya conversión falló (esperado: todo en 0)
SELECT
    COUNT(*)                                                AS total_rows,
    SUM(CASE WHEN message_id  IS NULL THEN 1 ELSE 0 END)    AS bad_message_id,
    SUM(CASE WHEN device_ts   IS NULL THEN 1 ELSE 0 END)    AS bad_device_ts,
    SUM(CASE WHEN received_at IS NULL THEN 1 ELSE 0 END)    AS bad_received_at,
    SUM(CASE WHEN voltage_v   IS NULL OR current_a IS NULL
               OR battery_pct IS NULL THEN 1 ELSE 0 END)    AS bad_measures
FROM stg.readings;
