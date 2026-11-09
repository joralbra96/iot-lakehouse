/* =============================================================================
   05 — Modelo estrella (DDL) + carga de dim_date y dim_pond
   -----------------------------------------------------------------------------
                      dim_date
                         │
        dim_pond ── fact_readings ── dim_feeder (SCD tipo 2)

   Grano de la tabla de hechos: UNA lectura de UN alimentador cada 5 minutos.
   Claves sustitutas (pond_key, feeder_key): enteros propios del warehouse,
   independientes de los IDs del sistema origen. Son imprescindibles en SCD2,
   donde un mismo feeder_id tiene varias versiones (varias filas).

   Ejecutar 05 → 06 → 07 juntos (05 borra y recrea el modelo).
   ========================================================================== */
USE IoTLakehouse;
GO

DROP TABLE IF EXISTS dw.fact_readings;   -- primero la tabla de hechos (tiene las FK)
DROP TABLE IF EXISTS dw.dim_feeder;
DROP TABLE IF EXISTS dw.dim_pond;
DROP TABLE IF EXISTS dw.dim_date;
GO

CREATE TABLE dw.dim_date (
    date_key     int          NOT NULL CONSTRAINT PK_dim_date PRIMARY KEY,  -- yyyymmdd
    full_date    date         NOT NULL,
    [year]       smallint     NOT NULL,
    [month]      tinyint      NOT NULL,
    month_name   nvarchar(20) NOT NULL,
    [day]        tinyint      NOT NULL,
    iso_week     tinyint      NOT NULL,
    day_of_week  tinyint      NOT NULL,   -- 1 = lunes ... 7 = domingo
    day_name     nvarchar(20) NOT NULL,
    is_weekend   bit          NOT NULL
);

CREATE TABLE dw.dim_pond (
    pond_key  int IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_pond PRIMARY KEY,
    pond_id   varchar(20)  NOT NULL CONSTRAINT UQ_dim_pond_pond_id UNIQUE,
    farm_id   varchar(20)  NOT NULL,
    farm_name nvarchar(50) NOT NULL,
    area_ha   decimal(4,1) NOT NULL,
    species   nvarchar(50) NOT NULL
);

CREATE TABLE dw.dim_feeder (
    feeder_key       int IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_feeder PRIMARY KEY,
    feeder_id        varchar(20)  NOT NULL,          -- clave de negocio (natural)
    pond_id          varchar(20)  NOT NULL,
    farm_id          varchar(20)  NOT NULL,
    model            varchar(20)  NOT NULL,
    firmware_version varchar(20)  NOT NULL,
    install_date     date         NOT NULL,
    -- Columnas SCD2: intervalo de vigencia [valid_from, valid_to)
    valid_from       datetime2(0) NOT NULL,
    valid_to         datetime2(0) NOT NULL CONSTRAINT DF_dim_feeder_valid_to DEFAULT ('9999-12-31'),
    is_current       bit          NOT NULL CONSTRAINT DF_dim_feeder_is_current DEFAULT (1)
);

-- Índice FILTRADO único: garantiza como máximo UNA versión vigente por equipo
CREATE UNIQUE NONCLUSTERED INDEX UX_dim_feeder_current
    ON dw.dim_feeder (feeder_id) WHERE is_current = 1;

-- Soporta el join "point-in-time" de la carga de hechos (script 07)
CREATE NONCLUSTERED INDEX IX_dim_feeder_validity
    ON dw.dim_feeder (feeder_id, valid_from, valid_to);

CREATE TABLE dw.fact_readings (
    date_key    int          NOT NULL CONSTRAINT FK_fact_readings_date   REFERENCES dw.dim_date (date_key),
    feeder_key  int          NOT NULL CONSTRAINT FK_fact_readings_feeder REFERENCES dw.dim_feeder (feeder_key),
    pond_key    int          NOT NULL CONSTRAINT FK_fact_readings_pond   REFERENCES dw.dim_pond (pond_key),
    reading_ts  datetime2(0) NOT NULL,
    voltage_v   decimal(5,2) NOT NULL,
    current_a   decimal(6,3) NOT NULL,
    battery_pct decimal(4,1) NOT NULL,
    is_feeding  bit          NOT NULL,
    ts_quality  varchar(10)  NOT NULL,
    -- Columna calculada persistida: energía del intervalo de 5 min (Wh)
    energy_wh   AS CAST(voltage_v * current_a * 5 / 60.0 AS decimal(9,3)) PERSISTED,
    -- Índice clustered con la fecha primero: las consultas de un warehouse
    -- casi siempre filtran por rango de fechas.
    CONSTRAINT PK_fact_readings PRIMARY KEY CLUSTERED (date_key, feeder_key, reading_ts)
);
/* Con cientos de millones de filas lo habitual sería un CLUSTERED COLUMNSTORE
   INDEX (almacenamiento por columnas, comprimido, ideal para agregaciones):
       CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_readings ON dw.fact_readings;
   (requiere declarar la PK como NONCLUSTERED). Synapse dedicated SQL pool lo
   usa por defecto.                                                          */
GO

/* --- dim_date con un CTE recursivo -------------------------------------------
   El CTE se llama a sí mismo: ancla (primer día) + paso recursivo (+1 día).
   MAXRECURSION 0 quita el límite por defecto de 100 iteraciones.
   day_of_week se calcula de forma independiente de SET DATEFIRST.            */
WITH dates AS (
    SELECT CAST('2026-01-01' AS date) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d) FROM dates WHERE d < '2027-12-31'
)
INSERT INTO dw.dim_date (date_key, full_date, [year], [month], month_name, [day],
                         iso_week, day_of_week, day_name, is_weekend)
SELECT
    CONVERT(int, CONVERT(char(8), d, 112)),
    d,
    YEAR(d),
    MONTH(d),
    DATENAME(MONTH, d),
    DAY(d),
    DATEPART(ISO_WEEK, d),
    w.dow,
    DATENAME(WEEKDAY, d),
    CASE WHEN w.dow IN (6, 7) THEN 1 ELSE 0 END
FROM dates
CROSS APPLY (VALUES ((DATEPART(WEEKDAY, d) + @@DATEFIRST - 2) % 7 + 1)) AS w(dow)
OPTION (MAXRECURSION 0);

INSERT INTO dw.dim_pond (pond_id, farm_id, farm_name, area_ha, species)
SELECT pond_id, farm_id, farm_name, area_ha, species
FROM stg.ponds;
GO

SELECT (SELECT COUNT(*) FROM dw.dim_date) AS dim_date_rows,
       (SELECT COUNT(*) FROM dw.dim_pond) AS dim_pond_rows;   -- 730 y 60

SELECT * from stg.feeder_snapshot_v2;