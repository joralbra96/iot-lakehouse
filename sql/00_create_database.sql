/* =============================================================================
   00 — Base de datos y esquemas
   -----------------------------------------------------------------------------
   stg = staging (datos tal cual llegan + limpieza intermedia)
   dw  = data warehouse (modelo estrella listo para análisis)
   ========================================================================== */
IF DB_ID(N'IoTLakehouse') IS NULL
    CREATE DATABASE IoTLakehouse;
GO

USE IoTLakehouse;
GO

-- CREATE SCHEMA debe ser la única sentencia de su lote, por eso se usa EXEC.
IF SCHEMA_ID(N'stg') IS NULL EXEC (N'CREATE SCHEMA stg');
IF SCHEMA_ID(N'dw')  IS NULL EXEC (N'CREATE SCHEMA dw');
GO
