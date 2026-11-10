# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: modelo estrella y agregados
# MAGIC
# MAGIC | Tabla | Equivale a | Técnica |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_date` | `sql/05` | `sequence` + `explode` (en vez del CTE recursivo) |
# MAGIC | `gold.dim_pond` | ejercicio 7 | SCD tipo 1 con `MERGE INTO` |
# MAGIC | `gold.dim_feeder` | `sql/06` | SCD tipo 2 con `MERGE INTO` |
# MAGIC | `gold.fact_readings` | `sql/07` | join *point-in-time* + liquid clustering |
# MAGIC | `gold.agg_pond_daily` | ejercicio 2 | agregado listo para un dashboard |

# COMMAND ----------

# MAGIC %run ./transforms

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Reconstruir Gold")
catalog = dbutils.widgets.get("catalog")
full_refresh = dbutils.widgets.get("full_refresh") == "true"
spark.conf.set("spark.sql.session.timeZone", "UTC")

gold = f"{catalog}.gold"

if full_refresh:
    # Borra también la historia de la SCD2: úsalo solo para empezar de cero
    for table in ["agg_pond_daily", "fact_readings", "dim_feeder", "dim_pond", "dim_date"]:
        spark.sql(f"DROP TABLE IF EXISTS {gold}.{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_date`
# MAGIC `day_of_week`: 1 = lunes … 7 = domingo (`dayofweek` de Spark empieza en domingo, de ahí la fórmula).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.dim_date AS
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d                                       AS full_date,
    year(d)                                 AS year,
    month(d)                                AS month,
    date_format(d, 'MMMM')                  AS month_name,
    day(d)                                  AS day,
    weekofyear(d)                           AS iso_week,
    (dayofweek(d) + 5) % 7 + 1              AS day_of_week,
    date_format(d, 'EEEE')                  AS day_name,
    dayofweek(d) IN (1, 7)                  AS is_weekend
FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_pond` — SCD tipo 1
# MAGIC
# MAGIC `<=>` es la igualdad que trata `NULL = NULL` como verdadero: cumple el papel del truco
# MAGIC `EXISTS (... EXCEPT ...)` de T-SQL para detectar cambios sin falsos positivos por `NULL`.
# MAGIC `GENERATED ALWAYS AS IDENTITY` es el `IDENTITY(1,1)` de Delta (puede tener huecos).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold}.dim_pond (
    pond_key  BIGINT GENERATED ALWAYS AS IDENTITY,
    pond_id   STRING NOT NULL,
    farm_id   STRING,
    farm_name STRING,
    area_ha   DOUBLE,
    species   STRING
)
""")

display(spark.sql(f"""
MERGE INTO {gold}.dim_pond AS tgt
USING {catalog}.bronze.ponds AS src
   ON tgt.pond_id = src.pond_id
WHEN MATCHED AND NOT (tgt.farm_id   <=> src.farm_id
                  AND tgt.farm_name <=> src.farm_name
                  AND tgt.area_ha   <=> src.area_ha
                  AND tgt.species   <=> src.species) THEN
    UPDATE SET farm_id = src.farm_id, farm_name = src.farm_name,
               area_ha = src.area_ha, species = src.species
WHEN NOT MATCHED THEN
    INSERT (pond_id, farm_id, farm_name, area_ha, species)
    VALUES (src.pond_id, src.farm_id, src.farm_name, src.area_ha, src.species)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## `dim_feeder` — SCD tipo 2
# MAGIC
# MAGIC Igual que en T-SQL, un `MERGE` no puede cerrar la versión vigente **e** insertar la nueva para
# MAGIC la misma clave en una sola pasada. En SQL Server lo resolvimos con `OUTPUT` + un `INSERT`
# MAGIC posterior. En Delta se usa el **truco de la unión**: el origen lleva cada equipo cambiado dos
# MAGIC veces, una con `merge_key = feeder_id` (encuentra y **cierra** la versión vigente) y otra con
# MAGIC `merge_key = NULL` (no encuentra nada y se **inserta** como versión nueva).
# MAGIC
# MAGIC Incluye la protección contra cargas fuera de orden que añadiste en `sql/06`.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold}.dim_feeder (
    feeder_key       BIGINT GENERATED ALWAYS AS IDENTITY,
    feeder_id        STRING    NOT NULL,
    pond_id          STRING    NOT NULL,
    farm_id          STRING    NOT NULL,
    model            STRING    NOT NULL,
    firmware_version STRING    NOT NULL,
    install_date     DATE      NOT NULL,
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP NOT NULL,
    is_current       BOOLEAN   NOT NULL
)
""")

CHANGED = """NOT (cur.pond_id          <=> src.pond_id
             AND cur.farm_id          <=> src.farm_id
             AND cur.model            <=> src.model
             AND cur.firmware_version <=> src.firmware_version
             AND cur.install_date     <=> src.install_date)"""


def merge_dim_feeder(snapshot_version: str, effective_from: str):
    later = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {gold}.dim_feeder
        WHERE valid_from > TIMESTAMP'{effective_from}'
    """).first()["n"]
    if later > 0:
        raise ValueError(f"Carga fuera de orden: dim_feeder ya tiene versiones posteriores a {effective_from}")

    (spark.table(f"{catalog}.bronze.feeder_snapshots")
        .filter(F.col("snapshot_version") == snapshot_version)
        .select("feeder_id", "pond_id", "farm_id", "model", "firmware_version", "install_date")
        .createOrReplaceTempView("feeder_updates"))

    return spark.sql(f"""
    MERGE INTO {gold}.dim_feeder AS cur
    USING (
        SELECT feeder_id AS merge_key, * FROM feeder_updates
        UNION ALL
        SELECT NULL AS merge_key, src.*                -- versión nueva de los que cambiaron
        FROM feeder_updates AS src
        JOIN {gold}.dim_feeder AS cur
          ON cur.feeder_id = src.feeder_id AND cur.is_current
        WHERE {CHANGED}
    ) AS src
       ON cur.feeder_id = src.merge_key AND cur.is_current
    WHEN MATCHED AND {CHANGED} THEN
        UPDATE SET valid_to = TIMESTAMP'{effective_from}', is_current = false
    WHEN NOT MATCHED THEN
        INSERT (feeder_id, pond_id, farm_id, model, firmware_version, install_date,
                valid_from, valid_to, is_current)
        VALUES (src.feeder_id, src.pond_id, src.farm_id, src.model, src.firmware_version,
                src.install_date, TIMESTAMP'{effective_from}', TIMESTAMP'9999-12-31 00:00:00', true)
    WHEN NOT MATCHED BY SOURCE AND cur.is_current THEN       -- equipo dado de baja
        UPDATE SET valid_to = TIMESTAMP'{effective_from}', is_current = false
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Se aplican solo los snapshots **pendientes** (posteriores a la última versión cargada). Así el
# MAGIC notebook se puede volver a ejecutar sin chocar con la protección: es como trabaja un pipeline
# MAGIC real, que recuerda hasta dónde llegó (*watermark*).

# COMMAND ----------

snapshots = [("v1", "2026-09-30 00:00:00"), ("v2", "2026-10-02 00:00:00")]

last_loaded = spark.sql(f"SELECT MAX(valid_from) AS m FROM {gold}.dim_feeder").first()["m"]

for version, effective_from in snapshots:
    if last_loaded is not None and effective_from < last_loaded.strftime("%Y-%m-%d %H:%M:%S"):
        print(f"{version}: ya aplicado, se omite")
        continue
    print(f"{version}: aplicando con vigencia desde {effective_from}")
    display(merge_dim_feeder(version, effective_from))

# COMMAND ----------

dim_feeder_checks = spark.sql(f"""
    SELECT
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END)            AS current_versions,
        SUM(CASE WHEN valid_to <= valid_from THEN 1 ELSE 0 END) AS invalid_intervals,
        COUNT(*)                                               AS total_rows
    FROM {gold}.dim_feeder
""").first()
print(dim_feeder_checks)
assert dim_feeder_checks["invalid_intervals"] == 0, "Hay versiones con valid_to <= valid_from"
assert dim_feeder_checks["current_versions"] == 300, "Debe haber exactamente una versión vigente por equipo"

display(spark.sql(f"""
    SELECT feeder_key, feeder_id, pond_id, firmware_version, valid_from, valid_to, is_current
    FROM {gold}.dim_feeder
    WHERE feeder_id IN (SELECT feeder_id FROM {gold}.dim_feeder GROUP BY feeder_id HAVING COUNT(*) > 1)
    ORDER BY feeder_id, valid_from
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## `fact_readings`
# MAGIC
# MAGIC **Liquid clustering** (`CLUSTER BY`) organiza los archivos por `date_key` y `feeder_id`, así
# MAGIC una consulta que filtra por fecha o equipo se salta los archivos que no le sirven
# MAGIC (*data skipping*). Cumple el papel que tenía el índice clustered en SQL Server.

# COMMAND ----------

readings = spark.table(f"{catalog}.silver.readings")
dim_feeder = spark.table(f"{gold}.dim_feeder")
dim_pond = spark.table(f"{gold}.dim_pond")

orphans = readings.join(
    dim_feeder,
    (readings.feeder_id == dim_feeder.feeder_id)
    & (readings.reading_ts >= dim_feeder.valid_from)
    & (readings.reading_ts < dim_feeder.valid_to),
    "left_anti",
).count()
assert orphans == 0, f"{orphans} lecturas sin versión de equipo vigente"

build_fact_readings(readings, dim_feeder, dim_pond).createOrReplaceTempView("fact_readings_new")

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.fact_readings
CLUSTER BY (date_key, feeder_id)
AS SELECT * FROM fact_readings_new
""")

fact_rows = spark.table(f"{gold}.fact_readings").count()
print("silver:", readings.count(), "| fact:", fact_rows)
assert fact_rows == readings.count(), "La tabla de hechos no tiene las mismas filas que Silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## `agg_pond_daily`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.agg_pond_daily AS
SELECT
    d.full_date,
    p.farm_id,
    p.pond_id,
    SUM(CAST(f.is_feeding AS INT)) * 5 AS feeding_minutes,
    ROUND(SUM(f.energy_wh), 1)         AS energy_wh,
    ROUND(AVG(f.battery_pct), 1)       AS avg_battery_pct,
    COUNT(*)                           AS readings
FROM {gold}.fact_readings AS f
JOIN {gold}.dim_pond AS p ON p.pond_key = f.pond_key
JOIN {gold}.dim_date AS d ON d.date_key = f.date_key
GROUP BY d.full_date, p.farm_id, p.pond_id
""")

display(spark.table(f"{gold}.agg_pond_daily").orderBy("full_date", "pond_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## La pregunta que justifica la SCD2

# COMMAND ----------

display(spark.sql(f"""
    SELECT df.firmware_version,
           COUNT(*)                                             AS readings,
           ROUND(AVG(CASE WHEN f.is_feeding THEN f.current_a END), 3) AS avg_current_feeding_a,
           ROUND(AVG(f.battery_pct), 1)                         AS avg_battery_pct
    FROM {gold}.fact_readings AS f
    JOIN {gold}.dim_feeder AS df ON df.feeder_key = f.feeder_key
    GROUP BY df.firmware_version
    ORDER BY df.firmware_version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel
# MAGIC Cada `MERGE` creó una versión de `dim_feeder`. Puedes consultar cómo estaba antes del snapshot v2.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {gold}.dim_feeder"))

# COMMAND ----------

# Cambia el número por la versión anterior al MERGE de v2 (míralo en la tabla de historial)
display(spark.sql(f"""
    SELECT COUNT(*) AS rows, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions
    FROM {gold}.dim_feeder VERSION AS OF 1
"""))
