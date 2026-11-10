# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: ingesta incremental con Auto Loader
# MAGIC
# MAGIC **Regla de Bronze:** guardar los datos *tal como llegan* y añadir metadatos de ingesta.
# MAGIC Aquí no se corrige nada; si mañana cambia una regla de limpieza, Silver se reprocesa desde Bronze.
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) recuerda en un *checkpoint* qué archivos ya procesó. Si ejecutas
# MAGIC este notebook dos veces, la segunda no inserta nada: la ingesta es **idempotente** sin escribir
# MAGIC lógica propia. Es el equivalente en el lakehouse al `BULK INSERT` de `sql/01`, pero incremental.

# COMMAND ----------

# MAGIC %run ./transforms

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
catalog = dbutils.widgets.get("catalog")
spark.conf.set("spark.sql.session.timeZone", "UTC")

raw_root = f"/Volumes/{catalog}/landing/raw"
checkpoint_root = f"{raw_root}/_checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecturas (JSON Lines) → `bronze.readings`
# MAGIC
# MAGIC - **Esquema explícito:** control total de tipos; los timestamps quedan como texto, igual que en
# MAGIC   `stg.readings_raw` (se tipan en Silver).
# MAGIC - **`rescuedDataColumn`:** si un dispositivo manda un campo nuevo o con otro tipo, no se pierde:
# MAGIC   queda en `_rescued_data`.
# MAGIC - **`_metadata.file_path`:** columna oculta con el archivo de origen (trazabilidad).
# MAGIC - **`trigger(availableNow=True)`:** procesa lo pendiente y se detiene, como un batch.

# COMMAND ----------

bronze_readings = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(READINGS_SCHEMA)
    .load(f"{raw_root}/readings/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    bronze_readings.writeStream
    .option("checkpointLocation", f"{checkpoint_root}/bronze_readings")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.readings")
)
query.awaitTermination()

print("Filas insertadas en esta ejecución:", sum(p["numInputRows"] for p in query.recentProgress))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maestros (CSV) → `bronze.ponds` y `bronze.feeder_snapshots`
# MAGIC
# MAGIC Son pocos datos y llegan como fotos completas, así que se cargan en batch y se sobrescriben.
# MAGIC De cada snapshot se guarda su versión (sacada del nombre del archivo) para el SCD2 de Gold.

# COMMAND ----------

ponds = (
    spark.read.option("header", True)
    .schema("pond_id STRING, farm_id STRING, farm_name STRING, area_ha DOUBLE, species STRING")
    .csv(f"{raw_root}/master/ponds.csv")
    .withColumn("_ingested_at", F.current_timestamp())
)
ponds.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.ponds")

feeder_snapshots = (
    spark.read.option("header", True)
    .schema("feeder_id STRING, pond_id STRING, farm_id STRING, model STRING, "
            "firmware_version STRING, install_date DATE")
    .csv(f"{raw_root}/master/feeders_snapshot_v*.csv")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("snapshot_version",
                F.regexp_extract("_source_file", r"feeders_snapshot_(v\d+)\.csv", 1))
    .withColumn("_ingested_at", F.current_timestamp())
)
feeder_snapshots.write.mode("overwrite").saveAsTable(f"{catalog}.bronze.feeder_snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

display(spark.sql(f"""
    SELECT _source_file, COUNT(*) AS rows, MIN(_ingested_at) AS first_ingested
    FROM {catalog}.bronze.readings
    GROUP BY _source_file
    ORDER BY _source_file
"""))

display(spark.sql(f"""
    SELECT snapshot_version, COUNT(*) AS feeders
    FROM {catalog}.bronze.feeder_snapshots
    GROUP BY snapshot_version
"""))

rescued = spark.table(f"{catalog}.bronze.readings").filter("_rescued_data IS NOT NULL").count()
print("Filas con datos rescatados:", rescued)   # esperado: 0 con los datos del generador

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake: historial de la tabla
# MAGIC
# MAGIC Cada escritura queda registrada en el log de transacciones. Ejecuta este notebook otra vez y
# MAGIC verás una versión nueva con `numOutputRows = 0`: el checkpoint evitó duplicar datos.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {catalog}.bronze.readings"))
