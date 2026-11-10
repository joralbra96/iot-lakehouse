# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: lecturas limpias
# MAGIC
# MAGIC Bronze → tipado → deduplicado → timestamps corregidos. Es la misma lógica de `sql/01`–`03`,
# MAGIC ahora en PySpark (ver `transforms.py`).
# MAGIC
# MAGIC Para mantenerlo simple, Silver se **recalcula completo** desde Bronze en cada ejecución
# MAGIC (`overwrite`). Hacerlo incremental con `MERGE INTO` es uno de los ejercicios de la semana.

# COMMAND ----------

# MAGIC %run ./transforms

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
catalog = dbutils.widgets.get("catalog")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.readings")
silver = build_silver_readings(bronze)

# Nada se ha ejecutado todavía: Spark solo construyó el plan (evaluación perezosa).
# Para ver el plan físico (shuffles = "Exchange"):
silver.explain()

# COMMAND ----------

(
    silver.write.mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(f"{catalog}.silver.readings")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validaciones
# MAGIC Si alguna falla, el notebook se detiene con error y no se publican datos malos hacia Gold.

# COMMAND ----------

silver = spark.table(f"{catalog}.silver.readings")

stats = silver.agg(
    F.count("*").alias("rows"),
    F.countDistinct("feeder_id", "reading_ts").alias("distinct_keys"),
    F.sum(F.when(F.second("reading_ts") != 0, 1).otherwise(0)).alias("seconds_not_zero"),
    F.sum(F.when(F.minute("reading_ts") % 5 != 0, 1).otherwise(0)).alias("minutes_not_aligned"),
    F.sum(F.when(F.col("device_ts").isNull() | F.col("received_at").isNull(), 1).otherwise(0)).alias("bad_timestamps"),
).first()

print(stats)
assert stats["rows"] == stats["distinct_keys"], "Hay lecturas duplicadas por (feeder_id, reading_ts)"
assert stats["seconds_not_zero"] == 0 and stats["minutes_not_aligned"] == 0, "reading_ts no está alineado a 5 min"
assert stats["bad_timestamps"] == 0, "Hay timestamps que no se pudieron convertir"

# COMMAND ----------

display(silver.groupBy("ts_quality").agg(F.count("*").alias("readings"),
                                        F.countDistinct("feeder_id").alias("feeders"))
        .orderBy(F.desc("readings")))

# COMMAND ----------

# Antes / después para los equipos con reloj desviado
display(silver.filter("ts_quality = 'DRIFT'")
        .select("feeder_id", "device_ts", "received_at", "reading_ts", "lag_s", "median_lag_s")
        .orderBy(F.desc(F.abs("median_lag_s")))
        .limit(20))
