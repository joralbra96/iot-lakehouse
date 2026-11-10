# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicios — Semana 2 (PySpark + Delta Lake)
# MAGIC
# MAGIC Requisito: haber ejecutado `00_setup` → `01_bronze_ingest` → `02_silver_readings` → `03_gold_model`.
# MAGIC
# MAGIC Guarda tu solución de cada ejercicio en `notebooks/exercises/solutions/` y haz un commit por
# MAGIC ejercicio. Igual que en la Semana 1: escribe primero sin mirar los notebooks y, al final de cada
# MAGIC uno, cómo lo explicarías en una entrevista en 2 frases.
# MAGIC
# MAGIC Varios ejercicios repiten problemas de la Semana 1 a propósito: el objetivo es aprender la
# MAGIC herramienta, no el problema.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catálogo")
catalog = dbutils.widgets.get("catalog")
spark.conf.set("spark.sql.session.timeZone", "UTC")

silver = spark.table(f"{catalog}.silver.readings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 1 — Huecos en la transmisión (DataFrame API)
# MAGIC Igual que el EJ 1 de la Semana 1, pero con `F.lag` y `Window`.
# MAGIC Salida: `feeder_id`, `prev_ts`, `reading_ts`, `gap_minutes` y luego el top 10 de equipos por
# MAGIC minutos perdidos (`gap_minutes - 5`).
# MAGIC Pista: la diferencia entre dos timestamps en segundos es
# MAGIC `F.unix_timestamp(a) - F.unix_timestamp(b)`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 2 — Lo mismo en Spark SQL
# MAGIC Resuelve el EJ 1 con `spark.sql(...)` y compara los planes con `.explain()`.
# MAGIC ¿Son distintos? ¿Por qué?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 3 — Top 3 piscinas por energía en cada granja (`QUALIFY`)
# MAGIC Sobre `gold.agg_pond_daily`, suma la energía de los dos días y quédate con las 3 primeras de cada
# MAGIC granja usando `DENSE_RANK()` y `QUALIFY` (sin CTE). ¿Qué ventaja tiene `QUALIFY` frente a lo que
# MAGIC hiciste en SQL Server?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 4 — Media móvil de 1 hora REAL (`rangeBetween`)
# MAGIC En el EJ 4 de la Semana 1 viste que "12 filas" no es "1 hora" cuando hay huecos.
# MAGIC Calcula la media de `battery_pct` de la última hora usando una ventana **por tiempo**:
# MAGIC ordena por `reading_ts` convertido a segundos y usa `rangeBetween(-3300, 0)`.
# MAGIC Compárala con la versión `rowsBetween(-11, 0)` para un equipo con huecos: ¿dónde difieren?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 5 — Sesiones de alimentación (gaps and islands)
# MAGIC Implementa en PySpark la versión **por tiempo** que vimos en la revisión del EJ 5:
# MAGIC `slot = unix_timestamp(reading_ts) / 300` menos `row_number()` sobre las lecturas con
# MAGIC `is_feeding = true`. Salida: `feeder_id`, `session_start`, `session_end`, `duration_min`.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 6 — Ingesta incremental con Auto Loader
# MAGIC 1. En tu PC genera un día nuevo con **otra semilla** (para no repetir `message_id`):
# MAGIC    `python data_generator/generate_feeder_data.py --start-date 2026-10-03 --days 1 --seed 7 --out data/day3`
# MAGIC 2. Sube **solo** `readings_2026-10-03.jsonl` a la carpeta `readings` del volumen
# MAGIC    (los maestros de esa carpeta son de otra flota simulada: no los subas).
# MAGIC 3. Ejecuta `01_bronze_ingest` y responde con consultas:
# MAGIC    - ¿Cuántas filas insertó? ¿Volvió a leer los archivos del 1 y 2 de octubre? ¿Por qué?
# MAGIC    - En `DESCRIBE HISTORY bronze.readings`, ¿qué muestra `operationMetrics` en la última versión?
# MAGIC 4. Ejecuta `01_bronze_ingest` otra vez sin subir nada. ¿Qué pasa?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 7 — Time travel y `RESTORE`
# MAGIC 1. Simula un error: `DELETE FROM silver.readings WHERE feeder_id = 'FD-0001'`.
# MAGIC 2. Con `DESCRIBE HISTORY` encuentra la versión anterior al borrado.
# MAGIC 3. Cuenta las filas de `FD-0001` en esa versión con `VERSION AS OF`.
# MAGIC 4. Recupera la tabla con `RESTORE TABLE ... TO VERSION AS OF ...` y comprueba.
# MAGIC 5. Pregunta: ¿qué haría `VACUUM` con esas versiones viejas y por qué importa antes de restaurar?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 8 — Datos inesperados: `_rescued_data`
# MAGIC 1. Crea un archivo `readings_bad.jsonl` con 3 líneas: una normal, una con un campo nuevo
# MAGIC    (`"temperature_c": 28.5`) y una con `voltage_v` como texto (`"voltage_v": "doce"`).
# MAGIC 2. Súbelo, ejecuta `01_bronze_ingest` y busca esas filas en `bronze.readings`.
# MAGIC 3. ¿Dónde quedó cada dato? ¿Qué habría pasado sin `rescuedDataColumn`?
# MAGIC 4. ¿Pasan esas filas a Silver? ¿Deberían? Propón una regla de calidad.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EJ 9 (desafío) — Silver incremental con `MERGE INTO`
# MAGIC `02_silver_readings` recalcula todo con `overwrite`. Con años de datos eso no escala.
# MAGIC Modifícalo para procesar solo lo nuevo de Bronze (`_ingested_at` mayor que la última carga) y
# MAGIC hacer `MERGE INTO silver.readings` por `(feeder_id, reading_ts)`.
# MAGIC Piensa antes de programar: la mediana del desfase se calcula por equipo y **hora**. ¿Qué pasa
# MAGIC si una hora llega repartida en dos cargas? ¿Cómo lo resolverías?

# COMMAND ----------


