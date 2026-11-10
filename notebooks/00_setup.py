# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Preparación del workspace
# MAGIC
# MAGIC Crea los esquemas de la arquitectura medallion y el volumen donde se suben los archivos.
# MAGIC
# MAGIC | Objeto | Para qué |
# MAGIC |---|---|
# MAGIC | `<catálogo>.landing.raw` (volumen) | Archivos tal como llegan + checkpoints de Auto Loader |
# MAGIC | `<catálogo>.bronze` | Tablas Delta con los datos crudos |
# MAGIC | `<catálogo>.silver` | Datos limpios |
# MAGIC | `<catálogo>.gold` | Modelo estrella y agregados |
# MAGIC
# MAGIC En **Databricks Free Edition** el catálogo por defecto se llama `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
catalog = dbutils.widgets.get("catalog")

for schema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw")

raw_root = f"/Volumes/{catalog}/landing/raw"
for folder in ["readings", "master"]:
    dbutils.fs.mkdirs(f"{raw_root}/{folder}")

print(f"Listo. Sube los archivos a {raw_root}/readings y {raw_root}/master")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subir los datos
# MAGIC
# MAGIC 1. En tu PC genera los datos: `python data_generator/generate_feeder_data.py --days 2 --format both`
# MAGIC 2. En Databricks: **Catalog** → `workspace` → `landing` → volumen `raw` → carpeta `readings`
# MAGIC    → **Upload to this volume** → sube los `readings_*.jsonl`.
# MAGIC 3. En la carpeta `master` sube `ponds.csv`, `feeders_snapshot_v1.csv` y `feeders_snapshot_v2.csv`.
# MAGIC 4. Ejecuta la celda de abajo para comprobarlo.

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_root}/readings"))
display(dbutils.fs.ls(f"{raw_root}/master"))
