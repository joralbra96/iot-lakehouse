# Semana 2 — PySpark, Databricks y Delta Lake para explicar en entrevista

Misma idea que en la Semana 1: cada concepto en una o dos frases y dónde aparece en el repo.

## Spark en 5 ideas

1. **Driver y executors.** El *driver* (donde corre tu notebook) planifica el trabajo; los
   *executors* lo ejecutan en paralelo sobre **particiones** (trozos) de los datos.
2. **Evaluación perezosa (*lazy*).** Las **transformaciones** (`select`, `filter`, `withColumn`,
   `join`, `groupBy`) solo construyen un plan. Nada se ejecuta hasta una **acción**
   (`count`, `show`, `display`, `write`, `collect`). Por eso un error de datos puede aparecer
   "tarde", en la acción.
3. **Shuffle.** Cuando una operación necesita juntar filas de distintas particiones (`groupBy`,
   `join`, `Window.partitionBy`, `distinct`), Spark las mueve por la red. Es lo más caro de un job.
4. **Catalyst.** El optimizador reescribe el plan (filtros antes, columnas que sobran fuera).
   `df.explain()` lo muestra; es el equivalente al plan de ejecución de SSMS.
5. **DataFrame API y Spark SQL son lo mismo por dentro.** `df.filter(F.col("x") > 1)` y
   `spark.sql("SELECT ... WHERE x > 1")` generan el mismo plan. Se elige por legibilidad.

## De T-SQL a PySpark

| Semana 1 (T-SQL) | Semana 2 (PySpark) |
|---|---|
| `TRY_CONVERT(datetime2, x, 126)` | `F.to_timestamp("x")` (devuelve `null` si no puede) |
| `ROW_NUMBER() OVER (PARTITION BY a ORDER BY b)` | `F.row_number().over(Window.partitionBy("a").orderBy("b"))` |
| `LAG(x) OVER (...)` | `F.lag("x").over(w)` |
| `PERCENTILE_CONT(0.5) WITHIN GROUP (...) OVER (...)` | `F.expr("percentile(x, 0.5)").over(w)` |
| `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW` | `w.rowsBetween(-11, Window.currentRow)` |
| `RANGE` por valor (no existe en SQL Server) | `w.rangeBetween(-3300, 0)` sobre segundos |
| CTE + `WHERE rn = 1` | `.filter("rn = 1")` o `QUALIFY` en Spark SQL |
| `MERGE` | `MERGE INTO` sobre una tabla Delta |
| `EXISTS (SELECT src... EXCEPT SELECT tgt...)` | `NOT (a <=> b AND ...)` (`<=>` = igualdad que trata `NULL = NULL`) |
| `IDENTITY(1,1)` | `GENERATED ALWAYS AS IDENTITY` |
| Índices | Liquid clustering / Z-ORDER + *data skipping* |

## Delta Lake

Formato de tabla sobre archivos Parquet + un **log de transacciones** (`_delta_log/`).

- **ACID:** cada escritura es una transacción; un job que falla a medias no deja datos a medias.
- **`MERGE INTO`:** upserts y SCD2 sobre el lake (antes de Delta esto era muy difícil).
- **Time travel:** `SELECT * FROM t VERSION AS OF 3`, `DESCRIBE HISTORY t`, `RESTORE TABLE`.
- **Schema enforcement:** rechaza escrituras con columnas o tipos que no encajan;
  **schema evolution** (`mergeSchema`) las permite de forma explícita.
- **Mantenimiento:** `OPTIMIZE` compacta archivos pequeños; `VACUUM` borra archivos viejos
  (después de `VACUUM` ya no se puede viajar a esas versiones).
- **Organización física:** *liquid clustering* (`CLUSTER BY`) es la opción actual; reemplaza al
  particionado por carpetas y a `ZORDER` en tablas nuevas. Sirve para que Spark se salte
  archivos que no contienen los valores filtrados (*data skipping*).

## Arquitectura medallion

| Capa | En este repo | Regla |
|---|---|---|
| **Bronze** | `bronze.readings`, `bronze.ponds`, `bronze.feeder_snapshots` | Tal cual llega + metadatos de ingesta (`_source_file`, `_ingested_at`). Nunca se corrige aquí. |
| **Silver** | `silver.readings` | Tipado, deduplicado, timestamps corregidos y marcados (`ts_quality`). |
| **Gold** | `gold.dim_*`, `gold.fact_readings`, `gold.agg_pond_daily` | Modelo y agregados para el negocio. |

Es la misma idea que `stg` → `dw` de la Semana 1, pero con Bronze **persistente**: se puede
reprocesar Silver y Gold desde Bronze cuando cambia una regla.

## Auto Loader e ingesta incremental

`spark.readStream.format("cloudFiles")` detecta archivos **nuevos** en una carpeta y guarda en un
**checkpoint** cuáles ya procesó. Con `trigger(availableNow=True)` procesa lo pendiente y se
detiene, como un batch. Resultado: ejecutar la ingesta dos veces **no duplica datos**
(idempotencia) sin escribir lógica propia.

- Esquema explícito (`.schema(...)`) → control total.
- `rescuedDataColumn` → los campos inesperados no se pierden, quedan en `_rescued_data`.

## Unity Catalog (lo justo para esta semana; se profundiza en la Semana 3)

Nombres de 3 niveles: `catálogo.esquema.tabla` (en Databricks Free Edition el catálogo es
`workspace`). Los archivos sueltos viven en **volúmenes**: `/Volumes/<catálogo>/<esquema>/<volumen>/`.

## Buenas prácticas que aparecen en los notebooks

- **Zona horaria explícita** (`spark.sql.session.timeZone = UTC`): evita sorpresas con horarios de
  verano al convertir timestamps.
- **Lógica en funciones** (`notebooks/transforms.py`), no pegada en celdas: se puede probar con
  pytest (Semana 4) y reutilizar.
- **Validaciones al final de cada capa** (`assert`): si algo no cuadra, el job falla en vez de
  publicar datos malos.
