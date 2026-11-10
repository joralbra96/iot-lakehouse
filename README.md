# IoT Lakehouse

Proyecto de estudio y portafolio: un pipeline de datos para **alimentadores IoT de piscinas acuícolas**,
construido con el stack típico de Data Engineering en Azure (SQL Server, Python, PySpark,
Azure Databricks, ADLS Gen2, Delta Lake).

> **Nota de honestidad.** Este repositorio es un proyecto de aprendizaje. Los datos son
> **100 % sintéticos** (los genera `data_generator/`); no contiene datos ni código de ninguna empresa.
> El escenario está inspirado en problemas reales que he resuelto con dispositivos IoT
> (lecturas duplicadas, deriva de reloj en los equipos), pero aquí se resuelven con un stack
> distinto al que usé en producción.

## Objetivo

Simular ~300 alimentadores que reportan cada 5 minutos (~86 400 lecturas/día) y construir sobre esos
datos un flujo completo: ingesta → limpieza (duplicados, timestamps con deriva) → modelo analítico.

## Arquitectura

```
 ┌──────────────────────────┐
 │ Alimentadores simulados  │  data_generator/generate_feeder_data.py
 │ 3 granjas × 20 piscinas  │  (CSV / JSON Lines, con duplicados y deriva de reloj inyectados)
 │ × 5 alimentadores        │
 └────────────┬─────────────┘
              │  Semana 3: carga a la nube (SAS / Service Principal / Managed Identity)
              ▼
 ┌──────────────────────────┐
 │ ADLS Gen2  (landing/raw) │
 └────────────┬─────────────┘
              │  Semana 2: PySpark en Databricks
              ▼
 ┌──────────────────────────────────────────────────────────┐
 │ Delta Lake – arquitectura medallion                      │
 │  Bronze: datos crudos tal cual llegan                    │
 │  Silver: deduplicados, tipados, timestamps corregidos    │
 │  Gold:   agregados de negocio (consumo, alimentación)    │
 └────────────┬─────────────────────────────────────────────┘
              │
              ▼
 ┌──────────────────────────┐
 │ SQL (SQL Server/Synapse) │  sql/  → esquema estrella + SCD tipo 2
 └──────────────────────────┘
   Semana 4: orquestación (Databricks Workflows), CI/CD con GitHub Actions + Asset Bundles, pytest
```

## Estructura

| Carpeta | Contenido |
|---|---|
| `data_generator/` | Generador de datos sintéticos (solo librería estándar de Python) |
| `sql/` | Scripts T-SQL de la Semana 1: carga, deduplicación, deriva, última lectura, estrella, SCD2 |
| `sql/exercises/` | Ejercicios para resolver por cuenta propia |
| `docs/` | Notas de conceptos (para explicar en entrevista) |
| `notebooks/` | Notebooks de Databricks/PySpark (Semana 2+) |
| `tests/` | Pruebas con pytest |

## Cómo ejecutar

### 1. Generar datos

Requiere Python 3.10+ (sin dependencias externas).

```bash
python data_generator/generate_feeder_data.py --start-date 2026-10-01 --days 2 --format both
```

Crea en `data/raw/`:

- `readings_YYYY-MM-DD.csv` y `.jsonl`: lecturas (una fila por mensaje recibido)
- `feeders_snapshot_v1.csv` y `feeders_snapshot_v2.csv`: maestro de alimentadores en dos fechas
  (la v2 trae cambios de firmware y reubicaciones, para practicar SCD tipo 2)
- `ponds.csv`: maestro de piscinas

Opciones útiles: `--seed`, `--dup-rate`, `--drift-share`, `--rtc-reset-prob`, `--gap-rate`
(ver `--help`).

### 2. SQL Server (Semana 1)

Requisitos: SQL Server 2019/2022 (Developer o Express, gratuitos) y SSMS o Azure Data Studio.
Ejecutar los scripts de `sql/` **en orden** (`00_…` → `07_…`), con los datos generados con
`--days 2` (los scripts cargan 2026-10-01 y 2026-10-02). En `01_load_staging.sql` hay que ajustar
`@data_path` a la carpeta `data/raw`, que debe ser legible por la cuenta del servicio de SQL Server.

| Script | Qué hace |
|---|---|
| `00_create_database.sql` | Base `IoTLakehouse`, esquemas `stg` y `dw` |
| `01_load_staging.sql` | `BULK INSERT` de los CSV como texto y tipado con `TRY_CONVERT` |
| `02_deduplicate.sql` | Duplicados exactos y reenvíos con `ROW_NUMBER()` |
| `03_timestamp_drift.sql` | Detecta deriva (mediana por equipo/hora), marca y corrige `reading_ts` |
| `04_latest_reading.sql` | Última lectura por equipo (3 métodos) + índice de cobertura |
| `05_star_schema.sql` | DDL de la estrella, `dim_date` (CTE recursivo) y `dim_pond` |
| `06_scd2_dim_feeder_merge.sql` | `dim_feeder` como SCD tipo 2 con `MERGE` + `OUTPUT` |
| `07_load_fact_readings.sql` | `fact_readings` con join *point-in-time* y consultas de ejemplo |

Ejercicios para resolver por cuenta propia: `sql/exercises/week1_exercises.sql`.
Explicaciones de los conceptos: `docs/week1_sql_concepts.md`.

### 3. Databricks (Semana 2)

Requisito: una cuenta de Databricks Free Edition. Importa la carpeta `notebooks/` (o conecta el
repositorio como *Git folder*) y ejecuta en orden:

| Notebook | Qué hace |
|---|---|
| `00_setup` | Esquemas `landing`, `bronze`, `silver`, `gold` y el volumen donde se suben los archivos |
| `01_bronze_ingest` | Auto Loader (incremental e idempotente) de los `.jsonl` + maestros CSV |
| `02_silver_readings` | Tipado, deduplicación y corrección de timestamps con PySpark + validaciones |
| `03_gold_model` | `dim_date`, `dim_pond` (SCD1), `dim_feeder` (SCD2 con `MERGE INTO`), `fact_readings` con liquid clustering, `agg_pond_daily` |
| `transforms` | Funciones de transformación compartidas (se cargan con `%run ./transforms`) |

Ejercicios: `notebooks/exercises/week2_exercises.py`. Conceptos: `docs/week2_pyspark_concepts.md`.

## Problemas de datos que se simulan

| Problema | Cómo aparece en los datos | Dónde se resuelve |
|---|---|---|
| Duplicados exactos | mismo `message_id`, fila idéntica | `sql/02_deduplicate.sql`, `notebooks/transforms.py` |
| Reenvíos | mismo `feeder_id` + `device_ts`, distinto `message_id` / `received_at` (sobre todo fuera de horario de alimentación) | `sql/02_deduplicate.sql`, `notebooks/transforms.py` |
| Deriva de reloj | `device_ts` se adelanta/atrasa respecto a `received_at` (se resincroniza a medianoche) | `sql/03_timestamp_drift.sql`, `notebooks/transforms.py` |
| Reinicio de RTC | `device_ts` vuelve a 2000-01-01 tras un reinicio | `sql/03_timestamp_drift.sql` |
| Huecos | lecturas que nunca llegan | `sql/exercises/` |

## Plan de estudio

- [x] **Semana 1:** SQL avanzado (T-SQL) + Git
- [ ] **Semana 2:** PySpark, Databricks Free Edition, Delta Lake (medallion) — en curso
- [ ] **Semana 3:** ADLS Gen2, autenticación (SAS, Service Principal, Managed Identity), Key Vault, costos, Unity Catalog
- [ ] **Semana 4:** Databricks Workflows / conceptos de ADF, GitHub Actions + Databricks Asset Bundles, pytest
