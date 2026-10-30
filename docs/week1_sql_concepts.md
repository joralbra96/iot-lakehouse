# Semana 1 — Conceptos SQL para explicar en entrevista

Cada concepto: qué es, en una o dos frases, y dónde se usa en este repo. La idea es poder decirlo
en voz alta sin leer.

## CTE (Common Table Expression)

`WITH nombre AS (SELECT ...)` define una consulta con nombre que solo existe durante la sentencia
siguiente. Sirve para escribir la lógica por pasos y que se pueda leer; no es más rápida que una
subconsulta (el optimizador las trata casi igual). Un CTE **recursivo** se referencia a sí mismo
(ancla + paso recursivo); aquí genera el calendario de `dim_date`.
→ `02_deduplicate.sql`, `03_timestamp_drift.sql`, `05_star_schema.sql`

## Funciones de ventana (window functions)

Calculan un valor por fila mirando un conjunto de filas relacionadas, **sin colapsarlas** como
hace `GROUP BY`. Estructura: `FUNCION() OVER (PARTITION BY ... ORDER BY ... [ROWS BETWEEN ...])`.

| Función | Para qué |
|---|---|
| `ROW_NUMBER()` | 1, 2, 3… sin empates → deduplicar, "último por grupo" |
| `RANK()` | empates comparten número y deja huecos (1, 1, 3) |
| `DENSE_RANK()` | empates comparten número sin huecos (1, 1, 2) → "top N" |
| `LAG()` / `LEAD()` | valor de la fila anterior / siguiente → huecos, diferencias |
| `SUM/AVG() OVER (... ROWS BETWEEN n PRECEDING AND CURRENT ROW)` | acumulados, medias móviles |
| `PERCENTILE_CONT(0.5) WITHIN GROUP (...) OVER (PARTITION BY ...)` | mediana por grupo |

- `ROWS` cuenta filas físicas; `RANGE` agrupa por valor del `ORDER BY` (filas empatadas entran juntas).
- Orden lógico de evaluación: `FROM → WHERE → GROUP BY → HAVING → ventana → SELECT → ORDER BY`.
  Por eso no se puede filtrar `WHERE rn = 1` en la misma consulta: hay que envolverla en un CTE.

→ Deduplicación (`02`), mediana del desfase (`03`), última lectura (`04`).

## Deduplicación: DISTINCT vs ROW_NUMBER

`DISTINCT` solo elimina filas **idénticas en todas las columnas**. Un reenvío IoT tiene otro
`message_id` y otro `received_at`, así que hay que definir la **clave de negocio**
(`feeder_id`, `device_ts`) y quedarse con una fila por clave con `ROW_NUMBER() ... WHERE rn = 1`,
eligiendo con el `ORDER BY` cuál conservar (la primera en llegar).

## Deriva de timestamps

`lag = received_at − device_ts`. Un valor alto aislado es latencia; un valor alto **sostenido** en
todas las lecturas de una hora es el reloj del equipo. La mediana por equipo y hora estima el desfase
(robusta frente a mensajes demorados), y luego se redondea al slot de 5 minutos. Se **marca** cada
fila (`OK`, `LATE`, `DRIFT`, `RTC_RESET`) en vez de corregir en silencio: así se puede auditar.

## MERGE

Compara origen y destino en una sola sentencia con ramas `WHEN MATCHED`, `WHEN NOT MATCHED BY TARGET`
y `WHEN NOT MATCHED BY SOURCE`. `OUTPUT $action` devuelve qué hizo con cada fila.
Cosas que conviene mencionar:

- Termina siempre en `;`.
- Con concurrencia se usa `WITH (HOLDLOCK)` para evitar condiciones de carrera.
- Algunos equipos prefieren `UPDATE` + `INSERT` separados por historial de bugs de `MERGE`;
  es una decisión válida.
- En Databricks/Delta Lake existe `MERGE INTO` con la misma idea → lo usaremos en la Semana 2.

→ `06_scd2_dim_feeder_merge.sql`

## SCD (Slowly Changing Dimensions)

- **Tipo 1:** se sobrescribe; no hay historia.
- **Tipo 2:** fila nueva por cada cambio con `valid_from`, `valid_to`, `is_current` y una
  **clave sustituta** (`feeder_key`). Los hechos apuntan a la versión vigente en su fecha
  (join *point-in-time*), y por eso se puede responder "¿consumía más con el firmware viejo?".
- Truco de comparación: `EXISTS (SELECT src.cols EXCEPT SELECT tgt.cols)` detecta cambios en varias
  columnas y trata `NULL` como igual a `NULL`.

## Modelo estrella

Tabla de **hechos** (eventos medibles, muchas filas: `fact_readings`) rodeada de **dimensiones**
(contexto descriptivo: equipo, piscina, fecha). Lo primero que hay que definir es el **grano**:
"una lectura de un alimentador cada 5 minutos". Las claves sustitutas aíslan el warehouse de los
IDs del origen.

## Índices

- **Clustered:** *es* la tabla, ordenada por la clave (solo uno por tabla). En hechos conviene que
  empiece por la fecha.
- **Nonclustered:** estructura aparte con la clave + puntero a la fila.
- **`INCLUDE`:** columnas extra en las hojas → **índice de cobertura** (la consulta no vuelve a la tabla).
- **Filtrado (`WHERE ...`):** indexa solo un subconjunto; aquí garantiza una sola versión vigente
  por equipo.
- **Columnstore:** almacena por columnas, comprimido; ideal para agregaciones sobre millones de filas
  (estándar en Synapse dedicated pools).
- **Seek vs scan:** *seek* navega el árbol hasta las filas que necesita; *scan* recorre todo.
  Se mide con `SET STATISTICS IO ON` (logical reads) y el plan de ejecución.
- Costo: cada índice acelera lecturas pero hace más lentas las escrituras y ocupa espacio.

## Patrones de pipeline que aparecen

- **Staging como texto + `TRY_CONVERT`:** una fila mala no tumba la carga.
- **Idempotencia:** ejecutar dos veces da el mismo resultado (`DROP/CREATE`, `TRUNCATE + INSERT`,
  `MERGE`). Importa porque los orquestadores reintentan.
- **Transacciones + `XACT_ABORT ON`:** todo o nada.

## Git (parte de la Semana 1)

```bash
git status                    # qué cambió
git add -p                    # elegir trozos a commitear
git commit -m "Mensaje en imperativo"
git log --oneline --graph     # historia
git switch -c feature/ex01    # rama por ejercicio
git diff main...feature/ex01  # qué trae la rama
```

Práctica sugerida: una rama por ejercicio, commit, y merge a `main`.
