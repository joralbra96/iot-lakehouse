# Databricks notebook source
# Funciones de transformación compartidas por los notebooks (se cargan con `%run ./transforms`).
#
# Es la misma lógica de sql/01–03 y 07, escrita con la DataFrame API. Vive en funciones y no en
# celdas sueltas para poder probarla con pytest (Semana 4) y reutilizarla entre notebooks.

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SLOT_SECONDS = 300                 # los equipos reportan cada 5 minutos
RTC_RESET_BEFORE = "2020-01-01"    # fechas anteriores = reloj del equipo reiniciado
DRIFT_THRESHOLD_S = 60             # desfase mediano a partir del cual el reloj se considera desviado
LATE_THRESHOLD_S = 120             # latencia a partir de la cual un mensaje se considera demorado

READINGS_SCHEMA = (
    "message_id STRING, farm_id STRING, pond_id STRING, feeder_id STRING, "
    "device_ts STRING, received_at STRING, "
    "voltage_v DOUBLE, current_a DOUBLE, battery_pct DOUBLE, is_feeding INT"
)


def _is_rtc_reset(col_name: str = "device_ts"):
    return F.col(col_name) < F.lit(RTC_RESET_BEFORE).cast("timestamp")


def parse_readings(bronze: DataFrame) -> DataFrame:
    """Bronze → tipos reales. to_timestamp devuelve null si no puede convertir (como TRY_CONVERT)."""
    return bronze.select(
        "message_id",
        "farm_id",
        "pond_id",
        "feeder_id",
        F.to_timestamp("device_ts").alias("device_ts"),
        F.to_timestamp("received_at").alias("received_at"),
        F.col("voltage_v").cast("double").alias("voltage_v"),
        F.col("current_a").cast("double").alias("current_a"),
        F.col("battery_pct").cast("double").alias("battery_pct"),
        (F.col("is_feeding") == 1).alias("is_feeding"),
    )


def deduplicate(readings: DataFrame) -> DataFrame:
    """Una fila por (feeder_id, device_ts): la primera que llegó. Equivale a sql/02.

    Tras un reinicio del RTC dos días distintos pueden repetir el mismo device_ts (2000-01-01...),
    por eso para esas filas también se particiona por el día de recepción.
    """
    rtc_day = F.when(_is_rtc_reset(), F.to_date("received_at"))
    w = Window.partitionBy("feeder_id", "device_ts", rtc_day).orderBy("received_at", "message_id")
    return (
        readings.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def correct_timestamps(readings: DataFrame) -> DataFrame:
    """Calcula reading_ts alineado a 5 min y marca ts_quality. Equivale a sql/03.

    El desfase del reloj se estima con la MEDIANA del lag por equipo y hora: un mensaje demorado
    aislado no la mueve, un reloj desviado sí.
    """
    w_hour = Window.partitionBy("feeder_id", F.to_date("received_at"), F.hour("received_at"))

    with_lag = (
        readings.withColumn("lag_s", F.unix_timestamp("received_at") - F.unix_timestamp("device_ts"))
        .withColumn("is_rtc_reset", _is_rtc_reset())
        .withColumn(
            "median_lag_s",
            # percentile ignora los null: las filas con RTC reiniciado no cuentan para la mediana
            F.expr("percentile(CASE WHEN NOT is_rtc_reset THEN lag_s END, 0.5)").over(w_hour),
        )
    )

    snapped = F.round((F.unix_timestamp("device_ts") + F.col("median_lag_s")) / SLOT_SECONDS) * SLOT_SECONDS
    floored = F.floor(F.unix_timestamp("received_at") / SLOT_SECONDS) * SLOT_SECONDS
    use_received = F.col("is_rtc_reset") | F.col("median_lag_s").isNull()

    corrected = (
        with_lag.withColumn(
            "reading_ts",
            F.timestamp_seconds(F.when(use_received, floored).otherwise(snapped).cast("long")),
        )
        .withColumn(
            "ts_quality",
            F.when(F.col("is_rtc_reset"), "RTC_RESET")
            .when(F.abs("median_lag_s") > DRIFT_THRESHOLD_S, "DRIFT")
            .when(F.col("lag_s") > LATE_THRESHOLD_S, "LATE")
            .otherwise("OK"),
        )
        .withColumn("median_lag_s", F.col("median_lag_s").cast("int"))
    )

    # Si dos lecturas caen en el mismo slot tras corregir, gana la que no viene de un reinicio
    w_slot = Window.partitionBy("feeder_id", "reading_ts").orderBy("is_rtc_reset", "received_at")
    return (
        corrected.withColumn("_rn", F.row_number().over(w_slot))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "is_rtc_reset")
    )


def build_silver_readings(bronze: DataFrame) -> DataFrame:
    return correct_timestamps(deduplicate(parse_readings(bronze)))


def build_fact_readings(readings: DataFrame, dim_feeder: DataFrame, dim_pond: DataFrame) -> DataFrame:
    """Join point-in-time con la SCD2: cada lectura toma la versión vigente en reading_ts. Equivale a sql/07."""
    r, f, p = readings.alias("r"), dim_feeder.alias("f"), dim_pond.alias("p")
    return (
        r.join(
            f,
            (F.col("f.feeder_id") == F.col("r.feeder_id"))
            & (F.col("r.reading_ts") >= F.col("f.valid_from"))
            & (F.col("r.reading_ts") < F.col("f.valid_to")),
        )
        .join(p, F.col("p.pond_id") == F.col("f.pond_id"))
        .select(
            F.date_format("r.reading_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("f.feeder_key"),
            F.col("p.pond_key"),
            F.col("r.feeder_id"),
            F.col("r.reading_ts"),
            F.col("r.voltage_v"),
            F.col("r.current_a"),
            F.col("r.battery_pct"),
            F.col("r.is_feeding"),
            F.col("r.ts_quality"),
            F.round(F.col("r.voltage_v") * F.col("r.current_a") * SLOT_SECONDS / 3600, 3).alias("energy_wh"),
        )
    )
