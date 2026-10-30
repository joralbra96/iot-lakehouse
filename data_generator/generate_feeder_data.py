"""Generador de lecturas sintéticas de alimentadores IoT para piscinas acuícolas.

Simula 3 granjas x 20 piscinas x 5 alimentadores (300 equipos) que reportan cada 5 minutos
(288 lecturas/equipo/día = 86 400 lecturas/día) e inyecta, a propósito, los problemas típicos
de datos IoT:

* Duplicados exactos (mismo message_id) y reenvíos (mismo feeder_id + device_ts, otro
  message_id y received_at más tarde), sobre todo fuera del horario de alimentación.
* Deriva de reloj: algunos equipos adelantan/atrasan su reloj unos segundos por hora y se
  resincronizan a medianoche.
* Reinicios del RTC: tras un reinicio el equipo reporta fechas desde 2000-01-01 durante un rato.
* Huecos: algunas lecturas nunca llegan.

Todos los datos son sintéticos. Solo usa la librería estándar.

Ejemplo:
    python data_generator/generate_feeder_data.py --start-date 2026-10-01 --days 2 --format both
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

N_FARMS = 3
PONDS_PER_FARM = 20
FEEDERS_PER_POND = 5
INTERVAL = timedelta(minutes=5)
SLOTS_PER_DAY = 24 * 60 // 5

# Ventanas de alimentación [hora_inicio, hora_fin)
FEEDING_WINDOWS = [(6, 8), (11, 13), (16, 18)]
SUN_HOURS = (7, 17)  # horas con carga solar
RTC_RESET_EPOCH = datetime(2000, 1, 1)
TS_FORMAT = "%Y-%m-%dT%H:%M:%S"

READING_FIELDS = [
    "message_id",
    "farm_id",
    "pond_id",
    "feeder_id",
    "device_ts",
    "received_at",
    "voltage_v",
    "current_a",
    "battery_pct",
    "is_feeding",
]
FEEDER_FIELDS = ["feeder_id", "pond_id", "farm_id", "model", "firmware_version", "install_date"]
POND_FIELDS = ["pond_id", "farm_id", "farm_name", "area_ha", "species"]


@dataclass
class Feeder:
    feeder_id: str
    pond_id: str
    farm_id: str
    model: str
    firmware_version: str
    install_date: date
    drift_s_per_hour: float  # 0 si el reloj del equipo es estable
    battery_pct: float


def build_fleet(rng: random.Random, drift_share: float) -> tuple[list[Feeder], list[dict]]:
    feeders: list[Feeder] = []
    ponds: list[dict] = []
    serial = 1
    for f in range(1, N_FARMS + 1):
        farm_id = f"FARM{f:02d}"
        for p in range(1, PONDS_PER_FARM + 1):
            pond_id = f"{farm_id}-P{p:02d}"
            ponds.append(
                {
                    "pond_id": pond_id,
                    "farm_id": farm_id,
                    "farm_name": f"Granja {f}",
                    "area_ha": round(rng.uniform(2.0, 12.0), 1),
                    "species": "Litopenaeus vannamei",
                }
            )
            for _ in range(FEEDERS_PER_POND):
                drift = 0.0
                if rng.random() < drift_share:
                    drift = rng.choice([-1, 1]) * rng.uniform(5, 60)
                feeders.append(
                    Feeder(
                        feeder_id=f"FD-{serial:04d}",
                        pond_id=pond_id,
                        farm_id=farm_id,
                        model=rng.choice(["SF-200", "SF-300"]),
                        firmware_version=rng.choice(["1.4.2", "1.5.0"]),
                        install_date=date(2024, 1, 1) + timedelta(days=rng.randint(0, 540)),
                        drift_s_per_hour=drift,
                        battery_pct=rng.uniform(60, 100),
                    )
                )
                serial += 1
    return feeders, ponds


def in_window(hour: int, windows: list[tuple[int, int]]) -> bool:
    return any(start <= hour < end for start, end in windows)


def simulate_day(
    rng: random.Random,
    feeders: list[Feeder],
    day: date,
    dup_rate: float,
    rtc_reset_prob: float,
    gap_rate: float,
) -> list[dict]:
    rows: list[dict] = []
    day_start = datetime.combine(day, datetime.min.time())

    for fd in feeders:
        # Posible reinicio del RTC hoy: desde el slot `reset_slot`, durante `reset_len` lecturas
        reset_slot, reset_len = -1, 0
        if rng.random() < rtc_reset_prob:
            reset_slot = rng.randrange(SLOTS_PER_DAY)
            reset_len = rng.randint(6, 36)

        for slot in range(SLOTS_PER_DAY):
            true_ts = day_start + slot * INTERVAL
            hour = true_ts.hour
            feeding_window = in_window(hour, FEEDING_WINDOWS)
            is_feeding = feeding_window and rng.random() < 0.7

            # Batería: carga solar de día, consumo del motor al alimentar
            if SUN_HOURS[0] <= hour < SUN_HOURS[1]:
                fd.battery_pct += rng.uniform(0.3, 0.7)
            fd.battery_pct -= rng.uniform(0.3, 0.5) if is_feeding else rng.uniform(0.03, 0.08)
            fd.battery_pct = min(100.0, max(5.0, fd.battery_pct))

            current = rng.gauss(2.5, 0.3) if is_feeding else rng.gauss(0.08, 0.02)
            current = max(0.0, current)
            voltage = 11.0 + fd.battery_pct * 0.018 - current * 0.15 + rng.gauss(0, 0.03)

            if rng.random() < gap_rate:
                continue  # la lectura se pierde (hueco)

            # Reloj del equipo: deriva acumulada desde la resincronización de medianoche
            hours_since_sync = slot * 5 / 60
            device_ts = true_ts + timedelta(seconds=fd.drift_s_per_hour * hours_since_sync)
            if reset_slot <= slot < reset_slot + reset_len:
                device_ts = RTC_RESET_EPOCH + (slot - reset_slot) * INTERVAL

            latency = rng.uniform(1, 30) if rng.random() < 0.98 else rng.uniform(60, 600)
            received_at = true_ts + timedelta(seconds=latency)

            row = {
                "message_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "farm_id": fd.farm_id,
                "pond_id": fd.pond_id,
                "feeder_id": fd.feeder_id,
                "device_ts": device_ts.strftime(TS_FORMAT),
                "received_at": received_at.strftime(TS_FORMAT),
                "voltage_v": round(voltage, 2),
                "current_a": round(current, 3),
                "battery_pct": round(fd.battery_pct, 1),
                "is_feeding": int(is_feeding),
            }
            rows.append(row)

            # Duplicados: mucho más frecuentes fuera del horario de alimentación
            rate = dup_rate if not feeding_window else dup_rate / 10
            if rng.random() < rate:
                if rng.random() < 0.3:
                    rows.append(dict(row))  # duplicado exacto
                else:
                    resend = dict(row)  # reenvío: mismo contenido, otro mensaje
                    resend["message_id"] = str(uuid.UUID(int=rng.getrandbits(128), version=4))
                    resend["received_at"] = (
                        received_at + timedelta(seconds=rng.uniform(30, 900))
                    ).strftime(TS_FORMAT)
                    rows.append(resend)

    rows.sort(key=lambda r: r["received_at"])  # orden de llegada, como un log de ingesta
    return rows


def feeder_snapshot(feeders: list[Feeder]) -> list[dict]:
    return [
        {
            "feeder_id": fd.feeder_id,
            "pond_id": fd.pond_id,
            "farm_id": fd.farm_id,
            "model": fd.model,
            "firmware_version": fd.firmware_version,
            "install_date": fd.install_date.isoformat(),
        }
        for fd in feeders
    ]


def evolve_snapshot(rng: random.Random, snapshot: list[dict], ponds: list[dict]) -> list[dict]:
    """Segunda versión del maestro: actualizaciones de firmware y algunas reubicaciones."""
    evolved = []
    for row in snapshot:
        row = dict(row)
        if rng.random() < 0.15:
            row["firmware_version"] = "1.6.0"
        if rng.random() < 0.02:
            pond = rng.choice([p for p in ponds if p["farm_id"] == row["farm_id"]])
            row["pond_id"] = pond["pond_id"]
        evolved.append(row)
    return evolved


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--format", choices=["csv", "jsonl", "both"], default="csv")
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dup-rate", type=float, default=0.03,
                        help="probabilidad de duplicar una lectura fuera de horario de alimentación")
    parser.add_argument("--drift-share", type=float, default=0.10,
                        help="fracción de equipos con deriva de reloj")
    parser.add_argument("--rtc-reset-prob", type=float, default=0.005,
                        help="probabilidad diaria de reinicio de RTC por equipo")
    parser.add_argument("--gap-rate", type=float, default=0.005,
                        help="probabilidad de que una lectura se pierda")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)

    feeders, ponds = build_fleet(rng, args.drift_share)
    snapshot_v1 = feeder_snapshot(feeders)
    write_csv(args.out / "ponds.csv", POND_FIELDS, ponds)
    write_csv(args.out / "feeders_snapshot_v1.csv", FEEDER_FIELDS, snapshot_v1)
    write_csv(args.out / "feeders_snapshot_v2.csv", FEEDER_FIELDS,
              evolve_snapshot(rng, snapshot_v1, ponds))

    for i in range(args.days):
        day = args.start_date + timedelta(days=i)
        rows = simulate_day(rng, feeders, day, args.dup_rate, args.rtc_reset_prob, args.gap_rate)
        stem = args.out / f"readings_{day.isoformat()}"
        if args.format in ("csv", "both"):
            write_csv(stem.with_suffix(".csv"), READING_FIELDS, rows)
        if args.format in ("jsonl", "both"):
            write_jsonl(stem.with_suffix(".jsonl"), rows)
        print(f"{day}: {len(rows):,} filas -> {stem}.*")


if __name__ == "__main__":
    main()
