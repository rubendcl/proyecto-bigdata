#!/usr/bin/env python3
"""Mide la asignación región-cliente a shards con tamaños y semillas controlados.

Ejemplo desde la raíz del repositorio:
    python scripts/medir_distribucion_shard.py --semillas 17 41 93 --lotes 100 500 1000

Las mediciones describen el generador sintético; no son una garantía de balance
para otras distribuciones ni para cargas de producción.
"""

import argparse
import random
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingesta.simulador_transacciones import generar_lote

NUM_SHARDS = 3


def medir(semillas: list[int], tamanos: list[int]) -> None:
    print("Semilla  Lote  " + "  ".join(f"shard_{i}" for i in range(NUM_SHARDS)) + "  Balance")
    for semilla in semillas:
        for tamano in tamanos:
            random.seed(semilla)
            registros = generar_lote(tamano)
            conteo = Counter(tx["shard_key"] for tx in registros)
            cantidades = [conteo[f"shard_{i}"] for i in range(NUM_SHARDS)]
            balance = min(cantidades) / max(cantidades) * 100 if max(cantidades) else 100.0
            celdas = "  ".join(f"{n:>7}" for n in cantidades)
            print(f"{semilla:>7}  {tamano:>4}  {celdas}  {balance:>6.1f}%")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--semillas", nargs="+", type=int, default=[17, 41, 93])
    parser.add_argument("--lotes", nargs="+", type=int, default=[100, 500, 1000])
    args = parser.parse_args()
    if any(n <= 0 for n in args.lotes):
        parser.error("cada tamaño de --lotes debe ser positivo")
    medir(args.semillas, args.lotes)
    print("\nNota: balance = mínimo/máximo; resultado de simulación, no SLA.")


if __name__ == "__main__":
    main()
