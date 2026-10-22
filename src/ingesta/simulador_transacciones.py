"""
simulador_transacciones.py
==========================
Genera transacciones financieras sintéticas para demostrar los conceptos de:
- Volumen: generación masiva de eventos
- Velocidad: producción continua en tiempo real
- Variedad: datos estructurados y con metadatos semiestructurados
- Veracidad: inclusión de anomalías y datos de fraude para calidad

Caso de uso: Sistema de monitoreo de transacciones bancarias.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List
import json
import time
import logging

logger = logging.getLogger(__name__)

# ── Catálogos de dominio ───────────────────────────────────────────────────────
TIPOS_TRANSACCION = ["COMPRA", "TRANSFERENCIA", "RETIRO", "DEPOSITO", "PAGO_SERVICIO"]
REGIONES = ["norte", "sur", "este", "oeste", "centro"]
MONEDAS = ["BOB", "USD", "EUR"]
CATEGORIAS_COMERCIO = [
    "RETAIL", "RESTAURANTE", "COMBUSTIBLE", "TECNOLOGIA",
    "SALUD", "EDUCACION", "TURISMO", "SUPERMERCADO",
]
CANALES = ["APP_MOVIL", "WEB", "ATM", "SUCURSAL", "POS"]
BANCOS = ["BNB", "Banco Unión", "Banco Mercantil", "BCP", "Banco FIE"]


def _determinar_shard(cliente_id: str, region: str, num_shards: int = 3) -> str:
    """
    Calcula la clave de shard usando hashing consistente.
    Particionamiento horizontal: las filas se distribuyen entre shards
    según una clave de partición (cliente_id + región).
    """
    clave_compuesta = f"{region}_{cliente_id}"
    indice_shard = hash(clave_compuesta) % num_shards
    return f"shard_{indice_shard}"


def generar_transaccion(probabilidad_fraude: float = 0.02) -> Dict[str, Any]:
    """
    Genera una única transacción financiera simulada.

    Parámetros
    ----------
    probabilidad_fraude : float
        Porcentaje de transacciones que serán marcadas como potencial fraude.
        Demuestra la dimensión de Veracidad del Big Data.

    Retorna
    -------
    dict con la transacción completa, lista para serializar a JSON.
    """
    es_fraude = random.random() < probabilidad_fraude
    cliente_id = f"CLI-{random.randint(1000, 9999):04d}"
    region = random.choice(REGIONES)
    monto_base = random.uniform(10.0, 5_000.0)

    if es_fraude:
        # Los fraudes suelen tener montos inusualmente altos o bajos
        monto_base *= random.choice([random.uniform(10, 30), random.uniform(0.01, 0.09)])

    ahora = datetime.now(timezone.utc)

    return {
        # ── Identificadores ──────────────────────────────────────────
        "id_transaccion": str(uuid.uuid4()),
        "timestamp_utc": ahora.isoformat(),
        "timestamp_unix": int(ahora.timestamp()),

        # ── Particionamiento (para sharding horizontal) ───────────────
        "shard_key": _determinar_shard(cliente_id, region),
        "region": region,

        # ── Datos del cliente ─────────────────────────────────────────
        "cliente_id": cliente_id,
        "banco_emisor": random.choice(BANCOS),

        # ── Datos de la transacción ───────────────────────────────────
        "tipo": random.choice(TIPOS_TRANSACCION),
        "monto": round(monto_base, 2),
        "moneda": random.choice(MONEDAS),
        "canal": random.choice(CANALES),

        # ── Datos del comercio ────────────────────────────────────────
        "comercio_id": f"COM-{random.randint(100, 999):03d}",
        "comercio_categoria": random.choice(CATEGORIAS_COMERCIO),
        "comercio_nombre": f"Comercio-{random.randint(1, 500)}",

        # ── Veracidad: indicadores de calidad y anomalías ─────────────
        "es_fraude_potencial": es_fraude,
        "score_riesgo": round(random.uniform(0.8, 1.0) if es_fraude else random.uniform(0.0, 0.3), 3),
        "datos_completos": random.random() > 0.01,  # 1% de datos incompletos

        # ── Metadatos semiestructurados (Variedad) ────────────────────
        "metadata": {
            "version_schema": "2.1",
            "fuente": "simulador-bigdata",
            "latitud": round(random.uniform(-22.0, -14.0), 6),   # Bolivia aprox.
            "longitud": round(random.uniform(-70.0, -57.0), 6),
            "ip_origen": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "dispositivo_id": f"DEV-{random.randint(10000, 99999)}",
            "version_app": random.choice(["3.1.0", "3.2.1", "3.3.0"]),
        },
    }


def generar_lote(n: int = 100, probabilidad_fraude: float = 0.02) -> List[Dict[str, Any]]:
    """
    Genera un lote de N transacciones (procesamiento Batch).
    Representa la capa Batch de la Arquitectura Lambda.
    """
    logger.info(f"Generando lote de {n} transacciones...")
    lote = [generar_transaccion(probabilidad_fraude) for _ in range(n)]
    fraudes = sum(1 for t in lote if t["es_fraude_potencial"])
    logger.info(f"Lote generado: {n} transacciones, {fraudes} posibles fraudes ({fraudes/n*100:.1f}%)")
    return lote


def flujo_continuo(
    intervalo_segundos: float = 0.5,
    probabilidad_fraude: float = 0.02,
) -> Generator[Dict[str, Any], None, None]:
    """
    Genera transacciones de forma continua (procesamiento Streaming).
    Representa la capa de Velocidad de la Arquitectura Lambda / Kappa.

    Uso:
        for transaccion in flujo_continuo(intervalo_segundos=0.1):
            procesar(transaccion)
    """
    logger.info(f"Iniciando flujo continuo (intervalo={intervalo_segundos}s)...")
    while True:
        yield generar_transaccion(probabilidad_fraude)
        time.sleep(intervalo_segundos)


def guardar_muestra_json(ruta: str = "datos/muestra_transacciones.json", n: int = 50) -> None:
    """Guarda una muestra de transacciones en disco para pruebas offline."""
    import os
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    lote = generar_lote(n)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(lote, f, ensure_ascii=False, indent=2)
    logger.info(f"Muestra guardada en {ruta}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print("=== Simulador de Transacciones Financieras ===\n")

    # Ejemplo 1: una sola transacción
    tx = generar_transaccion()
    print("Transacción individual:")
    print(json.dumps(tx, indent=2, ensure_ascii=False))

    # Ejemplo 2: lote de 10
    print(f"\nLote de 10 transacciones:")
    lote = generar_lote(10)
    for i, t in enumerate(lote, 1):
        estado = "⚠️  FRAUDE" if t["es_fraude_potencial"] else "✅ OK"
        print(f"  [{i:02d}] {t['id_transaccion'][:8]}... | {t['tipo']:20s} | "
              f"{t['monto']:>10.2f} {t['moneda']} | {t['region']:6s} | {estado}")

    # Guardar muestra
    guardar_muestra_json()
    print("\nMuestra guardada en datos/muestra_transacciones.json")
